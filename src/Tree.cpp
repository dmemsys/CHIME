#include "Tree.h"
#include "RdmaBuffer.h"
#include "Timer.h"
#include "LeafNode.h"
#include "InternalNode.h"
#include "Hash.h"

#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>
#include <set>
#include <map>
#include <atomic>
#include <chrono>
#include <functional>
#include <algorithm>
#include <mutex>
std::mutex debug_lock;

double cache_miss[MAX_APP_THREAD];
double cache_hit[MAX_APP_THREAD];
uint64_t lock_fail[MAX_APP_THREAD];
uint64_t write_handover_num[MAX_APP_THREAD];
uint64_t try_write_op[MAX_APP_THREAD];
uint64_t read_handover_num[MAX_APP_THREAD];
uint64_t try_read_op[MAX_APP_THREAD];
uint64_t read_leaf_retry[MAX_APP_THREAD];
uint64_t leaf_cache_invalid[MAX_APP_THREAD];
uint64_t leaf_read_sibling[MAX_APP_THREAD];
uint64_t correct_speculative_read[MAX_APP_THREAD];
uint64_t try_speculative_read[MAX_APP_THREAD];
uint64_t try_read_leaf[MAX_APP_THREAD];
uint64_t read_two_segments[MAX_APP_THREAD];
uint64_t try_read_hopscotch[MAX_APP_THREAD];
uint64_t retry_cnt[MAX_APP_THREAD][MAX_FLAG_NUM];
uint64_t try_insert_op[MAX_APP_THREAD];
uint64_t split_node[MAX_APP_THREAD];
uint64_t try_write_segment[MAX_APP_THREAD];
uint64_t write_two_segments[MAX_APP_THREAD];
double load_factor_sum[MAX_APP_THREAD];
uint64_t split_hopscotch[MAX_APP_THREAD];

uint64_t latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];
volatile bool need_stop = false;
volatile bool need_clear[MAX_APP_THREAD];

thread_local std::vector<CoroPush> Tree::workers;
thread_local CoroQueue Tree::busy_waiting_queue;
thread_local GlobalAddress path_stack[MAX_CORO_NUM][MAX_TREE_HEIGHT];


Tree::Tree(DSM *dsm, uint16_t tree_id, bool init_root) : dsm(dsm), tree_id(tree_id) {
  assert(dsm->is_register());
  std::fill(need_clear, need_clear + MAX_APP_THREAD, false);
  clear_debug_info();

  local_lock_table = new LocalLockTable();
  if (!init_root) return;

#ifdef TREE_ENABLE_CACHE
#ifdef SPECULATIVE_READ
  if (define::kIndexCacheSize > define::kHotspotBufSize + 20) tree_cache = new TreeCache(define::kIndexCacheSize - define::kHotspotBufSize, dsm);  // enable hotspot idx cache
  else tree_cache = new TreeCache(define::kIndexCacheSize, dsm);
#else
  tree_cache = new TreeCache(define::kIndexCacheSize, dsm);
#endif
#endif

#ifdef SPECULATIVE_READ
  if (define::kIndexCacheSize > define::kHotspotBufSize + 20) idx_cache = new IdxCache(define::kHotspotBufSize, dsm);
  else idx_cache = new IdxCache(0, dsm);
#endif

  root_ptr_ptr = get_root_ptr_ptr();

  if (dsm->getMyNodeID() == 0) {
    // init root page
    auto leaf_addr = dsm->alloc(define::allocationLeafSize, PACKED_ADDR_ALIGN_BIT);
    auto leaf_buffer = (dsm->get_rbuf(nullptr)).get_leaf_buffer();
    auto root_leaf = new (leaf_buffer) LeafNode;
    root_leaf->metadata.sibling_ptr = GlobalAddress::Widest();
    Key ghost_key;
    ghost_key.fill(0xff);
    ghost_key = ghost_key - 1;
    int max_key_idx = 0;
#ifdef HOPSCOTCH_LEAF_NODE
    max_key_idx = hopscotch_insert_locally(root_leaf->records, ghost_key, define::kValueNull);
#else
    root_leaf->records[max_key_idx].update(ghost_key, define::kValueNull);
#endif
#ifdef VACANCY_AWARE_LOCK
    // init insert friendly lock
    auto lock_buffer = (dsm->get_rbuf(nullptr)).get_lock_buffer();
    auto if_lock = new (lock_buffer) VALOCK(0ULL, max_key_idx);
    if_lock->update_vacancy(max_key_idx, max_key_idx, std::vector<int>{});
    auto lock_offset = get_lock_info(true);  // unlock
    dsm->write_sync((char*)lock_buffer, leaf_addr + lock_offset, sizeof(uint64_t));
#endif
    auto encoded_leaf_buffer = (dsm->get_rbuf(nullptr)).get_leaf_buffer();
#ifdef METADATA_REPLICATION
    auto intermediate_leaf_buffer = (dsm->get_rbuf(nullptr)).get_leaf_buffer();
    MetadataManager::encode_node_metadata(leaf_buffer, intermediate_leaf_buffer);
    LeafVersionManager::encode_node_versions(intermediate_leaf_buffer, encoded_leaf_buffer);
#else
    VersionManager<LeafNode, LeafEntry>::encode_node_versions(leaf_buffer, encoded_leaf_buffer);
#endif
    dsm->write_sync(encoded_leaf_buffer, leaf_addr, define::transLeafSize);

    // install root pointer
    auto cas_buffer = (dsm->get_rbuf(nullptr)).get_cas_buffer();
    auto root_entry = RootEntry(1, leaf_addr);
    auto p = 0ULL;
retry:
    bool res = dsm->cas_sync(root_ptr_ptr, p, root_entry, cas_buffer);
    if (!res && (p = *(uint64_t *)cas_buffer) != (uint64_t)root_entry) {
      goto retry;
    }
  }
}


GlobalAddress Tree::get_root_ptr_ptr() {
  return GlobalAddress{0, define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id};
}


RootEntry Tree::get_root_ptr(CoroPull* sink) {
  auto root_buffer = (dsm->get_rbuf(sink)).get_cas_buffer();
  dsm->read_sync((char *)root_buffer, root_ptr_ptr, sizeof(RootEntry), sink);
  auto root_entry = *(RootEntry *)root_buffer;
  rough_height.store(root_entry.level);
  return root_entry;
}


inline void Tree::before_operation(CoroPull* sink) {
  for (int i = 0; i < MAX_TREE_HEIGHT; ++ i) {
    path_stack[sink ? sink->get() : 0][i] = GlobalAddress::Null();
  }
  auto tid = dsm->getMyThreadID();
  if (need_clear[tid]) {
    cache_miss[tid]              = 0;
    cache_hit[tid]               = 0;
    lock_fail[tid]               = 0;
    write_handover_num[tid]      = 0;
    try_write_op[tid]            = 0;
    read_handover_num[tid]       = 0;
    try_read_op[tid]             = 0;
    read_leaf_retry[tid]         = 0;
    leaf_cache_invalid[tid]      = 0;
    leaf_read_sibling[tid]       = 0;
    try_speculative_read[tid]    = 0;
    correct_speculative_read[tid]= 0;
    try_read_leaf[tid]           = 0;
    read_two_segments[tid]       = 0;
    try_read_hopscotch[tid]      = 0;
    std::fill(retry_cnt[tid], retry_cnt[tid] + MAX_FLAG_NUM, 0);
    try_insert_op[tid]           = 0;
    split_node[tid]              = 0;
    // load_factor_sum[tid]         = 0;
    // split_hopscotch[tid]         = 0;
    try_write_segment[tid]       = 0;
    write_two_segments[tid]      = 0;
    need_clear[tid]              = false;
  }
}


inline void Tree::record_cache_hit_ratio(bool from_cache, int level) {
  if (!from_cache) {
    cache_miss[dsm->getMyThreadID()] += 1;
    return;
  }
  int h = rough_height.load();
  auto hit = (h ? 1 - ((double)level - 1) / h : 0);
  cache_hit[dsm->getMyThreadID()] += hit;
  cache_miss[dsm->getMyThreadID()] += (1 - hit);
}


void Tree::cache_node(InternalNode* node) {
#ifdef TREE_ENABLE_CACHE
#ifdef CACHE_MORE_INTERNAL_NODE
  tree_cache->add_to_cache(node);
#else
  if (node->metadata.level == 1) {  // only cache level-1 internal node
    tree_cache->add_to_cache(node);
  }
#endif
#endif
}


inline uint64_t Tree::get_lock_info(bool is_leaf) {
  return ROUND_UP(is_leaf ? define::transLeafSize : define::transInternalSize, 3);
}


void Tree::lock_node(const GlobalAddress &node_addr, uint64_t *lock_buffer, bool is_leaf, CoroPull* sink) {
  auto lock_offset = get_lock_info(is_leaf);

  // lock function
  auto acquire_lock = [=](const GlobalAddress &node_addr) {
    return dsm->cas_mask_sync(node_addr + lock_offset, 0UL, ~0UL, lock_buffer, 1ULL << 63, ~0ULL, sink);
  };

  uint64_t retry_cnt = 0;
re_acquire:
  if (retry_cnt++ > 10000000) {
    std::cout << "Deadlock " << node_addr << std::endl;
    std::cout << "is_leaf=" << is_leaf << std::endl;
    assert(false);
  }

  if (!acquire_lock(node_addr)){
    if (sink != nullptr) {
      busy_waiting_queue.push(sink->get());
      (*sink)();
    }
    lock_fail[dsm->getMyThreadID()] ++;
    goto re_acquire;
  }
  return;
}

void Tree::unlock_node(const GlobalAddress &node_addr, uint64_t* lock_buffer, bool is_leaf, CoroPull* sink, bool async) {
  auto lock_offset = get_lock_info(is_leaf);

  // unlock function
  auto unlock = [=](const GlobalAddress &node_addr){
    if (async) {
      dsm->write((char *)lock_buffer, node_addr + lock_offset, sizeof(uint64_t), false, sink);
    }
    else {
      dsm->write_sync((char *)lock_buffer, node_addr + lock_offset, sizeof(uint64_t), sink);
    }
  };
  unlock(node_addr);
  return;
}


void Tree::insert(const Key &k, Value v, CoroPull* sink) {
  assert(dsm->is_register());
  before_operation(sink);

  // handover
  bool write_handover = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);

  // cache
  bool from_cache = false;
  const TreeCacheEntry *cache_entry = nullptr;

  // traversal
  GlobalAddress p;
  GlobalAddress sibling_p;
  uint16_t level;
  int retry_flag = FIRST_TRY;

  try_write_op[dsm->getMyThreadID()] ++;
  try_insert_op[dsm->getMyThreadID()] ++;

#ifdef TREE_ENABLE_WRITE_COMBINING
  lock_res = local_lock_table->acquire_local_write_lock(k, v, &busy_waiting_queue, sink);
  write_handover = (lock_res.first && !lock_res.second);
#else
  UNUSED(lock_res);
#endif
  if (write_handover) {
    write_handover_num[dsm->getMyThreadID()]++;
    goto insert_finish;
  }

#ifdef TREE_ENABLE_CACHE
  cache_entry = tree_cache->search_from_cache(k, p, sibling_p, level);
  if (cache_entry) from_cache = true;
#endif
  if (!from_cache) {
    auto e = get_root_ptr(sink);
    p = e.ptr, sibling_p = GlobalAddress::Null(), level = e.level;
  }
  record_cache_hit_ratio(from_cache, level);
  assert(level != 0);

next:
  retry_cnt[dsm->getMyThreadID()][retry_flag] ++;
  // record search path, from cache is ok
  path_stack[sink ? sink->get() : 0][level - 1] = p;
  // read leaf node
  if (level == 1) {
    if (!leaf_node_insert(p, sibling_p, k, v, from_cache, sink)) {  // return false if cache validation fail
      if (cache_entry) tree_cache->invalidate(cache_entry);
#ifdef CACHE_MORE_INTERNAL_NODE
      cache_entry = tree_cache->search_from_cache(k, p, sibling_p, level);
      from_cache = cache_entry ? true : false;
#else
      from_cache = false;
#endif
      if (!from_cache) {
        auto e = get_root_ptr(sink);
        p = e.ptr, sibling_p = GlobalAddress::Null(), level = e.level;
      }
      retry_flag = INVALID_LEAF;
      goto next;
    }
    goto insert_finish;
  }
  // traverse internal nodes
  if (!internal_node_search(p, sibling_p, k, level, from_cache, sink)) {  // return false if cache validation fail
    // cache invalidation
    assert(from_cache);
    tree_cache->invalidate(cache_entry);
#ifdef CACHE_MORE_INTERNAL_NODE
    cache_entry = tree_cache->search_from_cache(k, p, sibling_p, level);
    from_cache = cache_entry ? true : false;
#else
    from_cache = false;
#endif
    if (!from_cache) {
      auto e = get_root_ptr(sink);
      p = e.ptr, sibling_p = GlobalAddress::Null(), level = e.level;
    }
    retry_flag = INVALID_NODE;
    goto next;
  }
  from_cache = false;
  retry_flag = FIND_NEXT;
  goto next;  // search next level

insert_finish:
#ifdef TREE_ENABLE_WRITE_COMBINING
  local_lock_table->release_local_write_lock(k, lock_res);
#endif
  return;
}


void Tree::insert_internal(const Key &k, const GlobalAddress& ptr, const RootEntry& root_entry, uint8_t target_level, CoroPull* sink) {
  // search from root
  auto [level, p] = (std::pair<uint16_t, GlobalAddress>)root_entry;
  GlobalAddress sibling_p = GlobalAddress::Null();

next:
  // record search path
  path_stack[sink ? sink->get() : 0][level - 1] = p;
  assert(level > 1);
  // target level
  if (level - 1 == target_level) {
    assert(internal_node_insert(p, k, ptr, false, target_level, sink));
    return;
  }
  // traverse internal nodes
  assert(internal_node_search(p, sibling_p, k, level, false, sink));
  goto next;  // search next level
}


bool Tree::internal_node_search(GlobalAddress& node_addr, GlobalAddress& sibling_addr, const Key &k, uint16_t& level, bool from_cache, CoroPull* sink) {
  assert(level > 1);
  auto raw_internal_buffer = (dsm->get_rbuf(sink)).get_internal_buffer();
  auto internal_buffer = (dsm->get_rbuf(sink)).get_internal_buffer();
  auto node = (InternalNode *)internal_buffer;
re_read:
  dsm->read_sync(raw_internal_buffer, node_addr, define::transInternalSize, sink);
  if (!VersionManager<InternalNode, InternalEntry>::decode_node_versions(raw_internal_buffer, internal_buffer)) {
    goto re_read;
  }
  const auto& fence_keys = node->metadata.fence_keys;
  if (from_cache && (!node->metadata.valid || k < fence_keys.lowest || k >= fence_keys.highest)) {  // cache is outdated
    return false;
  }
  if (k >= fence_keys.highest) {  // should turn right
    node_addr = node->metadata.sibling_ptr;
    path_stack[sink ? sink->get() : 0][level - 1] = node_addr;
    internal_node_search(node_addr, sibling_addr, k, level, false, sink);
    return true;
  }
  assert(k >= fence_keys.lowest);
  // search for the next pointer
  level = node->metadata.level;
  auto& records = node->records;
#ifdef UNORDERED_INTERNAL_NODE
  std::sort(records, records + define::internalSpanSize, [](const InternalEntry& a, const InternalEntry& b){
    if (a.key == define::kkeyNull) return false;
    if (b.key == define::kkeyNull) return true;
    return a.key < b.key;
  });
#endif
  cache_node(node);  // make sure the cached internal nodes are kv-sorted
  if (k < records[0].key) {
    node_addr = node->metadata.leftmost_ptr;
    sibling_addr = node->records[0].ptr;
    return true;
  }
  for (int i = 1; i < (int)define::internalSpanSize; ++ i) {
    if (k < records[i].key || records[i].key == define::kkeyNull) {
      node_addr = records[i - 1].ptr;
      sibling_addr = (records[i].key == define::kkeyNull ? node->metadata.sibling_leftmost_ptr : records[i].ptr);
      return true;
    }
  }
  node_addr = records[define::internalSpanSize - 1].ptr;
  sibling_addr = node->metadata.sibling_leftmost_ptr;
  return true;
}


bool Tree::leaf_node_insert(const GlobalAddress& node_addr, const GlobalAddress& sibling_addr, const Key &k, Value v,
                           bool from_cache, CoroPull* sink) {
  // lock node
  auto lock_buffer = (dsm->get_rbuf(sink)).get_lock_buffer();
  lock_node(node_addr, lock_buffer, true, sink);
  int read_entry_num = define::leafSpanSize;
#if (defined HOPSCOTCH_LEAF_NODE && defined VACANCY_AWARE_LOCK)
  auto if_lock = (VALOCK *)lock_buffer;
  auto max_key_idx = if_lock->get_max_key_idx();
  int l_idx = get_hashed_leaf_entry_index(k);
  read_entry_num = if_lock->get_read_entry_num_from_bitmap(l_idx, true); // [l_idx, r_idx)
  int r_idx = l_idx + read_entry_num;
#ifdef METADATA_REPLICATION
  // ensure to read one stattered metadata
  if (read_entry_num < (int)define::neighborSize
      && (l_idx % define::neighborSize)
      && (l_idx / define::neighborSize == (r_idx - 1) / define::neighborSize)) {
    r_idx = (r_idx - 1 + define::neighborSize) / define::neighborSize * define::neighborSize + 1;
    read_entry_num = r_idx - l_idx;
  }
#endif
  r_idx = r_idx % define::leafSpanSize;
#endif
  // read leaf
  auto raw_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  auto leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  memset(leaf_buffer, 0, define::allocationLeafSize);  // !!Important
  auto leaf = (LeafNode *) leaf_buffer;
  bool hopping_read = (read_entry_num < (int)define::leafSpanSize);
#if (defined HOPSCOTCH_LEAF_NODE && defined VACANCY_AWARE_LOCK)
  if (hopping_read) {
    hopscotch_search(node_addr, l_idx, raw_leaf_buffer, leaf_buffer, sink, read_entry_num, true);
  }
#endif
  if (!hopping_read) {
    dsm->read_sync(raw_leaf_buffer, node_addr, define::transLeafSize, sink);
#ifdef METADATA_REPLICATION
    auto intermediate_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    assert((LeafVersionManager::decode_node_versions(raw_leaf_buffer, intermediate_leaf_buffer)));
    MetadataManager::decode_node_metadata(intermediate_leaf_buffer, leaf_buffer);
#else
    assert((VersionManager<LeafNode, LeafEntry>::decode_node_versions(raw_leaf_buffer, leaf_buffer)));
#endif
  }

#ifdef SIBLING_BASED_VALIDATION
  const auto& sibling_ptr = (leaf->metadata.sibling_ptr == GlobalAddress::Widest() ? GlobalAddress::Null() : (GlobalAddress)leaf->metadata.sibling_ptr);
  // cache validation
  if (!leaf->metadata.valid || (from_cache && sibling_addr != sibling_ptr)) {  // invalid || cache is outdated
    unlock_node(node_addr, lock_buffer, true, sink, true);
    return false;
  }
  // turn right check
  if (sibling_addr != sibling_ptr) {
    Key split_key;
    if (leaf->is_root()) {
      split_key.fill(0xff);
    }
    else {
#if (defined HOPSCOTCH_LEAF_NODE && defined VACANCY_AWARE_LOCK)
      bool has_large_key = false;
      auto check_larger_key = [=](int l, int r) {
        for (int i = l; i < r; ++ i) if (leaf->records[i].key >= k) return true;
        return false;
      };
      if (l_idx < r_idx) has_large_key |= check_larger_key(l_idx, r_idx);
      else has_large_key |= (check_larger_key(0, r_idx) || check_larger_key(l_idx, define::leafSpanSize));
      if (!has_large_key) {
        if (((l_idx < r_idx) && (max_key_idx < l_idx || max_key_idx >= r_idx)) ||
            (l_idx >= r_idx && max_key_idx >= r_idx && max_key_idx < l_idx)) {
          leaf_entry_read(node_addr, max_key_idx, raw_leaf_buffer, leaf_buffer, sink, true);
        }
        auto max_key = leaf->records[max_key_idx].key;
        split_key = max_key + 1;
      }
      else split_key = k + 1;  // use faked split_key to avoid turning right
#else
      auto max_key = define::kkeyNull;
      for (const auto& e : leaf->records) if (e.key > max_key) max_key = e.key;
      split_key = max_key + 1;
#endif
    }
    if (k >= split_key) {  // should turn right
      unlock_node(node_addr, lock_buffer, true, sink, true);
      assert(leaf->metadata.sibling_ptr != GlobalAddress::Null());
      leaf_node_insert(leaf->metadata.sibling_ptr, GlobalAddress::Null(), k, v, false, sink);
      return true;
    }
  }
#else
  UNUSED(sibling_addr);
  // cache validation
  const auto& fence_keys = leaf->metadata.fence_keys;
  if (from_cache && (!leaf->metadata.valid || k < fence_keys.lowest || k >= fence_keys.highest)) {  // cache is outdated
    unlock_node(node_addr, lock_buffer, true, sink, true);
    return false;
  }
  // turn right check
  if (k >= fence_keys.highest) {  // should turn right
    unlock_node(node_addr, lock_buffer, true, sink, true);
    assert(leaf->metadata.sibling_ptr != GlobalAddress::Null());
    leaf_node_insert(leaf->metadata.sibling_ptr, GlobalAddress::Null(), k, v, false, sink);
    return true;
  }
  assert(k >= fence_keys.lowest);
#endif

  // start insert
#ifdef TREE_ENABLE_WRITE_COMBINING
  local_lock_table->get_combining_value(k, v);
#endif

#ifdef ENABLE_VAR_LEN_KV
  {
  // first write a new DataBlock out-of-place
  auto block_buffer = (dsm->get_rbuf(sink)).get_block_buffer();
  auto data_block = new (block_buffer) DataBlock(v);
  auto block_addr = dsm->alloc(define::dataBlockLen, PACKED_ADDR_ALIGN_BIT);
  dsm->write_sync(block_buffer, block_addr, define::dataBlockLen, sink);
  // change value into the DataPointer value pointing to the DataBlock
  v = (uint64_t)DataPointer(define::dataBlockLen, block_addr);
  }
#endif

  auto& records = leaf->records;
  int i;
  // search for existing key (update)
#if (defined HOPSCOTCH_LEAF_NODE && defined VACANCY_AWARE_LOCK)
  // empty check
  bool has_empty = false;
  for (i = l_idx; i != r_idx; i = (i + 1) % define::leafSpanSize) if (records[i].key == define::kkeyNull) has_empty = true;
  assert(read_entry_num == (int)define::leafSpanSize || has_empty);
#endif
  // update
  for (i = 0; i < (int)define::leafSpanSize; ++ i) if (records[i].key == k) break;
  if (i != (int)define::leafSpanSize) {
    entry_write_and_unlock<LeafNode, LeafEntry, Value>(leaf, i, k, v, node_addr, lock_buffer, sink);
    return true;
  }

  // search for empty entry (insert)
#ifdef HOPSCOTCH_LEAF_NODE
  // use a leaf copy to hop since it may fail
  auto leaf_copy_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  memcpy(leaf_copy_buffer, leaf_buffer, sizeof(LeafNode));
  if (!(hopscotch_insert_and_unlock((LeafNode *)leaf_copy_buffer, k, v, node_addr, lock_buffer, sink, read_entry_num))) {  // return false(remain locked) if need split
#ifdef VACANCY_AWARE_LOCK
    // read the rest of the leaf
    if (read_entry_num < (int)define::leafSpanSize) {
      hopscotch_search(node_addr, r_idx, raw_leaf_buffer, leaf_buffer, sink, define::leafSpanSize - read_entry_num, true);
    }
#endif
    // split leaf node
    hopscotch_split_and_unlock(leaf, k, v, node_addr, lock_buffer, sink);
  }
#else
  for (i = 0; i < (int)define::leafSpanSize; ++ i) if (records[i].key == define::kkeyNull) break;
  bool need_split = (i == define::leafSpanSize);
  if (!need_split) {
    entry_write_and_unlock<LeafNode, LeafEntry, Value>(leaf, i, k, v, node_addr, lock_buffer, sink);
  }
  else {
    // split leaf node, level(leaf) = 0
    node_split_and_unlock<LeafNode, LeafEntry, Value, define::leafSpanSize, define::allocationLeafSize, define::transLeafSize>(leaf, k, v, node_addr, lock_buffer, 0, sink);
  }
#endif
  return true;
}


#ifdef HOPSCOTCH_LEAF_NODE
bool Tree::hopscotch_insert_and_unlock(LeafNode* leaf, const Key& k, Value v, const GlobalAddress& node_addr, uint64_t* lock_buffer, CoroPull* sink, int entry_num) {
  auto& records = leaf->records;
  auto get_entry = [=, &records](int logical_idx) -> LeafEntry& {
    return records[(logical_idx + define::leafSpanSize) % define::leafSpanSize];
  };
  // caculate hash idx
  int hash_idx = get_hashed_leaf_entry_index(k);
  // find an empty slot
  int empty_idx = -1;
  for (int i = hash_idx; i < hash_idx + entry_num; ++ i) {
    if (get_entry(i).key == define::kkeyNull) {
      empty_idx = i;
      break;
    }
  }
  if (empty_idx < 0) return false;  // no empty slot
  // hop
  int j = empty_idx;
  std::vector<int> hopped_idxes;
next_hop:
  hopped_idxes.emplace_back(j % define::leafSpanSize);
  if (j < hash_idx + (int)define::neighborSize) {
    get_entry(j).update(k, v);
    get_entry(hash_idx).set_hop_bit(j - hash_idx);
    segment_write_and_unlock(leaf, hash_idx, empty_idx % define::leafSpanSize, hopped_idxes, node_addr, lock_buffer, sink);
    return true;
  }
  for (int offset = define::neighborSize - 1; offset > 0; -- offset) {
    int h = j - offset;
    // speed up using hopscotch bitmap
    // int h_hash_idx = get_hashed_leaf_entry_index(get_entry(h).key);
    int h_hash_idx = -1;
    for (int z = define::neighborSize - 1; z >= 0; -- z) {
      int h_home = (h - z + define::leafSpanSize) % define::leafSpanSize;
      // corner case
      if (h - h_home < 0) h_home -= define::leafSpanSize;
      else if (h - h_home >= (int)define::neighborSize) h_home += define::leafSpanSize;
      assert(h_home <= h);
      if (h_home + (int)define::neighborSize > j && (get_entry(h_home).hop_bitmap & (1ULL << (define::neighborSize - z - 1)))) {
        h_hash_idx = h_home;
        break;
      }
    }
    if (h_hash_idx < 0) continue;
    get_entry(j).update(get_entry(h).key, get_entry(h).value);
    get_entry(h_hash_idx).unset_hop_bit(h - h_hash_idx);
    get_entry(h_hash_idx).set_hop_bit(j - h_hash_idx);
    j = h;
    goto next_hop;
  }
  return false;  // hop fails
}


Key Tree::hopscotch_get_split_key(LeafEntry* records, const Key& k) {
  // calculate a proper split key to ensure k can be inserted after node split
  auto get_entry = [=](int logical_idx) -> const LeafEntry& {
    return records[(logical_idx + define::leafSpanSize) % define::leafSpanSize];
  };

  std::vector<Key> critical_keys;
  int hash_idx = get_hashed_leaf_entry_index(k);
  for (int empty_idx = hash_idx; empty_idx < hash_idx + (int)define::leafSpanSize; ++ empty_idx) {
    if (get_entry(empty_idx).key == define::kkeyNull) break;
    // try hopping assuming that get_entry(empty_idx) is empty
    int j = empty_idx;
next_hop:
    if (j < hash_idx + (int)define::neighborSize) {
      critical_keys.emplace_back(get_entry(empty_idx).key);
      continue;
    }
    for (int offset = define::neighborSize - 1; offset > 0; -- offset) {
      int h = j - offset;
      int h_hash_idx = get_hashed_leaf_entry_index(get_entry(h).key);
      // corner case
      if (h - h_hash_idx < 0) h_hash_idx -= define::leafSpanSize;
      else if (h - h_hash_idx >= (int)define::neighborSize) h_hash_idx += define::leafSpanSize;
      // hop h => j is ok
      if (h_hash_idx + (int)define::neighborSize > j) {
        j = h;
        goto next_hop;
      }
    }
  }
  assert(!critical_keys.empty());
  critical_keys.emplace_back(k);
  std::sort(critical_keys.begin(), critical_keys.end());
  return critical_keys.at(critical_keys.size() / 2);
}


int Tree::hopscotch_insert_locally(LeafEntry* records, const Key& k, Value v) {
  auto get_entry = [=, &records](int logical_idx) -> LeafEntry& {
    return records[(logical_idx + define::leafSpanSize) % define::leafSpanSize];
  };
  // caculate hash idx
  int hash_idx = get_hashed_leaf_entry_index(k);
  // find an empty slot
  int j = -1;
  for (int i = hash_idx; i < hash_idx + (int)define::leafSpanSize; ++ i) {
    if (get_entry(i).key == define::kkeyNull) {
      j = i;
      break;
    }
  }
  // hop
  assert(j >= 0);
next_hop:
  if (j < hash_idx + (int)define::neighborSize) {
    get_entry(j).update(k, v);
    get_entry(hash_idx).set_hop_bit(j - hash_idx);
    return j % define::leafSpanSize;
  }
  for (int offset = define::neighborSize - 1; offset > 0; -- offset) {
    int h = j - offset;
    // speed up using hopscotch bitmap
    // int h_hash_idx = get_hashed_leaf_entry_index(get_entry(h).key);
    int h_hash_idx = -1;
    for (int z = define::neighborSize - 1; z >= 0; -- z) {
      int h_home = (h - z + define::leafSpanSize) % define::leafSpanSize;
      // corner case
      if (h - h_home < 0) h_home -= define::leafSpanSize;
      else if (h - h_home >= (int)define::neighborSize) h_home += define::leafSpanSize;
      assert(h_home <= h);
      if (h_home + (int)define::neighborSize > j && (get_entry(h_home).hop_bitmap & (1ULL << (define::neighborSize - z - 1)))) {
        h_hash_idx = h_home;
        break;
      }
    }
    if (h_hash_idx < 0) continue;
    // hop h => j is ok
    get_entry(j).update(get_entry(h).key, get_entry(h).value);
    get_entry(h_hash_idx).unset_hop_bit(h - h_hash_idx);
    get_entry(h_hash_idx).set_hop_bit(j - h_hash_idx);
    j = h;
    goto next_hop;
  }
  assert(false);
}


void Tree::hopscotch_split_and_unlock(LeafNode* leaf, const Key& k, Value v, const GlobalAddress& node_addr, uint64_t* lock_buffer, CoroPull* sink) {
  split_node[dsm->getMyThreadID()] ++;
  split_hopscotch[dsm->getMyThreadID()] ++;
  bool is_root = leaf->is_root();
  auto& records = leaf->records;
  // calculate split_key
  auto split_key = hopscotch_get_split_key(records, k);

  // sibling node
  auto sibling_addr = dsm->alloc(define::allocationLeafSize);
  auto sibling_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  auto sibling_leaf = new (sibling_buffer) LeafNode;
  // move data
  int non_empty_entry_cnt = 0;
  for (int i = 0; i < (int)define::leafSpanSize; ++ i) {
    auto& old_e = records[i];
    if (old_e.key != define::kkeyNull) {
      ++ non_empty_entry_cnt;
      if (old_e.key >= split_key) {
        int hash_idx = get_hashed_leaf_entry_index(old_e.key);
        // move
        sibling_leaf->records[i].update(old_e.key, old_e.value);
        old_e.update(define::kkeyNull, define::kValueNull);
        // update hop_bit
        auto offset = (i >= hash_idx ? i - hash_idx : i + (int)define::leafSpanSize - hash_idx);
        sibling_leaf->records[hash_idx].set_hop_bit(offset);
        records[hash_idx].unset_hop_bit(offset);
      }
    }
  }
  load_factor_sum[dsm->getMyThreadID()] += (double)non_empty_entry_cnt / define::leafSpanSize;
  // newly insert kv
  if (k < split_key) hopscotch_insert_locally(records, k, v);
  else hopscotch_insert_locally(sibling_leaf->records, k, v);

  // change metadata
  auto get_max_key_idx = [=](LeafNode* leaf_node) {
    auto max_key = define::kkeyNull;
    int ret = 0;
    for (int i = 0; i < (int)define::leafSpanSize; ++ i) {
      const auto& e = leaf_node->records[i];
      if (e.key > max_key) max_key = e.key, ret = i;
    }
    return ret;
  };
  // update in-node metadata
#ifdef SIBLING_BASED_VALIDATION
  auto max_key = records[get_max_key_idx(leaf)].key;
  split_key = max_key + 1;
  if (leaf->metadata.sibling_ptr == GlobalAddress::Widest()) leaf->metadata.sibling_ptr = GlobalAddress::Null();  // remove the root tag
#endif
  sibling_leaf->metadata.fence_keys = FenceKeys{split_key, leaf->metadata.fence_keys.highest};
  leaf->metadata.fence_keys.highest = split_key;
  sibling_leaf->metadata.sibling_ptr = leaf->metadata.sibling_ptr;
  leaf->metadata.sibling_ptr = sibling_addr;

  // write sibling
  auto encoded_sibling_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
#ifdef METADATA_REPLICATION
  auto intermediate_sibling_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  MetadataManager::encode_node_metadata(sibling_buffer, intermediate_sibling_buffer);
  LeafVersionManager::encode_node_versions(intermediate_sibling_buffer, encoded_sibling_buffer);
#else
  VersionManager<LeafNode, LeafEntry>::encode_node_versions(sibling_buffer, encoded_sibling_buffer);
#endif
#ifdef VACANCY_AWARE_LOCK
  // update in-lock metadata
  auto update_vacancy_bitmap = [=](LeafNode* leaf_node, VALOCK *lock){
    std::vector<int> empty_idxes;
    for (int i = 0; i < (int)define::leafSpanSize; ++ i) {
      const auto& e = leaf_node->records[i];
      if (e.key == define::kkeyNull) empty_idxes.emplace_back(i);
    }
    lock->update_vacancy(0, define::leafSpanSize - 1, empty_idxes);
  };
  // update max_key_idx
  auto if_lock = ((VALOCK *)lock_buffer);
  if_lock->update_max_key_idx(get_max_key_idx(leaf));
  auto if_sibling_lock = new (encoded_sibling_buffer + get_lock_info(true)) VALOCK(0ULL, get_max_key_idx(sibling_leaf));  // unlock
  // update vacancy bitmap
  update_vacancy_bitmap(leaf, if_lock);
  update_vacancy_bitmap(sibling_leaf, if_sibling_lock);
  dsm->write_sync(encoded_sibling_buffer, sibling_addr, define::transLeafSize + define::allocationLockSize, sink);
#else
  dsm->write_sync(encoded_sibling_buffer, sibling_addr, define::transLeafSize, sink);
#endif

  // wirte split node and unlock
  auto encoded_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
#ifdef METADATA_REPLICATION
  auto intermediate_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  MetadataManager::encode_node_metadata((char *)leaf, intermediate_leaf_buffer);
  LeafVersionManager::encode_node_versions(intermediate_leaf_buffer, encoded_leaf_buffer);
#else
  VersionManager<LeafNode, LeafEntry>::encode_node_versions((char *)leaf, encoded_leaf_buffer);
#endif
  auto lock_offset = get_lock_info(true);
  assert(!(*lock_buffer & (1ULL << 63)));
#ifdef SPLIT_WRITE_UNLATCH
  memcpy(encoded_leaf_buffer + lock_offset, lock_buffer, sizeof(uint64_t));  // unlock
  // no need to signal
  dsm->write(encoded_leaf_buffer, node_addr, define::transLeafSize + define::allocationLockSize, false, sink);
#else
  std::vector<RdmaOpRegion> rs(2);
  rs[0].source = (uint64_t)encoded_leaf_buffer;
  rs[0].dest = node_addr.to_uint64();
  rs[0].size = define::transLeafSize;
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)lock_buffer;
  rs[1].dest = (node_addr + lock_offset).to_uint64();
  rs[1].size = sizeof(uint64_t);
  rs[1].is_on_chip = false;
  // no need to signal
  dsm->write_batch(&rs[0], 2, false, sink);
#endif

  // update parent node
  if (is_root) {  // node is root node
    auto root_addr = dsm->alloc(define::allocationInternalSize, PACKED_ADDR_ALIGN_BIT);
    auto root_buffer = (dsm->get_rbuf(sink)).get_internal_buffer();
    auto root_node = new (root_buffer) InternalNode;
    assert(root_node->is_root());
    root_node->metadata.level = 1;
    root_node->metadata.leftmost_ptr = node_addr;
    root_node->records[0] = InternalEntry(split_key, sibling_addr);
    // write new root node
    auto encoded_node_buffer = (dsm->get_rbuf(sink)).get_internal_buffer();
    VersionManager<InternalNode, InternalEntry>::encode_node_versions(root_buffer, encoded_node_buffer);
    dsm->write_sync(encoded_node_buffer, root_addr, define::transInternalSize, sink);
    // udpate root pointer
    auto cas_buffer = (dsm->get_rbuf(sink)).get_cas_buffer();
    if (dsm->cas_sync(root_ptr_ptr, RootEntry(1, node_addr), RootEntry(2, root_addr), cas_buffer, sink)) {  // cas root success
      printf("[INFO] new root level %d\n", 2);
      rough_height.store(2);
      return;
    }
    else {
      // find the internal node whose level is (level + 1), insert the slibling kp into it
      auto root_entry = *(RootEntry *)cas_buffer;
      printf("[INFO] cas root fail, insert from new root... level=%d\n", (int)root_entry.level);
      rough_height.store(root_entry.level);
      insert_internal(split_key, sibling_addr, root_entry, 1, sink);
      return;
    }
  }
  // insert to parent node, parent-child relationship is unchanged
  auto parent_node_addr = path_stack[sink ? sink->get() : 0][1];
#ifdef TREE_ENABLE_CACHE
  const TreeCacheEntry *cache_entry = nullptr;
  bool from_cache = false;
  if (parent_node_addr != GlobalAddress::Null() || (cache_entry = tree_cache->search_ptr_from_cache(split_key, parent_node_addr, 1))) {  // normal cases || in case the internal node needed to split is searched(pointed) from cache
    assert(parent_node_addr != GlobalAddress::Null());
    from_cache = (cache_entry != nullptr);
    if(internal_node_insert(parent_node_addr, split_key, sibling_addr, from_cache, 1, sink)) {
      return;
    }
  }
  if (from_cache) {  // cache invalidation
    tree_cache->invalidate(cache_entry);
  }
#else
  if (parent_node_addr != GlobalAddress::Null()) {
    assert(internal_node_insert(parent_node_addr, split_key, sibling_addr, false, 1, sink));
    return;
  }
#endif
  // static int cnt = 0;
  // printf("[INFO] get parent node fail, insert from root... cnt=%d\n", ++ cnt);
  insert_internal(split_key, sibling_addr, get_root_ptr(sink), 1, sink);
  return;
}

void Tree::hopscotch_search(const GlobalAddress& node_addr, int hash_idx, char *raw_leaf_buffer, char *leaf_buffer, CoroPull* sink, int entry_num, bool for_write) {
  try_read_hopscotch[dsm->getMyThreadID()] ++;
  auto leaf = (LeafNode *)leaf_buffer;
  auto segment_size_r = std::min(entry_num, (int)define::leafSpanSize - hash_idx);
  auto segment_size_l = entry_num <= (int)define::leafSpanSize - hash_idx ? 0 : entry_num - ((int)define::leafSpanSize - hash_idx);

#ifdef METADATA_REPLICATION
  auto [raw_offset_r, raw_len_r, first_offset_r] = LeafVersionManager::get_offset_info(hash_idx, segment_size_r);
  auto [raw_offset_l, raw_len_l, first_offset_l] = LeafVersionManager::get_offset_info(0, segment_size_l);
  assert(segment_size_l > 0 || !raw_len_l);
  if (raw_len_l) read_two_segments[dsm->getMyThreadID()] ++;

  auto raw_segment_buffer_r = raw_leaf_buffer + raw_offset_r;
  auto raw_segment_buffer_l = raw_leaf_buffer + raw_offset_l;
  if (raw_len_l) {  // read two hop segments (corner case)
re_read_2:
    std::vector<RdmaOpRegion> rs(2);
    rs[0].source = (uint64_t)raw_segment_buffer_l;
    rs[0].dest = (node_addr + raw_offset_l).to_uint64();
    rs[0].size = raw_len_l;
    rs[0].is_on_chip = false;

    rs[1].source = (uint64_t)raw_segment_buffer_r;
    rs[1].dest = (node_addr + raw_offset_r).to_uint64();
    rs[1].size = raw_len_r;
    rs[1].is_on_chip = false;
    dsm->read_batch_sync(&rs[0], 2, sink);

    uint8_t segment_node_versions_r = 0, segment_node_versions_l = 0;
    LeafMetadata metadata_l, metadata_r;
    auto intermediate_segment_buffer_l = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto intermediate_segment_buffer_r = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto [first_metadata_offset_l, new_len_l] = MetadataManager::get_offset_info(0, segment_size_l);
    auto [first_metadata_offset_r, new_len_r] = MetadataManager::get_offset_info(hash_idx, segment_size_r);
    if (for_write) {  // for locked node, consistency check is not needed
      assert((LeafVersionManager::decode_segment_versions(raw_segment_buffer_l, intermediate_segment_buffer_l, first_offset_l, segment_size_l, first_metadata_offset_l, new_len_l, segment_node_versions_l)));
      assert((LeafVersionManager::decode_segment_versions(raw_segment_buffer_r, intermediate_segment_buffer_r, first_offset_r, segment_size_r, first_metadata_offset_r, new_len_r, segment_node_versions_r)));
      assert(segment_node_versions_r == segment_node_versions_l);
    }
    else if (!LeafVersionManager::decode_segment_versions(raw_segment_buffer_l, intermediate_segment_buffer_l, first_offset_l, segment_size_l, first_metadata_offset_l, new_len_l, segment_node_versions_l) ||
             !LeafVersionManager::decode_segment_versions(raw_segment_buffer_r, intermediate_segment_buffer_r, first_offset_r, segment_size_r, first_metadata_offset_r, new_len_r, segment_node_versions_r) ||
             segment_node_versions_r != segment_node_versions_l) {  // consistency check
      read_leaf_retry[dsm->getMyThreadID()] ++;
      goto re_read_2;
    }
    bool has_metadata_l = MetadataManager::decode_segment_metadata(intermediate_segment_buffer_l, (char*)&(leaf->records[0]), first_metadata_offset_l, segment_size_l, metadata_l);
    bool has_metadata_r = MetadataManager::decode_segment_metadata(intermediate_segment_buffer_r, (char*)&(leaf->records[hash_idx]), first_metadata_offset_r, segment_size_r, metadata_r);
    assert(has_metadata_l || has_metadata_l);
    if (has_metadata_l && has_metadata_r) assert(metadata_l == metadata_r);
    leaf->metadata = (has_metadata_l ? metadata_l : metadata_r);
    return;
  }
  else {  // read only one hop segment
re_read_1:
    dsm->read_sync(raw_segment_buffer_r, node_addr + raw_offset_r, raw_len_r, sink);
    uint8_t segment_node_versions_r = 0;
    auto intermediate_segment_buffer_r = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto [first_metadata_offset_r, new_len_r] = MetadataManager::get_offset_info(hash_idx, segment_size_r);
    if (for_write) assert((LeafVersionManager::decode_segment_versions(raw_segment_buffer_r, intermediate_segment_buffer_r, first_offset_r, segment_size_r, first_metadata_offset_r, new_len_r, segment_node_versions_r)));
    else if (!LeafVersionManager::decode_segment_versions(raw_segment_buffer_r, intermediate_segment_buffer_r, first_offset_r, segment_size_r, first_metadata_offset_r, new_len_r, segment_node_versions_r)) {
      read_leaf_retry[dsm->getMyThreadID()] ++;
      goto re_read_1;
    }
    auto has_metadata = MetadataManager::decode_segment_metadata(intermediate_segment_buffer_r, (char*)&(leaf->records[hash_idx]), first_metadata_offset_r, segment_size_r, leaf->metadata);
    if (entry_num >= (int)define::neighborSize) assert(has_metadata);
    return;
  }
#else
  auto [raw_offset_r, raw_len_r, first_offset_r] = VersionManager<LeafNode, LeafEntry>::get_offset_info(hash_idx, segment_size_r);
  auto [raw_offset_l, raw_len_l, first_offset_l] = VersionManager<LeafNode, LeafEntry>::get_offset_info(0, segment_size_l);
  assert(segment_size_l > 0 || !raw_len_l);
  if (raw_len_l) read_two_segments[dsm->getMyThreadID()] ++;

  auto raw_segment_buffer_r = raw_leaf_buffer + raw_offset_r;
  auto raw_segment_buffer_l = raw_leaf_buffer + raw_offset_l;
re_read:
  // read metadata and the hop segment
  std::vector<RdmaOpRegion> rs(2);
  rs[0].source = (uint64_t)raw_leaf_buffer;
  rs[0].dest = node_addr.to_uint64();
  rs[0].size = raw_offset_l + raw_len_l;  // header + segment_l (corner case)
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)raw_segment_buffer_r;
  rs[1].dest = (node_addr + raw_offset_r).to_uint64();
  rs[1].size = raw_len_r;
  rs[1].is_on_chip = false;
  // note that the rs array will change by lower-level function
  dsm->read_batch_sync(&rs[0], 2, sink);
  uint8_t metadata_node_version = 0, segment_node_versions_r = 0, segment_node_versions_l = 0;
  if (for_write) {  // for locked node, consistency check is not needed
    assert((VersionManager<LeafNode, LeafEntry>::decode_header_versions(raw_leaf_buffer, leaf_buffer, metadata_node_version)));
    if (segment_size_l > 0) assert((VersionManager<LeafNode, LeafEntry>::decode_segment_versions(raw_segment_buffer_l, (char*)&(leaf->records[0]), first_offset_l, segment_size_l, segment_node_versions_l)));
    assert((VersionManager<LeafNode, LeafEntry>::decode_segment_versions(raw_segment_buffer_r, (char*)&(leaf->records[hash_idx]), first_offset_r, segment_size_r, segment_node_versions_r)));
    assert(metadata_node_version == segment_node_versions_r);
    if (segment_size_l > 0) assert(metadata_node_version == segment_node_versions_l);
    return;
  }
  // consistency check; note that: (segment_size_l > 0) => there are two segments
  if (!VersionManager<LeafNode, LeafEntry>::decode_header_versions(raw_leaf_buffer, leaf_buffer, metadata_node_version) ||
      (segment_size_l > 0 && !VersionManager<LeafNode, LeafEntry>::decode_segment_versions(raw_segment_buffer_l, (char*)&(leaf->records[0]), first_offset_l, segment_size_l, segment_node_versions_l)) ||
      !VersionManager<LeafNode, LeafEntry>::decode_segment_versions(raw_segment_buffer_r, (char*)&(leaf->records[hash_idx]), first_offset_r, segment_size_r, segment_node_versions_r) ||
      metadata_node_version != segment_node_versions_r ||
      (segment_size_l > 0 && metadata_node_version != segment_node_versions_l)) {
    read_leaf_retry[dsm->getMyThreadID()] ++;
    goto re_read;
  }
#endif
  return;
}
#endif


void Tree::segment_write_and_unlock(LeafNode* leaf, int l_idx, int r_idx, const std::vector<int>& hopped_idxes, const GlobalAddress& node_addr, uint64_t* lock_buffer, CoroPull* sink) {
  try_write_segment[dsm->getMyThreadID()] ++;
  auto& records = leaf->records;
  const auto& metadata = leaf->metadata;
  auto lock_offset = get_lock_info(true);
  assert(!(*lock_buffer & (1ULL << 63)));

#ifdef VACANCY_AWARE_LOCK
  auto if_lock = (VALOCK *)lock_buffer;
  // update max_key_idx
  auto max_key_idx = if_lock->get_max_key_idx();
  if (std::find(hopped_idxes.begin(), hopped_idxes.end(), max_key_idx) != hopped_idxes.end()) {
    auto new_max_key = define::kkeyNull;
    for (int idx : hopped_idxes) {
      const auto& e = records[idx];
      if (e.key > new_max_key) new_max_key = e.key, max_key_idx = idx;
    }
    if_lock->update_max_key_idx(max_key_idx);
  }
  // update vacancy bitmap
  std::vector<int> empty_idxes;
  auto get_empty_idxes = [=, &empty_idxes](int l, int r){
    for (int i = l; i < r; ++ i) if (leaf->records[i].key == define::kkeyNull) {
      empty_idxes.emplace_back(i);
    }
  };
  if (l_idx <= r_idx) get_empty_idxes(l_idx, r_idx + 1);
  else {
    get_empty_idxes(0, r_idx + 1);
    get_empty_idxes(l_idx, define::leafSpanSize);
  }
  if_lock->update_vacancy(l_idx, r_idx, empty_idxes);
#endif

  if (l_idx <= r_idx) {  // update with one WRITE + unlock
    auto encoded_segment_buffer = (dsm->get_rbuf(sink)).get_segment_buffer();
#ifdef METADATA_REPLICATION
    // segment [l_idx, r_idx]
    auto intermediate_segment_buffer = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto [first_metadata_offset, new_len] = MetadataManager::get_offset_info(l_idx, r_idx - l_idx + 1);
    MetadataManager::encode_segment_metadata((char *)&records[l_idx], intermediate_segment_buffer, first_metadata_offset, r_idx - l_idx + 1, metadata);
    auto [raw_offset, raw_len, first_offset] = LeafVersionManager::get_offset_info(l_idx, r_idx - l_idx + 1);
    LeafVersionManager::encode_segment_versions(intermediate_segment_buffer, encoded_segment_buffer, first_offset, hopped_idxes, l_idx, r_idx, first_metadata_offset, new_len);
#else
    // segment [l_idx, r_idx]
    auto [raw_offset, raw_len, first_offset] = VersionManager<LeafNode, LeafEntry>::get_offset_info(l_idx, r_idx - l_idx + 1);
    VersionManager<LeafNode, LeafEntry>::encode_segment_versions((char *)&records[l_idx], encoded_segment_buffer, first_offset, hopped_idxes, l_idx, r_idx);
#endif
    // write segment and unlock
    if (r_idx == define::leafSpanSize - 1) {
      memcpy(encoded_segment_buffer + raw_len + lock_offset - define::transLeafSize, lock_buffer, sizeof(uint64_t));  // unlock
      dsm->write_sync(encoded_segment_buffer, node_addr + raw_offset, raw_len + define::allocationLockSize, sink);
    }
    else {
      std::vector<RdmaOpRegion> rs(2);
      rs[0].source = (uint64_t)encoded_segment_buffer;
      rs[0].dest = (node_addr + raw_offset).to_uint64();
      rs[0].size = raw_len;
      rs[0].is_on_chip = false;

      rs[1].source = (uint64_t)lock_buffer;
      rs[1].dest = (node_addr + lock_offset).to_uint64();
      rs[1].size = sizeof(uint64_t);
      rs[1].is_on_chip = false;
      dsm->write_batch_sync(&rs[0], 2, sink);
    }
  }
  else {  // update with two WRITE + unlock
    write_two_segments[dsm->getMyThreadID()] ++;
    auto encoded_segment_buffer_1 = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto encoded_segment_buffer_2 = (dsm->get_rbuf(sink)).get_segment_buffer();
#ifdef METADATA_REPLICATION
    // segment [0, r_idx]
    auto intermediate_segment_buffer_1 = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto [first_metadata_offset_1, new_len_1] = MetadataManager::get_offset_info(0, r_idx + 1);
    MetadataManager::encode_segment_metadata((char *)&records[0], intermediate_segment_buffer_1, first_metadata_offset_1, r_idx + 1, metadata);
    auto [raw_offset_1, raw_len_1, first_offset_1] = LeafVersionManager::get_offset_info(0, r_idx + 1);
    LeafVersionManager::encode_segment_versions(intermediate_segment_buffer_1, encoded_segment_buffer_1, first_offset_1, hopped_idxes, 0, r_idx, first_metadata_offset_1, new_len_1);
    // segment [l_idx, SPAN_SIZE-1]
    auto intermediate_segment_buffer_2 = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto [first_metadata_offset_2, new_len_2] = MetadataManager::get_offset_info(l_idx, define::leafSpanSize - l_idx);
    MetadataManager::encode_segment_metadata((char *)&records[l_idx], intermediate_segment_buffer_2, first_metadata_offset_2, define::leafSpanSize - l_idx, metadata);
    auto [raw_offset_2, raw_len_2, first_offset_2] = LeafVersionManager::get_offset_info(l_idx, define::leafSpanSize - l_idx);
    LeafVersionManager::encode_segment_versions(intermediate_segment_buffer_2, encoded_segment_buffer_2, first_offset_2, hopped_idxes, l_idx, define::leafSpanSize - 1, first_metadata_offset_2, new_len_2);
#else
    // segment [0, r_idx]
    auto [raw_offset_1, raw_len_1, first_offset_1] = VersionManager<LeafNode, LeafEntry>::get_offset_info(0, r_idx + 1);
    VersionManager<LeafNode, LeafEntry>::encode_segment_versions((char *)&records[0], encoded_segment_buffer_1, first_offset_1, hopped_idxes, 0, r_idx);
    // segment [l_idx, SPAN_SIZE-1]
    auto [raw_offset_2, raw_len_2, first_offset_2] = VersionManager<LeafNode, LeafEntry>::get_offset_info(l_idx, define::leafSpanSize - l_idx);
    VersionManager<LeafNode, LeafEntry>::encode_segment_versions((char *)&records[l_idx], encoded_segment_buffer_2, first_offset_2, hopped_idxes, l_idx, define::leafSpanSize - 1);
#endif
    // write segments and unlock
    std::vector<RdmaOpRegion> rs(2);
    rs[0].source = (uint64_t)encoded_segment_buffer_1;
    rs[0].dest = (node_addr + raw_offset_1).to_uint64();
    rs[0].size = raw_len_1;
    rs[0].is_on_chip = false;

    memcpy(encoded_segment_buffer_2 + raw_len_2 + lock_offset - define::transLeafSize, lock_buffer, sizeof(uint64_t));  // unlock
    rs[1].source = (uint64_t)encoded_segment_buffer_2;
    rs[1].dest = (node_addr + raw_offset_2).to_uint64();
    rs[1].size = raw_len_2 + define::allocationLockSize;
    rs[1].is_on_chip = false;
    dsm->write_batch_sync(&rs[0], 2, sink);
  }
  return;
}


bool Tree::internal_node_insert(const GlobalAddress& node_addr, const Key &k, const GlobalAddress &v, bool from_cache, uint8_t level,
                               CoroPull* sink) {
  // lock node
  auto lock_buffer = (dsm->get_rbuf(sink)).get_lock_buffer();
  lock_node(node_addr, lock_buffer, false, sink);
  // read internal node
  auto raw_internal_buffer = (dsm->get_rbuf(sink)).get_internal_buffer();
  auto internal_buffer = (dsm->get_rbuf(sink)).get_internal_buffer();
  auto node = (InternalNode *) internal_buffer;
re_read:
  dsm->read_sync(raw_internal_buffer, node_addr, define::transInternalSize, sink);
  if (!VersionManager<InternalNode, InternalEntry>::decode_node_versions(raw_internal_buffer, internal_buffer)) {
    goto re_read;
  }
  const auto& fence_keys = node->metadata.fence_keys;
  if (from_cache && (!node->metadata.valid || k < fence_keys.lowest || k >= fence_keys.highest)) {  // cache is outdated
    unlock_node(node_addr, lock_buffer, false, sink, true);
    return false;
  }
  if (k >= fence_keys.highest) {  // should turn right
    unlock_node(node_addr, lock_buffer, false, sink, true);
    assert(node->metadata.sibling_ptr != GlobalAddress::Null());
    internal_node_insert(node->metadata.sibling_ptr, k, v, false, level, sink);
    return true;
  }
  assert(k >= fence_keys.lowest);

  // start insert
  auto& records = node->records;
  int i;
  // should not exist key=k
  for (i = 0; i < (int)define::internalSpanSize; ++ i) assert(records[i].key != k);
  // search for empty entry
  for (i = 0; i < (int)define::internalSpanSize; ++ i) if (records[i].key == define::kkeyNull) break;
  bool need_split = (i == define::internalSpanSize);
  if (!need_split) {
#ifdef UNORDERED_INTERNAL_NODE
    entry_write_and_unlock<InternalNode, InternalEntry, GlobalAddress>(node, i, k, v, node_addr, lock_buffer, sink, true);
#else
    // search for the insert position
    int insert_idx;
    for (insert_idx = 0; insert_idx < i; ++ insert_idx) if (records[insert_idx].key > k) break;
    // shift and insert
    for (int j = i - 1; j >= insert_idx; -- j) records[j + 1] = records[j];
    records[insert_idx].update(k, v);
    // write the whole node
    node_write_and_unlock<InternalNode, InternalEntry, define::transInternalSize>(node, node_addr, lock_buffer, sink, true);
#endif
  }
  else {
    // split internal node
    node_split_and_unlock<InternalNode, InternalEntry, GlobalAddress, define::internalSpanSize, define::allocationInternalSize, define::transInternalSize>(node, k, v,
                          node_addr, lock_buffer, level, sink);
  }
  return true;
}


template <class NODE, class ENTRY, class VAL, int SPAN_SIZE, int ALLOC_SIZE, int TRANS_SIZE>
void Tree::node_split_and_unlock(NODE* node, const Key& k, VAL v, const GlobalAddress& node_addr, uint64_t* lock_buffer, uint8_t level,
                                 CoroPull* sink) {
  split_node[dsm->getMyThreadID()] ++;
  bool is_root = node->is_root();
  auto& records = node->records;
  std::sort(records, records + SPAN_SIZE, [](const ENTRY& a, const ENTRY& b){
    if (a.key == define::kkeyNull) return false;
    if (b.key == define::kkeyNull) return true;
    return a.key < b.key;
  });
  int cnt = 0;
  for (const auto& e : records) if (e.key != define::kkeyNull) ++ cnt;
  int m = cnt / 2;
  auto split_key = records[m].key;
  assert(split_key != define::kkeyNull);
  // sibling node
  auto sibling_addr = dsm->alloc(ALLOC_SIZE);
  auto sibling_buffer = (dsm->get_rbuf(sink)).get_node_buffer<NODE>();
  auto sibling_node = new (sibling_buffer) NODE;
  // move && insert new kv && re-determined split-key (if needed)
  if (NODE::IS_LEAF) {
    for (int i = m; i < SPAN_SIZE; ++ i) {
      sibling_node->records[i - m] = records[i];
    }
    std::fill(records + m, records + SPAN_SIZE, ENTRY::Null());
    if (k < split_key) records[m] = ENTRY(k, v);
    else sibling_node->records[SPAN_SIZE - m] = ENTRY(k, v);
#ifdef SIBLING_BASED_VALIDATION
    auto max_key = ((k < split_key && records[m - 1].key < k) ? k : records[m - 1].key);
    split_key = max_key + 1;
#endif
  }
  else {
    for (int i = m + 1; i < SPAN_SIZE; ++ i) {
      sibling_node->records[i - m - 1] = records[i];
    }
    ((InternalNode*)sibling_node)->metadata.leftmost_ptr = ((InternalEntry*)records)[m].ptr;
    std::fill(records + m, records + SPAN_SIZE, ENTRY::Null());
#ifdef UNORDERED_INTERNAL_NODE
    if (k < split_key) records[m] = ENTRY(k, v);
    else sibling_node->records[SPAN_SIZE - m - 1] = ENTRY(k, v);
#else
    // search for the insert position
    auto shift_and_insert = [&k, &v](ENTRY* records, int num){
      int insert_idx;
      for (insert_idx = 0; insert_idx < num; ++ insert_idx) if (records[insert_idx].key > k) break;
      // shift and insert
      for (int j = num - 1; j >= insert_idx; -- j) records[j + 1] = records[j];
      records[insert_idx].update(k, v);
    };
    if (k < split_key) shift_and_insert(records, m);
    else shift_and_insert(sibling_node->records, SPAN_SIZE - m - 1);
#endif
  }
  // change metadata
  sibling_node->metadata.level = level;
  sibling_node->metadata.fence_keys = FenceKeys{split_key, node->metadata.fence_keys.highest};
  node->metadata.fence_keys.highest = split_key;
#ifdef SIBLING_BASED_VALIDATION
  if (NODE::IS_LEAF && node->metadata.sibling_ptr == GlobalAddress::Widest()) node->metadata.sibling_ptr = GlobalAddress::Null();  // remove the root tag
#endif
  sibling_node->metadata.sibling_ptr = node->metadata.sibling_ptr;
  node->metadata.sibling_ptr = sibling_addr;
  if (!NODE::IS_LEAF) {
    // update sibling_leftmost_ptr
    ((InternalNode*)sibling_node)->metadata.sibling_leftmost_ptr = ((InternalNode*)node)->metadata.sibling_leftmost_ptr;
    ((InternalNode*)node)->metadata.sibling_leftmost_ptr = ((InternalNode*)sibling_node)->metadata.leftmost_ptr;
  }
  // write sibling
  auto encoded_sibling_buffer = (dsm->get_rbuf(sink)).get_node_buffer<NODE>();
  VersionManager<NODE, ENTRY>::encode_node_versions(sibling_buffer, encoded_sibling_buffer);
  dsm->write_sync(encoded_sibling_buffer, sibling_addr, TRANS_SIZE, sink);
  // wirte split node and unlock
  auto encoded_node_buffer = (dsm->get_rbuf(sink)).get_node_buffer<NODE>();
  VersionManager<NODE, ENTRY>::encode_node_versions((char *)node, encoded_node_buffer);
  auto lock_offset = get_lock_info(NODE::IS_LEAF);
  assert(!(*lock_buffer & (1ULL << 63)));
#ifdef SPLIT_WRITE_UNLATCH
  memcpy(encoded_node_buffer + lock_offset, lock_buffer, sizeof(uint64_t));  // unlock
  // no need to signal
  dsm->write(encoded_node_buffer, node_addr, TRANS_SIZE + define::allocationLockSize, false, sink);
#else
  std::vector<RdmaOpRegion> rs(2);
  rs[0].source = (uint64_t)encoded_node_buffer;
  rs[0].dest = node_addr.to_uint64();
  rs[0].size = TRANS_SIZE;
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)lock_buffer;
  rs[1].dest = (node_addr + lock_offset).to_uint64();
  rs[1].size = sizeof(uint64_t);
  rs[1].is_on_chip = false;
  // no need to signal
  dsm->write_batch(&rs[0], 2, false, sink);
#endif
  // update parent node
  if (is_root) {  // node is root node
    auto root_addr = dsm->alloc(define::allocationInternalSize, PACKED_ADDR_ALIGN_BIT);
    auto root_buffer = (dsm->get_rbuf(sink)).get_internal_buffer();
    auto root_node = new (root_buffer) InternalNode;
    assert(root_node->is_root());
    root_node->metadata.level = level + 1;
    root_node->metadata.leftmost_ptr = node_addr;
    root_node->records[0] = InternalEntry(split_key, sibling_addr);
    // write new root node
    auto encoded_node_buffer = (dsm->get_rbuf(sink)).get_internal_buffer();
    VersionManager<InternalNode, InternalEntry>::encode_node_versions(root_buffer, encoded_node_buffer);
    dsm->write_sync(encoded_node_buffer, root_addr, define::transInternalSize, sink);
    // udpate root pointer
    auto cas_buffer = (dsm->get_rbuf(sink)).get_cas_buffer();
    if (dsm->cas_sync(root_ptr_ptr, RootEntry(level + 1, node_addr), RootEntry(level + 2, root_addr), cas_buffer, sink)) {  // cas root success
      printf("[INFO] new root level %d\n", (int)(level + 2));
      rough_height.store((uint16_t)(level + 2));
      return;
    }
    else {
      // find the internal node whose level is (level + 1), insert the slibling kp into it
      auto root_entry = *(RootEntry *)cas_buffer;
      printf("[INFO] cas root fail, insert from new root... level=%d\n", (int)root_entry.level);
      rough_height.store(root_entry.level);
      insert_internal(split_key, sibling_addr, root_entry, level + 1, sink);
      return;
    }
  }
  // insert to parent node, parent-child relationship is unchanged
  auto parent_node_addr = path_stack[sink ? sink->get() : 0][level + 1];
#ifdef TREE_ENABLE_CACHE
  const TreeCacheEntry *cache_entry = nullptr;
  bool from_cache = false;
  if (parent_node_addr != GlobalAddress::Null() || (cache_entry = tree_cache->search_ptr_from_cache(split_key, parent_node_addr, level + 1))) {  // normal cases || in case the internal node needed to split is searched(pointed) from cache
    assert(parent_node_addr != GlobalAddress::Null());
    from_cache = (cache_entry != nullptr);
    if(internal_node_insert(parent_node_addr, split_key, sibling_addr, from_cache, level + 1, sink)) {
      return;
    }
  }
  if (from_cache) {  // cache invalidation
    tree_cache->invalidate(cache_entry);
  }
#else
  if (parent_node_addr != GlobalAddress::Null()) {
    assert(internal_node_insert(parent_node_addr, split_key, sibling_addr, false, level + 1, sink));
    return;
  }
#endif
  // static int cnt = 0;
  // printf("[INFO] get parent node fail, insert from root... cnt=%d\n", ++ cnt);
  insert_internal(split_key, sibling_addr, get_root_ptr(sink), level + 1, sink);
  return;
}


template <class NODE, class ENTRY, class VAL>
void Tree::entry_write_and_unlock(NODE* node, const int idx, const Key& k, VAL v, const GlobalAddress& node_addr, uint64_t* lock_buffer,
                                  CoroPull* sink, bool async) {
  auto& entry = node->records[idx];
  const auto & metadata = node->metadata;
  entry.update(k, v);
  auto encoded_entry_buffer = (dsm->get_rbuf(sink)).get_entry_buffer();
#if (defined METADATA_REPLICATION && defined HOPSCOTCH_LEAF_NODE)
  auto get_info_and_encode_versions = [=, &entry, &metadata](int idx){
    if (NODE::IS_LEAF) {
      auto intermediate_segment_buffer = (dsm->get_rbuf(sink)).get_segment_buffer();
      auto [first_metadata_offset, new_len] = MetadataManager::get_offset_info(idx);
      MetadataManager::encode_segment_metadata((char *)&entry, intermediate_segment_buffer, first_metadata_offset, 1,
                                               LeafMetadata(metadata.h_version, 0, metadata.valid, metadata.sibling_ptr, metadata.fence_keys));
      auto [raw_offset, raw_len, first_offset] = LeafVersionManager::get_offset_info(idx);
      LeafVersionManager::encode_segment_versions(intermediate_segment_buffer, encoded_entry_buffer, first_offset, std::vector<int>{idx}, idx, idx, first_metadata_offset, new_len);
      return std::make_pair(raw_offset, raw_len);
    }
    else {
      auto [raw_offset, raw_len, first_offset] = VersionManager<NODE, ENTRY>::get_offset_info(idx);
      VersionManager<NODE, ENTRY>::encode_entry_versions((char *)&entry, encoded_entry_buffer, first_offset);
      return std::make_pair(raw_offset, raw_len);
    }
  };
  auto [raw_offset, raw_len] = get_info_and_encode_versions(idx);
#else
  auto [raw_offset, raw_len, first_offset] = VersionManager<NODE, ENTRY>::get_offset_info(idx);
  VersionManager<NODE, ENTRY>::encode_entry_versions((char *)&entry, encoded_entry_buffer, first_offset);
#endif
  // write entry and unlock
  auto lock_offset = get_lock_info(NODE::IS_LEAF);
  assert(!(*lock_buffer & (1ULL << 63)));
  if (idx == (NODE::IS_LEAF ? define::leafSpanSize : define::internalSpanSize) - 1) {
    auto node_size = NODE::IS_LEAF ? define::transLeafSize : define::transInternalSize;
    memcpy(encoded_entry_buffer + raw_len + lock_offset - node_size, lock_buffer, sizeof(uint64_t));  // unlock
    if (async) dsm->write(encoded_entry_buffer, node_addr + raw_offset, raw_len + define::allocationLockSize, false, sink);
    else dsm->write_sync(encoded_entry_buffer, node_addr + raw_offset, raw_len + define::allocationLockSize, sink);
  }
  else {
    std::vector<RdmaOpRegion> rs(2);
    rs[0].source = (uint64_t)encoded_entry_buffer;
    rs[0].dest = (node_addr + raw_offset).to_uint64();
    rs[0].size = raw_len;
    rs[0].is_on_chip = false;

    rs[1].source = (uint64_t)lock_buffer;
    rs[1].dest = (node_addr + lock_offset).to_uint64();
    rs[1].size = sizeof(uint64_t);
    rs[1].is_on_chip = false;
    if (async) dsm->write_batch(&rs[0], 2, false, sink);
    else dsm->write_batch_sync(&rs[0], 2, sink);
  }
  return;
}


template <class NODE, class ENTRY, int TRANS_SIZE>
void Tree::node_write_and_unlock(NODE* node, const GlobalAddress& node_addr, uint64_t* lock_buffer, CoroPull* sink, bool async) {
  // write the whole node
  auto encoded_node_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  VersionManager<NODE, ENTRY>::encode_node_versions((char*)node, encoded_node_buffer);
  // write node and unlock
  auto lock_offset = get_lock_info(NODE::IS_LEAF);
  assert(!(*lock_buffer & (1ULL << 63)));
#ifdef SPLIT_WRITE_UNLATCH
  memcpy(encoded_node_buffer + lock_offset, lock_buffer, sizeof(uint64_t));  // unlock
  dsm->write_sync(encoded_node_buffer, node_addr, TRANS_SIZE + define::allocationLockSize, sink);
#else
  std::vector<RdmaOpRegion> rs(2);
  rs[0].source = (uint64_t)encoded_node_buffer;
  rs[0].dest = node_addr.to_uint64();
  rs[0].size = TRANS_SIZE;
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)lock_buffer;
  rs[1].dest = (node_addr + lock_offset).to_uint64();
  rs[1].size = sizeof(uint64_t);
  rs[1].is_on_chip = false;
  if (async) dsm->write_batch(&rs[0], 2, false, sink);
  else dsm->write_batch_sync(&rs[0], 2, sink);
#endif
  return;
}


void Tree::update(const Key &k, Value v, CoroPull* sink) {
  assert(dsm->is_register());
  before_operation(sink);

  // handover
  bool write_handover = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);

  // cache
  bool from_cache = false;
  const TreeCacheEntry *cache_entry = nullptr;

  // traversal
  GlobalAddress p;
  GlobalAddress sibling_p;
  uint16_t level;
  int retry_flag = FIRST_TRY;

  try_write_op[dsm->getMyThreadID()]++;

#ifdef TREE_ENABLE_WRITE_COMBINING
  lock_res = local_lock_table->acquire_local_write_lock(k, v, &busy_waiting_queue, sink);
  write_handover = (lock_res.first && !lock_res.second);
#else
  UNUSED(lock_res);
#endif
  if (write_handover) {
    write_handover_num[dsm->getMyThreadID()]++;
    goto update_finish;
  }

#ifdef TREE_ENABLE_CACHE
  cache_entry = tree_cache->search_from_cache(k, p, sibling_p, level);
  if (cache_entry) from_cache = true;
#endif
  if (!from_cache) {
    auto e = get_root_ptr(sink);
    p = e.ptr, sibling_p = GlobalAddress::Null(), level = e.level;
  }
  record_cache_hit_ratio(from_cache, level);
  assert(level != 0);

next:
  retry_cnt[dsm->getMyThreadID()][retry_flag] ++;
  // read leaf node
  if (level == 1) {
    if (!leaf_node_update(p, sibling_p, k, v, from_cache, sink)) {  // return false if cache validation fail
      // cache invalidation
      assert(from_cache);
      tree_cache->invalidate(cache_entry);
#ifdef CACHE_MORE_INTERNAL_NODE
      cache_entry = tree_cache->search_from_cache(k, p, sibling_p, level);
      from_cache = cache_entry ? true : false;
#else
      from_cache = false;
#endif
      if (!from_cache) {
        auto e = get_root_ptr(sink);
        p = e.ptr, sibling_p = GlobalAddress::Null(), level = e.level;
      }
      retry_flag = INVALID_LEAF;
      goto next;
    }
    goto update_finish;
  }
  // traverse internal nodes
  if (!internal_node_search(p, sibling_p, k, level, from_cache, sink)) {  // return false if cache validation fail
    // cache invalidation
    assert(from_cache);
    tree_cache->invalidate(cache_entry);
#ifdef CACHE_MORE_INTERNAL_NODE
    cache_entry = tree_cache->search_from_cache(k, p, sibling_p, level);
    from_cache = cache_entry ? true : false;
#else
    from_cache = false;
#endif
    if (!from_cache) {
      auto e = get_root_ptr(sink);
      p = e.ptr, sibling_p = GlobalAddress::Null(), level = e.level;
    }
    retry_flag = INVALID_NODE;
    goto next;
  }
  from_cache = false;
  retry_flag = FIND_NEXT;
  goto next;  // search next level

update_finish:
#ifdef TREE_ENABLE_WRITE_COMBINING
  local_lock_table->release_local_write_lock(k, lock_res);
#endif
  return;
}


bool Tree::leaf_node_update(const GlobalAddress& node_addr, const GlobalAddress& sibling_addr, const Key &k, Value v, bool from_cache, CoroPull* sink) {
  int i;
  try_read_leaf[dsm->getMyThreadID()] ++;
  // lock node
  auto lock_buffer = (dsm->get_rbuf(sink)).get_lock_buffer();
  lock_node(node_addr, lock_buffer, true, sink);
  // read leaf
  auto raw_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  auto leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  auto leaf = (LeafNode *) leaf_buffer;
  auto& records = leaf->records;
  GlobalAddress sibling_ptr;
  FenceKeys fence_keys;

#ifdef HOPSCOTCH_LEAF_NODE
  int hash_idx = get_hashed_leaf_entry_index(k);
#ifdef SPECULATIVE_READ
  Value old_v;
  if (speculative_read(node_addr, std::make_pair(hash_idx, (hash_idx + define::neighborSize) % define::leafSpanSize), raw_leaf_buffer, leaf_buffer, k, old_v, i, sink, true)) {
    UNUSED(old_v);
    goto update_entry;
  }
#endif
  hopscotch_search(node_addr, hash_idx, raw_leaf_buffer, leaf_buffer, sink, define::neighborSize, true);
#else
#ifdef SPECULATIVE_READ
  Value old_v;
  if (speculative_read(node_addr, std::make_pair(0, define::leafSpanSize), raw_leaf_buffer, leaf_buffer, k, old_v, i, sink, true)) {
    UNUSED(old_v);
    goto update_entry;
  }
#endif
  dsm->read_sync(raw_leaf_buffer, node_addr, define::transLeafSize, sink);
  // no need to consistency check since the node is locked
  assert((VersionManager<LeafNode, LeafEntry>::decode_node_versions(raw_leaf_buffer, leaf_buffer)));
#endif

#ifdef SIBLING_BASED_VALIDATION
  UNUSED(fence_keys);
  sibling_ptr = (leaf->metadata.sibling_ptr == GlobalAddress::Widest() ? GlobalAddress::Null() : (GlobalAddress)leaf->metadata.sibling_ptr);
  // cache validation
  if (from_cache && (!leaf->metadata.valid || sibling_addr != sibling_ptr)) {  // cache is outdated
    unlock_node(node_addr, lock_buffer, true, sink, true);
    leaf_cache_invalid[dsm->getMyThreadID()] ++;
    return false;
  }
#else
  UNUSED(sibling_ptr);
  UNUSED(sibling_addr);
  // cache validation
  fence_keys = leaf->metadata.fence_keys;
  if (from_cache && (!leaf->metadata.valid || k < fence_keys.lowest || k >= fence_keys.highest)) {  // cache is outdated
    unlock_node(node_addr, lock_buffer, true, sink, true);
    leaf_cache_invalid[dsm->getMyThreadID()] ++;
    return false;
  }
  // turn right check
  if (k >= fence_keys.highest) {  // should turn right
    unlock_node(node_addr, lock_buffer, true, sink, true);
    assert(leaf->metadata.sibling_ptr != GlobalAddress::Null());
    leaf_read_sibling[dsm->getMyThreadID()] ++;
    leaf_node_update(leaf->metadata.sibling_ptr, GlobalAddress::Null(), k, v, false, sink);
    return true;
  }
  assert(k >= fence_keys.lowest);
#endif

  // start update
  // search for existing key
#ifdef HOPSCOTCH_LEAF_NODE
  int j;
  for (j = 0; j < (int)define::neighborSize; ++ j) {
    i = (hash_idx + j) % define::leafSpanSize;
    if (records[i].key == k) break;
  }
#ifdef SIBLING_BASED_VALIDATION
  // turn right check
  if (j == (int)define::neighborSize && sibling_addr != sibling_ptr) {  // search sibling node
    unlock_node(node_addr, lock_buffer, true, sink, true);
    assert(sibling_ptr != GlobalAddress::Null());
    leaf_read_sibling[dsm->getMyThreadID()] ++;
    leaf_node_update(sibling_ptr, sibling_addr, k, v, false, sink);
    return true;
  }
#endif
  assert(j != (int)define::neighborSize);
#else
  for (i = 0; i < (int)define::leafSpanSize; ++ i) if (records[i].key == k) break;
#ifdef SIBLING_BASED_VALIDATION
  if (i == (int)define::leafSpanSize && sibling_addr != sibling_ptr) {  // search sibling node
    unlock_node(node_addr, lock_buffer, true, sink, true);
    assert(sibling_ptr != GlobalAddress::Null());
    leaf_node_update(sibling_ptr, sibling_addr, k, v, false, sink);
    return true;
  }
#endif
  assert(i != (int)define::leafSpanSize);
#endif
#ifdef SPECULATIVE_READ
  idx_cache->add_to_cache(node_addr, i, k);
update_entry:
#endif
#ifdef TREE_ENABLE_WRITE_COMBINING
  local_lock_table->get_combining_value(k, v);
#endif

#ifdef ENABLE_VAR_LEN_KV
  {
  // first write a new DataBlock out-of-place
  auto block_buffer = (dsm->get_rbuf(sink)).get_block_buffer();
  auto data_block = new (block_buffer) DataBlock(v);
  auto block_addr = dsm->alloc(define::dataBlockLen, PACKED_ADDR_ALIGN_BIT);
  dsm->write_sync(block_buffer, block_addr, define::dataBlockLen, sink);
  // change value into the DataPointer value pointing to the DataBlock
  v = (uint64_t)DataPointer(define::dataBlockLen, block_addr);
  }
#endif
  entry_write_and_unlock<LeafNode, LeafEntry, Value>(leaf, i, k, v, node_addr, lock_buffer, sink);
  return true;
}


bool Tree::search(const Key &k, Value &v, CoroPull* sink) {
  assert(dsm->is_register());
  before_operation(sink);

  // handover
  bool search_res = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);
  bool read_handover = false;

  // cache
  bool from_cache = false;
  const TreeCacheEntry *cache_entry = nullptr;

  // traversal
  GlobalAddress p;
  GlobalAddress sibling_p;
  uint16_t level;
  int retry_flag = FIRST_TRY;

  try_read_op[dsm->getMyThreadID()] ++;

#ifdef TREE_ENABLE_READ_DELEGATION
  lock_res = local_lock_table->acquire_local_read_lock(k, &busy_waiting_queue, sink);
  read_handover = (lock_res.first && !lock_res.second);
#else
  UNUSED(lock_res);
#endif
  if (read_handover) {
    read_handover_num[dsm->getMyThreadID()]++;
    goto search_finish;
  }

#ifdef TREE_ENABLE_CACHE
  cache_entry = tree_cache->search_from_cache(k, p, sibling_p, level);
  if (cache_entry) from_cache = true;
#endif
  if (!from_cache) {
    auto e = get_root_ptr(sink);
    p = e.ptr, sibling_p = GlobalAddress::Null(), level = e.level;
  }
  record_cache_hit_ratio(from_cache, level);
  assert(level != 0);
  v = define::kValueNull;

next:
  retry_cnt[dsm->getMyThreadID()][retry_flag] ++;
  // read leaf node
  if (level == 1) {
    if (!leaf_node_search(p, sibling_p, k, v, from_cache, sink)) {  // return false if cache validation fail
      // cache invalidation
      assert(from_cache);
      tree_cache->invalidate(cache_entry);
#ifdef CACHE_MORE_INTERNAL_NODE
      cache_entry = tree_cache->search_from_cache(k, p, sibling_p, level);
      from_cache = cache_entry ? true : false;
#else
      from_cache = false;
#endif
      if (!from_cache) {
        auto e = get_root_ptr(sink);
        p = e.ptr, sibling_p = GlobalAddress::Null(), level = e.level;
      }
      retry_flag = INVALID_LEAF;
      goto next;
    }
    search_res = (v != define::kValueNull);  // search finish
#ifdef ENABLE_VAR_LEN_KV
    if (search_res) {
      // read the DataBlock
      auto block_len = ((DataPointer *)&v)->data_len;
      auto block_addr = ((DataPointer *)&v)->ptr;
      auto block_buffer = (dsm->get_rbuf(sink)).get_block_buffer();
      dsm->read_sync(block_buffer, (GlobalAddress)block_addr, block_len, sink);
      v = ((DataBlock*)block_buffer)->value;
    }
#endif
    goto search_finish;
  }
  // traverse internal nodes
  if (!internal_node_search(p, sibling_p, k, level, from_cache, sink)) {  // return false if cache validation fail
    // cache invalidation
    assert(from_cache);
    tree_cache->invalidate(cache_entry);
#ifdef CACHE_MORE_INTERNAL_NODE
    cache_entry = tree_cache->search_from_cache(k, p, sibling_p, level);
    from_cache = cache_entry ? true : false;
#else
    from_cache = false;
#endif
    if (!from_cache) {
      auto e = get_root_ptr(sink);
      p = e.ptr, sibling_p = GlobalAddress::Null(), level = e.level;
    }
    retry_flag = INVALID_NODE;
    goto next;
  }
  from_cache = false;
  retry_flag = FIND_NEXT;
  goto next;  // search next level

search_finish:
#ifdef TREE_ENABLE_READ_DELEGATION
  local_lock_table->release_local_read_lock(k, lock_res, search_res, v);  // handover the ret leaf addr
#endif
  return search_res;
}


void Tree::leaf_entry_read(const GlobalAddress& leaf_addr, const int idx, char *raw_leaf_buffer, char *leaf_buffer, CoroPull* sink, bool for_write) {
  auto leaf = (LeafNode *)leaf_buffer;
#ifdef METADATA_REPLICATION
  auto [raw_offset, raw_len, first_offset] = LeafVersionManager::get_offset_info(idx);
  auto raw_entry_buffer = raw_leaf_buffer + raw_offset;
re_read:
  dsm->read_sync(raw_entry_buffer, leaf_addr + raw_offset, raw_len, sink);
  uint8_t entry_node_version = 0;
  auto intermediate_entry_buffer = (dsm->get_rbuf(sink)).get_segment_buffer();
  auto [first_metadata_offset, new_len] = MetadataManager::get_offset_info(idx);
  if (for_write) assert((LeafVersionManager::decode_segment_versions(raw_entry_buffer, intermediate_entry_buffer, first_offset, 1, first_metadata_offset, new_len, entry_node_version)));
  else if (!LeafVersionManager::decode_segment_versions(raw_entry_buffer, intermediate_entry_buffer, first_offset, 1, first_metadata_offset, new_len, entry_node_version)) {
    goto re_read;
  }
  MetadataManager::decode_segment_metadata(intermediate_entry_buffer, (char*)&(leaf->records[idx]), first_metadata_offset, 1, leaf->metadata);
  return;
#else
  auto [raw_offset, raw_len, first_offset] = VersionManager<LeafNode, LeafEntry>::get_offset_info(idx);
  auto raw_entry_buffer = raw_leaf_buffer + raw_offset;
re_read:
  // read metadata and the hop segment
  std::vector<RdmaOpRegion> rs(2);
  rs[0].source = (uint64_t)raw_leaf_buffer;
  rs[0].dest = leaf_addr.to_uint64();
  rs[0].size = define::bufferMetadataSize;  // header
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)raw_entry_buffer;
  rs[1].dest = (leaf_addr + raw_offset).to_uint64();
  rs[1].size = raw_len;
  rs[1].is_on_chip = false;
  // note that the rs array will change by lower-level function
  dsm->read_batch_sync(&rs[0], 2, sink);
  uint8_t metadata_node_version = 0, entry_node_version = 0;
  if (for_write) {  // for locked node, consistency check is not needed
    assert((VersionManager<LeafNode, LeafEntry>::decode_header_versions(raw_leaf_buffer, leaf_buffer, metadata_node_version)));
    assert((VersionManager<LeafNode, LeafEntry>::decode_segment_versions(raw_entry_buffer, (char*)&(leaf->records[idx]), first_offset, 1, entry_node_version)));
    assert(metadata_node_version == entry_node_version);
    return;
  }
  // consistency check; note that: (segment_size_l > 0) => there are two segments
  if (!VersionManager<LeafNode, LeafEntry>::decode_header_versions(raw_leaf_buffer, leaf_buffer, metadata_node_version) ||
      !VersionManager<LeafNode, LeafEntry>::decode_segment_versions(raw_entry_buffer, (char*)&(leaf->records[idx]), first_offset, 1, entry_node_version) ||
      metadata_node_version != entry_node_version) {
    goto re_read;
  }
#endif
  return;
}


#ifdef SPECULATIVE_READ
bool Tree::speculative_read(const GlobalAddress& leaf_addr, std::pair<int, int> range, char *raw_leaf_buffer, char *leaf_buffer, const Key &k, Value &v,
                            int& speculative_idx, CoroPull* sink, bool for_write) {
  auto leaf = (LeafNode *)leaf_buffer;
  if (idx_cache->search_idx_from_cache(leaf_addr, range.first, range.second, k, speculative_idx)) {
    // read entry
    try_speculative_read[dsm->getMyThreadID()] ++;
    leaf_entry_read(leaf_addr, speculative_idx, raw_leaf_buffer, leaf_buffer, sink, for_write);
    const auto& entry = leaf->records[speculative_idx];
    if (entry.key == k) {
      correct_speculative_read[dsm->getMyThreadID()] ++;
      v = entry.value;
      idx_cache->add_to_cache(leaf_addr, speculative_idx, k);
      return true;
    }
  }
  return false;
}
#endif


bool Tree::leaf_node_search(const GlobalAddress& node_addr, const GlobalAddress& sibling_addr, const Key &k, Value &v, bool from_cache, CoroPull* sink) {
  try_read_leaf[dsm->getMyThreadID()] ++;
  auto raw_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  auto leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  auto leaf = (LeafNode *)leaf_buffer;
#ifdef HOPSCOTCH_LEAF_NODE
  int hash_idx = get_hashed_leaf_entry_index(k);
#ifdef SPECULATIVE_READ
  int speculative_idx;
  if (speculative_read(node_addr, std::make_pair(hash_idx, (hash_idx + define::neighborSize) % define::leafSpanSize), raw_leaf_buffer, leaf_buffer, k, v, speculative_idx, sink)) {
    UNUSED(speculative_idx);
    return true;
  }
#endif
re_read:
  hopscotch_search(node_addr, hash_idx, raw_leaf_buffer, leaf_buffer, sink);
#else
#ifdef SPECULATIVE_READ
  int speculative_idx;
  if (speculative_read(node_addr, std::make_pair(0, define::leafSpanSize), raw_leaf_buffer, leaf_buffer, k, v, speculative_idx, sink)) {
    UNUSED(speculative_idx);
    return true;
  }
#endif
re_read:
  dsm->read_sync(raw_leaf_buffer, node_addr, define::transLeafSize, sink);
  if (!VersionManager<LeafNode, LeafEntry>::decode_node_versions(raw_leaf_buffer, leaf_buffer)) {
    read_leaf_retry[dsm->getMyThreadID()] ++;
    goto re_read;
  }
#endif

#ifdef SIBLING_BASED_VALIDATION
  const auto& sibling_ptr = (leaf->metadata.sibling_ptr == GlobalAddress::Widest() ? GlobalAddress::Null() : (GlobalAddress)leaf->metadata.sibling_ptr);
  // cache validation
  if (from_cache && (!leaf->metadata.valid || sibling_addr != sibling_ptr)) {  // cache is outdated
    leaf_cache_invalid[dsm->getMyThreadID()] ++;
    return false;
  }
#else
  UNUSED(sibling_addr);
  // cache validation
  const auto& fence_keys = leaf->metadata.fence_keys;
  if (from_cache && (!leaf->metadata.valid || k < fence_keys.lowest || k >= fence_keys.highest)) {  // cache is outdated
    leaf_cache_invalid[dsm->getMyThreadID()] ++;
    return false;
  }
  // turn right check
  if (k >= fence_keys.highest) {  // should turn right
    assert(leaf->metadata.sibling_ptr != GlobalAddress::Null());
    leaf_read_sibling[dsm->getMyThreadID()] ++;
    leaf_node_search(leaf->metadata.sibling_ptr, GlobalAddress::Null(), k, v, false, sink);
    return true;
  }
  assert(k >= fence_keys.lowest);
#endif

  // search for the key
  auto& records = leaf->records;
#ifdef HOPSCOTCH_LEAF_NODE
  // check hopping consistency && search key from the segments
  uint16_t hop_bitmap = 0;
  for (int i = 0; i < (int)define::neighborSize; ++ i) {
    const auto& e = records[(hash_idx + i) % define::leafSpanSize];
    if (e.key != define::kkeyNull && (int)get_hashed_leaf_entry_index(e.key) == hash_idx) {
      hop_bitmap |= 1ULL << (define::neighborSize - i - 1);
      if (e.key == k) {  // optimization: if the target key is found, consistency check can be stopped
        v = e.value;
#ifdef SPECULATIVE_READ
        idx_cache->add_to_cache(node_addr, (hash_idx + i) % define::leafSpanSize, k);
#endif
        return true;
      }
    }
  }
  if (hop_bitmap != records[hash_idx].hop_bitmap) {
    read_leaf_retry[dsm->getMyThreadID()] ++;
    goto re_read;
  }
#else
  for (int i = 0; i < (int)define::leafSpanSize; ++ i) {
    const auto& e = records[i];
    if (e.key == k) {
      v = e.value;
#ifdef SPECULATIVE_READ
      idx_cache->add_to_cache(node_addr, i, k);
#endif
      return true;
    }
  }
#endif
#ifdef SIBLING_BASED_VALIDATION
  // turn right check
  if (v == define::kValueNull && sibling_addr != sibling_ptr) {  // search sibling node
    assert(sibling_ptr != GlobalAddress::Null());
    leaf_read_sibling[dsm->getMyThreadID()] ++;
    leaf_node_search(sibling_ptr, sibling_addr, k, v, false, sink);
    return true;
  }
#endif
  // key is not found
  return true;
}


/*
  range query
  DO NOT support coroutines currently
  SHOULD be called with other tree optimizations (e.g., HOPSCOTCH_LEAF_NODE, METADATA_REPLICATION) turned on
*/
bool Tree::range_query(const Key &from, const Key &to, std::map<Key, Value> &ret) {  // [from, to)
  assert(dsm->is_register());
  before_operation(nullptr);

  thread_local std::vector<InternalNode> cache_search_result;
  thread_local std::set<GlobalAddress> leaf_addrs;
  thread_local std::map<GlobalAddress, FenceKeys> leaf_fences;
  thread_local std::vector<RdmaOpRegion> rs;
#ifdef FINE_GRAINED_RANGE_QUERY
  using InfoMap = std::map<uint64_t, std::vector<std::tuple<int, int, GlobalAddress, uint64_t, uint64_t, uint64_t> > >;
  thread_local InfoMap leaf_info;  // [leaf_id, (hash_idx, seg_size, leaf_addr, raw_offset, raw_len, first_offset) * N]
#ifdef SPECULATIVE_READ
  thread_local std::vector<Key> speculative_keys;
  speculative_keys.clear();
#endif
#else
  using InfoMap = std::map<uint64_t, GlobalAddress>;
  thread_local InfoMap leaf_info;  // [leaf_id, leaf_addr]
#endif
  cache_search_result.clear();
  leaf_addrs.clear();
  leaf_fences.clear();
  rs.clear();
  leaf_info.clear();
  tree_cache->search_range_from_cache(from, to, cache_search_result);

  // FIXME: for simplicity, we assume all innernal nodes are cached in compute node like Sherman
  if (cache_search_result.empty()) {
    for(auto k = from; k < to; k = k + 1) {
      cache_miss[dsm->getMyThreadID()] ++;
      search(k, ret[k]);  // load into cache
    }
    return false;
  }
  // parse cached internal nodes
  for (const auto& node : cache_search_result) {
    const auto& metadata = node.metadata;
    assert(metadata.level == 1);
    const auto& records = node.records;
    bool no_fetch = from >= records[0].key || to <= metadata.fence_keys.lowest;
    if (!no_fetch) {
      leaf_addrs.insert(metadata.leftmost_ptr);
      leaf_fences[metadata.leftmost_ptr] = FenceKeys(metadata.fence_keys.lowest, records[0].key);
    }
    int i;
    for (i = 1; i <= (int)define::internalSpanSize; ++ i) {
      if (i == (int)define::internalSpanSize || records[i].key == define::kkeyNull) {
        no_fetch = from >= metadata.fence_keys.highest || to <= records[i - 1].key;
        if (!no_fetch) {
          leaf_addrs.insert(records[i - 1].ptr);
          leaf_fences[records[i - 1].ptr] = FenceKeys(records[i - 1].key, metadata.fence_keys.highest);
        }
        break;
      }
      no_fetch = from >= records[i].key || to <= records[i - 1].key;
      if (!no_fetch) {
        leaf_addrs.insert(records[i - 1].ptr);
        leaf_fences[records[i - 1].ptr] = FenceKeys(records[i - 1].key, records[i].key);
      }
    }
  }

  auto merge_internals = [](std::vector<std::pair<int, int> >& intervals, std::vector<std::pair<int, int> >& res){  // intervals: [l, r)
    std::sort(intervals.begin(), intervals.end(), [](const std::pair<int, int>& a, const std::pair<int, int>& b){
      return a.first < b.first;
    });
    int n = intervals.size();
    int i = 0;
    std::vector<std::pair<int, int> > temp;
    while (i < n) {
      int j = i + 1;
      int end = intervals[i].second;
      while (j < n && intervals[j].first <= end) end = std::max(end, intervals[j ++].second);
      temp.emplace_back(std::make_pair(intervals[i].first, end));
      i = j;
    }
#ifdef GREEDY_RANGE_QUERY
    n = temp.size();
    i = 0;
    while (i < n) {
      int j = i + 1;
      int end = temp[i].second;
      while (j < n && std::max(end, temp[j].second) <= (temp[i].first + (int)define::maxLeafEntryPerIO)) end = std::max(end, temp[j ++].second);
      res.emplace_back(std::make_pair(temp[i].first, end));
      i = j;
    }
#else
    res = temp;
#endif
  };

  int leaf_cnt = 0;
  auto range_buffer = (dsm->get_rbuf(nullptr)).get_range_buffer();
#ifdef FINE_GRAINED_RANGE_QUERY
  // generate read segments
  for (const auto& leaf_addr : leaf_addrs) {
    Key l_k = std::max(leaf_fences[leaf_addr].lowest, from);
    Key r_k = std::min(leaf_fences[leaf_addr].highest, to);
    std::vector<std::pair<int, int> > segments;
    if (l_k == leaf_fences[leaf_addr].lowest && r_k == leaf_fences[leaf_addr].highest) {
      segments.emplace_back(std::make_pair(0, (int)define::leafSpanSize));
    }
    else for (auto k = l_k; k < r_k; k = k + 1) {
      int hash_idx = get_hashed_leaf_entry_index(k);
#ifdef SPECULATIVE_READ
      try_read_leaf[dsm->getMyThreadID()] ++;
      int speculative_idx;
      if (idx_cache->search_idx_from_cache(leaf_addr, hash_idx, (hash_idx + define::neighborSize) % define::leafSpanSize, k, speculative_idx)) {
          try_speculative_read[dsm->getMyThreadID()] ++;
          speculative_keys.emplace_back(k);
          segments.emplace_back(std::make_pair(speculative_idx, speculative_idx + 1));
          continue;
      }
#endif
      if (hash_idx + (int)define::neighborSize <= (int)define::leafSpanSize) segments.emplace_back(std::make_pair(hash_idx, hash_idx + (int)define::neighborSize));
      else {
        segments.emplace_back(std::make_pair(hash_idx, (int)define::leafSpanSize));
        segments.emplace_back(std::make_pair(0,  (int)define::neighborSize - ((int)define::leafSpanSize - hash_idx)));
      }
    }
    // merge the intervals
    std::vector<std::pair<int, int> > merged_segments;
    merge_internals(segments, merged_segments);
    for (const auto& [l_idx, r_idx] : merged_segments) {
      // get info
      auto [raw_offset, raw_len, first_offset] = LeafVersionManager::get_offset_info(l_idx, r_idx - l_idx);
      RdmaOpRegion r;
      r.source     = (uint64_t)range_buffer + leaf_cnt * define::allocationLeafSize + raw_offset;
      r.dest       = (leaf_addr + raw_offset).to_uint64();
      r.size       = raw_len;
      r.is_on_chip = false;
      rs.push_back(r);
      leaf_info[leaf_cnt].emplace_back(std::make_tuple(l_idx, r_idx - l_idx, leaf_addr, raw_offset, raw_len, first_offset));
    }
    ++ leaf_cnt;
    assert((dsm->get_rbuf(nullptr)).is_safe(range_buffer + leaf_cnt * define::allocationLeafSize));
  }
  int next_leaf_cnt;
  InfoMap next_info;
  // batch read
re_read:
  dsm->read_batches_sync(rs);
  rs.clear();
  next_info.clear();
  next_leaf_cnt = 0;
  // parse read leaf segments
  for (int i = 0; i < leaf_cnt; ++ i) {
    auto leaf_buffer = (dsm->get_rbuf(nullptr)).get_leaf_buffer();
    auto leaf = (LeafNode *)leaf_buffer;
    for (const auto& [start_idx, segment_size, leaf_addr, raw_offset, raw_len, first_offset] : leaf_info[i]) {
      auto raw_buffer = range_buffer + i * define::allocationLeafSize + raw_offset;
      uint8_t segment_node_versions = 0;
      auto intermediate_buffer = (dsm->get_rbuf(nullptr)).get_segment_buffer();
      auto [first_metadata_offset, new_len] = MetadataManager::get_offset_info(start_idx, segment_size);
      // check versions consistency
      if (!(LeafVersionManager::decode_segment_versions(raw_buffer, intermediate_buffer, first_offset, segment_size, first_metadata_offset, new_len, segment_node_versions))) {
        RdmaOpRegion r;
        r.source     = (uint64_t)range_buffer + next_leaf_cnt * define::allocationLeafSize + raw_offset;
        r.dest       = (leaf_addr + raw_offset).to_uint64();
        r.size       = raw_len;
        r.is_on_chip = false;
        rs.push_back(r);
        next_info[next_leaf_cnt].emplace_back(std::make_tuple(start_idx, segment_size, leaf_addr, raw_offset, raw_len, first_offset));
        continue;
      }
      MetadataManager::decode_segment_metadata(intermediate_buffer, (char*)&(leaf->records[start_idx]), first_metadata_offset, segment_size, leaf->metadata);
      // check hopping consistency
      std::vector<int> hash_idxes;
      for (int j = 0; j < segment_size; ++ j) {
        const auto& e = leaf->records[start_idx + j];
        if (e.key == define::kkeyNull) hash_idxes.emplace_back(-1);
        else hash_idxes.emplace_back(get_hashed_leaf_entry_index(e.key));
      }
      bool is_ok = true;
      for (int j = 0; j < segment_size && is_ok; ++ j) {
        const auto& hop_bitmap = leaf->records[start_idx + j].hop_bitmap;
        for (int k = 0; k < std::min((int)define::neighborSize, segment_size - j); ++ k) {
          if ((hop_bitmap & (1ULL << (define::neighborSize - k - 1))) && hash_idxes[j + k] != start_idx + j) {
            is_ok = false;
            break;
          }
        }
      }
      if (!is_ok) {
        RdmaOpRegion r;
        r.source     = (uint64_t)range_buffer + next_leaf_cnt * define::allocationLeafSize + raw_offset;
        r.dest       = (leaf_addr + raw_offset).to_uint64();
        r.size       = raw_len;
        r.is_on_chip = false;
        rs.push_back(r);
        next_info[next_leaf_cnt].emplace_back(std::make_tuple(start_idx, segment_size, leaf_addr, raw_offset, raw_len, first_offset));
        continue;
      }
      // search key from the segments
      for (int j = start_idx; j < start_idx + segment_size; ++ j) {
        const auto& e = leaf->records[j];
        if (e.key != define::kkeyNull && e.key >= from && e.key < to) {
          ret[e.key] = e.value;
#ifdef SPECULATIVE_READ
          idx_cache->add_to_cache(leaf_addr, j, e.key);
#endif
        }
      }
    }
    if (next_info.find(next_leaf_cnt) != next_info.end()) next_leaf_cnt ++;
  }
#else
  UNUSED(merge_internals);
  // batch read
  for (const auto& leaf_addr : leaf_addrs) {
    RdmaOpRegion r;
    r.source     = (uint64_t)range_buffer + leaf_cnt * define::allocationLeafSize;
    r.dest       = leaf_addr.to_uint64();
    r.size       = define::allocationLeafSize;
    r.is_on_chip = false;
    rs.push_back(r);
    leaf_info[leaf_cnt ++] = leaf_addr;
  }
  assert((dsm->get_rbuf(nullptr)).is_safe(range_buffer + leaf_cnt * define::allocationLeafSize));
  int next_leaf_cnt;
  InfoMap next_info;
  // batch read
re_read:
  dsm->read_batches_sync(rs);
  rs.clear();
  next_info.clear();
  next_leaf_cnt = 0;
  // parse read leaf nodes
  for (int i = 0; i < leaf_cnt; ++ i) {
    auto raw_leaf_buffer = range_buffer + i * define::allocationLeafSize;
    auto leaf_buffer = (dsm->get_rbuf(nullptr)).get_leaf_buffer();
    auto leaf = (LeafNode *)leaf_buffer;
#ifdef METADATA_REPLICATION
    // check versions consistency
    auto intermediate_leaf_buffer = (dsm->get_rbuf(nullptr)).get_leaf_buffer();
    if (!LeafVersionManager::decode_node_versions(raw_leaf_buffer, intermediate_leaf_buffer)) {
      RdmaOpRegion r;
      r.source     = (uint64_t)range_buffer + next_leaf_cnt * define::allocationLeafSize;
      r.dest       = leaf_info[i].to_uint64();
      r.size       = define::allocationLeafSize;
      r.is_on_chip = false;
      rs.push_back(r);
      next_info[next_leaf_cnt ++] = leaf_info[i];
      continue;
    }
    MetadataManager::decode_node_metadata(intermediate_leaf_buffer, leaf_buffer);
#else
    // check versions consistency
    if (!VersionManager<LeafNode, LeafEntry>::decode_node_versions(raw_leaf_buffer, leaf_buffer)) {
      RdmaOpRegion r;
      r.source     = (uint64_t)range_buffer + next_leaf_cnt * define::allocationLeafSize;
      r.dest       = leaf_info[i].to_uint64();
      r.size       = define::allocationLeafSize;
      r.is_on_chip = false;
      rs.push_back(r);
      next_info[next_leaf_cnt ++] = leaf_info[i];
      continue;
    }
#endif
#ifdef HOPSCOTCH_LEAF_NODE
    // check hopping consistency
    bool is_ok = true;
    auto& records = leaf->records;
    std::vector<int> hash_idxes;
    for (const auto& e : records) {
      if (e.key == define::kkeyNull) hash_idxes.emplace_back(-1);
      else hash_idxes.emplace_back(get_hashed_leaf_entry_index(e.key));
    }
    for (int i = 0; i < (int)define::leafSpanSize; ++ i) {
      uint16_t hop_bitmap = 0;
      for (int j = 0; j < (int)define::neighborSize; ++ j) {
        if (hash_idxes[(i + j) % define::leafSpanSize] == i) {
          hop_bitmap |= 1ULL << (define::neighborSize - j - 1);
        }
      }
      if (hop_bitmap != records[i].hop_bitmap) {
        is_ok = false;
        break;
      }
    }
    if (!is_ok) {
      RdmaOpRegion r;
      r.source     = (uint64_t)range_buffer + next_leaf_cnt * define::allocationLeafSize;
      r.dest       = leaf_info[i].to_uint64();
      r.size       = define::allocationLeafSize;
      r.is_on_chip = false;
      rs.push_back(r);
      next_info[next_leaf_cnt ++] = leaf_info[i];
      continue;
    }
#endif
    // search key from the leaves
    for (const auto& e : leaf->records) {
      if (e.key != define::kkeyNull && e.key >= from && e.key < to) {
        ret[e.key] = e.value;
      }
    }
  }
#endif
#if (defined FINE_GRAINED_RANGE_QUERY && defined SPECULATIVE_READ)
  std::map<GlobalAddress, std::vector<Key> > retry_leaf_keys;
  for (const auto& k : speculative_keys) {
    if (ret.find(k) != ret.end()) correct_speculative_read[dsm->getMyThreadID()] ++;
    else {
      GlobalAddress p;
      GlobalAddress sibling_p;
      uint16_t level;
      auto cache_entry = tree_cache->search_from_cache(k, p, sibling_p, level);
      if (!cache_entry || level != 1) {
        search(k, ret[k]);  // load into cache
        continue;
      }
      retry_leaf_keys[p].emplace_back(k);
    }
  }
  speculative_keys.clear();
  for (const auto& [leaf_addr, keys] : retry_leaf_keys) {
    std::vector<std::pair<int, int> > segments;
    for (const auto& k : keys) {
      int hash_idx = get_hashed_leaf_entry_index(k);
      if (hash_idx + (int)define::neighborSize <= (int)define::leafSpanSize) segments.emplace_back(std::make_pair(hash_idx, hash_idx + (int)define::neighborSize));
      else {
        segments.emplace_back(std::make_pair(hash_idx, (int)define::leafSpanSize));
        segments.emplace_back(std::make_pair(0,  (int)define::neighborSize - ((int)define::leafSpanSize - hash_idx)));
      }
    }
    // merge the intervals
    std::vector<std::pair<int, int> > merged_segments;
    merge_internals(segments, merged_segments);
    for (const auto& [l_idx, r_idx] : merged_segments) {
      // get info
      auto [raw_offset, raw_len, first_offset] = LeafVersionManager::get_offset_info(l_idx, r_idx - l_idx);
      RdmaOpRegion r;
      r.source     = (uint64_t)range_buffer + next_leaf_cnt * define::allocationLeafSize + raw_offset;
      r.dest       = (leaf_addr + raw_offset).to_uint64();
      r.size       = raw_len;
      r.is_on_chip = false;
      rs.push_back(r);
      next_info[next_leaf_cnt].emplace_back(std::make_tuple(l_idx, r_idx - l_idx, leaf_addr, raw_offset, raw_len, first_offset));
    }
    ++ next_leaf_cnt;
  }
#endif
  if (!rs.empty()) {
    leaf_info = next_info;
    leaf_cnt = next_leaf_cnt;
    goto re_read;
  }
#ifdef ENABLE_VAR_LEN_KV
  // read DataBlocks via doorbell batching
  std::map<Key, Value> indirect_values;
  std::vector<RdmaOpRegion> kv_rs;
  int kv_cnt = 0;
  for (const auto& [_, data_ptr] : ret) {
    auto data_addr = ((DataPointer*)&data_ptr)->ptr;
    auto data_len  = ((DataPointer*)&data_ptr)->data_len;
    RdmaOpRegion r;
    r.source     = (uint64_t)range_buffer + kv_cnt * define::dataBlockLen;
    r.dest       = ((GlobalAddress)data_addr).to_uint64();
    r.size       = data_len;
    r.is_on_chip = false;
    kv_rs.push_back(r);
    kv_cnt ++;
  }
  assert((dsm->get_rbuf(nullptr)).is_safe(range_buffer + kv_cnt * define::dataBlockLen));
  dsm->read_batches_sync(kv_rs);
  kv_cnt = 0;
  for (auto& [_, v] : ret) {
    auto data_block = (DataBlock*)(range_buffer + kv_cnt * define::dataBlockLen);
    v = data_block->value;
    kv_cnt ++;
  }
#endif
  // FIXME: for simplicity, load uncached innernal nodes according to ret, leveraging the ordered YCSB E
  for(auto k = from; k < to; k = k + 1) {
    if (ret.find(k) != ret.end()) {
      cache_hit[dsm->getMyThreadID()]++;
    }
    else {
      cache_miss[dsm->getMyThreadID()]++;
      search(k, ret[k]);
    }
  }
  return true;
}


void Tree::run_coroutine(GenFunc gen_func, WorkFunc work_func, int coro_cnt, Request* req, int req_num) {
  assert(coro_cnt <= MAX_CORO_NUM);
  // define coroutines
  for (int i = 0; i < coro_cnt; ++i) {
    RequstGen *gen = gen_func(dsm, req, req_num, i, coro_cnt);
    workers.emplace_back([=](CoroPull& sink) {
      coro_worker(sink, gen, work_func);
    });
  }
  // start running coroutines
  for (int i = 0; i < coro_cnt; ++i) {
    workers[i](i);
  }
  while (!need_stop) {
    uint64_t next_coro_id;

    if (dsm->poll_rdma_cq_once(next_coro_id)) {
      workers[next_coro_id](next_coro_id);
    }
    if (!busy_waiting_queue.empty()) {
      auto next_coro_id = busy_waiting_queue.front();
      busy_waiting_queue.pop();
      workers[next_coro_id](next_coro_id);
    }
  }
}


void Tree::coro_worker(CoroPull &sink, RequstGen *gen, WorkFunc work_func) {
  Timer coro_timer;
  auto thread_id = dsm->getMyThreadID();

  while (!need_stop) {
    auto r = gen->next();

    coro_timer.begin();
    work_func(this, r, &sink);
    auto us_10 = coro_timer.end() / 100;

    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[thread_id][sink.get()][us_10]++;

    busy_waiting_queue.push(sink.get());
    sink();
  }
}

void Tree::statistics() {
#ifdef TREE_ENABLE_CACHE
  tree_cache->statistics();
#endif
#ifdef SPECULATIVE_READ
  idx_cache->statistics();
#endif
}

void Tree::clear_debug_info() {
  memset(cache_miss, 0, sizeof(double) * MAX_APP_THREAD);
  memset(cache_hit, 0, sizeof(double) * MAX_APP_THREAD);
  memset(lock_fail, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(write_handover_num, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_write_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_handover_num, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_leaf_retry, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(leaf_cache_invalid, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_speculative_read, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(correct_speculative_read, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_leaf, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_two_segments, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_hopscotch, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(leaf_read_sibling, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(retry_cnt, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_FLAG_NUM);
  memset(try_insert_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(split_node, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_write_segment, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(write_two_segments, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(load_factor_sum, 0, sizeof(double) * MAX_APP_THREAD);
  memset(split_hopscotch, 0, sizeof(uint64_t) * MAX_APP_THREAD);
}
