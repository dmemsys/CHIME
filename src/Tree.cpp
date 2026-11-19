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

namespace {
struct alignas(64) ThreadLocalTreeStats {
  double cache_miss = 0;
  double cache_hit = 0;
  uint64_t lock_fail = 0;
  uint64_t write_handover_num = 0;
  uint64_t try_write_op = 0;
  uint64_t read_handover_num = 0;
  uint64_t try_read_op = 0;
  uint64_t read_leaf_retry = 0;
  uint64_t leaf_cache_invalid = 0;
  uint64_t leaf_read_sibling = 0;
  uint64_t correct_speculative_read = 0;
  uint64_t try_speculative_read = 0;
  uint64_t try_read_leaf = 0;
  uint64_t read_two_segments = 0;
  uint64_t try_read_hopscotch = 0;
  uint64_t retry_cnt[MAX_FLAG_NUM] = {0};
  uint64_t try_insert_op = 0;
  uint64_t split_node = 0;
  uint64_t try_write_segment = 0;
  uint64_t write_two_segments = 0;
  double load_factor_sum = 0;
  uint64_t split_hopscotch = 0;
  uint32_t sync_tick = 0;

  void flush_periodic(int tid) const {
    ::cache_miss[tid] = cache_miss;
    ::cache_hit[tid] = cache_hit;
    ::lock_fail[tid] = lock_fail;
    ::write_handover_num[tid] = write_handover_num;
    ::try_write_op[tid] = try_write_op;
    ::read_handover_num[tid] = read_handover_num;
    ::try_read_op[tid] = try_read_op;
    ::read_leaf_retry[tid] = read_leaf_retry;
    ::leaf_cache_invalid[tid] = leaf_cache_invalid;
    ::leaf_read_sibling[tid] = leaf_read_sibling;
    ::correct_speculative_read[tid] = correct_speculative_read;
    ::try_speculative_read[tid] = try_speculative_read;
    ::try_read_leaf[tid] = try_read_leaf;
    ::read_two_segments[tid] = read_two_segments;
    ::try_read_hopscotch[tid] = try_read_hopscotch;
    for (int i = 0; i < MAX_FLAG_NUM; ++i) {
      ::retry_cnt[tid][i] = retry_cnt[i];
    }
    ::try_insert_op[tid] = try_insert_op;
    ::split_node[tid] = split_node;
    ::try_write_segment[tid] = try_write_segment;
    ::write_two_segments[tid] = write_two_segments;
    ::load_factor_sum[tid] = load_factor_sum;
    ::split_hopscotch[tid] = split_hopscotch;
  }

  void clear_epoch(int tid) {
    cache_miss = 0;
    cache_hit = 0;
    lock_fail = 0;
    write_handover_num = 0;
    try_write_op = 0;
    read_handover_num = 0;
    try_read_op = 0;
    read_leaf_retry = 0;
    leaf_cache_invalid = 0;
    leaf_read_sibling = 0;
    correct_speculative_read = 0;
    try_speculative_read = 0;
    try_read_leaf = 0;
    read_two_segments = 0;
    try_read_hopscotch = 0;
    for (int i = 0; i < MAX_FLAG_NUM; ++i) {
      retry_cnt[i] = 0;
    }
    try_insert_op = 0;
    split_node = 0;
    try_write_segment = 0;
    write_two_segments = 0;
    sync_tick = 0;
    flush_periodic(tid);
  }
};

thread_local ThreadLocalTreeStats tl_stats;
} // namespace

thread_local std::vector<CoroPush> Tree::workers;
thread_local CoroQueue Tree::busy_waiting_queue;
thread_local GlobalAddress path_stack[MAX_CORO_NUM][MAX_TREE_HEIGHT];
thread_local uint64_t tl_coro_resume_seq[MAX_CORO_NUM];


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
    if_lock->update_vacancy(max_key_idx, max_key_idx, nullptr, 0);
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
  UNUSED(sink);
  auto tid = dsm->getMyThreadID();
  if (__builtin_expect(need_clear[tid], 0)) {
    tl_stats.clear_epoch(tid);
    need_clear[tid] = false;
  } else if (__builtin_expect((++tl_stats.sync_tick & 255u) == 0, 0)) {
    tl_stats.flush_periodic(tid);
  }
}


inline void Tree::record_cache_hit_ratio(bool from_cache, int level) {
  if (!from_cache) {
    tl_stats.cache_miss += 1;
    return;
  }
  int h = rough_height.load(std::memory_order_relaxed);
  auto hit = (h ? 1 - ((double)level - 1) / h : 0);
  tl_stats.cache_hit += hit;
  tl_stats.cache_miss += (1 - hit);
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
    tl_stats.lock_fail ++;
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

  tl_stats.try_write_op ++;
  tl_stats.try_insert_op ++;

#ifdef TREE_ENABLE_WRITE_COMBINING
  lock_res = local_lock_table->acquire_local_write_lock(k, v, &busy_waiting_queue, sink);
  write_handover = (lock_res.first && !lock_res.second);
#else
  UNUSED(lock_res);
#endif
  if (write_handover) {
    tl_stats.write_handover_num++;
    goto insert_finish;
  }
  {
    auto *ps = path_stack[sink ? sink->get() : 0];
    for (int i = 0; i < MAX_TREE_HEIGHT; ++i) {
      ps[i] = GlobalAddress::Null();
    }
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
  tl_stats.retry_cnt[retry_flag] ++;
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
  const uint64_t k_int = key2int(k);
  const uint64_t k0 = key2int(records[0].key);
  if (__builtin_expect(k0 == 0ULL, 0) || k_int < k0) {
    node_addr = node->metadata.leftmost_ptr;
    sibling_addr = (__builtin_expect(k0 == 0ULL, 0) ? node->metadata.sibling_leftmost_ptr : node->records[0].ptr);
    return true;
  }
  int lo = 1;
  int hi = (int)define::internalSpanSize;
  while (lo < hi) {
    int mid = (lo + hi) >> 1;
    uint64_t mk = key2int(records[mid].key);
    if (mk == 0ULL || k_int < mk) {
      hi = mid;
    } else {
      lo = mid + 1;
    }
  }
  if (lo < (int)define::internalSpanSize) {
    node_addr = records[lo - 1].ptr;
    sibling_addr = (key2int(records[lo].key) == 0ULL ? node->metadata.sibling_leftmost_ptr : records[lo].ptr);
    return true;
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
  auto leaf = (LeafNode *) leaf_buffer;
  bool hopping_read = (read_entry_num < (int)define::leafSpanSize);
  if (hopping_read) {
#if (defined HOPSCOTCH_LEAF_NODE && defined VACANCY_AWARE_LOCK)
    const int zero_cnt = std::min((int)define::neighborSize - 1, (int)define::leafSpanSize - read_entry_num);
    const int z_start = (l_idx - zero_cnt + (int)define::leafSpanSize) % (int)define::leafSpanSize;
    if (z_start + zero_cnt <= (int)define::leafSpanSize) {
      memset((void *)&leaf->records[z_start], 0, (size_t)zero_cnt * sizeof(LeafEntry));
    } else {
      const int first_part = (int)define::leafSpanSize - z_start;
      memset((void *)&leaf->records[z_start], 0, (size_t)first_part * sizeof(LeafEntry));
      memset((void *)&leaf->records[0], 0, (size_t)(zero_cnt - first_part) * sizeof(LeafEntry));
    }
#else
    memset(leaf_buffer, 0, sizeof(LeafNode));
#endif
  }
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
#ifdef HOPSCOTCH_LEAF_NODE
  {
    int hash_idx = get_hashed_leaf_entry_index(k);
    int check_cnt = std::min(read_entry_num, (int)define::neighborSize);
    int found_idx = -1;
    for (int step = 0; step < check_cnt; ++ step) {
      int cand = (hash_idx + step) % define::leafSpanSize;
      if (records[cand].key == k) {
        found_idx = cand;
        break;
      }
    }
    if (found_idx >= 0) {
      entry_write_and_unlock<LeafNode, LeafEntry, Value>(leaf, found_idx, k, v, node_addr, lock_buffer, sink);
      return true;
    }
  }
#else
  for (i = 0; i < (int)define::leafSpanSize; ++ i) if (records[i].key == k) break;
  if (i != (int)define::leafSpanSize) {
    entry_write_and_unlock<LeafNode, LeafEntry, Value>(leaf, i, k, v, node_addr, lock_buffer, sink);
    return true;
  }
#endif

  // search for empty entry (insert)
#ifdef HOPSCOTCH_LEAF_NODE
  // Fast path: if an empty slot already sits within k's hopscotch neighborhood, hopping cannot fail (no copy needed)
  {
    int hash_idx = get_hashed_leaf_entry_index(k);
    int direct_limit = std::min(read_entry_num, (int)define::neighborSize);
    for (int step = 0; step < direct_limit; ++ step) {
      if (records[(hash_idx + step) % define::leafSpanSize].key == define::kkeyNull) {
        hopscotch_insert_and_unlock(leaf, k, v, node_addr, lock_buffer, sink, read_entry_num);
        return true;
      }
    }
  }
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
  int hopped_idxes[define::leafSpanSize];
  int hopped_cnt = 0;
next_hop:
  hopped_idxes[hopped_cnt ++] = j % define::leafSpanSize;
  if (j < hash_idx + (int)define::neighborSize) {
    get_entry(j).update(k, v);
    get_entry(hash_idx).set_hop_bit(j - hash_idx);
    segment_write_and_unlock(leaf, hash_idx, empty_idx % define::leafSpanSize, hopped_idxes, hopped_cnt, node_addr, lock_buffer, sink);
#ifdef SPECULATIVE_READ
    for (int hi = 0; hi < hopped_cnt; ++hi) {
      const int s = hopped_idxes[hi];
      idx_cache->add_to_cache(node_addr, s, records[s].key);
    }
#endif
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
  constexpr int kSpan = (int)define::leafSpanSize;
  int home_idx[kSpan];
  for (int i = 0; i < kSpan; ++i) {
    home_idx[i] = (key_to_u64_raw(records[i].key) == 0ULL) ? -1 : (int)get_hashed_leaf_entry_index(records[i].key);
  }

  uint64_t critical_keys[kSpan + 1];
  int crit_cnt = 0;
  int hash_idx = (int)get_hashed_leaf_entry_index(k);
  for (int empty_idx = hash_idx; empty_idx < hash_idx + kSpan; ++ empty_idx) {
    const int slot = empty_idx & (kSpan - 1);
    if (home_idx[slot] < 0) break;
    // try hopping assuming that records[slot] is empty
    int j = empty_idx;
next_hop:
    if (j < hash_idx + (int)define::neighborSize) {
      critical_keys[crit_cnt++] = key2int(records[slot].key);
      continue;
    }
    for (int offset = define::neighborSize - 1; offset > 0; -- offset) {
      int h = j - offset;
      int h_hash_idx = home_idx[(h + kSpan) & (kSpan - 1)];
      // corner case
      if (h - h_hash_idx < 0) h_hash_idx -= kSpan;
      else if (h - h_hash_idx >= (int)define::neighborSize) h_hash_idx += kSpan;
      // hop h => j is ok
      if (h_hash_idx + (int)define::neighborSize > j) {
        j = h;
        goto next_hop;
      }
    }
  }
  assert(crit_cnt > 0);
  critical_keys[crit_cnt++] = key2int(k);
  std::sort(critical_keys, critical_keys + crit_cnt);
  Key split_res;
  uint64_t split_be = __builtin_bswap64(critical_keys[crit_cnt / 2]);
  __builtin_memcpy(split_res.data(), &split_be, sizeof(uint64_t));
  return split_res;
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
  tl_stats.split_node ++;
  tl_stats.split_hopscotch ++;
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
  tl_stats.load_factor_sum += (double)non_empty_entry_cnt / define::leafSpanSize;
  // newly insert kv
  if (k < split_key) hopscotch_insert_locally(records, k, v);
  else hopscotch_insert_locally(sibling_leaf->records, k, v);
#ifdef SPECULATIVE_READ
  for (int i = 0; i < (int)define::leafSpanSize; ++ i) {
    if (sibling_leaf->records[i].key != define::kkeyNull) {
      idx_cache->invalidate(node_addr, sibling_leaf->records[i].key);
      idx_cache->add_to_cache(sibling_addr, i, sibling_leaf->records[i].key);
    } else if (records[i].key != define::kkeyNull) {
      idx_cache->add_to_cache(node_addr, i, records[i].key);
    }
  }
#endif

  // change metadata
  auto get_max_key_idx = [](const LeafNode* leaf_node) {
    uint64_t max_k = 0;
    int ret = 0;
    for (int i = 0; i < (int)define::leafSpanSize; ++ i) {
      const uint64_t ek = key2int(leaf_node->records[i].key);
      if (ek > max_k) {
        max_k = ek;
        ret = i;
      }
    }
    return ret;
  };
  const int leaf_max_idx = get_max_key_idx(leaf);
  // update in-node metadata
#ifdef SIBLING_BASED_VALIDATION
  auto max_key = records[leaf_max_idx].key;
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
    int empty_idxes[define::leafSpanSize];
    int empty_cnt = 0;
    for (int i = 0; i < (int)define::leafSpanSize; ++ i) {
      const auto& e = leaf_node->records[i];
      if (key_to_u64_raw(e.key) == 0ULL) empty_idxes[empty_cnt++] = i;
    }
    lock->update_vacancy(0, define::leafSpanSize - 1, empty_idxes, empty_cnt);
  };
  // update max_key_idx
  auto if_lock = ((VALOCK *)lock_buffer);
  if_lock->update_max_key_idx(leaf_max_idx);
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
  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)encoded_leaf_buffer;
  rs[0].dest = node_addr.to_uint64();
  rs[0].size = define::transLeafSize;
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)lock_buffer;
  rs[1].dest = (node_addr + lock_offset).to_uint64();
  rs[1].size = sizeof(uint64_t);
  rs[1].is_on_chip = false;
  // no need to signal
  dsm->write_batch(rs, 2, false, sink);
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
      cache_node(root_node);
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
  tl_stats.try_read_hopscotch ++;
  auto leaf = (LeafNode *)leaf_buffer;
  auto segment_size_r = std::min(entry_num, (int)define::leafSpanSize - hash_idx);
  auto segment_size_l = entry_num <= (int)define::leafSpanSize - hash_idx ? 0 : entry_num - ((int)define::leafSpanSize - hash_idx);

#ifdef METADATA_REPLICATION
  auto [raw_offset_r, raw_len_r, first_offset_r] = LeafVersionManager::get_offset_info(hash_idx, segment_size_r);
  auto [raw_offset_l, raw_len_l, first_offset_l] = LeafVersionManager::get_offset_info(0, segment_size_l);
  assert(segment_size_l > 0 || !raw_len_l);
  if (raw_len_l) tl_stats.read_two_segments ++;

  auto raw_segment_buffer_r = raw_leaf_buffer + raw_offset_r;
  auto raw_segment_buffer_l = raw_leaf_buffer + raw_offset_l;
  if (raw_len_l) {  // read two hop segments (corner case)
re_read_2:
    RdmaOpRegion rs[2];
    rs[0].source = (uint64_t)raw_segment_buffer_l;
    rs[0].dest = (node_addr + raw_offset_l).to_uint64();
    rs[0].size = raw_len_l;
    rs[0].is_on_chip = false;

    rs[1].source = (uint64_t)raw_segment_buffer_r;
    rs[1].dest = (node_addr + raw_offset_r).to_uint64();
    rs[1].size = raw_len_r;
    rs[1].is_on_chip = false;
    dsm->read_batch_sync(rs, 2, sink);

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
      tl_stats.read_leaf_retry ++;
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
      tl_stats.read_leaf_retry ++;
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
  if (raw_len_l) tl_stats.read_two_segments ++;

  auto raw_segment_buffer_r = raw_leaf_buffer + raw_offset_r;
  auto raw_segment_buffer_l = raw_leaf_buffer + raw_offset_l;
re_read:
  // read metadata and the hop segment
  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)raw_leaf_buffer;
  rs[0].dest = node_addr.to_uint64();
  rs[0].size = raw_offset_l + raw_len_l;  // header + segment_l (corner case)
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)raw_segment_buffer_r;
  rs[1].dest = (node_addr + raw_offset_r).to_uint64();
  rs[1].size = raw_len_r;
  rs[1].is_on_chip = false;
  // note that the rs array will change by lower-level function
  dsm->read_batch_sync(rs, 2, sink);
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
    tl_stats.read_leaf_retry ++;
    goto re_read;
  }
#endif
  return;
}
#endif


void Tree::segment_write_and_unlock(LeafNode* leaf, int l_idx, int r_idx, const int* hopped_idxes, int hopped_cnt, const GlobalAddress& node_addr, uint64_t* lock_buffer, CoroPull* sink) {
  tl_stats.try_write_segment ++;
  auto& records = leaf->records;
  const auto& metadata = leaf->metadata;
  auto lock_offset = get_lock_info(true);
  assert(!(*lock_buffer & (1ULL << 63)));

#ifdef VACANCY_AWARE_LOCK
  auto if_lock = (VALOCK *)lock_buffer;
  // update max_key_idx
  auto max_key_idx = if_lock->get_max_key_idx();
  bool touched_max = false;
  for (int h_i = 0; h_i < hopped_cnt; ++ h_i) {
    if (hopped_idxes[h_i] == max_key_idx) {
      touched_max = true;
      break;
    }
  }
  if (touched_max) {
    auto new_max_key = define::kkeyNull;
    for (int h_i = 0; h_i < hopped_cnt; ++ h_i) {
      int idx = hopped_idxes[h_i];
      const auto& e = records[idx];
      if (e.key > new_max_key) new_max_key = e.key, max_key_idx = idx;
    }
    if_lock->update_max_key_idx(max_key_idx);
  }
  // update vacancy bitmap
  int empty_idxes[define::leafSpanSize];
  int empty_cnt = 0;
  auto get_empty_idxes = [=, &empty_idxes, &empty_cnt](int l, int r){
    for (int i = l; i < r; ++ i) if (leaf->records[i].key == define::kkeyNull) {
      empty_idxes[empty_cnt ++] = i;
    }
  };
  if (l_idx <= r_idx) get_empty_idxes(l_idx, r_idx + 1);
  else {
    get_empty_idxes(0, r_idx + 1);
    get_empty_idxes(l_idx, define::leafSpanSize);
  }
  if_lock->update_vacancy(l_idx, r_idx, empty_idxes, empty_cnt);
#endif

  if (l_idx <= r_idx) {  // update with one WRITE + unlock
    auto encoded_segment_buffer = (dsm->get_rbuf(sink)).get_segment_buffer();
#ifdef METADATA_REPLICATION
    // segment [l_idx, r_idx]
    auto intermediate_segment_buffer = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto [first_metadata_offset, new_len] = MetadataManager::get_offset_info(l_idx, r_idx - l_idx + 1);
    MetadataManager::encode_segment_metadata((char *)&records[l_idx], intermediate_segment_buffer, first_metadata_offset, r_idx - l_idx + 1, metadata);
    auto [raw_offset, raw_len, first_offset] = LeafVersionManager::get_offset_info(l_idx, r_idx - l_idx + 1);
    LeafVersionManager::encode_segment_versions(intermediate_segment_buffer, encoded_segment_buffer, first_offset, hopped_idxes, hopped_cnt, l_idx, r_idx, first_metadata_offset, new_len);
#else
    // segment [l_idx, r_idx]
    auto [raw_offset, raw_len, first_offset] = VersionManager<LeafNode, LeafEntry>::get_offset_info(l_idx, r_idx - l_idx + 1);
    VersionManager<LeafNode, LeafEntry>::encode_segment_versions((char *)&records[l_idx], encoded_segment_buffer, first_offset, hopped_idxes, hopped_cnt, l_idx, r_idx);
#endif
    // write segment and unlock
    if (r_idx == define::leafSpanSize - 1) {
      memcpy(encoded_segment_buffer + raw_len + lock_offset - define::transLeafSize, lock_buffer, sizeof(uint64_t));  // unlock
      dsm->write_sync(encoded_segment_buffer, node_addr + raw_offset, raw_len + define::allocationLockSize, sink);
    }
    else {
      RdmaOpRegion rs[2];
      rs[0].source = (uint64_t)encoded_segment_buffer;
      rs[0].dest = (node_addr + raw_offset).to_uint64();
      rs[0].size = raw_len;
      rs[0].is_on_chip = false;

      rs[1].source = (uint64_t)lock_buffer;
      rs[1].dest = (node_addr + lock_offset).to_uint64();
      rs[1].size = sizeof(uint64_t);
      rs[1].is_on_chip = false;
      dsm->write_batch_sync(rs, 2, sink);
    }
  }
  else {  // update with two WRITE + unlock
    tl_stats.write_two_segments ++;
    auto encoded_segment_buffer_1 = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto encoded_segment_buffer_2 = (dsm->get_rbuf(sink)).get_segment_buffer();
#ifdef METADATA_REPLICATION
    // segment [0, r_idx]
    auto intermediate_segment_buffer_1 = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto [first_metadata_offset_1, new_len_1] = MetadataManager::get_offset_info(0, r_idx + 1);
    MetadataManager::encode_segment_metadata((char *)&records[0], intermediate_segment_buffer_1, first_metadata_offset_1, r_idx + 1, metadata);
    auto [raw_offset_1, raw_len_1, first_offset_1] = LeafVersionManager::get_offset_info(0, r_idx + 1);
    LeafVersionManager::encode_segment_versions(intermediate_segment_buffer_1, encoded_segment_buffer_1, first_offset_1, hopped_idxes, hopped_cnt, 0, r_idx, first_metadata_offset_1, new_len_1);
    // segment [l_idx, SPAN_SIZE-1]
    auto intermediate_segment_buffer_2 = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto [first_metadata_offset_2, new_len_2] = MetadataManager::get_offset_info(l_idx, define::leafSpanSize - l_idx);
    MetadataManager::encode_segment_metadata((char *)&records[l_idx], intermediate_segment_buffer_2, first_metadata_offset_2, define::leafSpanSize - l_idx, metadata);
    auto [raw_offset_2, raw_len_2, first_offset_2] = LeafVersionManager::get_offset_info(l_idx, define::leafSpanSize - l_idx);
    LeafVersionManager::encode_segment_versions(intermediate_segment_buffer_2, encoded_segment_buffer_2, first_offset_2, hopped_idxes, hopped_cnt, l_idx, define::leafSpanSize - 1, first_metadata_offset_2, new_len_2);
#else
    // segment [0, r_idx]
    auto [raw_offset_1, raw_len_1, first_offset_1] = VersionManager<LeafNode, LeafEntry>::get_offset_info(0, r_idx + 1);
    VersionManager<LeafNode, LeafEntry>::encode_segment_versions((char *)&records[0], encoded_segment_buffer_1, first_offset_1, hopped_idxes, hopped_cnt, 0, r_idx);
    // segment [l_idx, SPAN_SIZE-1]
    auto [raw_offset_2, raw_len_2, first_offset_2] = VersionManager<LeafNode, LeafEntry>::get_offset_info(l_idx, define::leafSpanSize - l_idx);
    VersionManager<LeafNode, LeafEntry>::encode_segment_versions((char *)&records[l_idx], encoded_segment_buffer_2, first_offset_2, hopped_idxes, hopped_cnt, l_idx, define::leafSpanSize - 1);
#endif
    // write segments and unlock
    RdmaOpRegion rs[2];
    rs[0].source = (uint64_t)encoded_segment_buffer_1;
    rs[0].dest = (node_addr + raw_offset_1).to_uint64();
    rs[0].size = raw_len_1;
    rs[0].is_on_chip = false;

    memcpy(encoded_segment_buffer_2 + raw_len_2 + lock_offset - define::transLeafSize, lock_buffer, sizeof(uint64_t));  // unlock
    rs[1].source = (uint64_t)encoded_segment_buffer_2;
    rs[1].dest = (node_addr + raw_offset_2).to_uint64();
    rs[1].size = raw_len_2 + define::allocationLockSize;
    rs[1].is_on_chip = false;
    dsm->write_batch_sync(rs, 2, sink);
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
    cache_node(node);
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
  tl_stats.split_node ++;
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
  const FenceKeys old_fence_keys = node->metadata.fence_keys;
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
  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)encoded_node_buffer;
  rs[0].dest = node_addr.to_uint64();
  rs[0].size = TRANS_SIZE;
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)lock_buffer;
  rs[1].dest = (node_addr + lock_offset).to_uint64();
  rs[1].size = sizeof(uint64_t);
  rs[1].is_on_chip = false;
  // no need to signal
  dsm->write_batch(rs, 2, false, sink);
#endif
  if (!NODE::IS_LEAF) {
#ifdef TREE_ENABLE_CACHE
    tree_cache->invalidate_range(old_fence_keys.lowest, old_fence_keys.highest);
    cache_node((InternalNode *)node);
    cache_node((InternalNode *)sibling_node);
#endif
  }
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
      cache_node(root_node);
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
  entry.update(k, v);
  auto encoded_entry_buffer = (dsm->get_rbuf(sink)).get_entry_buffer();
#if (defined METADATA_REPLICATION && defined HOPSCOTCH_LEAF_NODE)
  auto get_info_and_encode_versions = [=, &entry](int idx){
    if constexpr (NODE::IS_LEAF) {
      const auto info = LeafVersionManager::get_single_entry_info(idx);
      LeafVersionManager::encode_single_entry(entry, encoded_entry_buffer, info.first_offset);
      return std::make_pair<uint64_t, uint64_t>(info.raw_offset, info.raw_len);
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
    RdmaOpRegion rs[2];
    rs[0].source = (uint64_t)encoded_entry_buffer;
    rs[0].dest = (node_addr + raw_offset).to_uint64();
    rs[0].size = raw_len;
    rs[0].is_on_chip = false;

    rs[1].source = (uint64_t)lock_buffer;
    rs[1].dest = (node_addr + lock_offset).to_uint64();
    rs[1].size = sizeof(uint64_t);
    rs[1].is_on_chip = false;
    if (async) dsm->write_batch(rs, 2, false, sink);
    else dsm->write_batch_sync(rs, 2, sink);
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
  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)encoded_node_buffer;
  rs[0].dest = node_addr.to_uint64();
  rs[0].size = TRANS_SIZE;
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)lock_buffer;
  rs[1].dest = (node_addr + lock_offset).to_uint64();
  rs[1].size = sizeof(uint64_t);
  rs[1].is_on_chip = false;
  if (async) dsm->write_batch(rs, 2, false, sink);
  else dsm->write_batch_sync(rs, 2, sink);
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

  tl_stats.try_write_op++;

#ifdef TREE_ENABLE_WRITE_COMBINING
  lock_res = local_lock_table->acquire_local_write_lock(k, v, &busy_waiting_queue, sink);
  write_handover = (lock_res.first && !lock_res.second);
#else
  UNUSED(lock_res);
#endif
  if (write_handover) {
    tl_stats.write_handover_num++;
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
  tl_stats.retry_cnt[retry_flag] ++;
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
  tl_stats.try_read_leaf ++;
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
  if (speculative_read(node_addr, std::make_pair(hash_idx, (hash_idx + define::neighborSize - 1) % define::leafSpanSize), raw_leaf_buffer, leaf_buffer, k, old_v, i, sink, true)) {
    UNUSED(old_v);
    goto update_entry;
  }
#endif
  hopscotch_search(node_addr, hash_idx, raw_leaf_buffer, leaf_buffer, sink, define::neighborSize, true);
#else
#ifdef SPECULATIVE_READ
  Value old_v;
  if (speculative_read(node_addr, std::make_pair(0, (int)define::leafSpanSize - 1), raw_leaf_buffer, leaf_buffer, k, old_v, i, sink, true)) {
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
    tl_stats.leaf_cache_invalid ++;
    return false;
  }
#else
  UNUSED(sibling_ptr);
  UNUSED(sibling_addr);
  // cache validation
  fence_keys = leaf->metadata.fence_keys;
  if (from_cache && (!leaf->metadata.valid || k < fence_keys.lowest || k >= fence_keys.highest)) {  // cache is outdated
    unlock_node(node_addr, lock_buffer, true, sink, true);
    tl_stats.leaf_cache_invalid ++;
    return false;
  }
  // turn right check
  if (k >= fence_keys.highest) {  // should turn right
    unlock_node(node_addr, lock_buffer, true, sink, true);
    assert(leaf->metadata.sibling_ptr != GlobalAddress::Null());
    tl_stats.leaf_read_sibling ++;
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
    tl_stats.leaf_read_sibling ++;
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
    tl_stats.leaf_read_sibling ++;
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

  tl_stats.try_read_op ++;

#ifdef TREE_ENABLE_READ_DELEGATION
  lock_res = local_lock_table->acquire_local_read_lock(k, &busy_waiting_queue, sink);
  read_handover = (lock_res.first && !lock_res.second);
#else
  UNUSED(lock_res);
#endif
  if (read_handover) {
    tl_stats.read_handover_num++;
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
  tl_stats.retry_cnt[retry_flag] ++;
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
  const auto info = LeafVersionManager::get_single_entry_info(idx);
  auto raw_entry_buffer = raw_leaf_buffer + info.raw_offset;
re_read:
  dsm->read_sync(raw_entry_buffer, leaf_addr + info.raw_offset, info.raw_len, sink);
  if (for_write) {
    assert((LeafVersionManager::decode_single_entry(raw_entry_buffer, info.first_offset, leaf->records[idx])));
  } else if (!LeafVersionManager::decode_single_entry(raw_entry_buffer, info.first_offset, leaf->records[idx])) {
    goto re_read;
  }
  return;
#else
  auto [raw_offset, raw_len, first_offset] = VersionManager<LeafNode, LeafEntry>::get_offset_info(idx);
  auto raw_entry_buffer = raw_leaf_buffer + raw_offset;
re_read:
  // read metadata and the hop segment
  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)raw_leaf_buffer;
  rs[0].dest = leaf_addr.to_uint64();
  rs[0].size = define::bufferMetadataSize;  // header
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)raw_entry_buffer;
  rs[1].dest = (leaf_addr + raw_offset).to_uint64();
  rs[1].size = raw_len;
  rs[1].is_on_chip = false;
  // note that the rs array will change by lower-level function
  dsm->read_batch_sync(rs, 2, sink);
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
  if (idx_cache->search_idx_from_cache(leaf_addr, range.first, range.second, k, speculative_idx)) {
    tl_stats.try_speculative_read ++;
#ifdef METADATA_REPLICATION
    const auto info = LeafVersionManager::get_single_entry_info(speculative_idx);
    auto raw_entry_buffer = raw_leaf_buffer + info.raw_offset;
    LeafEntry local_entry;
    LeafEntry *target_entry = leaf_buffer ? &(((LeafNode *)leaf_buffer)->records[speculative_idx]) : &local_entry;
  re_read_spec:
    dsm->read_sync(raw_entry_buffer, leaf_addr + info.raw_offset, info.raw_len, sink);
    if (for_write) {
      assert((LeafVersionManager::decode_single_entry(raw_entry_buffer, info.first_offset, *target_entry)));
    } else if (!LeafVersionManager::decode_single_entry(raw_entry_buffer, info.first_offset, *target_entry)) {
      goto re_read_spec;
    }
    const auto& entry = *target_entry;
#else
    auto leaf = (LeafNode *)leaf_buffer;
    leaf_entry_read(leaf_addr, speculative_idx, raw_leaf_buffer, leaf_buffer, sink, for_write);
    const auto& entry = leaf->records[speculative_idx];
#endif
    if (entry.key == k) {
      tl_stats.correct_speculative_read ++;
      v = entry.value;
      idx_cache->add_to_cache(leaf_addr, speculative_idx, k);
      return true;
    }
    idx_cache->invalidate(leaf_addr, k);
    if (entry.key != define::kkeyNull) {
      idx_cache->add_to_cache(leaf_addr, speculative_idx, entry.key);
    }
  }
  return false;
}
#endif


bool Tree::leaf_node_search(const GlobalAddress& node_addr, const GlobalAddress& sibling_addr, const Key &k, Value &v, bool from_cache, CoroPull* sink) {
  tl_stats.try_read_leaf ++;
  auto raw_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  char *leaf_buffer = nullptr;
  LeafNode *leaf = nullptr;
#ifdef HOPSCOTCH_LEAF_NODE
  int hash_idx = get_hashed_leaf_entry_index(k);
#ifdef SPECULATIVE_READ
  int speculative_idx;
  if (speculative_read(node_addr, std::make_pair(hash_idx, (hash_idx + define::neighborSize - 1) % define::leafSpanSize), raw_leaf_buffer, nullptr, k, v, speculative_idx, sink)) {
    UNUSED(speculative_idx);
    return true;
  }
#endif
  leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  leaf = (LeafNode *)leaf_buffer;
re_read:
  hopscotch_search(node_addr, hash_idx, raw_leaf_buffer, leaf_buffer, sink);
#else
  leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  leaf = (LeafNode *)leaf_buffer;
#ifdef SPECULATIVE_READ
  int speculative_idx;
  if (speculative_read(node_addr, std::make_pair(0, (int)define::leafSpanSize - 1), raw_leaf_buffer, leaf_buffer, k, v, speculative_idx, sink)) {
    UNUSED(speculative_idx);
    return true;
  }
#endif
re_read:
  dsm->read_sync(raw_leaf_buffer, node_addr, define::transLeafSize, sink);
  if (!VersionManager<LeafNode, LeafEntry>::decode_node_versions(raw_leaf_buffer, leaf_buffer)) {
    tl_stats.read_leaf_retry ++;
    goto re_read;
  }
#endif

#ifdef SIBLING_BASED_VALIDATION
  const auto& sibling_ptr = (leaf->metadata.sibling_ptr == GlobalAddress::Widest() ? GlobalAddress::Null() : (GlobalAddress)leaf->metadata.sibling_ptr);
  // cache validation
  if (from_cache && (!leaf->metadata.valid || sibling_addr != sibling_ptr)) {  // cache is outdated
    tl_stats.leaf_cache_invalid ++;
    return false;
  }
#else
  UNUSED(sibling_addr);
  // cache validation
  const auto& fence_keys = leaf->metadata.fence_keys;
  if (from_cache && (!leaf->metadata.valid || k < fence_keys.lowest || k >= fence_keys.highest)) {  // cache is outdated
    tl_stats.leaf_cache_invalid ++;
    return false;
  }
  // turn right check
  if (k >= fence_keys.highest) {  // should turn right
    assert(leaf->metadata.sibling_ptr != GlobalAddress::Null());
    tl_stats.leaf_read_sibling ++;
    leaf_node_search(leaf->metadata.sibling_ptr, GlobalAddress::Null(), k, v, false, sink);
    return true;
  }
  assert(k >= fence_keys.lowest);
#endif

  // search for the key
  auto& records = leaf->records;
#ifdef HOPSCOTCH_LEAF_NODE
  // Fast key match scan first (avoids hashing preceding entries when k is present)
  if (__builtin_expect(k != define::kkeyNull, 1)) {
    const uint64_t raw_k = key_to_u64_raw(k);
    for (int i = 0; i < (int)define::neighborSize; ++ i) {
      int slot = (hash_idx + i) % define::leafSpanSize;
      const auto& e = records[slot];
      if (key_to_u64_raw(e.key) == raw_k) {
        v = e.value;
#ifdef SPECULATIVE_READ
        idx_cache->add_to_cache(node_addr, slot, k);
#endif
        return true;
      }
    }
  }
  // Key not found in neighborhood: verify hop_bitmap consistency against concurrent hopscotch relocation
  uint16_t hop_bitmap = 0;
  for (int i = 0; i < (int)define::neighborSize; ++ i) {
    const auto& e = records[(hash_idx + i) % define::leafSpanSize];
    if (e.key != define::kkeyNull && (int)get_hashed_leaf_entry_index(e.key) == hash_idx) {
      hop_bitmap |= 1ULL << (define::neighborSize - i - 1);
    }
  }
  if (hop_bitmap != records[hash_idx].hop_bitmap) {
    tl_stats.read_leaf_retry ++;
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
    tl_stats.leaf_read_sibling ++;
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

  struct LeafRangeTarget {
    GlobalAddress addr;
    FenceKeys fences;
  };
  thread_local std::vector<const InternalNode *> cache_search_result;
  thread_local std::vector<LeafRangeTarget> leaf_targets;
  thread_local std::vector<RdmaOpRegion> rs;
  thread_local std::vector<std::pair<int, int> > segments;
  thread_local std::vector<std::pair<int, int> > merge_temp;
  thread_local std::vector<std::pair<int, int> > merged_segments;
#ifdef FINE_GRAINED_RANGE_QUERY
  struct SegReadInfo {
    int buf_slot;
    int start_idx;
    int segment_size;
    GlobalAddress leaf_addr;
    uint64_t raw_offset;
    uint64_t raw_len;
    uint64_t first_offset;
    bool single_entry;
  };
  thread_local std::vector<SegReadInfo> seg_infos;
  thread_local std::vector<SegReadInfo> next_seg_infos;
#ifdef SPECULATIVE_READ
  struct SpecRangeKey {
    GlobalAddress leaf_addr;
    Key k;
  };
  thread_local std::vector<SpecRangeKey> speculative_keys;
  struct RetryLeafKey {
    GlobalAddress addr;
    Key k;
  };
  thread_local std::vector<RetryLeafKey> retry_leaf_keys;
  speculative_keys.clear();
  retry_leaf_keys.clear();
#endif
  seg_infos.clear();
  next_seg_infos.clear();
#else
  thread_local std::vector<GlobalAddress> leaf_info;
  thread_local std::vector<GlobalAddress> next_info;
  leaf_info.clear();
  next_info.clear();
#endif
  cache_search_result.clear();
  leaf_targets.clear();
  rs.clear();
  tree_cache->search_range_ptrs_from_cache(from, to, cache_search_result);

  // FIXME: for simplicity, we assume all innernal nodes are cached in compute node like Sherman
  if (cache_search_result.empty()) {
    for(auto k = from; k < to; k = k + 1) {
      tl_stats.cache_miss ++;
      search(k, ret[k]);  // load into cache
    }
    return false;
  }

  auto upsert_leaf_target = [&](const GlobalAddress &addr, const Key &low, const Key &high) {
    for (auto &t : leaf_targets) {
      if (t.addr == addr) {
        t.fences = FenceKeys(low, high);
        return;
      }
    }
    leaf_targets.push_back(LeafRangeTarget{addr, FenceKeys(low, high)});
  };

  // parse cached internal nodes
  for (const auto *node_ptr : cache_search_result) {
    const auto& node = *node_ptr;
    const auto& metadata = node.metadata;
    assert(metadata.level == 1);
    const auto& records = node.records;
    bool no_fetch = from >= records[0].key || to <= metadata.fence_keys.lowest;
    if (!no_fetch) {
      upsert_leaf_target(metadata.leftmost_ptr, metadata.fence_keys.lowest, records[0].key);
    }
    int i;
    for (i = 1; i <= (int)define::internalSpanSize; ++ i) {
      if (i == (int)define::internalSpanSize || records[i].key == define::kkeyNull) {
        no_fetch = from >= metadata.fence_keys.highest || to <= records[i - 1].key;
        if (!no_fetch) {
          upsert_leaf_target(records[i - 1].ptr, records[i - 1].key, metadata.fence_keys.highest);
        }
        break;
      }
      no_fetch = from >= records[i].key || to <= records[i - 1].key;
      if (!no_fetch) {
        upsert_leaf_target(records[i - 1].ptr, records[i - 1].key, records[i].key);
      }
    }
  }
  if (leaf_targets.size() > 1) {
    std::sort(leaf_targets.begin(), leaf_targets.end(), [](const LeafRangeTarget &a, const LeafRangeTarget &b) {
      return a.addr < b.addr;
    });
  }

  const uint64_t from_int = key2int(from);
  const uint64_t to_int = key2int(to);
  if (to_int <= from_int) {
    return true;
  }
#ifndef ENABLE_VAR_LEN_KV
  const uint64_t span = to_int - from_int;
  const bool use_dense_span = ret.empty() && span <= 4096;
  thread_local std::vector<uint8_t> span_found;
  thread_local std::vector<Value> span_vals;
  if (use_dense_span) {
    span_found.assign((size_t)span, 0);
    span_vals.resize((size_t)span);
  }
#endif

  auto merge_internals = [&](std::vector<std::pair<int, int> >& intervals, std::vector<std::pair<int, int> >& res){  // intervals: [l, r)
    std::sort(intervals.begin(), intervals.end(), [](const std::pair<int, int>& a, const std::pair<int, int>& b){
      return a.first < b.first;
    });
    int n = intervals.size();
    int i = 0;
    merge_temp.clear();
    while (i < n) {
      int j = i + 1;
      int end = intervals[i].second;
      while (j < n && intervals[j].first <= end) end = std::max(end, intervals[j ++].second);
      merge_temp.emplace_back(intervals[i].first, end);
      i = j;
    }
    res.clear();
#ifdef GREEDY_RANGE_QUERY
    constexpr int kMaxMergeGapEntries = 6;
    n = merge_temp.size();
    i = 0;
    while (i < n) {
      int j = i + 1;
      int end = merge_temp[i].second;
      while (j < n &&
             (merge_temp[j].first - end) <= kMaxMergeGapEntries &&
             std::max(end, merge_temp[j].second) <= (merge_temp[i].first + (int)define::maxLeafEntryPerIO)) {
        end = std::max(end, merge_temp[j ++].second);
      }
      res.emplace_back(merge_temp[i].first, end);
      i = j;
    }
#else
    res = merge_temp;
#endif
  };

  int leaf_cnt = 0;
  auto range_buffer = (dsm->get_rbuf(nullptr)).get_range_buffer();
#ifdef FINE_GRAINED_RANGE_QUERY
  // generate read segments
  for (const auto& target : leaf_targets) {
    const GlobalAddress& leaf_addr = target.addr;
    Key l_k = std::max(target.fences.lowest, from);
    Key r_k = std::min(target.fences.highest, to);
    segments.clear();
    if (l_k == target.fences.lowest && r_k == target.fences.highest) {
      segments.emplace_back(0, (int)define::leafSpanSize);
    }
    else for (auto k = l_k; k < r_k; add_one(k)) {
      int hash_idx = get_hashed_leaf_entry_index(k);
#ifdef SPECULATIVE_READ
      tl_stats.try_read_leaf ++;
      int speculative_idx;
      if (idx_cache->search_idx_from_cache(leaf_addr, hash_idx, (hash_idx + define::neighborSize - 1) % define::leafSpanSize, k, speculative_idx)) {
          tl_stats.try_speculative_read ++;
          speculative_keys.push_back(SpecRangeKey{leaf_addr, k});
          segments.emplace_back(speculative_idx, speculative_idx + 1);
          continue;
      }
#endif
      if (hash_idx + (int)define::neighborSize <= (int)define::leafSpanSize) segments.emplace_back(hash_idx, hash_idx + (int)define::neighborSize);
      else {
        segments.emplace_back(hash_idx, (int)define::leafSpanSize);
        segments.emplace_back(0, (int)define::neighborSize - ((int)define::leafSpanSize - hash_idx));
      }
    }
    // merge the intervals
    merge_internals(segments, merged_segments);
    for (const auto& [l_idx, r_idx] : merged_segments) {
      const int seg_len = r_idx - l_idx;
      uint64_t raw_offset = 0, raw_len = 0, first_offset = 0;
      const bool is_single = (seg_len == 1);
      if (is_single) {
        const auto sinfo = LeafVersionManager::get_single_entry_info(l_idx);
        raw_offset = sinfo.raw_offset;
        raw_len = sinfo.raw_len;
        first_offset = sinfo.first_offset;
      } else {
        std::tie(raw_offset, raw_len, first_offset) = LeafVersionManager::get_offset_info(l_idx, seg_len);
      }
      RdmaOpRegion r;
      r.source     = (uint64_t)range_buffer + leaf_cnt * define::allocationLeafSize + raw_offset;
      r.dest       = (leaf_addr + raw_offset).to_uint64();
      r.size       = raw_len;
      r.is_on_chip = false;
      rs.push_back(r);
      seg_infos.push_back(SegReadInfo{leaf_cnt, l_idx, seg_len, leaf_addr, raw_offset, raw_len, first_offset, is_single});
    }
    ++ leaf_cnt;
    assert((dsm->get_rbuf(nullptr)).is_safe(range_buffer + leaf_cnt * define::allocationLeafSize));
  }
  int next_leaf_cnt;
  // batch read
re_read:
  dsm->read_batches_sync(rs);
  rs.clear();
  next_seg_infos.clear();
  next_leaf_cnt = 0;
  // parse read leaf segments
  {
    auto leaf_buffer = (dsm->get_rbuf(nullptr)).get_leaf_buffer();
    auto leaf = (LeafNode *)leaf_buffer;
    auto intermediate_buffer = (dsm->get_rbuf(nullptr)).get_segment_buffer();
    int cur_slot = -1;
    bool cur_slot_used = false;
    int hash_idxes[define::leafSpanSize];
    for (const auto& seg : seg_infos) {
      if (seg.buf_slot != cur_slot) {
        if (cur_slot_used) {
          ++ next_leaf_cnt;
          cur_slot_used = false;
        }
        cur_slot = seg.buf_slot;
      }
      const int start_idx = seg.start_idx;
      const int segment_size = seg.segment_size;
      const GlobalAddress& leaf_addr = seg.leaf_addr;
      const uint64_t raw_offset = seg.raw_offset;
      const uint64_t raw_len = seg.raw_len;
      const uint64_t first_offset = seg.first_offset;

      auto raw_buffer = range_buffer + cur_slot * define::allocationLeafSize + raw_offset;
      if (seg.single_entry) {
        LeafEntry single_e;
        if (!LeafVersionManager::decode_single_entry(raw_buffer, first_offset, single_e)) {
          RdmaOpRegion r;
          r.source     = (uint64_t)range_buffer + next_leaf_cnt * define::allocationLeafSize + raw_offset;
          r.dest       = (leaf_addr + raw_offset).to_uint64();
          r.size       = raw_len;
          r.is_on_chip = false;
          rs.push_back(r);
          next_seg_infos.push_back(SegReadInfo{next_leaf_cnt, start_idx, segment_size, leaf_addr, raw_offset, raw_len, first_offset, true});
          cur_slot_used = true;
          continue;
        }
        const uint64_t ek = key2int(single_e.key);
        if (ek != 0ULL) {
          if (ek >= from_int && ek < to_int) {
#ifndef ENABLE_VAR_LEN_KV
            if (use_dense_span) {
              const size_t off = (size_t)(ek - from_int);
              span_found[off] = 1;
              span_vals[off] = single_e.value;
            } else {
              ret[single_e.key] = single_e.value;
            }
#else
            ret[single_e.key] = single_e.value;
#endif
          }
#ifdef SPECULATIVE_READ
          idx_cache->add_to_cache(leaf_addr, start_idx, single_e.key);
#endif
        }
        continue;
      }
      uint8_t segment_node_versions = 0;
      auto [first_metadata_offset, new_len] = MetadataManager::get_offset_info(start_idx, segment_size);
      // check versions consistency
      if (!(LeafVersionManager::decode_segment_versions(raw_buffer, intermediate_buffer, first_offset, segment_size, first_metadata_offset, new_len, segment_node_versions))) {
        RdmaOpRegion r;
        r.source     = (uint64_t)range_buffer + next_leaf_cnt * define::allocationLeafSize + raw_offset;
        r.dest       = (leaf_addr + raw_offset).to_uint64();
        r.size       = raw_len;
        r.is_on_chip = false;
        rs.push_back(r);
        next_seg_infos.push_back(SegReadInfo{next_leaf_cnt, start_idx, segment_size, leaf_addr, raw_offset, raw_len, first_offset, false});
        cur_slot_used = true;
        continue;
      }
      MetadataManager::decode_segment_metadata(intermediate_buffer, (char*)&(leaf->records[start_idx]), first_metadata_offset, segment_size, leaf->metadata);
      // check hopping consistency
      for (int j = 0; j < segment_size; ++ j) {
        const auto& e = leaf->records[start_idx + j];
        hash_idxes[j] = (key_to_u64_raw(e.key) == 0ULL) ? -1 : (int)get_hashed_leaf_entry_index(e.key);
      }
      bool is_ok = true;
      for (int j = 0; j < segment_size && is_ok; ++ j) {
        const auto hop_bitmap = leaf->records[start_idx + j].hop_bitmap;
        if (hop_bitmap == 0) continue;
        const int lim = std::min((int)define::neighborSize, segment_size - j);
        for (int k = 0; k < lim; ++ k) {
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
        next_seg_infos.push_back(SegReadInfo{next_leaf_cnt, start_idx, segment_size, leaf_addr, raw_offset, raw_len, first_offset, false});
        cur_slot_used = true;
        continue;
      }
      // search key from the segments
      for (int j = start_idx; j < start_idx + segment_size; ++ j) {
        const auto& e = leaf->records[j];
        const uint64_t ek = key2int(e.key);
        if (ek != 0ULL && ek >= from_int && ek < to_int) {
#ifndef ENABLE_VAR_LEN_KV
          if (use_dense_span) {
            const size_t off = (size_t)(ek - from_int);
            span_found[off] = 1;
            span_vals[off] = e.value;
          } else {
            ret[e.key] = e.value;
          }
#else
          ret[e.key] = e.value;
#endif
#ifdef SPECULATIVE_READ
          idx_cache->add_to_cache(leaf_addr, j, e.key);
#endif
        }
      }
    }
    if (cur_slot_used) ++ next_leaf_cnt;
  }
#else
  UNUSED(merge_internals);
  // batch read
  for (const auto& target : leaf_targets) {
    const GlobalAddress& leaf_addr = target.addr;
    RdmaOpRegion r;
    r.source     = (uint64_t)range_buffer + leaf_cnt * define::allocationLeafSize;
    r.dest       = leaf_addr.to_uint64();
    r.size       = define::allocationLeafSize;
    r.is_on_chip = false;
    rs.push_back(r);
    leaf_info.push_back(leaf_addr);
    ++ leaf_cnt;
  }
  assert((dsm->get_rbuf(nullptr)).is_safe(range_buffer + leaf_cnt * define::allocationLeafSize));
  int next_leaf_cnt;
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
      next_info.push_back(leaf_info[i]);
      ++ next_leaf_cnt;
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
      next_info.push_back(leaf_info[i]);
      ++ next_leaf_cnt;
      continue;
    }
#endif
#ifdef HOPSCOTCH_LEAF_NODE
    // check hopping consistency
    bool is_ok = true;
    auto& records = leaf->records;
    int hash_idxes[define::leafSpanSize];
    for (int idx = 0; idx < (int)define::leafSpanSize; ++ idx) {
      const auto& e = records[idx];
      hash_idxes[idx] = (key_to_u64_raw(e.key) == 0ULL) ? -1 : (int)get_hashed_leaf_entry_index(e.key);
    }
    for (int idx = 0; idx < (int)define::leafSpanSize; ++ idx) {
      uint16_t hop_bitmap = 0;
      for (int j = 0; j < (int)define::neighborSize; ++ j) {
        if (hash_idxes[(idx + j) % define::leafSpanSize] == idx) {
          hop_bitmap |= 1ULL << (define::neighborSize - j - 1);
        }
      }
      if (hop_bitmap != records[idx].hop_bitmap) {
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
      next_info.push_back(leaf_info[i]);
      ++ next_leaf_cnt;
      continue;
    }
#endif
    // search key from the leaves
    for (const auto& e : leaf->records) {
      const uint64_t ek = key2int(e.key);
      if (ek != 0ULL && ek >= from_int && ek < to_int) {
#ifndef ENABLE_VAR_LEN_KV
        if (use_dense_span) {
          const size_t off = (size_t)(ek - from_int);
          span_found[off] = 1;
          span_vals[off] = e.value;
        } else {
          ret[e.key] = e.value;
        }
#else
        ret[e.key] = e.value;
#endif
      }
    }
  }
#endif
#if (defined FINE_GRAINED_RANGE_QUERY && defined SPECULATIVE_READ)
  if (!speculative_keys.empty()) {
    retry_leaf_keys.clear();
    for (const auto& sk : speculative_keys) {
      const Key& k = sk.k;
      bool hit;
#ifndef ENABLE_VAR_LEN_KV
      if (use_dense_span) {
        hit = (span_found[(size_t)(key2int(k) - from_int)] != 0);
      } else {
        hit = (ret.find(k) != ret.end());
      }
#else
      hit = (ret.find(k) != ret.end());
#endif
      if (hit) {
        tl_stats.correct_speculative_read ++;
      } else {
        idx_cache->invalidate(sk.leaf_addr, k);
        GlobalAddress p;
        GlobalAddress sibling_p;
        uint16_t level;
        auto cache_entry = tree_cache->search_from_cache(k, p, sibling_p, level);
        if (!cache_entry || level != 1) {
#ifndef ENABLE_VAR_LEN_KV
          if (use_dense_span) {
            const size_t off = (size_t)(key2int(k) - from_int);
            Value v{};
            search(k, v);  // load into cache
            span_vals[off] = v;
            span_found[off] = 1;
          } else {
            search(k, ret[k]);  // load into cache
          }
#else
          search(k, ret[k]);  // load into cache
#endif
          continue;
        }
        retry_leaf_keys.push_back(RetryLeafKey{p, k});
      }
    }
    speculative_keys.clear();
    if (!retry_leaf_keys.empty()) {
      std::sort(retry_leaf_keys.begin(), retry_leaf_keys.end(), [](const RetryLeafKey &a, const RetryLeafKey &b) {
        return a.addr < b.addr;
      });
      size_t g = 0;
      while (g < retry_leaf_keys.size()) {
        const GlobalAddress leaf_addr = retry_leaf_keys[g].addr;
        segments.clear();
        while (g < retry_leaf_keys.size() && retry_leaf_keys[g].addr == leaf_addr) {
          int hash_idx = get_hashed_leaf_entry_index(retry_leaf_keys[g].k);
          if (hash_idx + (int)define::neighborSize <= (int)define::leafSpanSize) segments.emplace_back(hash_idx, hash_idx + (int)define::neighborSize);
          else {
            segments.emplace_back(hash_idx, (int)define::leafSpanSize);
            segments.emplace_back(0, (int)define::neighborSize - ((int)define::leafSpanSize - hash_idx));
          }
          ++ g;
        }
        // merge the intervals
        merge_internals(segments, merged_segments);
        for (const auto& [l_idx, r_idx] : merged_segments) {
          const int seg_len = r_idx - l_idx;
          uint64_t raw_offset = 0, raw_len = 0, first_offset = 0;
          const bool is_single = (seg_len == 1);
          if (is_single) {
            const auto sinfo = LeafVersionManager::get_single_entry_info(l_idx);
            raw_offset = sinfo.raw_offset;
            raw_len = sinfo.raw_len;
            first_offset = sinfo.first_offset;
          } else {
            std::tie(raw_offset, raw_len, first_offset) = LeafVersionManager::get_offset_info(l_idx, seg_len);
          }
          RdmaOpRegion r;
          r.source     = (uint64_t)range_buffer + next_leaf_cnt * define::allocationLeafSize + raw_offset;
          r.dest       = (leaf_addr + raw_offset).to_uint64();
          r.size       = raw_len;
          r.is_on_chip = false;
          rs.push_back(r);
          next_seg_infos.push_back(SegReadInfo{next_leaf_cnt, l_idx, seg_len, leaf_addr, raw_offset, raw_len, first_offset, is_single});
        }
        ++ next_leaf_cnt;
      }
    }
  }
#endif
  if (!rs.empty()) {
#ifdef FINE_GRAINED_RANGE_QUERY
    seg_infos.swap(next_seg_infos);
#else
    leaf_info.swap(next_info);
#endif
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
#ifndef ENABLE_VAR_LEN_KV
  if (use_dense_span) {
    Key k = from;
    for (size_t off = 0; off < (size_t)span; ++ off, add_one(k)) {
      if (span_found[off]) {
        tl_stats.cache_hit ++;
        ret.emplace_hint(ret.end(), k, span_vals[off]);
      } else {
        tl_stats.cache_miss ++;
        Value v{};
        search(k, v);
        ret.emplace_hint(ret.end(), k, v);
      }
    }
    return true;
  }
#endif
  auto it = ret.lower_bound(from);
  for(auto k = from; k < to; add_one(k)) {
    while (it != ret.end() && it->first < k) ++ it;
    if (it != ret.end() && it->first == k) {
      tl_stats.cache_hit ++;
      ++ it;
    }
    else {
      tl_stats.cache_miss ++;
      Value v{};
      search(k, v);
      it = ret.emplace_hint(it, k, v);
      ++ it;
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
    tl_coro_resume_seq[i] = 0;
    workers[i](i);
  }
  while (!need_stop) {
    uint64_t next_coro_id;

    if (dsm->poll_rdma_cq_once(next_coro_id)) {
      ++tl_coro_resume_seq[next_coro_id];
      workers[next_coro_id](next_coro_id);
    }
    size_t qn = busy_waiting_queue.size();
    while (qn--) {
      auto next_coro_id = busy_waiting_queue.front();
      busy_waiting_queue.pop();
      ++tl_coro_resume_seq[next_coro_id];
      workers[next_coro_id](next_coro_id);
    }
  }
}


void Tree::coro_worker(CoroPull &sink, RequstGen *gen, WorkFunc work_func) {
  Timer coro_timer;
  auto thread_id = dsm->getMyThreadID();
  const int coro_id = sink.get();

  while (!need_stop) {
    auto r = gen->next();
    const uint64_t seq0 = tl_coro_resume_seq[coro_id];

    coro_timer.begin();
    work_func(this, r, &sink);
    auto us_10 = coro_timer.end() / 100;

    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[thread_id][coro_id][us_10]++;

    if (tl_coro_resume_seq[coro_id] == seq0) {
      busy_waiting_queue.push((uint16_t)coro_id);
      sink();
    }
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
