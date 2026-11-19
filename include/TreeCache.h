#if !defined(_TREE_CACHE_H_)
#define _TREE_CACHE_H_

#include "TreeCacheEntry.h"
#include "HugePageAlloc.h"
#include "Timer.h"
#include "third_party/inlineskiplist.h"
#include "DSM.h"

#include <cstdlib>
#include <mutex>
#include <queue>
#include <atomic>
#include <vector>


using TreeCacheSkipList = InlineSkipList<TreeCacheEntryComparator>;

class TreeCache {

public:
  TreeCache(int cache_size, DSM* dsm);
  ~TreeCache();

  bool add_to_cache(InternalNode *page);
  const TreeCacheEntry *search_from_cache(const Key &k, GlobalAddress& addr, GlobalAddress& sibling_addr, uint16_t& level);
  const TreeCacheEntry *search_ptr_from_cache(const Key &k, GlobalAddress& addr, const uint16_t& level);
  void search_range_from_cache(const Key &from, const Key &to, std::vector<InternalNode> &result);
  void search_range_ptrs_from_cache(const Key &from, const Key &to, std::vector<const InternalNode *> &result);
  bool invalidate(const TreeCacheEntry *entry);
  void invalidate_range(const Key &lowest, const Key &highest);
  void statistics();

private:
  static constexpr int kSlabNodes = 64;
  static constexpr int kRetireRingSize = 1024;

  struct alignas(64) ThreadNodePool {
    InternalNode *free_list[kSlabNodes] = {};
    int free_cnt = 0;
    InternalNode *retire_ring[kRetireRingSize] = {};
    uint32_t retire_pos = 0;
  };

  ThreadNodePool &thread_pool() {
    const int tid = dsm->is_register() ? dsm->getMyThreadID() : 0;
    const int idx = (tid >= 0 && tid <= MAX_APP_THREAD) ? tid : 0;
    return pools_[idx];
  }

  InternalNode *alloc_node() {
    auto &p = thread_pool();
    if (__builtin_expect(p.free_cnt == 0, 0)) {
      auto *slab = static_cast<InternalNode *>(std::malloc(sizeof(InternalNode) * kSlabNodes));
      {
        std::lock_guard<std::mutex> g(slab_mu_);
        slabs_.push_back(slab);
      }
      for (int i = 0; i < kSlabNodes; ++i) {
        p.free_list[p.free_cnt++] = &slab[i];
      }
    }
    return p.free_list[--p.free_cnt];
  }

  void recycle_node(InternalNode *node) {
    auto &p = thread_pool();
    if (p.free_cnt < kSlabNodes) {
      p.free_list[p.free_cnt++] = node;
    }
  }

  void evict_one();
  void evict();

  bool add_entry(const Key &from, const Key &to, InternalNode *ptr);
  const TreeCacheEntry *find_entry(const Key &k);
  const TreeCacheEntry *find_entry_int(uint64_t k_int);
  const TreeCacheEntry *find_entry(const Key &from, const Key &to);
  const TreeCacheEntry *seek_entry(const Key &k);  // try to return an non-nullptr cache_entry
  const TreeCacheEntry *seek_entry_int(uint64_t k_int);
  const TreeCacheEntry *seek_entry(const Key &from, const Key &to);
  const TreeCacheEntry *get_a_random_entry(uint64_t &freq);
  void safely_delete(InternalNode* cached_node);

private:
  uint64_t cache_size; // MB;
  std::atomic<int64_t> free_size;
  std::atomic<int64_t> skiplist_node_cnt;
  DSM *dsm;

  // SkipList
  TreeCacheSkipList *skiplist;
  TreeCacheEntryComparator cmp;
  Allocator alloc;

  ThreadNodePool pools_[MAX_APP_THREAD + 1];
  std::mutex slab_mu_;
  std::vector<InternalNode *> slabs_;
};

inline TreeCache::TreeCache(int cache_size, DSM* dsm) : cache_size(cache_size), dsm(dsm) {
  skiplist = new TreeCacheSkipList(cmp, &alloc, 21);  // 21 [TUNE]
  free_size.store(define::MB * cache_size);
  skiplist_node_cnt.store(0);
}

inline TreeCache::~TreeCache() {
  delete skiplist;
  for (InternalNode *slab : slabs_) {
    std::free(slab);
  }
}

// [from, to）
inline bool TreeCache::add_entry(const Key &from, const Key &to, InternalNode *ptr) {
  // TODO: memory leak
  auto buf = skiplist->AllocateKey(sizeof(TreeCacheEntry));
  auto &e = *(TreeCacheEntry *)buf;
  e.from = from;
  e.to = to - 1; // !IMPORTANT;
  e.cache_entry_freq = 0;
  e.ptr = ptr;

  auto res = skiplist->InsertConcurrently(buf);
  return res;
}

inline const TreeCacheEntry *TreeCache::find_entry(const Key &from, const Key &to) {
  TreeCacheSkipList::Iterator iter(skiplist);

  TreeCacheEntry e;
  e.from = from;
  e.to = to - 1;
  iter.Seek((char *)&e);
  if (iter.Valid()) {
    auto val = (const TreeCacheEntry *)iter.key();
    return val;
  }
  else return nullptr;
}

inline const TreeCacheEntry *TreeCache::seek_entry(const Key &from, const Key &to) {
  TreeCacheSkipList::Iterator iter(skiplist);

  TreeCacheEntry e;
  e.from = from;
  e.to = to - 1;
  iter.Seek((char *)&e);
seek_next:
  if (iter.Valid()) {
    auto val = (const TreeCacheEntry *)iter.key();
    if (val && val->ptr) return val;
    iter.Next();
    goto seek_next;
  }
  else return nullptr;
}

inline const TreeCacheEntry *TreeCache::find_entry_int(uint64_t k_int) {
  TreeCacheSkipList::Iterator iter(skiplist);
  iter.SeekPoint(k_int);
  if (iter.Valid()) {
    return (const TreeCacheEntry *)iter.key();
  }
  return nullptr;
}

inline const TreeCacheEntry *TreeCache::find_entry(const Key &k) {
  return find_entry_int(key2int(k));
}

inline const TreeCacheEntry *TreeCache::seek_entry_int(uint64_t k_int) {
  TreeCacheSkipList::Iterator iter(skiplist);
  iter.SeekPoint(k_int);
  while (iter.Valid()) {
    auto val = (const TreeCacheEntry *)iter.key();
    if (val && val->ptr) return val;
    iter.Next();
  }
  return nullptr;
}

inline const TreeCacheEntry *TreeCache::seek_entry(const Key &k) {
  return seek_entry_int(key2int(k));
}

inline bool TreeCache::add_to_cache(InternalNode *page) {
  InternalNode *new_page = alloc_node();
  __builtin_memcpy(static_cast<void *>(new_page), static_cast<const void *>(page), sizeof(InternalNode));

  const auto lowest = page->metadata.fence_keys.lowest;
  const auto highest = page->metadata.fence_keys.highest;
  const auto highest_m1 = highest - 1;

  auto existing = this->find_entry(lowest, highest);
  if (existing && existing->from == lowest && existing->to == highest_m1) {
    auto ptr = existing->ptr;
    auto ret_val = __sync_val_compare_and_swap(&(existing->ptr), ptr, new_page);
    if (ret_val == ptr) {
      if (ret_val == nullptr) {
        auto v = free_size.fetch_add(-new_page->consumed_cache_size(), std::memory_order_relaxed);
        if (__builtin_expect(v < 0, 0)) {
          evict();
        }
      } else {
        safely_delete(ptr);
      }
      return true;
    }
    recycle_node(new_page);
    return false;
  }

  if (this->add_entry(lowest, highest, new_page)) {
    skiplist_node_cnt.fetch_add(1, std::memory_order_relaxed);
    auto v = free_size.fetch_add(-new_page->consumed_cache_size(), std::memory_order_relaxed);
    if (__builtin_expect(v < 0, 0)) {
      evict();
    }
    return true;
  }
  else { // conflicted
    auto e = this->find_entry(lowest, highest);
    if (e && e->from == lowest && e->to == highest_m1) {
      auto ptr = e->ptr;
      auto ret_val = __sync_val_compare_and_swap(&(e->ptr), ptr, new_page);
      if (ret_val == ptr) {  // cas success
        if (ret_val == nullptr) {
          auto v = free_size.fetch_add(-new_page->consumed_cache_size(), std::memory_order_relaxed);
          if (__builtin_expect(v < 0, 0)) {
            evict();
          }
        } else {
          safely_delete(ptr);
        }
        return true;
      }
    }
    recycle_node(new_page);
    return false;
  }
}

inline const TreeCacheEntry *TreeCache::search_from_cache(const Key &k, GlobalAddress& addr, GlobalAddress& sibling_addr, uint16_t& level) {
  const uint64_t k_int = key2int(k);
#ifdef CACHE_MORE_INTERNAL_NODE
  auto entry = seek_entry_int(k_int);  // seek the lowest cached internal node surrounding k
#else
  auto entry = find_entry_int(k_int);
#endif
  InternalNode *node = entry ? entry->ptr : nullptr;
  if (!node) return nullptr;

  if (key2int(entry->from) <= k_int && k_int <= key2int(entry->to)) {
    thread_local uint8_t freq_sample = 0;
    if (__builtin_expect(++freq_sample == 0, 0)) {
      __atomic_fetch_add(&(entry->cache_entry_freq), 256, __ATOMIC_RELAXED);
    }

    auto& records = node->records;
    // the cached internal nodes are kv-sorted, followed by kkeyNull (0ULL)
    const uint64_t k0 = key2int(records[0].key);
    if (__builtin_expect(k0 == 0ULL, 0) || k_int < k0) {
      addr = node->metadata.leftmost_ptr;
      sibling_addr = (__builtin_expect(k0 == 0ULL, 0) ? node->metadata.sibling_leftmost_ptr : records[0].ptr);
    }
    else {
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
        addr = records[lo - 1].ptr;
        sibling_addr = (key2int(records[lo].key) == 0ULL ? node->metadata.sibling_leftmost_ptr : records[lo].ptr);
      } else {
        addr = records[define::internalSpanSize - 1].ptr;
        sibling_addr = node->metadata.sibling_leftmost_ptr;
      }
    }
    level = node->metadata.level;

    compiler_barrier();
    if (entry->ptr) { // check if it is freed/invalidated
      return entry;
    }
  }
  return nullptr;
}

inline const TreeCacheEntry *TreeCache::search_ptr_from_cache(const Key &k, GlobalAddress& addr, const uint16_t& level) {  // get the addr of the internal node with a target level, surrounding k
#ifndef CACHE_MORE_INTERNAL_NODE
  return nullptr;
#endif
  TreeCacheSkipList::Iterator iter(skiplist);

  const uint64_t k_int = key2int(k);
  iter.SeekPoint(k_int);

  while (iter.Valid()) {
    auto entry = (const TreeCacheEntry *)iter.key();
    if (key2int(entry->from) > k_int || key2int(entry->to) < k_int) {
      return nullptr;
    }
    InternalNode *node = entry->ptr;
    if (node) {
      if (node->metadata.level == level + 1) {  // is the parent node
        thread_local uint8_t freq_sample = 0;
        if (__builtin_expect(++freq_sample == 0, 0)) {
          __atomic_fetch_add(&(entry->cache_entry_freq), 256, __ATOMIC_RELAXED);
        }

        auto& records = node->records;
        const uint64_t k0 = key2int(records[0].key);
        if (__builtin_expect(k0 == 0ULL, 0) || k_int < k0) {
          addr = node->metadata.leftmost_ptr;
        }
        else {
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
          addr = records[lo - 1].ptr;
        }
        return entry;
      }
    }
    iter.Next();
  }
  return nullptr;
}

inline void TreeCache::search_range_from_cache(const Key &from, const Key &to, std::vector<InternalNode> &result) {
  TreeCacheSkipList::Iterator iter(skiplist);

  result.clear();
  iter.SeekPoint(key2int(from));

  const uint64_t to_int = key2int(to);
  while (iter.Valid()) {
    auto entry = (const TreeCacheEntry *)iter.key();
    InternalNode *node = entry->ptr;
    if (node) {
      if (key2int(entry->from) > to_int) {
        return;
      }
      if (node->metadata.level == 1) result.push_back(*node);  // filter: level == 1
    }
    iter.Next();
  }
}

inline void TreeCache::search_range_ptrs_from_cache(const Key &from, const Key &to, std::vector<const InternalNode *> &result) {
  TreeCacheSkipList::Iterator iter(skiplist);

  result.clear();
  iter.SeekPoint(key2int(from));

  const uint64_t to_int = key2int(to);
  while (iter.Valid()) {
    auto entry = (const TreeCacheEntry *)iter.key();
    const InternalNode *node = entry->ptr;
    if (node) {
      if (key2int(entry->from) > to_int) {
        return;
      }
      if (node->metadata.level == 1) result.push_back(node);  // filter: level == 1
    }
    iter.Next();
  }
}

inline bool TreeCache::invalidate(const TreeCacheEntry *entry) {
  auto ptr = entry->ptr;

  if (ptr == nullptr) {
    return false;
  }
  if (__sync_bool_compare_and_swap(&(entry->ptr), ptr, 0)) {
    free_size.fetch_add(ptr->consumed_cache_size(), std::memory_order_relaxed);
    safely_delete(ptr);
    return true;
  }
  return false;
}

inline void TreeCache::invalidate_range(const Key &lowest, const Key &highest) {
  auto e = find_entry(lowest, highest);
  if (e && e->from == lowest && e->to == highest - 1) {
    invalidate(e);
  }
}

inline const TreeCacheEntry *TreeCache::get_a_random_entry(uint64_t &freq) {
retry:
#ifdef CACHE_MORE_INTERNAL_NODE
  auto e = this->seek_entry(dsm->getRandomKey());
#else
  auto e = this->fine_entry(dsm->getRandomKey());
#endif

  if (!e) {
    goto retry;
  }
  auto ptr = e->ptr;
  if (!ptr) {
    goto retry;
  }

  freq = e->cache_entry_freq;
  if (e->ptr != ptr) {
    goto retry;
  }
  return e;
}

inline void TreeCache::evict_one() {
  uint64_t freq1, freq2;
  auto e1 = get_a_random_entry(freq1);
  auto e2 = get_a_random_entry(freq2);

  if (freq1 < freq2) {
    invalidate(e1);
  } else {
    invalidate(e2);
  }
}

inline void TreeCache::evict() {
  do {
    evict_one();
  } while (free_size.load() < 0);
}

inline void TreeCache::safely_delete(InternalNode* cached_node) {
  auto &p = thread_pool();
  const uint32_t slot = p.retire_pos++ & (kRetireRingSize - 1);
  InternalNode *old = p.retire_ring[slot];
  p.retire_ring[slot] = cached_node;
  if (old != nullptr) {
    recycle_node(old);
  }
}

inline void TreeCache::statistics() {
  printf(" ----- [TreeCache]:  cache size=%lu MB free_size=%.3lf MB skiplist_node_cnt=%d ----- \n", cache_size, (double)free_size.load() / define::MB, (int)skiplist_node_cnt.load());
  printf("consumed cache size = %.3lf MB\n", (double)cache_size - (double)free_size.load() / define::MB);
}

#endif // _TREE_CACHE_H_
