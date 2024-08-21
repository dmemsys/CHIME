#if !defined(_TREE_CACHE_H_)
#define _TREE_CACHE_H_

#include "TreeCacheEntry.h"
#include "HugePageAlloc.h"
#include "Timer.h"
#include "third_party/inlineskiplist.h"
#include "DSM.h"

#include <tbb/concurrent_queue.h>
#include <queue>
#include <atomic>
#include <vector>


using TreeCacheSkipList = InlineSkipList<TreeCacheEntryComparator>;

class TreeCache {

public:
  TreeCache(int cache_size, DSM* dsm);

  bool add_to_cache(InternalNode *page);
  const TreeCacheEntry *search_from_cache(const Key &k, GlobalAddress& addr, GlobalAddress& sibling_addr, uint16_t& level);
  const TreeCacheEntry *search_ptr_from_cache(const Key &k, GlobalAddress& addr, const uint16_t& level);
  void search_range_from_cache(const Key &from, const Key &to, std::vector<InternalNode> &result);
  bool invalidate(const TreeCacheEntry *entry);
  void statistics();

private:
  void evict_one();
  void evict();

  bool add_entry(const Key &from, const Key &to, InternalNode *ptr);
  const TreeCacheEntry *find_entry(const Key &k);
  const TreeCacheEntry *find_entry(const Key &from, const Key &to);
  const TreeCacheEntry *seek_entry(const Key &k);  // try to return an non-nullptr cache_entry
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

  // GC
  tbb::concurrent_queue<InternalNode*> cached_node_gc;
  static const int safely_free_epoch = 20 * MAX_APP_THREAD * MAX_CORO_NUM;
};

inline TreeCache::TreeCache(int cache_size, DSM* dsm) : cache_size(cache_size), dsm(dsm) {
  skiplist = new TreeCacheSkipList(cmp, &alloc, 21);  // 21 [TUNE]
  free_size.store(define::MB * cache_size);
  skiplist_node_cnt.store(0);
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

inline const TreeCacheEntry *TreeCache::find_entry(const Key &k) {
  return find_entry(k, k + 1);
}

inline const TreeCacheEntry *TreeCache::seek_entry(const Key &k) {
  return seek_entry(k, k + 1);
}

inline bool TreeCache::add_to_cache(InternalNode *page) {
  auto new_page = (InternalNode *)malloc(sizeof(InternalNode));
  memcpy(new_page, page, sizeof(InternalNode));

  auto lowest = page->metadata.fence_keys.lowest;
  auto highest = page->metadata.fence_keys.highest;
  if (this->add_entry(lowest, highest, new_page)) {
    skiplist_node_cnt.fetch_add(1);
    auto v = free_size.fetch_add(-new_page->consumed_cache_size());
    if (v < 0) {
      evict();
    }
    return true;
  }
  else { // conflicted
    auto e = this->find_entry(lowest, highest);
    if (e && e->from == lowest && e->to == highest - 1) {
      auto ptr = e->ptr;
      auto ret_val = __sync_val_compare_and_swap(&(e->ptr), ptr, new_page);
      if (ret_val == ptr) {  // cas success
        if (ret_val == nullptr) {
          auto v = free_size.fetch_add(-new_page->consumed_cache_size());
          if (v < 0) {
            evict();
          }
        }
        return true;
      }
    }
    free(new_page);
    return false;
  }
}

inline const TreeCacheEntry *TreeCache::search_from_cache(const Key &k, GlobalAddress& addr, GlobalAddress& sibling_addr, uint16_t& level) {
#ifdef CACHE_MORE_INTERNAL_NODE
  auto entry = seek_entry(k);  // seek the lowest cached internal node surrounding k
#else
  auto entry = find_entry(k);
#endif
  InternalNode *node = entry ? entry->ptr : nullptr;

  if (node && entry->from <= k && entry->to >= k) {
    __sync_fetch_and_add(&(entry->cache_entry_freq), 1);

    auto& records = node->records;
    // the cached internal nodes are kv-sorted
    if (k < records[0].key) {
      addr = node->metadata.leftmost_ptr;
      sibling_addr = records[0].ptr;  // cached nodes are kv-ordered
    }
    else {
      bool find = false;
      for (int i = 1; i < (int)define::internalSpanSize; ++ i) {
        if (k < records[i].key || records[i].key == define::kkeyNull) {
          find = true;
          addr = records[i - 1].ptr;
          sibling_addr = (records[i].key == define::kkeyNull ? node->metadata.sibling_leftmost_ptr : records[i].ptr);
          break;
        }
      }
      if (!find) {
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

  TreeCacheEntry e;
  e.from = k;
  e.to = k;
  iter.Seek((char *)&e);

  while (iter.Valid()) {
    auto entry = (const TreeCacheEntry *)iter.key();
    if (entry->from > k || entry->to < k) {
      return nullptr;
    }
    InternalNode *node = entry->ptr;
    if (node) {
      if (node->metadata.level == level + 1) {  // is the parent node
        __sync_fetch_and_add(&(entry->cache_entry_freq), 1);

        auto& records = node->records;
        if (k < records[0].key) {
          addr = node->metadata.leftmost_ptr;
        }
        else {
          bool find = false;
          for (int i = 1; i < (int)define::internalSpanSize; ++ i) {
            if (k < records[i].key || records[i].key == define::kkeyNull) {
              find = true;
              addr = records[i - 1].ptr;
              break;
            }
          }
          if (!find) addr = records[define::internalSpanSize - 1].ptr;
        }
        return (const TreeCacheEntry *)iter.key();
      }
    }
    iter.Next();
  }
  return nullptr;
}

inline void TreeCache::search_range_from_cache(const Key &from, const Key &to, std::vector<InternalNode> &result) {
  TreeCacheSkipList::Iterator iter(skiplist);

  result.clear();
  TreeCacheEntry e;
  e.from = from;
  e.to = from;
  iter.Seek((char *)&e);

  while (iter.Valid()) {
    auto entry = (const TreeCacheEntry *)iter.key();
    if (entry->ptr) {
      if (entry->from > to) {
        return;
      }
      if (entry->ptr->metadata.level == 1) result.push_back(*(entry->ptr));  // filter: level == 1
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
    free_size.fetch_add(ptr->consumed_cache_size());
    safely_delete(ptr);
    return true;
  }
  return false;
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
  cached_node_gc.push(cached_node);
  while (cached_node_gc.unsafe_size() > safely_free_epoch) {
    InternalNode* next;
    if (cached_node_gc.try_pop(next)) {
      delete next;
    }
  }
}

inline void TreeCache::statistics() {
  printf(" ----- [TreeCache]:  cache size=%lu MB free_size=%.3lf MB skiplist_node_cnt=%d ----- \n", cache_size, (double)free_size.load() / define::MB, (int)skiplist_node_cnt.load());
  printf("consumed cache size = %.3lf MB\n", (double)cache_size - (double)free_size.load() / define::MB);
}

#endif // _TREE_CACHE_H_
