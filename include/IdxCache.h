#if !defined(_IDX_CACHE_H_)
#define _IDX_CACHE_H_

#include "Key.h"
#include "Common.h"
#include "Key.h"
#include "Hash.h"
#include "GlobalAddress.h"
#include "DSM.h"

#include <tbb/concurrent_queue.h>
#include <atomic>
#include <vector>
#include <random>

#define FOOTPRINT_SPECULATIVE_READ


struct IdxCacheEntry {
  GlobalAddress leaf_addr;  // key-1
  uint16_t kv_idx;          // key-2
  mutable uint16_t fingerprint;
  mutable int32_t cache_entry_freq;

  IdxCacheEntry(const GlobalAddress& leaf_addr, int kv_idx, const Key& k) : leaf_addr(leaf_addr), kv_idx(kv_idx), fingerprint(key2fp(k)), cache_entry_freq(1) {}
} __attribute__((packed));

static_assert(sizeof(IdxCacheEntry) == sizeof(GlobalAddress) + sizeof(uint16_t) * 2 + sizeof(int32_t));


class IdxCache {

public:
  IdxCache(int cache_size, DSM* dsm);

  bool add_to_cache(const GlobalAddress& leaf_addr, int kv_idx, const Key& k);
  bool search_idx_from_cache(const GlobalAddress& leaf_addr, int l_idx, int r_idx, const Key &k, int& kv_idx);
  void statistics();

private:
  void evict_one();
  void evict();
  void safely_delete(IdxCacheEntry* cache_entry);

private:
  uint64_t cache_size; // MB;
  std::atomic<int64_t> free_size;
  std::atomic<int64_t> delay_cnt;
  DSM *dsm;
  bool is_disabled;

  // HashTable
  static const int BUCKET_SIZE = std::max(HASH_BUCKET_SIZE, (int)ROUND_UP(define::kHotspotBufSize  * define::MB / sizeof(IdxCacheEntry*) / HASH_TABLE_SIZE, 3));
  IdxCacheEntry* hash_table[HASH_TABLE_SIZE][BUCKET_SIZE];

  // GC
  tbb::concurrent_queue<IdxCacheEntry*> cache_entry_gc;
  static const int safely_free_epoch = 20 * MAX_APP_THREAD * MAX_CORO_NUM;
};


inline IdxCache::IdxCache(int cache_size, DSM* dsm) : cache_size(cache_size), dsm(dsm) {
  memset(hash_table, 0, sizeof(IdxCacheEntry*) * HASH_TABLE_SIZE * BUCKET_SIZE);
  free_size.store(define::MB * cache_size);
  delay_cnt.store(0);
  is_disabled = (cache_size == 0);
}


inline bool IdxCache::add_to_cache(const GlobalAddress& leaf_addr, int kv_idx, const Key& k) {
  if (is_disabled) {
    return false;
  }
  uint64_t ht_idx = get_hashed_cache_table_index(leaf_addr, kv_idx);
  auto& bucket = hash_table[ht_idx];
  // search from existing slot
  for (const auto& e : bucket) if (e && e->leaf_addr == leaf_addr && e->kv_idx == kv_idx) {
    if (e->fingerprint != key2fp(k)) {
      __sync_bool_compare_and_swap(&(e->fingerprint), e->fingerprint, key2fp(k));
      __sync_bool_compare_and_swap(&(e->cache_entry_freq), e->cache_entry_freq, 1);
    }
    else {
      __sync_fetch_and_add(&(e->cache_entry_freq), 1);
    }
    return true;  // is found
  }
  if (delay_cnt.fetch_add(-1) > 0) {
    return false;
  }
  auto new_entry = new IdxCacheEntry(leaf_addr, kv_idx, k);
  int min_idx = -1, min_freq = INT_MAX;
  // insert into empty slot
  for (int i = 0; i < BUCKET_SIZE; ++ i) {
    auto& e = bucket[i];
    if (!e) {
      if (__sync_bool_compare_and_swap(&e, 0ULL, new_entry)) {
        auto v = free_size.fetch_add(-sizeof(IdxCacheEntry));
        if (v < 0) {
          evict();
        }
        return true;
      }
      delete new_entry;
      return false;
    }
    else if (e->cache_entry_freq < min_freq) min_idx = i, min_freq = e->cache_entry_freq;
  }
  // evict the lfu one
  auto& e = bucket[min_idx];
  if (__sync_bool_compare_and_swap(&e, e, new_entry)) {
    safely_delete(e);
    return true;
  }
  delete new_entry;
  return false;
}


inline bool IdxCache::search_idx_from_cache(const GlobalAddress& leaf_addr, int l_idx, int r_idx, const Key &k, int& kv_idx) {
  thread_local std::vector<std::pair<int, int32_t> > candidates;
  if (is_disabled) {
    return false;
  }

  auto search_for_candidate = [&](int idx){
    uint64_t ht_idx = get_hashed_cache_table_index(leaf_addr, idx);
    auto& bucket = hash_table[ht_idx];
    for (const auto& e : bucket) if (e && e->leaf_addr == leaf_addr && e->kv_idx == idx) {
#ifdef FOOTPRINT_SPECULATIVE_READ
      if (e->fingerprint != key2fp(k)) continue;
#endif
      int32_t cnt = e->cache_entry_freq;
      candidates.emplace_back(std::make_pair(idx, cnt));
    }
    return;
  };

  candidates.clear();
  if (r_idx < l_idx) {
    for (auto idx = 0; idx <= r_idx; ++ idx) {
      search_for_candidate(idx);
    }
    for (auto idx = l_idx; idx < (int)define::leafSpanSize; ++ idx) {
      search_for_candidate(idx);
    }
  }
  else {
    for (auto idx = l_idx; idx <= r_idx; ++ idx) {
      search_for_candidate(idx);
    }
  }
  if (candidates.empty()) return false;
  std::sort(candidates.begin(), candidates.end(), [](const auto& a, const auto& b){
    return a.second > b.second;
  });
  kv_idx = candidates.front().first;
  return true;
}


inline void IdxCache::evict_one() {
  static std::random_device rd;
  static std::mt19937 e(rd());
  static std::uniform_int_distribution<uint64_t> u(0, HASH_TABLE_SIZE-1);
  static std::uniform_int_distribution<uint64_t> v(0, BUCKET_SIZE-1);

  auto ht_idx_1 = u(e);
  auto ht_idx_2 = u(e);
  while (ht_idx_1 == ht_idx_2) ht_idx_2 = u(e);
  auto& bucket_1 = hash_table[ht_idx_1];
  auto& bucket_2 = hash_table[ht_idx_2];

  int min_idx_1 = -1, min_freq_1 = INT_MAX;
  int min_idx_2 = -1, min_freq_2 = INT_MAX;

  auto find_lfu_idx = [](IdxCacheEntry **bucket, int& min_idx, int& min_freq){
    for (int i = 0; i < BUCKET_SIZE; ++ i) {
      const auto& e = bucket[i];
      if (e && e->cache_entry_freq < min_freq) min_idx = i, min_freq = e->cache_entry_freq;
    }
  };

  // find lfu one
  find_lfu_idx(bucket_1, min_idx_1, min_freq_1);
  find_lfu_idx(bucket_2, min_idx_2, min_freq_2);
  if (min_idx_1 < 0 && min_idx_2 < 0) return;

  // erase an entry
  auto& entry = (min_freq_1 < min_freq_2 ? bucket_1[min_idx_1] : bucket_2[min_idx_2]);
  if (entry && __sync_bool_compare_and_swap(&entry, entry, 0ULL)) {
    safely_delete(entry);
    free_size.fetch_add(sizeof(IdxCacheEntry));
  }
  return;
}

inline void IdxCache::evict() {
  delay_cnt.store(10);
  do {
    evict_one();
  } while (free_size.load() < 0);
}


inline void IdxCache::safely_delete(IdxCacheEntry* cache_entry) {
  cache_entry_gc.push(cache_entry);
  while (cache_entry_gc.unsafe_size() > safely_free_epoch) {
    IdxCacheEntry* next;
    if (cache_entry_gc.try_pop(next)) {
      delete next;
    }
  }
}


inline void IdxCache::statistics() {
  printf(" ----- [IdxCache]:  cache size=%lu MB free_size=%.3lf MB ---- \n", cache_size, (double)free_size.load() / define::MB);
  printf("consumed hotspot buffer size = %lf MB\n\n", (double)cache_size - (double)free_size.load() / define::MB);
}

#endif // _IDX_CACHE_H_
