#if !defined(_IDX_CACHE_H_)
#define _IDX_CACHE_H_

#include "Common.h"
#include "Key.h"
#include "Hash.h"
#include "GlobalAddress.h"
#include "DSM.h"

#include <atomic>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#define FOOTPRINT_SPECULATIVE_READ


struct IdxCacheEntry {
  GlobalAddress leaf_addr;  // key-1
  uint16_t kv_idx;          // key-2
  mutable uint16_t fingerprint;
  mutable int32_t cache_entry_freq;

  IdxCacheEntry() : leaf_addr(), kv_idx(0), fingerprint(0), cache_entry_freq(0) {}
  IdxCacheEntry(const GlobalAddress& leaf_addr, int kv_idx, const Key& k)
      : leaf_addr(leaf_addr), kv_idx((uint16_t)kv_idx), fingerprint(key2fp(k)), cache_entry_freq(1) {}
} __attribute__((packed));

static_assert(sizeof(IdxCacheEntry) == sizeof(GlobalAddress) + sizeof(uint16_t) * 2 + sizeof(int32_t));
static_assert(define::leafSpanSize <= 64, "IdxCache packs 6-bit kv_idx");


class IdxCache {

public:
  IdxCache(int cache_size, DSM* dsm);
  ~IdxCache();

  bool add_to_cache(const GlobalAddress& leaf_addr, int kv_idx, const Key& k);
  bool search_idx_from_cache(const GlobalAddress& leaf_addr, int l_idx, int r_idx, const Key &k, int& kv_idx);
  void invalidate(const GlobalAddress& leaf_addr, const Key& k);
  void statistics();

private:
  static constexpr uint32_t kWays = 8;

  // Each 8-way set fits in one 64-byte cache line.
  // Packed 64-bit word (0ULL == empty):
  //   bits [63:14] : 50-bit non-zero tag (bit 49 forced to 1)
  //   bits [13: 8] : 6-bit kv_idx (0..63)
  //   bits [ 7: 0] : 8-bit saturating LFU counter (1..255)
  struct alignas(64) CacheSet {
    uint64_t slots[kWays];
  };
  static_assert(sizeof(CacheSet) == 64);

  struct alignas(64) ThreadTick {
    uint32_t freq_tick = 0;
  };

  inline void compute_set_and_tag(const GlobalAddress& leaf_addr, const Key& k,
                                  uint64_t& set_idx, uint64_t& tag50) const noexcept {
    const uint64_t a = leaf_addr.to_uint64();
    const uint64_t rk = key_to_u64_raw(k);
    const uint64_t h1 = chime_hash_mix64(a ^ (rk * 0x9e3779b97f4a7c15ULL));
    const uint64_t h2 = chime_hash_mix64(rk ^ (a * 0xd1b54a32d192ed03ULL));
    set_idx = (uint64_t)(((__uint128_t)h1 * num_sets_) >> 64);
    tag50 = (h2 >> 14) | (1ULL << 49);
  }

  static inline uint64_t pack_slot(uint64_t tag50, uint32_t kv_idx, uint32_t freq) noexcept {
    return (tag50 << 14) | ((uint64_t)(kv_idx & 63u) << 8) | (uint64_t)(freq & 0xffu);
  }

  ThreadTick& get_tls() noexcept {
    int tid = dsm ? dsm->getMyThreadID() : 0;
    if (__builtin_expect(tid < 0 || tid >= MAX_APP_THREAD + 2, 0)) tid = MAX_APP_THREAD + 2;
    return tls_[tid];
  }

  uint64_t cache_size; // MB
  std::atomic<int64_t> free_size;
  DSM *dsm;
  bool is_disabled;
  uint64_t num_sets_;
  CacheSet* sets_;
  ThreadTick tls_[MAX_APP_THREAD + 4];
};


inline IdxCache::IdxCache(int cache_size, DSM* dsm)
    : cache_size(cache_size), free_size((int64_t)(define::MB * cache_size)),
      dsm(dsm), is_disabled(cache_size <= 0), num_sets_(0), sets_(nullptr) {
  if (!is_disabled) {
    const size_t total_bytes = ((size_t)cache_size * define::MB) & ~(sizeof(CacheSet) - 1);
    num_sets_ = total_bytes / sizeof(CacheSet);
    void* mem = nullptr;
    if (num_sets_ == 0 || posix_memalign(&mem, 64, num_sets_ * sizeof(CacheSet)) != 0 || !mem) {
      is_disabled = true;
      num_sets_ = 0;
      sets_ = nullptr;
      return;
    }
    std::memset(mem, 0, num_sets_ * sizeof(CacheSet));
    sets_ = static_cast<CacheSet*>(mem);
  }
}

inline IdxCache::~IdxCache() {
  std::free(sets_);
}


inline bool IdxCache::add_to_cache(const GlobalAddress& leaf_addr, int kv_idx, const Key& k) {
  if (__builtin_expect(is_disabled, 0)) {
    return false;
  }
  uint64_t set_idx, tag50;
  compute_set_and_tag(leaf_addr, k, set_idx, tag50);
  CacheSet& set = sets_[set_idx];
  const uint32_t target_idx = (uint32_t)kv_idx & 63u;

  for (uint32_t i = 0; i < kWays; ++i) {
    uint64_t w = __atomic_load_n(&set.slots[i], __ATOMIC_RELAXED);
    if ((w >> 14) == tag50) {
      const uint32_t cur_idx = (uint32_t)((w >> 8) & 63u);
      const uint32_t cur_freq = (uint32_t)(w & 0xffu);
      if (cur_idx != target_idx) {
        const uint64_t nw = pack_slot(tag50, target_idx, cur_freq < 2u ? 2u : cur_freq);
        __atomic_compare_exchange_n(&set.slots[i], &w, nw, true, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
      } else if (cur_freq < 16u) {
        const uint64_t nw = pack_slot(tag50, target_idx, cur_freq + 1u);
        __atomic_store_n(&set.slots[i], nw, __ATOMIC_RELAXED);
      } else if (cur_freq < 255u) {
        auto& tls = get_tls();
        if (__builtin_expect((++tls.freq_tick & 15u) == 0, 0)) {
          const uint32_t nf = (cur_freq + 8u <= 255u) ? (cur_freq + 8u) : 255u;
          __atomic_store_n(&set.slots[i], pack_slot(tag50, target_idx, nf), __ATOMIC_RELAXED);
        }
      }
      return true;
    }
  }

  const uint32_t rot = (uint32_t)(tag50 & 7u);
  int victim_way = -1;
  uint32_t min_freq = 256u;
  uint64_t victim_old = 0ULL;

  for (uint32_t step = 0; step < kWays; ++step) {
    const uint32_t way = (step + rot) & (kWays - 1);
    uint64_t w = __atomic_load_n(&set.slots[way], __ATOMIC_RELAXED);
    if (w == 0ULL) {
      const uint64_t nw = pack_slot(tag50, target_idx, 2u);
      if (__atomic_compare_exchange_n(&set.slots[way], &w, nw, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
        return true;
      }
      if ((w >> 14) == tag50) {
        return true;
      }
    }
    const uint32_t f = (uint32_t)(w & 0xffu);
    if (f < min_freq) {
      min_freq = f;
      victim_way = (int)way;
      victim_old = w;
    }
  }

  // TinyLFU clock-hand aging on occupied sets so newly hot keys (e.g. read-latest) displace stale hot keys.
  const uint32_t age_way = ((uint32_t)victim_way + 1u) & (kWays - 1);
  uint64_t age_w = __atomic_load_n(&set.slots[age_way], __ATOMIC_RELAXED);
  const uint32_t age_f = (uint32_t)(age_w & 0xffu);
  if (age_w != 0ULL && age_f > 2u) {
    const uint64_t aged = (age_w & ~0xffULL) | (uint64_t)(age_f >> 1);
    __atomic_compare_exchange_n(&set.slots[age_way], &age_w, aged, true, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
  }

  const uint64_t nw = pack_slot(tag50, target_idx, 2u);
  return __atomic_compare_exchange_n(&set.slots[victim_way], &victim_old, nw, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
}


inline bool IdxCache::search_idx_from_cache(const GlobalAddress& leaf_addr, int l_idx, int r_idx, const Key &k, int& kv_idx) {
  if (__builtin_expect(is_disabled, 0)) {
    return false;
  }
  uint64_t set_idx, tag50;
  compute_set_and_tag(leaf_addr, k, set_idx, tag50);
  const CacheSet& set = sets_[set_idx];

  for (uint32_t i = 0; i < kWays; ++i) {
    const uint64_t w = __atomic_load_n(&set.slots[i], __ATOMIC_RELAXED);
    if ((w >> 14) == tag50) {
      const int idx = (int)((w >> 8) & 63u);
      const bool in_range = (r_idx >= l_idx) ? (idx >= l_idx && idx <= r_idx)
                                             : (idx >= l_idx || idx <= r_idx);
      if (__builtin_expect(in_range, 1)) {
        kv_idx = idx;
        return true;
      }
      return false;
    }
  }
  return false;
}


inline void IdxCache::invalidate(const GlobalAddress& leaf_addr, const Key& k) {
  if (__builtin_expect(is_disabled, 0)) {
    return;
  }
  uint64_t set_idx, tag50;
  compute_set_and_tag(leaf_addr, k, set_idx, tag50);
  CacheSet& set = sets_[set_idx];
  for (uint32_t i = 0; i < kWays; ++i) {
    uint64_t w = __atomic_load_n(&set.slots[i], __ATOMIC_RELAXED);
    if ((w >> 14) == tag50) {
      __atomic_compare_exchange_n(&set.slots[i], &w, 0ULL, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
    }
  }
}


inline void IdxCache::statistics() {
  if (!is_disabled && sets_) {
    uint64_t used_slots = 0;
    for (uint64_t s = 0; s < num_sets_; ++s) {
      for (uint32_t w = 0; w < kWays; ++w) {
        if (__atomic_load_n(&sets_[s].slots[w], __ATOMIC_RELAXED) != 0ULL) {
          ++used_slots;
        }
      }
    }
    const int64_t total_b = (int64_t)(cache_size * define::MB);
    const int64_t used_b = (int64_t)(used_slots * sizeof(uint64_t));
    free_size.store(total_b > used_b ? (total_b - used_b) : 0, std::memory_order_relaxed);
  }
  printf(" ----- [IdxCache]:  cache size=%lu MB free_size=%.3lf MB ---- \n", cache_size, (double)free_size.load() / define::MB);
  printf("consumed hotspot buffer size = %lf MB\n\n", (double)cache_size - (double)free_size.load() / define::MB);
}

#endif // _IDX_CACHE_H_
