#if !defined(_HASH_H_)
#define _HASH_H_

#include "Common.h"
#include "Key.h"
#include "GlobalAddress.h"
#include "city.h"


inline uint64_t chime_hash_mix64(uint64_t x) noexcept {
  x ^= x >> 30;
  x *= 0xbf58476d1ce4e5b9ULL;
  x ^= x >> 27;
  x *= 0x94d049bb133111ebULL;
  x ^= x >> 31;
  return x;
}

inline uint64_t get_hashed_local_lock_index(const Key& k) {
  static_assert((define::kLocalLockNum & (define::kLocalLockNum - 1)) == 0);
  return chime_hash_mix64(key_to_u64_raw(k)) & (define::kLocalLockNum - 1);
}


inline uint64_t get_hashed_local_lock_index(const GlobalAddress& addr) {
  static_assert((define::kLocalLockNum & (define::kLocalLockNum - 1)) == 0);
  return chime_hash_mix64(addr.to_uint64()) & (define::kLocalLockNum - 1);
}

inline uint64_t get_hashed_leaf_entry_index(const Key& k) {
  return chime_hash_mix64(key_to_u64_raw(k)) % define::leafSpanSize;
}

#define HASH_TABLE_SIZE 1000000
#define HASH_BUCKET_SIZE 16

inline uint64_t get_hashed_cache_table_index(const GlobalAddress& leaf_addr, int kv_idx) {
  // auto info = std::make_pair(leaf_addr, kv_idx);
  // return CityHash64((char *)&info, sizeof(info)) % HASH_TABLE_SIZE;
  return (leaf_addr.to_uint64() + kv_idx) % HASH_TABLE_SIZE;
}

#endif // _HASH_H_
