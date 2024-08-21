#if !defined(_HASH_H_)
#define _HASH_H_

#include "Common.h"
#include "Key.h"
#include "GlobalAddress.h"
#include "city.h"


inline uint64_t get_hashed_local_lock_index(const Key& k) {
  // return CityHash64((char *)&k, sizeof(k)) % define::kLocalLockNum;
  uint64_t res = 0, cnt = 0;
  for (auto a : k) if (cnt ++ < 8) res = (res << 8) + a;
  return res % define::kLocalLockNum;
}


inline uint64_t get_hashed_local_lock_index(const GlobalAddress& addr) {
  // return CityHash64((char *)&addr, sizeof(addr)) % define::kLocalLockNum;
  return addr.to_uint64() % define::kLocalLockNum;
}

inline uint64_t get_hashed_leaf_entry_index(const Key& k) {
  return CityHash64((char *)&k, sizeof(k)) % define::leafSpanSize;
}

#define HASH_TABLE_SIZE 1000000
#define HASH_BUCKET_SIZE 16

inline uint64_t get_hashed_cache_table_index(const GlobalAddress& leaf_addr, int kv_idx) {
  // auto info = std::make_pair(leaf_addr, kv_idx);
  // return CityHash64((char *)&info, sizeof(info)) % HASH_TABLE_SIZE;
  return (leaf_addr.to_uint64() + kv_idx) % HASH_TABLE_SIZE;
}

#endif // _HASH_H_
