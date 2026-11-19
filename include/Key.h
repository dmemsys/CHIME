#if !defined(_KEY_H_)
#define _KEY_H_

#include "Common.h"


inline uint8_t get_partial(const Key& key, int depth) {
  return depth == 0 ? 0 : key.at(depth - 1);
}


inline Key get_leftmost(const Key& key, int depth) {
  Key res{};
  std::copy(key.begin(), key.begin() + depth, res.begin());
  return res;
}


inline Key get_rightmost(const Key& key, int depth) {
  Key res{};
  std::copy(key.begin(), key.begin() + depth, res.begin());
  std::fill(res.begin() + depth, res.end(), (1UL << 8) - 1);
  return res;
}


using Prefix = std::vector<uint8_t>;
inline Key get_leftmost(const Prefix& prefix) {
  Key res{};
  std::copy(prefix.begin(), prefix.end(), res.begin());
  return res;
}


inline Key get_rightmost(const Prefix& prefix) {
  Key res{};
  std::copy(prefix.begin(), prefix.end(), res.begin());
  std::fill(res.begin() + prefix.size(), res.end(), (1UL << 8) - 1);
  return res;
}


inline Key remake_prefix(const Key& key, int depth, uint8_t diff_partial) {
  Key res{};
  if (depth > 0) {
    std::copy(key.begin(), key.begin() + depth - 1, res.begin());
    res.at(depth - 1) = diff_partial;
  }
  return res;
}


inline int longest_common_prefix(const Key &k1, const Key &k2, int depth) {
  assert((uint32_t)depth <= define::keyLen);

  int idx, max_cmp = define::keyLen - depth;

  for (idx = 0; idx <= max_cmp; ++ idx) {
    if (get_partial(k1, depth + idx) != get_partial(k2, depth + idx))
      return idx;
  }
  return idx;
}

inline uint64_t key_to_u64_raw(const Key& key) {
  static_assert(define::keyLen == sizeof(uint64_t), "Key fast path expects 8-byte keys");
  uint64_t raw;
  __builtin_memcpy(&raw, key.data(), sizeof(uint64_t));
  return raw;
}

inline uint64_t key2int(const Key& key) {
  return __builtin_bswap64(key_to_u64_raw(key));
}

inline void add_one(Key& a) {
  uint64_t be = __builtin_bswap64(key2int(a) + 1ULL);
  __builtin_memcpy(a.data(), &be, sizeof(uint64_t));
}

inline Key operator+(const Key& a, uint8_t b) {
  Key res;
  uint64_t be = __builtin_bswap64(key2int(a) + (uint64_t)b);
  __builtin_memcpy(res.data(), &be, sizeof(uint64_t));
  return res;
}

inline Key operator-(const Key& a, uint8_t b) {
  Key res;
  uint64_t be = __builtin_bswap64(key2int(a) - (uint64_t)b);
  __builtin_memcpy(res.data(), &be, sizeof(uint64_t));
  return res;
}

inline Key int2key(uint64_t key) {
#ifdef KEY_SPACE_LIMIT
  key = key % (define::kKeyMax - define::kKeyMin) + define::kKeyMin;
#else
  if (key == 0ULL) key = define::kKeyMin;
#endif
  Key res;
  uint64_t be = __builtin_bswap64(key);
  __builtin_memcpy(res.data(), &be, sizeof(uint64_t));
  return res;
}

inline Key str2key(const std::string &key) {
  // assert(key.size() <= define::keyLen);
  Key res{};
  std::copy(key.begin(), key.size() <= define::keyLen ? key.end() : key.begin() + define::keyLen, res.begin());
  return res;
}

inline uint16_t key2fp(const Key& key) {
  return (static_cast<uint16_t>(key[define::keyLen - 1]) << 8) | key[define::keyLen - 2];
}

#endif // _KEY_H_
