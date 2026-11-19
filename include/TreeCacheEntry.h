#if !defined(_TREE_CACHE_ENTRY_H_)
#define _TREE_CACHE_ENTRY_H_

#include "Common.h"
#include "InternalNode.h"
#include "Key.h"
#include "Tree.h"

struct TreeCacheEntry {
  Key from;
  Key to; // [from, to]
  mutable uint64_t cache_entry_freq;
  mutable InternalNode *ptr;
}
 __attribute__((packed));

static_assert(sizeof(TreeCacheEntry) == 2 * sizeof(Key) + sizeof(uint64_t) * 2);

inline std::ostream &operator<<(std::ostream &os, const TreeCacheEntry &obj) {
  os << "[" << key2int(obj.from) << ", " << key2int(obj.to + 1) << ")";
  return os;
}

inline static TreeCacheEntry Decode(const char *val) { return *(TreeCacheEntry *)val; }

struct TreeCacheEntryComparator {
  typedef TreeCacheEntry DecodedType;

  static DecodedType decode_key(const char *b) { return Decode(b); }

  int cmp(const DecodedType& a_v, const DecodedType& b_v) const {  // entry larger => (to larger, from smaller)
    const uint64_t a_to = key2int(a_v.to);
    const uint64_t b_to = key2int(b_v.to);
    if (a_to != b_to) {
      return a_to < b_to ? -1 : +1;
    }
    const uint64_t a_from = key2int(a_v.from);
    const uint64_t b_from = key2int(b_v.from);
    if (a_from != b_from) {
      return a_from < b_from ? +1 : -1;
    }
    return 0;
  }

  int operator()(const char *a, const char *b) const {
    uint64_t a_to, b_to;
    __builtin_memcpy(&a_to, a + sizeof(Key), sizeof(uint64_t));
    __builtin_memcpy(&b_to, b + sizeof(Key), sizeof(uint64_t));
    a_to = __builtin_bswap64(a_to);
    b_to = __builtin_bswap64(b_to);
    if (a_to != b_to) {
      return a_to < b_to ? -1 : +1;
    }
    uint64_t a_from, b_from;
    __builtin_memcpy(&a_from, a, sizeof(uint64_t));
    __builtin_memcpy(&b_from, b, sizeof(uint64_t));
    a_from = __builtin_bswap64(a_from);
    b_from = __builtin_bswap64(b_from);
    return a_from != b_from ? (a_from < b_from ? +1 : -1) : 0;
  }

  int operator()(const char *a, const DecodedType& b) const {
    uint64_t a_to;
    __builtin_memcpy(&a_to, a + sizeof(Key), sizeof(uint64_t));
    a_to = __builtin_bswap64(a_to);
    const uint64_t b_to = key2int(b.to);
    if (a_to != b_to) {
      return a_to < b_to ? -1 : +1;
    }
    uint64_t a_from;
    __builtin_memcpy(&a_from, a, sizeof(uint64_t));
    a_from = __builtin_bswap64(a_from);
    const uint64_t b_from = key2int(b.from);
    return a_from != b_from ? (a_from < b_from ? +1 : -1) : 0;
  }

  int cmp_point(const char *a, uint64_t k_int) const {
    uint64_t a_to;
    __builtin_memcpy(&a_to, a + sizeof(Key), sizeof(uint64_t));
    a_to = __builtin_bswap64(a_to);
    if (a_to != k_int) {
      return a_to < k_int ? -1 : +1;
    }
    uint64_t a_from;
    __builtin_memcpy(&a_from, a, sizeof(uint64_t));
    a_from = __builtin_bswap64(a_from);
    return a_from != k_int ? (a_from < k_int ? +1 : -1) : 0;
  }
};

#endif // _TREE_CACHE_ENTRY_H_
