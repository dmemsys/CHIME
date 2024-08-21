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

  int cmp(const DecodedType a_v, const DecodedType b_v) const {  // entry larger => (to larger, from smaller)
    if (a_v.to < b_v.to) {
      return -1;
    }

    if (a_v.to > b_v.to) {
      return +1;
    }

    if (a_v.from < b_v.from) {
      return +1;
    } else if (a_v.from > b_v.from) {
      return -1;
    } else {
      return 0;
    }
  }

  int operator()(const char *a, const char *b) const {
    return cmp(Decode(a), Decode(b));
  }

  int operator()(const char *a, const DecodedType b) const {
    return cmp(Decode(a), b);
  }
};

#endif // _TREE_CACHE_ENTRY_H_
