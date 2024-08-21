#if !defined(_INTERNAL_NODE_H_)
#define _INTERNAL_NODE_H_

#include "Metadata.h"


/* Internal Node Metadata: [obj_version, fence keys, sibling pointer] */
class InternalMetadata {
public:
  PackedVersion h_version;
  // metadata
  uint8_t level;  // leaf node level = 0, internal node level >= 1
  uint8_t valid;
  GlobalAddress sibling_ptr;
  GlobalAddress leftmost_ptr;
  GlobalAddress sibling_leftmost_ptr;
  FenceKeys fence_keys;

public:
  InternalMetadata() : h_version(), level(1), valid(1), sibling_ptr(), leftmost_ptr(), sibling_leftmost_ptr(), fence_keys() {}
} __attribute__((packed));

static_assert(sizeof(InternalMetadata) == define::internalMetadataSize);


/* Internal Entry */
class InternalEntry {
public:
  PackedVersion h_version;
  // kp
  Key key;
  GlobalAddress ptr;

public:
  InternalEntry() : h_version(), key(define::kkeyNull), ptr() {}
  InternalEntry(const Key& k, const GlobalAddress& ptr) : key(k), ptr(ptr) {}

  void update(const Key& k, const GlobalAddress& ptr) { key = k, this->ptr = ptr; }

  static InternalEntry Null() {
    static InternalEntry _zero;
    return _zero;
  };
} __attribute__((packed));

static_assert(sizeof(InternalEntry) == define::internalEntrySize);


/* Internal Node */
class InternalNode {  // must be cacheline-align
public:
  // cacheline-versions will be embedded from here with an 64-byte offset (can be skipped if obj version is here)
  InternalMetadata metadata;
  InternalEntry records[define::internalSpanSize];

public:
  InternalNode() : metadata(), records{} {}

  bool is_root() const {
    return metadata.fence_keys == FenceKeys::Widest();
  }

  int64_t consumed_cache_size() const {
    int cnt = 0;
    for (const auto& e : records) if (e.key != define::kkeyNull) ++ cnt;
    return sizeof(uint64_t) + sizeof(InternalMetadata) + cnt * sizeof(InternalEntry);
  }

  static const bool IS_LEAF = false;
} __attribute__((packed));

static_assert(sizeof(InternalNode) == sizeof(InternalMetadata) + sizeof(InternalEntry) * define::internalSpanSize);

inline bool operator==(const InternalNode &lhs, const InternalNode &rhs) {
  return !strncmp((char *)&lhs, (char*)&rhs, sizeof(InternalNode));
}


#endif // _INTERNAL_NODE_H_
