#ifndef __GLOBAL_ADDRESS_H__
#define __GLOBAL_ADDRESS_H__

#include "Common.h"

#include <iostream>

/* Global Address */
class GlobalAddress {
public:
  union {
  struct {
  uint64_t nodeID : 16;
  uint64_t offset : 48;
  };
  uint64_t val;
  };

  GlobalAddress() : val(0) {}
  GlobalAddress(uint64_t nodeID, uint64_t offset) : nodeID(nodeID), offset(offset) {}
  GlobalAddress(uint64_t val) : val(val) {}
  GlobalAddress(const GlobalAddress& gaddr) : val(gaddr.val) {}

  // operator uint64_t() const { return val; }
  uint64_t to_uint64() const { return val; }

  static GlobalAddress Null() {
    return GlobalAddress();
  };

  static GlobalAddress Max() {
    return GlobalAddress(MEMORY_NODE_NUM, 0);
  };

  GlobalAddress operator+(int b) const {
    return GlobalAddress(nodeID, offset + b);
  }

  GlobalAddress operator-(int b) const {
    return GlobalAddress(nodeID, offset - b);
  }

  static GlobalAddress Widest() {
    return GlobalAddress(~0ULL);
  };
} __attribute__((packed));

static_assert(sizeof(GlobalAddress) == sizeof(uint64_t));


inline bool operator==(const GlobalAddress &lhs, const GlobalAddress &rhs) {
  return (lhs.nodeID == rhs.nodeID) && (lhs.offset == rhs.offset);
}

inline bool operator<(const GlobalAddress &lhs, const GlobalAddress &rhs) {
  return std::make_pair(lhs.nodeID, lhs.offset) < std::make_pair(rhs.nodeID, rhs.offset);
}

inline bool operator!=(const GlobalAddress &lhs, const GlobalAddress &rhs) {
  return !(lhs == rhs);
}

inline std::ostream &operator<<(std::ostream &os, const GlobalAddress &obj) {
  os << "[" << (int)obj.nodeID << ", 0x" << std::hex << obj.offset << std::dec << "]";
  return os;
}


#endif /* __GLOBAL_ADDRESS_H__ */
