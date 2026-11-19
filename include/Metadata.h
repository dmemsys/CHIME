#if !defined(_METADATA_H_)
#define _METADATA_H_

#include "Common.h"
#include "GlobalAddress.h"
#include "Key.h"

#include <vector>
#include <cmath>


/* Packed Global Address */
class PackedGAddr {  // 48-bit, used by node addr/leaf addr (not entry addr)
public:
  uint64_t mn_id    : define::mnIdBit;
  uint64_t offset   : define::offsetBit;

  PackedGAddr() : mn_id(0), offset(0) {}
  PackedGAddr(const GlobalAddress &addr) : mn_id(addr.nodeID), offset(addr.offset >> PACKED_ADDR_ALIGN_BIT) {}
  operator GlobalAddress() const { return GlobalAddress{mn_id, offset << PACKED_ADDR_ALIGN_BIT}; }

  PackedGAddr(uint64_t val) : mn_id(val & ((1UL << define::mnIdBit) - 1)), offset(val >> define::mnIdBit) {}
  operator uint64_t() const { return (offset << define::mnIdBit) | mn_id; }

} __attribute__((packed));

static_assert(sizeof(PackedGAddr) == define::packedGAddrSize);

inline bool operator==(const PackedGAddr &lhs, const PackedGAddr &rhs) {
  return (lhs.mn_id == rhs.mn_id) && (lhs.offset == rhs.offset);
}


#ifdef VACANCY_AWARE_LOCK
class VALOCK {
private:
  uint64_t vacancy_bitmap  : define::vacancyMapBit;
  uint64_t max_key_idx     : define::maxKeyIdxBit;
  uint64_t lock_bit        : 1;

public:
  VALOCK(uint64_t vacancy_bitmap, uint64_t max_key_idx) : vacancy_bitmap(vacancy_bitmap), max_key_idx(max_key_idx), lock_bit(0) {}

  void update_vacancy(int l, int r, const int* empty_idxes, int empty_cnt, bool is_leaf=true) {  // [l, r]
    const int span_size = is_leaf ? (int)define::leafSpanSize : (int)define::internalSpanSize;
    const int l_bit = find_bucket(l, span_size), r_bit = find_bucket(r, span_size);
    constexpr uint64_t kValidMask = (1ULL << define::vacancyMapBit) - 1ULL;

    uint64_t vb = vacancy_bitmap;
    if (l_bit <= r_bit) {
      const uint64_t hi = (r_bit + 1 >= 64) ? ~0ULL : ((1ULL << (r_bit + 1)) - 1ULL);
      vb |= (hi & ~((1ULL << l_bit) - 1ULL)) & kValidMask;
    } else {
      const uint64_t lo = ((1ULL << (r_bit + 1)) - 1ULL);
      const uint64_t hi = ~((1ULL << l_bit) - 1ULL) & kValidMask;
      vb |= (lo | hi);
    }
    for (int k = 0; k < empty_cnt; ++ k) {
      int empty_idx = empty_idxes[k];
      int i = find_bucket(empty_idx, span_size);
      if (l_bit <= r_bit) assert(i >= l_bit && i <= r_bit);
      else assert((i >= 0 && i <= r_bit) || (i >= l_bit && i < (int)define::vacancyMapBit));
      vb &= ~(1ULL << i);
    }
    vacancy_bitmap = vb;
  }

  void update_vacancy(int l, int r, const std::vector<int>& empty_idxes, bool is_leaf=true) {  // [l, r]
    update_vacancy(l, r, empty_idxes.data(), (int)empty_idxes.size(), is_leaf);
  }

  int get_read_entry_num_from_bitmap(int start_idx, bool is_leaf) {
    constexpr uint64_t kValidMask = (1ULL << define::vacancyMapBit) - 1ULL;
    const int span_size = is_leaf ? (int)define::leafSpanSize : (int)define::internalSpanSize;
    const uint64_t free_bits = (~(uint64_t)vacancy_bitmap) & kValidMask;
    if (free_bits == 0ULL) {
      return span_size;
    }
    const int s_bit = find_bucket(start_idx, span_size);
    uint64_t hi = free_bits >> s_bit;
    while (hi != 0ULL) {
      const int e_bit = s_bit + __builtin_ctzll(hi);
      auto [l_idx, r_idx] = get_idx_range_in_bucket(e_bit, span_size);
      if (start_idx <= l_idx) {
        int entry_num = (r_idx + span_size - start_idx + 1) % span_size;
        return entry_num ? entry_num : span_size;
      }
      hi &= (hi - 1ULL);
    }
    uint64_t lo = free_bits & ((1ULL << s_bit) - 1ULL);
    while (lo != 0ULL) {
      const int e_bit = __builtin_ctzll(lo);
      auto [l_idx, r_idx] = get_idx_range_in_bucket(e_bit, span_size);
      if (start_idx <= l_idx) {
        int entry_num = (r_idx + span_size - start_idx + 1) % span_size;
        return entry_num ? entry_num : span_size;
      }
      lo &= (lo - 1ULL);
    }
    return span_size;
  }

  void update_max_key_idx(uint64_t max_key_idx) {
    this->max_key_idx = max_key_idx;
  }

  int get_max_key_idx() {
    return max_key_idx;
  }

private:
  int find_bucket(int kv_idx, int span_size) const {  // 0~span_size => 0~bitmap_size
    if (span_size == 64 && (uint32_t)kv_idx < 64u) {
      return (int)(((uint32_t)kv_idx * define::vacancyMapBit) >> 6);
    }
    return (int)((((uint32_t)kv_idx * define::vacancyMapBit) / (uint32_t)span_size) % define::vacancyMapBit);
  }

  static int bucket_first_idx(int bucket_id, int span_size) {
    return (int)(((uint32_t)bucket_id * (uint32_t)span_size + define::vacancyMapBit - 1u) / define::vacancyMapBit);
  }

  std::pair<int, int> get_idx_range_in_bucket(int bucket, int span_size) const {  // 0~bitmap_size => 0~span_size
    int l_idx = bucket_first_idx(bucket, span_size);
    int r_idx = bucket_first_idx(bucket + 1, span_size) - 1;
    if (r_idx >= span_size) r_idx = span_size - 1;
    return std::make_pair(l_idx, r_idx);
  }
} __attribute__((packed));

static_assert(sizeof(VALOCK) == sizeof(uint64_t));
#endif


#ifdef ENABLE_VAR_LEN_KV
class DataPointer {
public:
  uint64_t    data_len : 64 - define::packedGaddrBit;
  PackedGAddr ptr;

  DataPointer(const uint64_t data_len, const GlobalAddress& ptr) : data_len(data_len), ptr(ptr) {}

  operator uint64_t() const { return ((uint64_t)ptr << 16) | data_len; }
  operator std::pair<uint64_t, GlobalAddress>() const { return std::make_pair(data_len, (GlobalAddress)ptr); }
} __attribute__((packed));

static_assert(sizeof(DataPointer) == 8);


class DataBlock {  // For brievity, we assume the whole key can be stored inline, and only changing the value size for evaluation
public:
  uint64_t rest_of_key_len = 0;
  uint64_t value_len;
  // Key rest_of_key;
  union {
  Value value;
  uint8_t _padding[define::indirectValLen];
  };

  DataBlock(const Value value) : rest_of_key_len(0), value_len(define::indirectValLen), value(value) {}
} __attribute__((packed));

static_assert(sizeof(DataBlock) == define::dataBlockLen);
#endif


/* Fence Keys */
struct FenceKeys {
  Key lowest;
  Key highest;

  FenceKeys() : lowest{} { lowest = lowest + define::kKeyMin; highest.fill(0xff); }
  FenceKeys(const Key& lowest, const Key& highest) : lowest(lowest), highest(highest) {}

  static FenceKeys Widest() {
    return FenceKeys();
  };
} __attribute__((packed));

static_assert(sizeof(FenceKeys) == define::keyLen * 2);

inline bool operator==(const FenceKeys &lhs, const FenceKeys &rhs) {
  return (lhs.lowest == rhs.lowest) && (lhs.highest == rhs.highest);
}

inline std::ostream &operator<<(std::ostream &os, const FenceKeys &obj) {
  os << "[" << key2int(obj.lowest) << ", " << key2int(obj.highest) << "]";
  return os;
}


/* Packed Versions */
struct PackedVersion {
  uint8_t node_version  : define::nodeVersionBit;
  uint8_t entry_version : define::entryVersionBit;

  PackedVersion() : node_version(0), entry_version(0) {}
} __attribute__((packed));

static_assert(sizeof(PackedVersion) == define::versionSize);


inline bool operator==(const PackedVersion &lhs, const PackedVersion &rhs) {
  uint8_t a, b;
  __builtin_memcpy(&a, &lhs, 1);
  __builtin_memcpy(&b, &rhs, 1);
  return a == b;
}

inline bool operator!=(const PackedVersion &lhs, const PackedVersion &rhs) {
  return !(lhs == rhs);
}

inline std::ostream &operator<<(std::ostream &os, const PackedVersion &obj) {
  os << "[" << (int)obj.node_version << ", " << (int)obj.entry_version << "]";
  return os;
}

#endif // _METADATA_H_
