#if !defined(_LEAF_VERSION_MANAGER_H_)
#define _LEAF_VERSION_MANAGER_H_

#include "Common.h"
#include "Metadata.h"
#include "LeafNode.h"

#include <vector>

namespace detail_lvm {
struct GroupOffsetLut {
  uint8_t record_idx[sizeof(LeafEntryGroup)];
  constexpr GroupOffsetLut() : record_idx{} {
    for (size_t off = 0; off < sizeof(LeafEntryGroup); ++off) {
      record_idx[off] = (off < sizeof(ScatteredMetadata))
                            ? 0xFFu
                            : static_cast<uint8_t>((off - sizeof(ScatteredMetadata)) / sizeof(LeafEntry));
    }
  }
};

struct EntryOffsetLut {
  uint8_t idx[define::groupSize];
  constexpr EntryOffsetLut() : idx{} {
    for (size_t off = 0; off < define::groupSize; ++off) {
      idx[off] = static_cast<uint8_t>(off / sizeof(LeafEntry));
    }
  }
};

struct SingleEntryOffsetInfo {
  uint16_t raw_offset;
  uint8_t raw_len;
  uint8_t first_offset;
};

struct SingleEntryLut {
  SingleEntryOffsetInfo info[define::leafSpanSize];
  constexpr SingleEntryLut() : info{} {
    constexpr int kFirstOff = (define::cachelineSize < define::decodedLeafSize)
                                  ? (int)define::cachelineSize
                                  : (int)define::decodedLeafSize;
    for (int idx = 0; idx < (int)define::leafSpanSize; ++idx) {
      const int g = idx / (int)define::neighborSize;
      const int r = idx % (int)define::neighborSize;
      const int dec_start = (int)__builtin_offsetof(ScatteredLeafNode, record_groups) +
                            g * (int)sizeof(LeafEntryGroup) +
                            (int)sizeof(ScatteredMetadata) + r * (int)sizeof(LeafEntry);
      const int dec_end = dec_start + (int)sizeof(LeafEntry);
      int raw_off = 0;
      int first_off = 0;
      const int dist = dec_start - kFirstOff;
      if (dist < 0) {
        raw_off = dec_start;
        const int rem = kFirstOff - dec_start;
        first_off = rem < (int)sizeof(LeafEntry) ? rem : (int)sizeof(LeafEntry);
      } else {
        const int v_cnt = dist / (int)define::blockSize + ((dist % (int)define::blockSize) ? 1 : 0);
        raw_off = dec_start + v_cnt * (int)define::versionSize;
        const int rem = (kFirstOff + v_cnt * (int)define::blockSize) - dec_start;
        first_off = rem < (int)sizeof(LeafEntry) ? rem : (int)sizeof(LeafEntry);
      }
      auto raw_dist = [](int dlen) constexpr -> int {
        if (dlen <= 0) return dlen;
        const int vc = dlen / (int)define::blockSize + ((dlen % (int)define::blockSize) ? 1 : 0);
        return dlen + vc * (int)define::versionSize;
      };
      const int raw_len = raw_dist(dec_end - kFirstOff) - raw_dist(dec_start - kFirstOff);
      info[idx] = SingleEntryOffsetInfo{
          static_cast<uint16_t>(raw_off),
          static_cast<uint8_t>(raw_len),
          static_cast<uint8_t>(first_off)};
    }
  }
};
}  // namespace detail_lvm


class LeafVersionManager {
public:
  LeafVersionManager() {}

  static bool decode_node_versions(char *input_buffer, char *output_buffer);
  static bool decode_segment_versions(char *input_buffer, char *output_buffer, uint64_t first_offset, int entry_num,
                                      uint64_t metadata_offset, uint64_t segment_len, uint8_t& node_version);

  static void encode_node_versions(char *input_buffer, char *output_buffer);
  static void encode_segment_versions(char *input_buffer, char *output_buffer, uint64_t first_offset,
                                      const int* hopped_idxes, int hopped_cnt, int l_idx, int r_idx,
                                      uint64_t metadata_offset, uint64_t segment_len);
  static void encode_segment_versions(char *input_buffer, char *output_buffer, uint64_t first_offset,
                                      const std::vector<int>& hopped_idxes, int l_idx, int r_idx,
                                      uint64_t metadata_offset, uint64_t segment_len);

  static std::tuple<uint64_t, uint64_t, uint64_t> get_offset_info(int start_entry_idx, int entry_num = 1);

  static inline detail_lvm::SingleEntryOffsetInfo get_single_entry_info(int entry_idx) {
    return kSingleEntryLut.info[entry_idx];
  }

  static inline bool decode_single_entry(const char *raw_buf, uint64_t first_offset, LeafEntry &out) {
    if (__builtin_expect(first_offset == sizeof(LeafEntry), 1)) {
      __builtin_memcpy(&out, raw_buf, sizeof(LeafEntry));
      return true;
    }
    char *dst = reinterpret_cast<char *>(&out);
    if (first_offset > 0) {
      memcpy(dst, raw_buf, first_offset);
    }
    const PackedVersion cl_ver = *reinterpret_cast<const PackedVersion *>(raw_buf + first_offset);
    memcpy(dst + first_offset, raw_buf + first_offset + sizeof(PackedVersion), sizeof(LeafEntry) - first_offset);
    return out.h_version == cl_ver;
  }

  static inline void encode_single_entry(LeafEntry &entry, char *encoded_buf, uint64_t first_offset) {
    ++entry.h_version.entry_version;
    if (__builtin_expect(first_offset == sizeof(LeafEntry), 1)) {
      __builtin_memcpy(encoded_buf, &entry, sizeof(LeafEntry));
      return;
    }
    const char *src = reinterpret_cast<const char *>(&entry);
    if (first_offset > 0) {
      memcpy(encoded_buf, src, first_offset);
    }
    *reinterpret_cast<PackedVersion *>(encoded_buf + first_offset) = entry.h_version;
    memcpy(encoded_buf + first_offset + sizeof(PackedVersion), src + first_offset, sizeof(LeafEntry) - first_offset);
  }

private:
  static const int FIRST_OFFSET = std::min(define::cachelineSize, define::decodedLeafSize);  // the address of the first encoded cacheline version
  static inline constexpr detail_lvm::GroupOffsetLut kGroupLut{};
  static inline constexpr detail_lvm::EntryOffsetLut kEntryLut{};
  static inline constexpr detail_lvm::SingleEntryLut kSingleEntryLut{};
};


/* Call after read the entire node. Both entry- and node-level consistency are checked. */
inline bool LeafVersionManager::decode_node_versions(char *input_buffer, char *output_buffer) {
  auto leaf = (ScatteredLeafNode *)output_buffer;

  __builtin_memcpy(output_buffer, input_buffer, FIRST_OFFSET);
  const uint64_t groups_base = (uint64_t)STRUCT_OFFSET(ScatteredLeafNode, record_groups);
  size_t group_idx = (FIRST_OFFSET - groups_base) / sizeof(LeafEntryGroup);
  size_t off = (FIRST_OFFSET - groups_base) % sizeof(LeafEntryGroup);

  uint64_t i = FIRST_OFFSET;
  uint64_t j = FIRST_OFFSET;
  while (j + define::blockSize <= define::decodedLeafSize) {
    auto cacheline_version = *(PackedVersion *)(input_buffer + i);
    i += sizeof(PackedVersion);
    __builtin_memcpy(output_buffer + j, input_buffer + i, define::blockSize);
    i += define::blockSize;
    j += define::blockSize;

    const uint8_t ridx = kGroupLut.record_idx[off];
    PackedVersion obj_version = (ridx == 0xFFu)
                                    ? leaf->record_groups[group_idx].metadata.h_version
                                    : leaf->record_groups[group_idx].records[ridx].h_version;
    if (__builtin_expect(obj_version != cacheline_version, 0)) {
      return false;
    }
    off += define::blockSize;
    if (off >= sizeof(LeafEntryGroup)) {
      off -= sizeof(LeafEntryGroup);
      ++group_idx;
    }
  }
  if (j < define::decodedLeafSize) {
    auto cacheline_version = *(PackedVersion *)(input_buffer + i);
    i += sizeof(PackedVersion);
    memcpy(output_buffer + j, input_buffer + i, define::decodedLeafSize - j);
    const uint8_t ridx = kGroupLut.record_idx[off];
    PackedVersion obj_version = (ridx == 0xFFu)
                                    ? leaf->record_groups[group_idx].metadata.h_version
                                    : leaf->record_groups[group_idx].records[ridx].h_version;
    if (__builtin_expect(obj_version != cacheline_version, 0)) {
      return false;
    }
  }

  // node-level joint consistency check
  int entry_cnt = 0;
  const uint8_t node_version = leaf->record_groups[0].metadata.h_version.node_version;
  for (const auto& group : leaf->record_groups) {
    if (__builtin_expect(node_version != group.metadata.h_version.node_version, 0)) {
      return false;
    }
    for (const auto& entry : group.records) {
      if (entry_cnt++ < (int)define::leafSpanSize &&
          __builtin_expect(node_version != entry.h_version.node_version, 0)) {
        return false;
      }
    }
  }
  return true;
}


/* Call before write the entire node. Only the node_versions will be incremented. */
inline void LeafVersionManager::encode_node_versions(char *input_buffer, char *output_buffer) {
  // increment node versions
  auto leaf = (ScatteredLeafNode *)input_buffer;
  uint8_t node_version = leaf->record_groups[0].metadata.h_version.node_version;
  ++ node_version;
  for (auto& group : leaf->record_groups) {
    group.metadata.h_version.node_version = node_version;
    for (auto& entry : group.records) {
      entry.h_version.node_version = node_version;
    }
  }

  // generate cacheline versions
  __builtin_memcpy(output_buffer, input_buffer, FIRST_OFFSET);
  const uint64_t groups_base = (uint64_t)STRUCT_OFFSET(ScatteredLeafNode, record_groups);
  size_t group_idx = (FIRST_OFFSET - groups_base) / sizeof(LeafEntryGroup);
  size_t off = (FIRST_OFFSET - groups_base) % sizeof(LeafEntryGroup);

  uint64_t i = FIRST_OFFSET;
  uint64_t j = FIRST_OFFSET;
  while (i + define::blockSize <= define::decodedLeafSize) {
    const uint8_t ridx = kGroupLut.record_idx[off];
    PackedVersion obj_version = (ridx == 0xFFu)
                                    ? leaf->record_groups[group_idx].metadata.h_version
                                    : leaf->record_groups[group_idx].records[ridx].h_version;
    __builtin_memcpy(output_buffer + j, &obj_version, sizeof(PackedVersion));
    j += sizeof(PackedVersion);
    __builtin_memcpy(output_buffer + j, input_buffer + i, define::blockSize);
    i += define::blockSize;
    j += define::blockSize;

    off += define::blockSize;
    if (off >= sizeof(LeafEntryGroup)) {
      off -= sizeof(LeafEntryGroup);
      ++group_idx;
    }
  }
  if (i < define::decodedLeafSize) {
    const uint8_t ridx = kGroupLut.record_idx[off];
    PackedVersion obj_version = (ridx == 0xFFu)
                                    ? leaf->record_groups[group_idx].metadata.h_version
                                    : leaf->record_groups[group_idx].records[ridx].h_version;
    __builtin_memcpy(output_buffer + j, &obj_version, sizeof(PackedVersion));
    j += sizeof(PackedVersion);
    memcpy(output_buffer + j, input_buffer + i, define::decodedLeafSize - i);
  }
}


inline std::tuple<uint64_t, uint64_t, uint64_t> LeafVersionManager::get_offset_info(int start_entry_idx, int entry_num) {  // [raw_offset(encoded), raw length(encoded), first_offset(decoded)]
  uint64_t raw_offset, raw_length, first_offset;
  int end_entry_idx = start_entry_idx + entry_num;
  int decoded_start_offset = (start_entry_idx % define::neighborSize ?
                              STRUCT_OFFSET(ScatteredLeafNode, record_groups[start_entry_idx / define::neighborSize].records[start_entry_idx % define::neighborSize]) :
                              STRUCT_OFFSET(ScatteredLeafNode, record_groups[start_entry_idx / define::neighborSize].metadata));
  int decoded_end_offset = (end_entry_idx % define::neighborSize ?
                            STRUCT_OFFSET(ScatteredLeafNode, record_groups[end_entry_idx / define::neighborSize].records[end_entry_idx % define::neighborSize]) :
                            STRUCT_OFFSET(ScatteredLeafNode, record_groups[end_entry_idx / define::neighborSize].metadata));

  // calculate raw_offset, first_offset
  auto dist = decoded_start_offset - FIRST_OFFSET;
  if (dist < 0) {
    raw_offset = decoded_start_offset;
    first_offset = std::min((int)FIRST_OFFSET - decoded_start_offset, decoded_end_offset - decoded_start_offset);
  }
  else {
    auto version_cnt = dist / define::blockSize + (dist % define::blockSize ? 1 : 0);
    raw_offset = decoded_start_offset + version_cnt * define::versionSize;
    first_offset = std::min((int)(FIRST_OFFSET + version_cnt * define::blockSize) - decoded_start_offset, decoded_end_offset - decoded_start_offset);
  }
  // calculate raw_length
  auto get_raw_distance = [=](int decoded_len){
    if (decoded_len <= 0) {
      return decoded_len;
    }
    auto version_cnt = decoded_len / define::blockSize + (decoded_len % define::blockSize ? 1 : 0);
    return decoded_len + (int)(version_cnt * define::versionSize);
  };

  auto raw_end_distance = get_raw_distance(decoded_end_offset - FIRST_OFFSET);
  auto raw_start_distance = get_raw_distance(decoded_start_offset - FIRST_OFFSET);
  raw_length = raw_end_distance - raw_start_distance;

  return std::make_tuple(raw_offset, raw_length, first_offset);
}


/* Call before write a segment of leaf node. Only partial entry_versions (the versions of the hopped entries) will be incremented. */
inline void LeafVersionManager::encode_segment_versions(char *input_buffer, char *output_buffer, uint64_t first_offset,
                                                        const int* hopped_idxes, int hopped_cnt, int l_idx, int r_idx,  // segment_range: [l_idx, r_idx]
                                                        uint64_t metadata_offset, uint64_t segment_len) {
  // increment hopped entry versions
  auto entries_l = (LeafEntry*)input_buffer;
  auto groups = (LeafEntryGroup*)(input_buffer + metadata_offset);
  int size_l = metadata_offset / sizeof(LeafEntry);
  int remain_size = r_idx - l_idx + 1 - size_l;
  for (int h_i = 0; h_i < hopped_cnt; ++ h_i) {
    int hopped_idx = hopped_idxes[h_i];
    if (hopped_idx >= l_idx && hopped_idx <= r_idx) {
      if (hopped_idx - l_idx < size_l) {
        auto& obj_version = entries_l[hopped_idx - l_idx].h_version;
        ++ obj_version.entry_version;
      }
      else {
        int idx = hopped_idx - l_idx - size_l;
        assert(idx < remain_size);
        auto& obj_version = groups[idx / define::neighborSize].records[idx % define::neighborSize].h_version;
        ++ obj_version.entry_version;
      }
    }
  }

  // generate cacheline versions
  memcpy(output_buffer, input_buffer, first_offset);
  uint64_t i = first_offset;
  uint64_t j = first_offset;
  while (i < metadata_offset && i < segment_len) {
    PackedVersion obj_version = entries_l[kEntryLut.idx[i]].h_version;
    __builtin_memcpy(output_buffer + j, &obj_version, sizeof(PackedVersion));
    j += sizeof(PackedVersion);
    if (i + define::blockSize <= segment_len) {
      __builtin_memcpy(output_buffer + j, input_buffer + i, define::blockSize);
      i += define::blockSize;
      j += define::blockSize;
    } else {
      memcpy(output_buffer + j, input_buffer + i, segment_len - i);
      return;
    }
  }
  if (i < segment_len) {
    const size_t rel = (size_t)(i - metadata_offset);
    size_t group_idx = 0;
    size_t off = rel;
    if (__builtin_expect(rel >= sizeof(LeafEntryGroup), 0)) {
      group_idx = rel / sizeof(LeafEntryGroup);
      off = rel % sizeof(LeafEntryGroup);
    }
    while (i + define::blockSize <= segment_len) {
      const uint8_t ridx = kGroupLut.record_idx[off];
      PackedVersion obj_version = (ridx == 0xFFu)
                                      ? groups[group_idx].metadata.h_version
                                      : groups[group_idx].records[ridx].h_version;
      __builtin_memcpy(output_buffer + j, &obj_version, sizeof(PackedVersion));
      j += sizeof(PackedVersion);
      __builtin_memcpy(output_buffer + j, input_buffer + i, define::blockSize);
      i += define::blockSize;
      j += define::blockSize;
      off += define::blockSize;
      if (off >= sizeof(LeafEntryGroup)) {
        off -= sizeof(LeafEntryGroup);
        ++group_idx;
      }
    }
    if (i < segment_len) {
      const uint8_t ridx = kGroupLut.record_idx[off];
      PackedVersion obj_version = (ridx == 0xFFu)
                                      ? groups[group_idx].metadata.h_version
                                      : groups[group_idx].records[ridx].h_version;
      __builtin_memcpy(output_buffer + j, &obj_version, sizeof(PackedVersion));
      j += sizeof(PackedVersion);
      memcpy(output_buffer + j, input_buffer + i, segment_len - i);
    }
  }
}

inline void LeafVersionManager::encode_segment_versions(char *input_buffer, char *output_buffer, uint64_t first_offset,
                                                        const std::vector<int>& hopped_idxes, int l_idx, int r_idx,
                                                        uint64_t metadata_offset, uint64_t segment_len) {
  encode_segment_versions(input_buffer, output_buffer, first_offset, hopped_idxes.data(), (int)hopped_idxes.size(), l_idx, r_idx, metadata_offset, segment_len);
}


inline bool LeafVersionManager::decode_segment_versions(char *input_buffer, char *output_buffer, uint64_t first_offset, int entry_num,
                                                        uint64_t metadata_offset, uint64_t segment_len, uint8_t& node_version) {
  auto entries_l = (LeafEntry*)output_buffer;
  auto groups = (LeafEntryGroup*)(output_buffer + metadata_offset);

  memcpy(output_buffer, input_buffer, first_offset);
  uint64_t i = first_offset;
  uint64_t j = first_offset;
  while (j < metadata_offset && j < segment_len) {
    auto cacheline_version = *(PackedVersion *)(input_buffer + i);
    i += sizeof(PackedVersion);
    const uint64_t cur_j = j;
    if (j + define::blockSize <= segment_len) {
      __builtin_memcpy(output_buffer + j, input_buffer + i, define::blockSize);
      i += define::blockSize;
      j += define::blockSize;
    } else {
      memcpy(output_buffer + j, input_buffer + i, segment_len - j);
      j = segment_len;
    }
    PackedVersion obj_version = entries_l[kEntryLut.idx[cur_j]].h_version;
    if (__builtin_expect(obj_version != cacheline_version, 0)) {
      return false;
    }
  }
  if (j < segment_len) {
    const size_t rel = (size_t)(j - metadata_offset);
    size_t group_idx = 0;
    size_t off = rel;
    if (__builtin_expect(rel >= sizeof(LeafEntryGroup), 0)) {
      group_idx = rel / sizeof(LeafEntryGroup);
      off = rel % sizeof(LeafEntryGroup);
    }
    while (j + define::blockSize <= segment_len) {
      auto cacheline_version = *(PackedVersion *)(input_buffer + i);
      i += sizeof(PackedVersion);
      __builtin_memcpy(output_buffer + j, input_buffer + i, define::blockSize);
      i += define::blockSize;
      j += define::blockSize;

      const uint8_t ridx = kGroupLut.record_idx[off];
      PackedVersion obj_version = (ridx == 0xFFu)
                                      ? groups[group_idx].metadata.h_version
                                      : groups[group_idx].records[ridx].h_version;
      if (__builtin_expect(obj_version != cacheline_version, 0)) {
        return false;
      }
      off += define::blockSize;
      if (off >= sizeof(LeafEntryGroup)) {
        off -= sizeof(LeafEntryGroup);
        ++group_idx;
      }
    }
    if (j < segment_len) {
      auto cacheline_version = *(PackedVersion *)(input_buffer + i);
      i += sizeof(PackedVersion);
      memcpy(output_buffer + j, input_buffer + i, segment_len - j);
      const uint8_t ridx = kGroupLut.record_idx[off];
      PackedVersion obj_version = (ridx == 0xFFu)
                                      ? groups[group_idx].metadata.h_version
                                      : groups[group_idx].records[ridx].h_version;
      if (__builtin_expect(obj_version != cacheline_version, 0)) {
        return false;
      }
    }
  }
  // node-level joint consistency check
  const int size_l = (int)kEntryLut.idx[metadata_offset];
  node_version = (size_l ? entries_l->h_version.node_version : groups[0].metadata.h_version.node_version);
  for (int k = 0; k < size_l; ++k) {
    if (__builtin_expect(node_version != entries_l[k].h_version.node_version, 0)) return false;
  }
  int remain_size = entry_num - size_l;
  if (remain_size <= 0) return true;
  int group_num = (remain_size + (int)define::neighborSize - 1) / (int)define::neighborSize;
  for (int g = 0; g < group_num; ++g) {
    if (__builtin_expect(node_version != groups[g].metadata.h_version.node_version, 0)) return false;
    const int cnt = remain_size < (int)define::neighborSize ? remain_size : (int)define::neighborSize;
    for (int r = 0; r < cnt; ++r) {
      if (__builtin_expect(node_version != groups[g].records[r].h_version.node_version, 0)) return false;
    }
    remain_size -= cnt;
  }
  return true;
}


#endif // _LEAF_VERSION_MANAGER_H_