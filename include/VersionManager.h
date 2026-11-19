#if !defined(_VERSION_MANAGER_H_)
#define _VERSION_MANAGER_H_

#include "Common.h"
#include "Metadata.h"
#include "LeafNode.h"
#include "InternalNode.h"

#include <cstddef>
#include <vector>

namespace detail_vm {
template <class NODE, class ENTRY>
struct NodeOffsetLut {
  uint16_t record_idx[sizeof(NODE)];
  constexpr NodeOffsetLut() : record_idx{} {
    constexpr size_t rec_off = offsetof(NODE, records);
    for (size_t off = 0; off < sizeof(NODE); ++off) {
      record_idx[off] = (off < rec_off)
                            ? 0xFFFFu
                            : static_cast<uint16_t>((off - rec_off) / sizeof(ENTRY));
    }
  }
};

template <class NODE, class ENTRY>
struct SegOffsetLut {
  uint16_t idx[sizeof(NODE)];
  constexpr SegOffsetLut() : idx{} {
    for (size_t off = 0; off < sizeof(NODE); ++off) {
      idx[off] = static_cast<uint16_t>(off / sizeof(ENTRY));
    }
  }
};
}  // namespace detail_vm


template <class NODE, class ENTRY>
class VersionManager {
public:
  VersionManager() {}

  static bool decode_node_versions(char *input_buffer, char *output_buffer);
  static bool decode_header_versions(char *input_buffer, char *output_buffer, uint8_t& node_version);
  static bool decode_segment_versions(char *input_buffer, char *output_buffer, uint64_t first_offset, int entry_num, uint8_t& node_version);

  static void encode_node_versions(char *input_buffer, char *output_buffer);
  static void encode_entry_versions(char *input_buffer, char *output_buffer, uint64_t first_offset);
  static void encode_segment_versions(char *input_buffer, char *output_buffer, uint64_t first_offset, const int* hopped_idxes, int hopped_cnt, int l_idx, int r_idx);
  static void encode_segment_versions(char *input_buffer, char *output_buffer, uint64_t first_offset, const std::vector<int>& hopped_idxes, int l_idx, int r_idx);

  static std::tuple<uint64_t, uint64_t, uint64_t> get_offset_info(int start_entry_idx, int entry_num = 1);

private:
  static const int FIRST_OFFSET = std::min((uint64_t)define::cachelineSize, sizeof(NODE));  // the address of the first encoded cacheline version
  static inline constexpr detail_vm::NodeOffsetLut<NODE, ENTRY> kNodeLut{};
  static inline constexpr detail_vm::SegOffsetLut<NODE, ENTRY> kSegLut{};
};


/* Call after read the entire node. Both entry- and node-level consistency are checked. */
template <class NODE, class ENTRY>
inline bool VersionManager<NODE, ENTRY>::decode_node_versions(char *input_buffer, char *output_buffer) {
  auto node = (NODE *)output_buffer;
  auto& entries = node->records;  // auto& will parse entries as an array
  const auto& metadata = node->metadata;

  __builtin_memcpy(output_buffer, input_buffer, FIRST_OFFSET);
  uint64_t i = FIRST_OFFSET;
  uint64_t j = FIRST_OFFSET;
  while (j + define::blockSize <= sizeof(NODE)) {
    auto cacheline_version = *(PackedVersion *)(input_buffer + i);
    i += sizeof(PackedVersion);
    const uint16_t ridx = kNodeLut.record_idx[j];
    __builtin_memcpy(output_buffer + j, input_buffer + i, define::blockSize);
    i += define::blockSize;
    j += define::blockSize;
    PackedVersion obj_version = (ridx == 0xFFFFu) ? metadata.h_version : entries[ridx].h_version;
    if (__builtin_expect(obj_version != cacheline_version, 0)) {
      return false;
    }
  }
  if (j < sizeof(NODE)) {
    auto cacheline_version = *(PackedVersion *)(input_buffer + i);
    i += sizeof(PackedVersion);
    const uint16_t ridx = kNodeLut.record_idx[j];
    memcpy(output_buffer + j, input_buffer + i, sizeof(NODE) - j);
    PackedVersion obj_version = (ridx == 0xFFFFu) ? metadata.h_version : entries[ridx].h_version;
    if (__builtin_expect(obj_version != cacheline_version, 0)) {
      return false;
    }
  }
  // node-level joint consistency check
  const uint8_t node_version = entries[0].h_version.node_version;
  if (__builtin_expect(node_version != metadata.h_version.node_version, 0)) {
    return false;
  }
  for (const auto& entry : entries) {
    if (__builtin_expect(node_version != entry.h_version.node_version, 0)) {
      return false;
    }
  }
  return true;
}


/* Call before write the entire node. Only the node_versions will be incremented. */
template <class NODE, class ENTRY>
inline void VersionManager<NODE, ENTRY>::encode_node_versions(char *input_buffer, char *output_buffer) {
  // increment node versions
  auto node = (NODE *)input_buffer;
  auto& entries = node->records;  // auto& will parse entries as an array
  auto& metadata = node->metadata;
  uint8_t node_version = entries[0].h_version.node_version;
  ++ node_version;
  metadata.h_version.node_version = node_version;
  for (auto& entry : entries) entry.h_version.node_version = node_version;

  // generate cacheline versions
  __builtin_memcpy(output_buffer, input_buffer, FIRST_OFFSET);
  uint64_t i = FIRST_OFFSET;
  uint64_t j = FIRST_OFFSET;
  while (i + define::blockSize <= sizeof(NODE)) {
    const uint16_t ridx = kNodeLut.record_idx[i];
    PackedVersion obj_version = (ridx == 0xFFFFu) ? metadata.h_version : entries[ridx].h_version;
    __builtin_memcpy(output_buffer + j, &obj_version, sizeof(PackedVersion));
    j += sizeof(PackedVersion);
    __builtin_memcpy(output_buffer + j, input_buffer + i, define::blockSize);
    i += define::blockSize;
    j += define::blockSize;
  }
  if (i < sizeof(NODE)) {
    const uint16_t ridx = kNodeLut.record_idx[i];
    PackedVersion obj_version = (ridx == 0xFFFFu) ? metadata.h_version : entries[ridx].h_version;
    __builtin_memcpy(output_buffer + j, &obj_version, sizeof(PackedVersion));
    j += sizeof(PackedVersion);
    memcpy(output_buffer + j, input_buffer + i, sizeof(NODE) - i);
  }
}


template <class NODE, class ENTRY>
inline std::tuple<uint64_t, uint64_t, uint64_t> VersionManager<NODE, ENTRY>::get_offset_info(int start_entry_idx, int entry_num) {  // [raw_offset(encoded), raw length(encoded), first_offset(decoded)]
  uint64_t raw_offset, raw_length, first_offset;
  int end_entry_idx = start_entry_idx + entry_num;
  int decoded_start_offset = STRUCT_OFFSET(NODE, records[start_entry_idx]);
  int decoded_end_offset = STRUCT_OFFSET(NODE, records[end_entry_idx]);

  // calculate raw_offset, first_offset
  auto dist = decoded_start_offset - FIRST_OFFSET;
  if (dist < 0) {
    raw_offset = decoded_start_offset;
    first_offset = std::min((uint64_t)FIRST_OFFSET - decoded_start_offset, sizeof(ENTRY) * entry_num);
  }
  else {
    auto version_cnt = dist / define::blockSize + (dist % define::blockSize ? 1 : 0);
    raw_offset = decoded_start_offset + version_cnt * define::versionSize;
    first_offset = std::min((uint64_t)FIRST_OFFSET + version_cnt * define::blockSize - decoded_start_offset, sizeof(ENTRY) * entry_num);
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


/* Call before write the entire entry. Only the entry_versions will be incremented. */
template <class NODE, class ENTRY>
inline void VersionManager<NODE, ENTRY>::encode_entry_versions(char *input_buffer, char *output_buffer, uint64_t first_offset) {
  // increment entry versions
  auto& obj_version = ((ENTRY *)input_buffer)->h_version;
  ++ obj_version.entry_version;

  // generate cacheline versions
  memcpy(output_buffer, input_buffer, first_offset);
  for (uint64_t i = first_offset, j = first_offset; i < sizeof(ENTRY); i += define::blockSize, j += define::blockSize) {
    memcpy(output_buffer + j, (void *)&obj_version, sizeof(PackedVersion));
    j += sizeof(PackedVersion);
    memcpy(output_buffer + j, input_buffer + i, std::min((size_t)define::blockSize, sizeof(ENTRY) - i));
  }
}


/* Call before write a segment of leaf node. Only partial entry_versions (the versions of the hopped entries) will be incremented. */
template <class NODE, class ENTRY>
inline void VersionManager<NODE, ENTRY>::encode_segment_versions(char *input_buffer, char *output_buffer, uint64_t first_offset, const int* hopped_idxes, int hopped_cnt, int l_idx, int r_idx) {  // segment_range: [l_idx, r_idx]
  // increment hopped entry versions
  auto entries = (ENTRY*)input_buffer;
  for (int h_i = 0; h_i < hopped_cnt; ++ h_i) {
    int hopped_idx = hopped_idxes[h_i];
    if (hopped_idx >= l_idx && hopped_idx <= r_idx) {
      auto& obj_version = entries[hopped_idx - l_idx].h_version;
      ++ obj_version.entry_version;
    }
  }

  // generate cacheline versions
  memcpy(output_buffer, input_buffer, first_offset);
  const uint64_t segment_len = sizeof(ENTRY) * (uint64_t)(r_idx - l_idx + 1);
  uint64_t i = first_offset;
  uint64_t j = first_offset;
  while (i + define::blockSize <= segment_len) {
    auto obj_version = entries[kSegLut.idx[i]].h_version;
    __builtin_memcpy(output_buffer + j, &obj_version, sizeof(PackedVersion));
    j += sizeof(PackedVersion);
    __builtin_memcpy(output_buffer + j, input_buffer + i, define::blockSize);
    i += define::blockSize;
    j += define::blockSize;
  }
  if (i < segment_len) {
    auto obj_version = entries[kSegLut.idx[i]].h_version;
    __builtin_memcpy(output_buffer + j, &obj_version, sizeof(PackedVersion));
    j += sizeof(PackedVersion);
    memcpy(output_buffer + j, input_buffer + i, segment_len - i);
  }
}

template <class NODE, class ENTRY>
inline void VersionManager<NODE, ENTRY>::encode_segment_versions(char *input_buffer, char *output_buffer, uint64_t first_offset, const std::vector<int>& hopped_idxes, int l_idx, int r_idx) {
  encode_segment_versions(input_buffer, output_buffer, first_offset, hopped_idxes.data(), (int)hopped_idxes.size(), l_idx, r_idx);
}


template <class NODE, class ENTRY>
inline bool VersionManager<NODE, ENTRY>::decode_header_versions(char *input_buffer, char *output_buffer, uint8_t& node_version) {
  memcpy(output_buffer, input_buffer, FIRST_OFFSET);
  PackedVersion obj_version = ((NODE *)output_buffer)->metadata.h_version;
  node_version = obj_version.node_version;

  auto header_size = (uint64_t)STRUCT_OFFSET(NODE, records);
  for (uint64_t i = FIRST_OFFSET, j = FIRST_OFFSET; j < header_size; i += define::blockSize, j += define::blockSize) {
    auto cacheline_version = *(PackedVersion *)(input_buffer + i);
    i += sizeof(PackedVersion);
    memcpy(output_buffer + j, input_buffer + i, std::min((size_t)define::blockSize, header_size - j));
    // node-level consistency check
    if (obj_version != cacheline_version) {
      return false;
    }
  }
  return true;
}

template <class NODE, class ENTRY>
inline bool VersionManager<NODE, ENTRY>::decode_segment_versions(char *input_buffer, char *output_buffer, uint64_t first_offset, int entry_num, uint8_t& node_version) {
  auto entries = (ENTRY *)output_buffer;

  memcpy(output_buffer, input_buffer, first_offset);
  const uint64_t segment_len = sizeof(ENTRY) * (uint64_t)entry_num;
  uint64_t i = first_offset;
  uint64_t j = first_offset;
  while (j + define::blockSize <= segment_len) {
    auto cacheline_version = *(PackedVersion *)(input_buffer + i);
    i += sizeof(PackedVersion);
    const uint16_t ridx = kSegLut.idx[j];
    __builtin_memcpy(output_buffer + j, input_buffer + i, define::blockSize);
    i += define::blockSize;
    j += define::blockSize;
    auto obj_version = entries[ridx].h_version;
    if (__builtin_expect(obj_version != cacheline_version, 0)) {
      return false;
    }
  }
  if (j < segment_len) {
    auto cacheline_version = *(PackedVersion *)(input_buffer + i);
    i += sizeof(PackedVersion);
    const uint16_t ridx = kSegLut.idx[j];
    memcpy(output_buffer + j, input_buffer + i, segment_len - j);
    auto obj_version = entries[ridx].h_version;
    if (__builtin_expect(obj_version != cacheline_version, 0)) {
      return false;
    }
  }
  // node-level joint consistency check
  node_version = entries[0].h_version.node_version;
  for (int k = 1; k < entry_num; ++k) {
    if (__builtin_expect(node_version != entries[k].h_version.node_version, 0)) {
      return false;
    }
  }
  return true;
}


#endif // _VERSION_MANAGER_H_