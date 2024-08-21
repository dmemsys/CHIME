#if !defined(_METADATA_MANAGER_H_)
#define _METADATA_MANAGER_H_

#include "Common.h"
#include "Metadata.h"
#include "LeafNode.h"

#include <vector>


class MetadataManager {
public:
  MetadataManager() {}

  static void decode_node_metadata(char *input_buffer, char *output_buffer);
  static bool decode_segment_metadata(char *input_buffer, char *output_buffer, uint64_t first_metadata_offset, int entry_num, LeafMetadata& metadata);

  static void encode_node_metadata(char *input_buffer, char *output_buffer);
  static void encode_segment_metadata(char *input_buffer, char *output_buffer, uint64_t first_metadata_offset, int entry_num, const LeafMetadata& metadata);

  static std::pair<uint64_t, uint64_t> get_offset_info(int start_entry_idx, int entry_num = 1);
};


/* Call after read the entire leaf and decode the versions. This function decodes the scattered metadata. */
inline void MetadataManager::decode_node_metadata(char *input_buffer, char *output_buffer) {  // ScatterLeafNode => LeafNode
  auto scatter_leaf = (ScatteredLeafNode *)input_buffer;
  auto leaf = (LeafNode *)output_buffer;
  // copy lock and leaf metadata
  const auto& first_group = scatter_leaf->record_groups[0];
#ifdef SIBLING_BASED_VALIDATION
  leaf->metadata = LeafMetadata(first_group.metadata.h_version, 0U, first_group.metadata.valid, first_group.metadata.sibling_ptr, FenceKeys{});
#else
  leaf->metadata = LeafMetadata(first_group.metadata.h_version, 0U, first_group.metadata.valid, first_group.metadata.sibling_ptr, first_group.metadata.fence_keys);
#endif
  int i = 0;
  for (const auto& group : scatter_leaf->record_groups) {
    assert(group.metadata == first_group.metadata);
    for (const auto& record : group.records) {
      leaf->records[i ++] = record;
      if (i == define::leafSpanSize) return;
    }
  }
  return;
}


/* Call before encode the versions and write the entire node. This function encodes the scattered metadata. */
inline void MetadataManager::encode_node_metadata(char *input_buffer, char *output_buffer) {  // LeafNode => ScatterLeafNode
  auto leaf = (LeafNode *)input_buffer;
  auto scatter_leaf = (ScatteredLeafNode *)output_buffer;
  int i = 0;
  for (auto& group : scatter_leaf->record_groups) {
    group.metadata = ScatteredMetadata(leaf->metadata);
    for (auto& record : group.records) {
      record = leaf->records[i ++];
      if (i == define::leafSpanSize) return;
    }
  }
  return;
}


/* Call after read the segment and decode the versions. If no metadata is contained in the segment, return false. */
inline bool MetadataManager::decode_segment_metadata(char *input_buffer, char *output_buffer, uint64_t first_metadata_offset, int entry_num, LeafMetadata& metadata) {  // ScatterLeafNode => LeafNode
  auto segment_len = sizeof(LeafEntry) * entry_num;
  memcpy(output_buffer, input_buffer, first_metadata_offset);
  if (first_metadata_offset >= segment_len) {
    return false;
  }
  auto scattered_metadata = (ScatteredMetadata *)(input_buffer + first_metadata_offset);
  for (uint64_t i = first_metadata_offset, j = first_metadata_offset; j < segment_len; i += define::groupSize, j += define::groupSize) {
    assert(*scattered_metadata == *(ScatteredMetadata *)(input_buffer + i));
    i += sizeof(ScatteredMetadata);
    memcpy(output_buffer + j, input_buffer + i, std::min((size_t)define::groupSize, segment_len - j));
  }
#ifdef SIBLING_BASED_VALIDATION
  metadata = LeafMetadata(scattered_metadata->h_version, 0U, scattered_metadata->valid, scattered_metadata->sibling_ptr, FenceKeys{});
#else
  metadata = LeafMetadata(scattered_metadata->h_version, 0U, scattered_metadata->valid, scattered_metadata->sibling_ptr, scattered_metadata->fence_keys);
#endif
  return true;
}


/* Call before encode the versions and write the segment. This function ecodes the scattered metadata. */
inline void MetadataManager::encode_segment_metadata(char *input_buffer, char *output_buffer, uint64_t first_metadata_offset, int entry_num, const LeafMetadata& metadata) {  // LeafNode => ScatterLeafNode
  auto scattered_metadata = ScatteredMetadata(metadata);
  auto segment_len = sizeof(LeafEntry) * entry_num;
  memcpy(output_buffer, input_buffer, first_metadata_offset);
  for (uint64_t i = first_metadata_offset, j = first_metadata_offset; i < segment_len; i += define::groupSize, j += define::groupSize) {
    memcpy(output_buffer + j, (void*)&scattered_metadata, sizeof(ScatteredMetadata));
    j += sizeof(ScatteredMetadata);
    memcpy(output_buffer + j, input_buffer + i, std::min((size_t)define::groupSize, segment_len - i));
  }
  return;
}


inline std::pair<uint64_t, uint64_t> MetadataManager::get_offset_info(int start_entry_idx, int entry_num) {  // [first_metadata_offset, new_len]
  auto segment_len = sizeof(LeafEntry) * entry_num;
  uint64_t first_metadata_offset = std::min((define::neighborSize - start_entry_idx % define::neighborSize) % define::neighborSize * sizeof(LeafEntry), segment_len);
  auto remain_len = segment_len - first_metadata_offset;
  uint64_t new_len = segment_len + sizeof(ScatteredMetadata) * (remain_len / define::groupSize + (remain_len % define::groupSize ? 1 : 0));
  return std::make_pair(first_metadata_offset, new_len);
}

#endif // _METADATA_MANAGER_H_