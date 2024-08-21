from typing import Optional


def sed_workloads_dir(config_path: str, workloads_dir: str):
    return f"sed -i 's#.*#{workloads_dir}#g' {config_path}"


def sed_key_len(config_path: str, key_size: int):
    old_key_code   = "^constexpr uint32_t keyLen .*"
    new_key_code   = f"constexpr uint32_t keyLen = {key_size};"
    return f"sed -i 's/{old_key_code}/{new_key_code}/g' {config_path}"


def sed_val_len(config_path: str, value_size: int):
    old_val_code   = "^constexpr uint32_t simulatedValLen .*"
    new_val_code   = f"constexpr uint32_t simulatedValLen = {value_size};"
    return f"sed -i 's/{old_val_code}/{new_val_code}/g' {config_path}"


def sed_cache_size(config_path: str, cache_size: int):
    old_cache_code = "^constexpr int kIndexCacheSize .*"
    new_cache_node = f"constexpr int kIndexCacheSize = {cache_size};"
    return f"sed -i 's/{old_cache_code}/{new_cache_node}/g' {config_path}"


def sed_MN_num(config_path: str, MN_num: int):
    old_MN_code = "^#define MEMORY_NODE_NUM .*"
    new_MN_node = f"#define MEMORY_NODE_NUM {MN_num}"
    return f"sed -i 's/{old_MN_code}/{new_MN_node}/g' {config_path}"


def sed_span_size(config_path: str, span_size: int):
    old_span_code = "^constexpr uint32_t leafSpanSize .*"
    new_span_node = f"constexpr uint32_t leafSpanSize = {span_size};"
    return f"sed -i 's/{old_span_code}/{new_span_node}/g' {config_path}"


def sed_epsilon(config_path: str, epsilon: int):  # only for ROLEX
    old_epsilon = "^constexpr uint64_t epsilon .*"
    new_epsilon = f"constexpr uint64_t epsilon = {epsilon};"
    return f"sed -i 's/{old_epsilon}/{new_epsilon}/g' {config_path}"

def sed_neighbor_size(config_path: str, neighbor_size: int):  # only for CHIME
    old_neighbor_size = "^constexpr uint32_t neighborSize .*"
    new_neighbor_size = f"constexpr uint32_t neighborSize = {neighbor_size};"
    return f"sed -i 's/{old_neighbor_size}/{new_neighbor_size}/g' {config_path}"


def sed_hotspot_buffer_size(config_path: str, buffer_size: int):  # only for CHIME
    old_buffer_size = "^constexpr int kHotspotBufSize .*"
    new_buffer_size = f"constexpr int kHotspotBufSize = {buffer_size};"
    return f"sed -i 's/{old_buffer_size}/{new_buffer_size}/g' {config_path}"


def sed_greedy_IO_size(config_path: str, IO_size: int):  # only for CHIME
    old_IO_size = "^constexpr uint64_t greedySizePerIO .*"
    new_IO_size = f"constexpr uint64_t greedySizePerIO = {IO_size};"
    return f"sed -i 's/{old_IO_size}/{new_IO_size}/g' {config_path}"


def generate_sed_cmd(config_path: str, method: str, key_size: int, value_size: int, cache_size: int, MN_num: int, span_size: Optional[int] = None, other_params: dict = {}):
    cmd = f"{sed_key_len(config_path, key_size)} && {sed_val_len(config_path, value_size)} && {sed_cache_size(config_path, cache_size)} && {sed_MN_num(config_path, MN_num)}"
    if method != 'SMART':
        assert(span_size is not None)
        cmd += f" && {sed_span_size(config_path, span_size)}"
    if method == 'ROLEX':
        assert('epsilon' in other_params)
        cmd += f" && {sed_epsilon(config_path, other_params['epsilon'])}"
    if method in ['CHIME', 'ROLEX']:
        assert('neighbor_size' in other_params)
        cmd += f" && {sed_neighbor_size(config_path, other_params['neighbor_size'])}"
        assert('hotspot_buffer_size' in other_params)
        cmd += f" && {sed_hotspot_buffer_size(config_path, other_params['hotspot_buffer_size'])}"
        if 'greedy_IO_size' in other_params:
            cmd += f" && {sed_greedy_IO_size(config_path, other_params['greedy_IO_size'])}"
    return cmd
