#
# Fig.16: The contribution of sibling-based validation to metadata size
#
from func_timeout import FunctionTimedOut
from pathlib import Path
import json

from utils.cmd_manager import CMDManager
from utils.log_parser import LogParser
from utils.sed_generator import sed_workloads_dir, generate_sed_cmd
from utils.color_printer import print_GOOD, print_WARNING
from utils.func_timer import print_func_time
from utils.pic_generator import PicGenerator


input_path = './params'
style_path = "./styles"
output_path = './results'
exp_num = '16'

# common params
with (Path(input_path) / f'common.json').open(mode='r') as f:
    params = json.load(f)
home_dir      = params['home_dir']
cluster_ips   = params['cluster_ips']
master_ip     = params['master_ip']
common_options= params['common_options']
cmake_options = params['cmake_options']

# fig params
with (Path(input_path) / f'fig_{exp_num}.json').open(mode='r') as f:
    exp_params = json.load(f)
methods                   = exp_params['methods']
key_sizes                 = exp_params['key_size']
value_size                = exp_params['value_size']
span_size                 = exp_params['span_size']


@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    plot_data = {
        'methods': methods,
        'X_data': key_sizes,                         # key size
        'Y_data': {method: [] for method in methods} # metadata size per kv
    }
    for method in methods:
        project_dir = f"{home_dir}/{'ROLEX' if method == 'ROLEX' else 'CHIME'}"
        work_dir = f"{project_dir}/build"
        env_cmd = f"cd {work_dir}"

        for key_size in key_sizes:
            if method == 'SMART':
                plot_data['Y_data'][method].append(8 + 8 + 1 + 1)  # 8B reverse poiner + 8B checksum + 1B valid field + 1B lock, according to the source code & paper of SMART
                continue

            # change config
            sed_cmd = (generate_sed_cmd('./include/Common.h', 'CHIME', key_size, value_size,
                                        100, 1, span_size,
                                        {'neighbor_size': 8, 'hotspot_buffer_size': 30}))
            cmake_option = f'{common_options} {cmake_options[method]}'
            BUILD_PROJECT = f"cd {project_dir} && {sed_cmd} && mkdir -p build && cd build && cmake {cmake_option} .. && make clean && make -j"

            META_TEST = f"{env_cmd} && ./metadata_size_test"
            KILL_PROCESS = f"{env_cmd} && killall -9 metadata_size_test"

            cmd.one_execute(BUILD_PROJECT)
            while True:
                try:
                    cmd.one_execute(KILL_PROCESS)
                    line = cmd.one_execute(META_TEST)[0]
                    data = line.strip().split(' ')[5]
                    if data.replace('.', '').isdigit():
                        metadata_size_per_kv = float(data)
                        break
                except (FunctionTimedOut, Exception) as e:
                    print_WARNING(f"Error! Retry... {e}")

            print_GOOD(f"[FINISHED POINT] method={method} key_size={key_size} metadata_size_per_kv={metadata_size_per_kv}")
            plot_data['Y_data'][method].append(metadata_size_per_kv)
    # save data
    Path(output_path).mkdir(exist_ok=True)
    with (Path(output_path) / f'fig_{exp_num}.json').open(mode='w') as f:
        json.dump(plot_data, f, indent=2)


if __name__ == '__main__':
    cmd = CMDManager(cluster_ips, master_ip)
    tp = LogParser()
    t = main(cmd, tp)
    with (Path(output_path) / 'time.log').open(mode="a+") as f:
        f.write(f"fig_{exp_num}.py execution time: {int(t//60)} min {int(t%60)} s\n")

    pg = PicGenerator(output_path, style_path)
    pg.generate(exp_num)
