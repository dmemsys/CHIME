#
# Fig.4a: Expensive hopscotch metadata access
#
from func_timeout import FunctionTimedOut
from pathlib import Path
import json

from utils.cmd_manager import CMDManager
from utils.log_parser import LogParser
from utils.color_printer import print_GOOD, print_WARNING
from utils.func_timer import print_func_time
from utils.pic_generator import PicGenerator


input_path = './params'
style_path = "./styles"
output_path = './results'
exp_num = '04a'

# common params
with (Path(input_path) / f'common.json').open(mode='r') as f:
    params = json.load(f)
home_dir      = params['home_dir']
cluster_ips   = params['cluster_ips']
master_ip     = params['master_ip']

# fig params
with (Path(input_path) / f'fig_{exp_num}.json').open(mode='r') as f:
    exp_params = json.load(f)
methods                   = exp_params['methods']
target_epoch              = exp_params['target_epoch']
CN_num, client_num_per_CN = exp_params['client_num']
MN_num                    = exp_params['MN_num']
key_type                  = exp_params['key_size']
value_size                = exp_params['value_size']
span_size                 = exp_params['span_size']
segment_sizes             = exp_params['segment_size']
metadata_len              = 8
test_type_idx = {
    'Segment': 2,
    'Metadata before Segment': 1,
    'Node with Metadata': 0
}


@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    plot_data = {
        'methods': methods,
        'X_data': {method: [] for method in methods},  # segment size
        'Y_data': {method: [] for method in methods}   # tpt
    }
    for method in methods:
        project_dir = f"{home_dir}/CHIME"
        work_dir = f"{project_dir}/build"
        env_cmd = f"cd {work_dir}"

        for segment_size in segment_sizes:
            # change config
            cmake_option = '-DENABLE_CORO=on -DREAD_DELEGATION=on -DWRITE_COMBINING=on'
            BUILD_PROJECT = f"cd {project_dir} && mkdir -p build && cd build && cmake {cmake_option} .. && make clean && make -j"
            cmd.all_execute(BUILD_PROJECT)

            CLEAR_MEMC = f"{env_cmd} && /bin/bash ../script/restartMemc.sh"
            RDMA_TEST = f"{env_cmd} && ./zipf_rdma_test {CN_num} {client_num_per_CN} 2 {8 if key_type == 'randint' else 32} {value_size} {span_size} {segment_size} {metadata_len} {test_type_idx[method]}"
            print(RDMA_TEST)
            KILL_PROCESS = f"{env_cmd} && killall -9 zipf_rdma_test"

            while True:
                try:
                    cmd.all_execute(KILL_PROCESS, CN_num)
                    cmd.one_execute(CLEAR_MEMC)
                    logs = cmd.all_long_execute(RDMA_TEST, CN_num)
                    tpt, _, _, _, _, _, _ = tp.get_statistics(logs, target_epoch, get_avg=True)
                    break
                except (FunctionTimedOut, Exception) as e:
                    print_WARNING(f"Error! Retry... {e}")

            print_GOOD(f"[FINISHED POINT] method={method} segment_size={segment_size} tpt={tpt}")
            plot_data['X_data'][method].append(segment_size)
            plot_data['Y_data'][method].append(tpt)
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
