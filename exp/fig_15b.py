#
# Fig.15b: Factor analysis (ROLEX => CHIME-Learned)
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
exp_num = '15b'

# common params
with (Path(input_path) / f'common.json').open(mode='r') as f:
    params = json.load(f)
home_dir      = params['home_dir']
workloads_dir = params['workloads_dir']
ycsb_dir      = str(Path(workloads_dir).parent)
cluster_ips   = params['cluster_ips']
master_ip     = params['master_ip']
common_options= params['common_options']
cmake_options = params['cmake_options']

# fig params
with (Path(input_path) / f'fig_{exp_num}.json').open(mode='r') as f:
    exp_params = json.load(f)
methods                   = exp_params['methods']
workload_names            = exp_params['workload_names']
target_epoch              = exp_params['target_epoch']
CN_num, client_num_per_CN = exp_params['client_num']
MN_num                    = exp_params['MN_num']
key_type                  = exp_params['key_size']
value_size                = exp_params['value_size']
cache_size                = exp_params['cache_size']
span_size                 = exp_params['span_size']
epsilon                   = exp_params['epsilon']
neighbor_size             = exp_params['neighbor_size']
hotspot_buffer_size       = exp_params['hotspot_buffer_size']
method_maps = {
    'ROLEX': 'Baseline (ROLEX)',
    'R +Hopscotch Leaf Node'     : '+Hopscotch Leaf Node',
    'R +Vacancy-Aware Lock'      : '+Vacancy-Aware Lock',
    'R +Metadata Replication'    : '+Metadata Replication',
    'R +Speculative Leaf Read'        : '+Speculative Leaf Read'
}


@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    metrics = ['Throughput', 'P50 Latency', 'P99 Latency']
    plot_data = {
        'methods': list(method_maps.values()),
        'bar_groups': list(workload_names.keys()),
        'metrics': metrics,
        'Y_data': {
            method: {
                workload: {}  # store tpt, p50, p99, respectively
                for workload in workload_names.keys()
            }
            for method in method_maps.values()
        }
    }
    for workload, workload_name in workload_names.items():
        project_dir = f"{home_dir}/ROLEX"
        work_dir = f"{project_dir}/build"
        env_cmd = f"cd {work_dir}"

        # change config
        sed_cmd = (sed_workloads_dir('./workloads.conf', workloads_dir) + " && " +
                    generate_sed_cmd('./include/Common.h', 'ROLEX', 8 if key_type == 'randint' else 32, value_size,
                                    cache_size, MN_num, span_size,
                                    {'epsilon': epsilon, 'neighbor_size': neighbor_size, 'hotspot_buffer_size': hotspot_buffer_size}))
        SPLIT_WORKLOADS = f"{env_cmd} && python3 {ycsb_dir}/split_workload.py {workload_name} {key_type} {CN_num} {client_num_per_CN}"
        cmd.all_execute(SPLIT_WORKLOADS, CN_num)

        for method in methods:
            cmake_option = f'{common_options} {cmake_options[method]}'
            BUILD_PROJECT = f"cd {project_dir} && {sed_cmd} && mkdir -p build && cd build && cmake {cmake_option} .. && make clean && make -j"

            cmd.all_execute(BUILD_PROJECT)

            CLEAR_MEMC = f"{env_cmd} && /bin/bash ../script/restartMemc.sh"
            YCSB_TEST = f"{env_cmd} && ./ycsb_test {CN_num} {client_num_per_CN} 2 {key_type} {workload_name}"
            KILL_PROCESS = f"{env_cmd} && killall -9 ycsb_test"

            while True:
                try:
                    cmd.all_execute(KILL_PROCESS, CN_num)
                    cmd.one_execute(CLEAR_MEMC)
                    logs = cmd.all_long_execute(YCSB_TEST, CN_num)
                    p50_lat, p99_lat = cmd.get_cluster_lats(str(Path(project_dir) / 'us_lat'), CN_num, target_epoch, get_avg=True)
                    tpt, _, _, _, _, _, _ = tp.get_statistics(logs, target_epoch, get_avg=True)
                    break
                except (FunctionTimedOut, Exception) as e:
                    print_WARNING(f"Error! Retry... {e}")

            print_GOOD(f"[FINISHED POINT] method={method_maps[method]} workload={workload} tpt={tpt} p50_lat={p50_lat} p99_lat={p99_lat}")
            plot_data['Y_data'][method_maps[method]][workload] = {metrics[0]: tpt, metrics[1]: p50_lat, metrics[2]: p99_lat}
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