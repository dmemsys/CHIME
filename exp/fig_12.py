#
# Fig.12: Performance comparison of all methods with inline value mode
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
exp_num = '12'

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
methods             = exp_params['methods']
workload_names      = exp_params['workload_names']
target_epochs       = exp_params['target_epoch']
CN_and_client_nums  = exp_params['client_num']
MN_num              = exp_params['MN_num']
key_type            = exp_params['key_size']
value_size          = exp_params['value_size']
cache_size          = exp_params['cache_size']
span_sizes          = exp_params['span_size']
epsilon             = exp_params['epsilon']
neighbor_size       = exp_params['neighbor_size']
hotspot_buffer_size = exp_params['hotspot_buffer_size']


@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    for workload, workload_name in workload_names.items():
        plot_methods = methods if workload_name != 'la' else [method for method in methods if method != 'ROLEX']
        plot_data = {
            'methods': plot_methods,
            'X_data': {method: [] for method in plot_methods},  # tpt
            'Y_data': {method: [] for method in plot_methods},   # p99 lat
            'BACKUP_data': {method: [] for method in plot_methods}   # p50 lat
        }
        for method in plot_methods:

            for CN_num, client_num_per_CN in CN_and_client_nums[workload][method]:
                sufficient_cache = False
                if method == 'SMART-SC':
                    method = 'SMART'
                    sufficient_cache = True
                project_dir = f"{home_dir}/{'CHIME' if method == 'Sherman' else method}"
                work_dir = f"{project_dir}/build"
                env_cmd = f"cd {work_dir}"

                # change config
                sed_cmd = (sed_workloads_dir('./workloads.conf', workloads_dir) + " && " +
                        generate_sed_cmd('./include/Common.h', method, 8 if key_type == 'randint' else 32, value_size,
                                            1000 if sufficient_cache else cache_size, MN_num, span_sizes[method],
                                            {'epsilon': epsilon, 'neighbor_size': neighbor_size, 'hotspot_buffer_size': hotspot_buffer_size}))
                cmake_option = f'{common_options} {cmake_options[method]}'
                if workload in ['YCSB C']:
                    cmake_option = cmake_option.replace('-DMIDDLE_TEST_EPOCH=off', '-DMIDDLE_TEST_EPOCH=on')
                BUILD_PROJECT = f"cd {project_dir} && {sed_cmd} && mkdir -p build && cd build && cmake {cmake_option} .. && make clean && make -j"

                cmd.all_execute(BUILD_PROJECT)

                CLEAR_MEMC = f"{env_cmd} && /bin/bash ../script/restartMemc.sh"
                SPLIT_WORKLOADS = f"{env_cmd} && python3 {ycsb_dir}/split_workload.py {workload_name} {key_type} {CN_num} {client_num_per_CN}"
                YCSB_TEST = f"{env_cmd} && ./ycsb_test {CN_num} {client_num_per_CN} 2 {key_type} {workload_name}"
                KILL_PROCESS = f"{env_cmd} && killall -9 ycsb_test"

                cmd.all_execute(SPLIT_WORKLOADS, CN_num)
                while True:
                    try:
                        cmd.all_execute(KILL_PROCESS, CN_num)
                        cmd.one_execute(CLEAR_MEMC)
                        logs = cmd.all_long_execute(YCSB_TEST, CN_num, only_need_tpt=True)
                        tpt, _, _, _, _, _, _ = tp.get_statistics(logs, target_epochs[workload], get_avg=True)
                        p50_lat, p99_lat = cmd.get_cluster_lats(str(Path(project_dir) / 'us_lat'), CN_num, target_epochs[workload], get_avg=True)
                        break
                    except (FunctionTimedOut, Exception) as e:
                        print_WARNING(f"Error! Retry... {e}")

                if sufficient_cache is True:
                    method = 'SMART-SC'
                print_GOOD(f"[FINISHED POINT] workload={workload} method={method} client_num={CN_num*client_num_per_CN} tpt={tpt} p99_lat={p99_lat}")
                plot_data['X_data'][method].append(tpt)
                plot_data['Y_data'][method].append(p99_lat)
                plot_data['BACKUP_data'][method].append(p50_lat)
        # save data
        Path(output_path).mkdir(exist_ok=True)
        with (Path(output_path) / f'fig_{exp_num}_{workload_name}.json').open(mode='w') as f:
            json.dump(plot_data, f, indent=2)


if __name__ == '__main__':
    cmd = CMDManager(cluster_ips, master_ip)
    tp = LogParser()
    t = main(cmd, tp)
    with (Path(output_path) / 'time.log').open(mode="a+") as f:
        f.write(f"fig_{exp_num}.py execution time: {int(t//60)} min {int(t%60)} s\n")

    pg = PicGenerator(output_path, style_path)
    for workload_name in workload_names.values():
        pg.generate(f'{exp_num}_{workload_name}')
