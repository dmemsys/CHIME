#
# Figs.18f & 19b: Sensitivity: neighborhood size => tpt/cache efficiency/load factor
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
exp_num = '18f_19b'
sub_exp_1, sub_exp_2 = exp_num.split('_', 1)

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
method                    = exp_params['methods'][0]
workload, workload_name   = exp_params['workload_names']
target_epoch              = exp_params['target_epoch']
CN_num, client_num_per_CN = exp_params['client_num']
MN_num                    = exp_params['MN_num']
key_type                  = exp_params['key_size']
value_size                = exp_params['value_size']
cache_size                = exp_params['cache_size']
span_size                 = exp_params['span_size']
neighbor_sizes            = exp_params['neighbor_size']
hotspot_buffer_size       = exp_params['hotspot_buffer_size']


@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    plot_data_a = {
        'methods': [method],
        'X_data': neighbor_sizes,
        'Y_data': {method: []}  # tpt
    }
    plot_data_b = {
        'methods': ['Cache Efficiency', 'Load Factor'],
        'X_data': neighbor_sizes,
        'Y_data': {'Cache Efficiency': [], 'Load Factor': []}
    }
    project_dir = f"{home_dir}/CHIME"
    work_dir = f"{project_dir}/build"
    env_cmd = f"cd {work_dir}"

    SPLIT_WORKLOADS = f"{env_cmd} && python3 {ycsb_dir}/split_workload.py {workload_name} {key_type} {CN_num} {client_num_per_CN}"
    cmd.all_execute(SPLIT_WORKLOADS, CN_num)

    for neighbor_size in neighbor_sizes:
        # change config
        sed_cmd = (sed_workloads_dir('./workloads.conf', workloads_dir) + " && " +
                    generate_sed_cmd('./include/Common.h', method, 8 if key_type == 'randint' else 32, value_size,
                                    cache_size, MN_num, span_size,
                                    {'neighbor_size': neighbor_size, 'hotspot_buffer_size': hotspot_buffer_size}))
        cmake_option = f'{common_options} {cmake_options[method]}'
        cmake_option = cmake_option.replace('-DMIDDLE_TEST_EPOCH=off', '-DMIDDLE_TEST_EPOCH=on')

        BUILD_PROJECT = f"cd {project_dir} && {sed_cmd} && mkdir -p build && cd build && cmake {cmake_option} .. && make clean && make -j"
        CLEAR_MEMC = f"{env_cmd} && /bin/bash ../script/restartMemc.sh"
        YCSB_TEST = f"{env_cmd} && ./ycsb_test {CN_num} {client_num_per_CN} 2 {key_type} {workload_name}"
        KILL_PROCESS = f"{env_cmd} && killall -9 ycsb_test"

        cmd.all_execute(BUILD_PROJECT)
        while True:
            try:
                cmd.all_execute(KILL_PROCESS, CN_num)
                cmd.one_execute(CLEAR_MEMC)
                logs = cmd.all_long_execute(YCSB_TEST, CN_num)
                tpt, _, _, _, _, _, load_factor = tp.get_statistics(logs, target_epoch, get_avg=True)
                consumed_cache_size, _ = tp.get_cache_statistics(logs)
                break
            except (FunctionTimedOut, Exception) as e:
                print_WARNING(f"Error! Retry... {e}")

        print_GOOD(f"[FINISHED POINT] method={method} span_size={span_size} tpt={tpt} concumed_cache_size={consumed_cache_size} load_factor={load_factor}")
        plot_data_a['Y_data'][method].append(tpt)
        plot_data_b['Y_data']['Cache Efficiency'].append(consumed_cache_size)
        plot_data_b['Y_data']['Load Factor'].append(load_factor)

    # save data
    Path(output_path).mkdir(exist_ok=True)
    with (Path(output_path) / f'fig_{sub_exp_1}.json').open(mode='w') as f:
        json.dump(plot_data_a, f, indent=2)
    with (Path(output_path) / f'fig_{sub_exp_2}.json').open(mode='w') as f:
        json.dump(plot_data_b, f, indent=2)


if __name__ == '__main__':
    cmd = CMDManager(cluster_ips, master_ip)
    tp = LogParser()
    t = main(cmd, tp)
    with (Path(output_path) / 'time.log').open(mode="a+") as f:
        f.write(f"fig_{exp_num}.py execution time: {int(t//60)} min {int(t%60)} s\n")

    pg = PicGenerator(output_path, style_path)
    pg.generate(f'{sub_exp_1}')
    pg.generate(f'{sub_exp_2}')
