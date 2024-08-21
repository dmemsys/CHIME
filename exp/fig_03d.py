#
# Fig.3d: Tradeoff of amplification factor and load factor of hash schemes
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
exp_num = '03d'

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
table_size                = exp_params['table_size']
ampl_sizes                = exp_params['ampl_size']
execution_names = {
    'Associativity': 'hash_test',
    'RACE': 'race_hash_test',
    'Hopscotch': 'hopscotch_hash_test',
    'FaRM': 'farm_hash_test'
}


@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    plot_data = {
        'methods': methods,
        'X_data': {method: [] for method in methods},  # ampl. factor
        'Y_data': {method: [] for method in methods}   # load factor
    }
    for method in methods:
        project_dir = f"{home_dir}/CHIME"
        work_dir = f"{project_dir}/build"
        env_cmd = f"cd {work_dir}"

        for ampl_size in ampl_sizes:
            # change config
            BUILD_PROJECT = f"cd {project_dir} && mkdir -p build && cd build && cmake .. && make clean && make -j"
            cmd.one_execute(BUILD_PROJECT)

            HASH_TEST = f"{env_cmd} && ./{execution_names[method]} {table_size} {ampl_size}"
            KILL_PROCESS = f"{env_cmd} && killall -9 {execution_names[method]}"

            load_factor = 0
            while True:
                try:
                    cmd.one_execute(KILL_PROCESS)
                    line = cmd.one_execute(HASH_TEST)[0]
                    data = line.strip().split(' ')[3]
                    if data.replace('.', '').isdigit():
                        load_factor = float(data) * 100
                        break
                except (FunctionTimedOut, Exception) as e:
                    print_WARNING(f"Error! Retry... {e}")

            print_GOOD(f"[FINISHED POINT] method={method} ampl_size={ampl_size} load_factor={load_factor}")
            plot_data['X_data'][method].append(ampl_size)
            plot_data['Y_data'][method].append(load_factor)
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
