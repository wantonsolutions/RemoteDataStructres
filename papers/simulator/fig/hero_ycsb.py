import lib
lib.import_rmem_simulator()

import plot_cuckoo as plot_cuckoo
import log as log
import state_machines as sm
import simulator as simulator
# import run_experiments as re
import data_management as dm
import cuckoo as cuckoo
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm

def run_hero_ycsb():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")
    # table_size = 1680  * 512
    table_size=420
    clients = [1,2,4,8,16,32,64,128]
    # clients = [1,2]
    state_machines = [cuckoo.rcuckoobatch,sm.race]

    master_config = lib.get_config()
    master_config["bucket_size"]=8
    master_config['num_steps'] = 100000000000
    master_config['bucket_size'] = 8
    master_config['read_threshold_bytes'] = 512
    master_config['indexes'] = table_size
    master_config['trials'] = 1
    master_config['max_fill']= 90
    # workloads = ["ycsb-a", "ycsb-b","ycsb-c", "ycsb-w"]
    workloads = ["ycsb-a", "ycsb-b", "ycsb-w"]
    log.set_off()

    #ycsb-a
    for workload in workloads:
        multi_runs=[]
        for s in state_machines:
            runs=[]
            for c in clients:
                config = master_config.copy()
                config['num_clients'] = c
                config['state_machine']=s
                config['workload']=workload
                runs.append(simulator.run_trials(config))
            dm.save_statistics(runs)
            # plot_cuckoo.plot_general_stats_last_run()
            multi_runs.append(runs)
        dirname="hero_ycsb/"+workload
        dm.save_statistics(multi_runs, dirname=dirname)
        # plot_cuckoo.plot_general_stats_last_run(dirname=dirname)

def plot_hero_ycsb_throughput():
    workloads = ["ycsb-a", "ycsb-b", "ycsb-w"]

    fig, axs = plt.subplots(1,len(workloads), figsize=(12,3))
    for i in range(len(workloads)):
        dirname="hero_ycsb/"+workloads[i]
        ax = axs[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        plot_cuckoo.throughput_approximation(ax, stats, decoration=False)
        ax.legend()
        ax.set_xlabel("clients")
        ax.set_title(workloads[i])
        ax.set_ylabel("throughput \n(ops/rtts)*clients")

    plt.tight_layout()
    plt.savefig("hero_ycsb_throughput.pdf")

# run_hero_ycsb()
plot_hero_ycsb_throughput()