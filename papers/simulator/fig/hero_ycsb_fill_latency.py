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

def run_hero_ycsb_fill_latency():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")
    table_size = 1680  * 512
    # table_size=1680
    clients = 128
    fills = [10, 20, 30, 40, 50, 60, 70, 80, 90]
    # fills = [10, 20]
    # clients = [100]
    state_machines = [cuckoo.rcuckoobatch,sm.race]

    master_config = lib.get_config()
    master_config["bucket_size"]=8
    master_config['num_steps'] = 100000000000
    master_config['bucket_size'] = 8
    master_config['read_threshold_bytes'] = 512
    master_config['locks_per_message'] = 64
    master_config['indexes'] = table_size
    master_config['trials'] = 1
    workloads = ["ycsb-a", "ycsb-b","ycsb-c", "ycsb-w"]
    # workloads = ["ycsb-a", "ycsb-b", "ycsb-w"]
    # workloads = ["ycsb-c"]
    log.set_off()

    #ycsb-a
    for workload in workloads:
        multi_runs=[]
        for s in state_machines:
            runs=[]
            for fill in fills:
                config = master_config.copy()
                config['num_clients'] = clients
                config['state_machine']=s
                config['workload']=workload
                config['max_fill']=fill
                steps = 1000000
                r = simulator.fill_then_run_trials(config, fill_to=config['max_fill'], max_fill=config['max_fill']+5, max_steps=steps)
                runs.append(r)
            # dm.save_statistics(runs)
            # plot_cuckoo.plot_general_stats_last_run()
            multi_runs.append(runs)
        dirname="hero_ycsb_latency/"+workload
        dm.save_statistics(multi_runs, dirname=dirname)
        # plot_cuckoo.plot_general_stats_last_run(dirname=dirname)

def plot_hero_ycsb_fill_latency():
    # workloads = ["ycsb-w", "ycsb-a", "ycsb-b", "ycsb-c"]
    workloads = [ "ycsb-w", "ycsb-a", "ycsb-b", "ycsb-c"]
    # workloads = [ "ycsb-w"]
    # workloads = ["ycsb-c"]

    fig, axs = plt.subplots(1,len(workloads), figsize=(12,3))
    if len(workloads) == 1:
        axs = [axs]
    for i in range(len(workloads)):
        dirname="hero_ycsb_latency/"+workloads[i]
        ax = axs[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        # import json
        # print(json.dumps(stats, indent=4))
        # exit(1)
        plot_cuckoo.fill_vs_latency(ax, stats, decoration=False)
        if workloads[i] == "ycsb-a" or workloads[i] == "ycsb-b":
            ax.legend(fontsize=6, ncol=2)
        else:
            ax.legend(fontsize=8)
        # ax.set_xlabel("clients")
        ax.set_title(workloads[i])
        ax.set_ylabel("average rtt")
        ax.set_ylim(bottom=0, top=3.5)
        ax.set_xlim(left=5, right=100)
        ax.set_xlabel("fill factor")

    plt.tight_layout()
    plt.savefig("hero_ycsb_fill_latency.pdf")

# run_hero_ycsb_fill_latency()
plot_hero_ycsb_fill_latency()