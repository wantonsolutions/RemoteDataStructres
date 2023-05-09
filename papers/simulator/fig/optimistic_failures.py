import lib
lib.import_rmem_simulator()

import plot_cuckoo as pc
import log as log
import state_machines as sm
import simulator as sim
# import run_experiments as re
import data_management as dm
import cuckoo as cuck
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm

import random

data_dir = "optimistic_failures"

def plot_general_stats_last_run():
    stats, directory = dm.load_statistics(data_dir)
    print("plot general stats")
    plot_names = [
        "general_stats",
        "cas_success_rate",
        "read_write_ratio",
        "request_success_rate",
        "rtt_per_operation",
        "bytes_per_operation",
        "messages_per_operation",
        "fill_factor"
        ]
    pc.multi_plot_runs(stats, plot_names, directory)

def success_rate_contention_machines():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")
    multi_runs=[]
    # table_size = 1680  * 4 #lcm of 3,4,5,6,7,8,10,12,14,16
    # table_size = 1680  * 64 #lcm of 3,4,5,6,7,8,10,12,14,16
    table_size = (8 * 1024) * 16
    # clients=[1,2,4,8,16,32]
    clients=[1,2,4,8,16,32,64,128]
    # clients=[1]
    # clients=[32]
    bucket_size=8
    log.set_off()
    runs=[]
    print(clients)
    for c in clients:
        config = sim.default_config()
        config['description'] = "Optimistic Failures"
        config['name'] = "optimistic_failures"
        config['num_clients'] = c
        config['num_steps'] = 100000000000
        config['bucket_size'] = bucket_size
        config['read_threshold_bytes'] = config['entry_size'] * bucket_size
        config['indexes'] = table_size
        config['trials'] = 1
        config['state_machine']=sm.rcuckoo
        config['max_fill']= 90
        runs.append(sim.run_trials(config))
    dm.save_statistics(runs, dirname=data_dir)

def single_run_op_success_rate(stat, op_string):
    op_percent = []
    for client in stat['clients']:
        ops = client['stats']['completed_'+op_string+'_count']
        op_failures = client['stats']['failed_'+op_string+'_count']

        op_success_rate = 0
        if ops != 0:
            op_success_rate = float(ops)/(ops+op_failures)
        op_percent.append(op_success_rate)

    return op_percent

def request_success_rate_line(ax, stats, label, x_axis="clients"):
    #read write ratio should work for both single and multi run
    if isinstance(stats, dict):
        stats = [stats]

    avg_op_rates=[]
    avg_op_err=[]
    x_axis_vals = pc.get_x_axis(stats, x_axis)
    for stat in stats:
        op_percentages, op_errors = [], []
        for run in stat:
            op_percent = single_run_op_success_rate(run, "insert")
            op_percentages.append(np.mean(op_percent))
            op_errors.append(pc.stderr(op_percent))
        avg_op_rates.append(np.mean(op_percentages))
        avg_op_err.append(np.mean(op_errors))
    
    x = np.arange(len(x_axis_vals))
    avg_op_rates = [ 1 - x for x in avg_op_rates]
    ax.bar(x, avg_op_rates, yerr=avg_op_err, label=label,width=0.75, edgecolor='black' )
    ax.set_xticks(x, x_axis_vals)

def request_success_rate_decoration(ax, x_axis):
    ax.set_ylabel("Insert Failure Rate")
    ax.set_xlabel(x_axis)
    ax.set_ylim(0,1)
    # ax.set_xticks(x_pos+bar_width/2)
    # ax.set_xticklabels(x_axis_vals)
    # ax.legend()

def request_success_rate(ax, stats, x_axis="clients"):
    stats = pc.correct_stat_shape(stats)
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        request_success_rate_line(ax, stat, label=state_machine_label, x_axis=x_axis)
    request_success_rate_decoration(ax, x_axis)

def plot_request_success_rate():
    stats, directory = dm.load_statistics(data_dir)
    fig, ax = plt.subplots(1,1,figsize=(4,2.5))
    request_success_rate(ax, stats)
    fig.tight_layout()
    fig.savefig(data_dir+".pdf")



success_rate_contention_machines()
plot_general_stats_last_run()
plot_request_success_rate()