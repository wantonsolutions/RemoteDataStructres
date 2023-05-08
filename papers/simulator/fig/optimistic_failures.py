import sys
# caution: path[0] is reserved for script path (or '' in REPL)
sys.path.insert(1, '/home/ena/RemoteDataStructres/rmem_simulator')
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
    table_size = 1680  * 1 #lcm of 3,4,5,6,7,8,10,12,14,16
    # clients=[1,2,4,8,16,32]
    clients=[1,2,4,8]
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
        config['max_fill']= 100
        runs.append(sim.run_trials(config))
    dm.save_statistics(runs, dirname=data_dir)
    plot_general_stats_last_run()

success_rate_contention_machines()