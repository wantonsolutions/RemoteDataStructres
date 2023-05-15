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


def run_search_dependence():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")
    table_size = 1680 * 512

    locks_per_message = [1, 2, 4, 8, 16, 32, 64]

    master_config = lib.get_config()
    master_config['num_clients'] = 120
    master_config["bucket_size"]=8
    master_config['num_steps'] = 100000000000
    master_config['bucket_size'] = 8
    master_config['read_threshold_bytes'] = 512
    master_config['locks_per_message'] = 64
    master_config['indexes'] = table_size
    master_config['trials'] = 1
    master_config['max_fill']= 90
    master_config['workload'] = "ycsb-w"
    master_config['state_machine'] = cuckoo.rcuckoobatch


    log.set_off()

    configs = [ 
                ("random", "independent"),
                ("random", "dependent"),
                ("a_star", "independent"),
                ("a_star", "dependent"),
            ]
    for c in configs:
        runs=[]
        for locks in locks_per_message:
            config = master_config.copy()
            search = c[0]
            dependent = c[1]
            config['search_function'] = search
            config['location_function'] = dependent
            config['locks_per_message'] = locks
            runs.append(simulator.run_trials(config))
        dm.save_statistics(runs, dirname="search_dependence/"+search+"_"+dependent)


def plot_search_dependence():
    random_independent, _ = dm.load_statistics("search_dependence/random_independent")
    random_dependent, _ = dm.load_statistics("search_dependence/random_dependent")
    a_star_dependent, _ = dm.load_statistics("search_dependence/a_star_dependent")

    percentile=99
    fig, ax = plt.subplots(1, 1, figsize=(4, 3))

    x_axis=plot_cuckoo.detect_x_axis(random_independent)
    x_axis_vals = plot_cuckoo.get_x_axis(random_independent, x_axis)
    x = np.arange(len(x_axis_vals))


    random_independent_rtt, random_independent_err = plot_cuckoo.client_stats_get_percentile_err_trials(random_independent, 'insert_rtt', percentile)
    h1 = ax.errorbar(x,random_independent_rtt,yerr=random_independent_err, linestyle="-", marker="o", label="random independent")

    random_dependent_rtt, random_dependent_err = plot_cuckoo.client_stats_get_percentile_err_trials(random_dependent, 'insert_rtt', percentile)
    h1 = ax.errorbar(x,random_dependent_rtt,yerr=random_dependent_err, linestyle="-", marker="^", label="random (dependent)")

    a_star_dependent_rtt, a_star_dependent_err = plot_cuckoo.client_stats_get_percentile_err_trials(a_star_dependent, 'insert_rtt', percentile)
    h1 = ax.errorbar(x,a_star_dependent_rtt,yerr=a_star_dependent_err, linestyle="-", marker="s", label="a star (dependent)")

    race_mean=3
    ax.axhline(y=race_mean, color='r', linestyle=':', label="race mean (" +str(race_mean) + ")")
    ax.set_ylabel("RTT "+str(percentile)+"th percentile")
    ax.set_xticks(x)
    ax.set_xticklabels(x_axis_vals)

    ax.set_xlabel(x_axis)
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=8)

    plt.tight_layout()
    plt.savefig("search_dependence.pdf")

run_search_dependence()
plot_search_dependence()