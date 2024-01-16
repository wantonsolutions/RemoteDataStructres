import lib

import experiments.plot_cuckoo as plot_cuckoo
# import run_experiments as re
import experiments.data_management as dm
import experiments.orchestrator as orchestrator
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


def search_success_lock_size():
    table_size = 1024 * 1024 * 1
    entry_size = 8
    config = lib.get_config()
    memory_size = entry_size * table_size
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)

    clients = 400
    # fills = [50]
    # fills = [90]
    # fills = [10,20,30]
    # fills = [10,20,30,40,50]
    # fills=[10]
    # fills = [10,50,80]

    config['trials'] = 1
    config['num_clients'] = str(clients)
    # workloads = ["ycsb-b"]
    config["workload"] = "ycsb-w"
    # workloads = ["ycsb-c", "ycsb-w"]
    config['search_function'] = "bfs"
    config["buckets_per_lock"] = str(16)
    config["locks_per_message"] = str(64)
    config["prime_fill"]="85"
    config["max_fill"]="90"
    config["prime"]="true"
    config["deterministic"]="False"
    config["read_threshold_bytes"] = str(256)
    config["runtime"]=str(1)


    # rows_per_lock = [1,2,4,8,16,32,64]
    rows_per_lock = [1,2,4,8,16,32,64,128]
    # rows_per_lock = [1]
    orchestrator.boot(config)
    runs=[]
    for rpl in rows_per_lock:
        lconfig=config.copy()
        lconfig["buckets_per_lock"] = str(rpl)
        runs.append(orchestrator.run_trials(lconfig))
        dirname="search_success_lock_size"
    dm.save_statistics(runs, dirname=dirname)
        # plot_general_stats_last_run(dirname=dirname)

def plot_search_success_lock_size():
    fig, ax = plt.subplots(1,1, figsize=(3,2.5))
    dirname="search_success_lock_size"
    stats = dm.load_statistics(dirname=dirname)
    stats=stats[0]


    first_search_fails, first_search_errors = plot_cuckoo.client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'failed_insert_first_search_count', 'completed_insert_count')
    second_search_fails, second_search_errors = plot_cuckoo.client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'failed_insert_second_search_count', 'completed_insert_count')
    x_axis_vals = plot_cuckoo.get_x_axis(stats, "buckets per lock")

    ax.errorbar(x_axis_vals, first_search_fails, yerr=first_search_errors, label="first search retries", marker="s")
    ax.errorbar(x_axis_vals, second_search_fails, yerr=second_search_errors, label="second search retries", marker="o")
    ax.legend()
    ax.set_xlabel("rows per lock")
    ax.set_ylabel("failures per insert")
    ax.set_ylim(0,5)
    plt.tight_layout()
    plt.savefig("search_success_lock_size.pdf")

    # p = ax.errorbar(x_axis_vals,first_search_fails,yerr=first_search_errors,label="first search retries", marker="s")
    # color = p[0].get_color()
    # ax.errorbar(x_axis_vals,lock_retries,yerr=lock_errors,label="failed lock aquisitions", marker="o", linestyle=":", color=color)

def plot_search_success_lock_size_static():
    fig, ax = plt.subplots(1,1, figsize=(3,2.5))

    first_search_fails=[4.598514299393423,3.5937194462323604, 2.8302924266573823 ,2.1008837486960084 ,1.4873636954870904 ,1.1852649674092721 ,1.0151446920478682 ,0.9307166854219571 ]
    second_search_fails=[3.937585437932405  ,2.973340926977462  ,2.0747392797475492 ,1.2378803095887552 ,0.5375662427481417 ,0.217275521568523  ,0.0560772062348307 ,0.01250699956548677,]
    x_axis=[1,2,4,8,16,32,64,128]
    x_axis=[str(x) for x in x_axis]
    success_rate = [(f-s)/f for s,f in zip(second_search_fails,first_search_fails)]
    success_rate = [s  * 100 for s in success_rate]
    print(success_rate)
    ax.plot(x_axis, success_rate, marker="s", color=lib.rcuckoo_color)
    # ax.plot(x_axis, success_rate, label="search success ratio", marker="s", color=lib.rcuckoo_color)
    # ax.plot(x_axis, first_search_fails, label="first search", marker="s", color=lib.fusee_color)
    # ax.plot(x_axis, second_search_fails, label="second search", marker="o", color=lib.rcuckoo_color)
    # ax.legend()
    ax.set_xlabel("rows per lock")
    ax.set_ylabel("success rate (%)")
    ax.set_ylim(0,100)
    plt.tight_layout()
    plt.savefig("search_success_lock_size.pdf")


# search_success_lock_size()
# plot_search_success_lock_size()
plot_search_success_lock_size_static()