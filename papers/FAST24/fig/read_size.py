from experiments import plot_cuckoo as plot_cuckoo
from experiments import data_management as dm
import experiments.orchestrator as orchestrator
import numpy as np
import matplotlib.pyplot as plt
import lib

def read_size_sensitivity():
    read_sizes = [64,128,256,512,1024,2048,4096] #8192,16384,32768]
    # read_sizes = [64, 12]
    # read_sizes = [64, 128]
    config = lib.get_config()
    clients = 40
    config['num_clients'] = clients
    config['workload'] = "ycsb-c"
    config["prime"]="true"
    config["prime_fill"]="80"
    config["max_fill"]="85"
    config["search_function"]="bfs"
    config["runtime"]="10"
    entry_size = 8

    table_size = 1024 * 1024 * 10
    memory_size = entry_size * table_size
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)
    config["trials"] = 10


    orchestrator.boot(config)
    runs = []
    for read_threshold in read_sizes:
        lconfig = config.copy()
        print("running read threshold", read_threshold)
        lconfig["read_threshold_bytes"] = str(read_threshold)
        results = orchestrator.run_trials(lconfig)
        if len(results) > 0:
            print("APPENDING RESULTS")
            runs.append(results)
    directory = "read_size"
    dm.save_statistics(runs, dirname=directory)

def plot_read_size_sensitivity():
    fig, ax = plt.subplots(1,1, figsize=(5,3))
    dirname = "read_size"
    stats = dm.load_statistics(dirname=dirname)
    stats=stats[0]
    plot_cuckoo.throughput(ax, stats, decoration=False, x_axis="read threshold bytes")

    # ax.legend()
    ax.set_xlabel("read threshold bytes")
    ax.set_ylabel("MOPS")
    # ax.set_ylim(0,15)

    plt.tight_layout()
    plt.savefig("read_size.pdf")


# read_size_sensitivity()
plot_read_size_sensitivity()