import lib

import experiments.plot_cuckoo as plot_cuckoo
import experiments.data_management as dm
import experiments.orchestrator as orchestrator
# import run_experiments as re
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm
import json

def sherman_client_count_index(data, index):
    clients = data["clients"]
    if index not in clients:
        raise Exception("Index {} not found in clients".format(index))
    for i, c in enumerate(clients):
        if c == index:
            return i
    

def plot_sherman_latency(axs):

    # data, dir = dm.load_statistics("data/sherman_ycsb_uniform")
    data, dir = dm.load_statistics("../../../experiments/systems/Sherman/data/sherman_ycsb_uniform")
    clients = data["clients"]

    # workloads = ["workloada", "workloadb", "workloadc", "workloadupd100"]
    # workload_labels = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-d"]
    # clients = data["clients"]

    i=0
    clients=5

    index = sherman_client_count_index(data, clients)
    sherman_read_latency = lib.tofloat(data["workloadc"]['th50'])[index]
    sherman_write_latency = lib.tofloat(data["workloadupd100"]['th50'])[index]

    for ax in axs:
        ax.axhline(y=sherman_read_latency, color='r', linestyle='-', label="Sherman Read Latency "+str(clients)+" threads")
        ax.axhline(y=sherman_write_latency, color='b', linestyle='-', label="Sherman Write Latency "+str(clients)+" threads")

def plot_fusee_latency(axs):
    data, dir = dm.load_statistics("../../../experiments/systems/FUSEE/data/fusee_latency")
    data = json.loads(data)
    for k in data:
        print(k)

    insert_latencies=data["insert_latency"]
    read_latencies=data["read_latency"]

    avg_read_latency = np.mean(read_latencies)
    avg_insert_latency = np.mean(insert_latencies)

    for ax in axs:
        ax.axhline(y=avg_read_latency, color='r', linestyle=':', label="FUSEE Read Latency 1 thread")
        ax.axhline(y=avg_insert_latency, color='b', linestyle=':', label="FUSEE Insert Latency 1 thread")





def run_hero_ycsb_fill():
    table_size = 1024 * 1024
    clients = 4
    fills = [10,20,30,40,50,60,70,80]
    config = lib.get_config()

    config["prime"]="true"
    config['trials'] = 1
    config['num_clients'] = str(clients)
    workloads = ["ycsb-a", "ycsb-b","ycsb-c", "ycsb-w"]

    orchestrator.boot(config.copy())
    for workload in workloads:
        runs=[]
        for fill in fills:
            lconfig = config.copy()
            lconfig['max_fill']=str(fill)
            lconfig['prime_fill']=str(fill-10)
            lconfig['workload']=workload
            runs.append(orchestrator.run_trials(lconfig))
        dirname="hero_ycsb_latency/"+workload
        dm.save_statistics(runs, dirname=dirname)
        # plot_general_stats_last_run(dirname=dirname)


def plot_hero_ycsb_fill():
    workloads = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-w"]
    fig, axs = plt.subplots(1,len(workloads), figsize=(12,3))
    for i in range(len(workloads)):
        dirname="hero_ycsb_latency/"+workloads[i]
        ax = axs[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        plot_cuckoo.latency_per_operation(ax, stats, x_axis="max fill", twin=False, decoration=False, hide_zeros=True)
        ax.set_xlabel("fill_factor")
        ax.set_title(workloads[i])
        ax.set_ylabel("us")
        ax.set_ylim(0,15)

    plot_sherman_latency(axs)
    plot_fusee_latency(axs)
    axs[0].legend(prop={'size': 6})

    plt.tight_layout()
    plt.savefig("hero_ycsb_fill_latency.pdf")

# run_hero_ycsb_fill()
plot_hero_ycsb_fill()