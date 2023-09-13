import lib

import experiments.plot_cuckoo as plot_cuckoo
# import run_experiments as re
import experiments.data_management as dm
import experiments.orchestrator as orchestrator
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


def extract_multiple_fusee_ycsb_runs(total_runs=10):
    runs = []
    fusee_data_dir = "../../../experiments/systems/FUSEE/data"
    print("Attempting to extract {} ycsb runs".format(total_runs))
    for i in range(total_runs):
        try :
            data, dir = dm.load_statistics(fusee_data_dir + "/fusee_ycsb_{}".format(i))
            runs.append(data)
        except:
            print("Failed to load data for run {}".format(i))

    print("Successfully able to extract {} ycsb runs".format(len(runs)))
    return runs

def average_runs(runs, workload):
    workload_data = []
    for run in runs:
        workload_data.append(lib.mops(run[workload]))
    workload_data = np.array(workload_data)
    workload_data[workload_data == 0] = np.nan
    avg = np.nanmean(workload_data, axis=0)
    err = np.nanstd(workload_data, axis=0)
    return avg, err


def plot_fusee_ycsb_multi_run(axs):
    runs = extract_multiple_fusee_ycsb_runs(5)

    workloads = ["workloada", "workloadb", "workloadc", "workloadd"]
    workload_labels = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-d"]

    i=0
    for load, label in zip(workloads, workload_labels):
        avg, err = average_runs(runs, load)
        avg = avg / 1000000
        err = err / 1000000
        print(label)
        print("threads=", runs[0]["clients"])
        print("avg_ops=", avg)
        # exit(0)
        # print(avg, err, runs[0]["clients"], label)
        axs[i].errorbar(runs[0]["clients"], avg, yerr=err, marker='x', label="fusee")
        i=i+1



def plot_sherman_ycsb(axs):
    data, dir = dm.load_statistics("../../../experiments/systems/Sherman/data/sherman_ycsb_uniform")
    # data, dir = dm.load_statistics("data/sherman_ycsb_zipf")
    # data = json.loads(data)

    workloads = ["workloada", "workloadb", "workloadc", "workloadupd100"]
    workload_labels = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-d"]
    clients = data["clients"]
    i=0
    for load, label in zip(workloads, workload_labels):
        try:
            # print("tring to plot", load, label)
            workload_data = lib.tofloat(data[load]['tput'])
            print(label)
            print("avg_ops=", lib.ops(workload_data))
            print("threads=", clients)

            axs[i].plot(clients,workload_data, marker='x', label="Sherman")
        except:
            continue
        i+=1


def run_hero_ycsb():
    table_size = 1024 * 1024 * 10
    clients = [4, 8, 16, 32, 64, 128, 160]
    config = lib.get_config()
    # clients = [64]
    # clients = [2]

    config["prime"]="true"
    config["prime_fill"]="40"
    config["max_fill"]="50"

    # config['runtime'] = str(10)
    # config['read_threshold_bytes'] = str(256)

    # config["memory_size"] = str(memory_size)
    # config['indexes'] = str(table_size)
    config['trials'] = 1
    # config['deterministic'] = "true"
    workloads = ["ycsb-a", "ycsb-b","ycsb-c", "ycsb-w"]
    # workloads = ["ycsb-a", "ycsb-b"]
    # workloads = ["ycsb-w"]
    # workloads = ["ycsb-a", "ycsb-a"]
    # log.set_debug()

    #ycsb-a
    orchestrator.boot(config.copy())
    for workload in workloads:
        runs=[]
        for c in clients:
            lconfig = config.copy()
            lconfig['num_clients'] = str(c)
            lconfig['workload']=workload
            runs.append(orchestrator.run_trials(lconfig))
        # dm.save_statistics(runs)
        # plot_general_stats_last_run()
        # runs.append(runs)
        dirname="hero_ycsb/"+workload
        dm.save_statistics(runs, dirname=dirname)
        # plot_cuckoo.plot_general_stats_last_run(dirname=dirname)


def plot_hero_ycsb_throughput():
    workloads = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-w"]

    #plot cuckoo
    fig, axs = plt.subplots(1,len(workloads), figsize=(15,3))
    for i in range(len(workloads)):
        dirname="hero_ycsb/"+workloads[i]
        ax = axs[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        plot_cuckoo.throughput(ax, stats, decoration=False)
        ax.set_xlabel("clients")
        ax.set_title(workloads[i])
        ax.set_ylabel("MOPS")


    #plot fusee
    plot_fusee_ycsb_multi_run(axs)
    plot_sherman_ycsb(axs)

    axs[0].legend()
    

    plt.tight_layout()
    plt.savefig("hero_ycsb_throughput.pdf")

# def plot_hero_ycsb_throughput():
#     workloads = ["ycsb-w", "ycsb-a", "ycsb-b", "ycsb-c"]
#     # workloads = ["ycsb-a", "ycsb-b", "ycsb-w", "ycsb-c"]
#     # workloads = ["ycsb-c"]

#     fig, axs = plt.subplots(1,len(workloads), figsize=(15,3))
#     if len(workloads) == 1:
#         axs = [axs]
#     for i in range(len(workloads)):
#         dirname="hero_ycsb/"+workloads[i]
#         ax = axs[i]
#         stats = dm.load_statistics(dirname=dirname)
#         stats=stats[0]
#         plot_cuckoo.throughput_approximation(ax, stats, decoration=False)
#         ax.legend()
#         ax.set_xlabel("clients")
#         ax.set_title(workloads[i])
#         ax.set_ylabel("throughput \n(ops/rtts)*clients")

#     plt.tight_layout()
#     plt.savefig("hero_ycsb_throughput.pdf")


# run_hero_ycsb()
plot_hero_ycsb_throughput()