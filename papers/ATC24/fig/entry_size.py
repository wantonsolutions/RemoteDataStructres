from experiments import plot_cuckoo as plot_cuckoo
from experiments import data_management as dm
import experiments.orchestrator as orchestrator
import numpy as np
import matplotlib.pyplot as plt
import lib

def run_entry_size_exp():
    # entry_sizes = [8,16,32,64]
    table_size = 1024 * 1024 * 10
    read_threshold_bytes=1024
    entry_sizes = [8,16,32,64,128]

    config = lib.get_config()
    config["prime"]="true"
    config["prime_fill"]="40"
    config["max_fill"]="50"
    config["search_function"]="bfs"

    workloads = ["ycsb-a", "ycsb-c"]
    value_sizes=[x-4 for x in entry_sizes]

    clients=400
    config["read_threshold_bytes"] = str(read_threshold_bytes)
    for workload in workloads:
        runs = []
        for i in range(len(entry_sizes)):
            lconfig = config.copy()
            lconfig['workload'] = workload
            if read_threshold_bytes < entry_sizes[i] * int(lconfig['bucket_size']):
                print("WARNING READ THRESHOLD BEING CHANGED TO MAKE RUN WORK")
                lconfig['read_threshold_bytes'] = entry_sizes[i] * int(lconfig['bucket_size'])

            lconfig['entry_size'] = str(entry_sizes[i])
            lconfig['value_size'] = str(value_sizes[i])

            memory_size = entry_sizes[i] * table_size
            lconfig["indexes"] = str(table_size)
            lconfig["memory_size"] = str(memory_size)
            lconfig['num_clients'] = str(clients)
            orchestrator.build(lconfig)
            orchestrator.boot(lconfig)
            results = orchestrator.run_trials(lconfig)
            if len(results) > 0:
                runs.append(results)
        dirname="entry_size/entry_size_"+workload
        dm.save_statistics(runs, dirname=dirname)

def plot_entry_size_exp():
    print("plotting entry size exp")
    workloads = ["ycsb-a", "ycsb-c"]
    entry_size_order=[]
    entry_sizes = dict()

    fig, ax = plt.subplots(1,1, figsize=(4,2))
    for i in range(len(workloads)):
        print("plotting workload", workloads[i])
        dirname="entry_size/entry_size_"+workloads[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        # print(stats)
        for run in stats:
            run = run[0] #only do a single run
            run_tput = plot_cuckoo.single_run_throughput(run)
            run_tput = round(run_tput, 2)
            entry_size = int(run['config']['entry_size'])
            print(entry_size, run_tput)
            if entry_size not in entry_sizes:
                entry_sizes[entry_size] = []
            entry_sizes[entry_size].append(run_tput)
            if i==0:
                entry_size_order.append(entry_size)
    hatches = ['/', 'o', '\\', '.']
    colors = [lib.rcuckoo_color, lib.clover_color, lib.sherman_color, lib.fusee_color]
    print(entry_size_order)
    print(entry_sizes)
    print(workloads)
    width = 0.2
    multiplier=0
    x=np.arange(len(workloads))
    for attribute, measurement in entry_sizes.items():
        offset = width * multiplier
        # rects = ax.bar(x + offset, measurement, width,hatch=hatches[multiplier], label=str(attribute) + " KV", alpha=0.99, edgecolor="black", color=colors[multiplier])
        rects = ax.bar(x + offset, measurement, width, label=str(attribute) + " KV", alpha=0.99, edgecolor="black", color=colors[multiplier])
        # ax.bar_label(rects, padding=3 )
        multiplier += 1
    
    ax.set_ylabel("MOPS")
    # ax.set_title("Entry Size vs Throughput")

    capitalized_workloads = [x.upper() for x in workloads]
    labels = ["50% insert, 50% read", "read only"]
    size = len(entry_size_order)
    visual_offset = (size-1) * width / 2
    ax.set_xticks(x + visual_offset, labels)
    # ax.set_xticks(x, workloads)
    ax.set_ylim(0,50)
    ax.legend()
    ax.grid(axis='y')
    plt.tight_layout()
    plt.savefig("entry_size.pdf")

# run_entry_size_exp()
plot_entry_size_exp()