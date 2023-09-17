
from experiments import plot_cuckoo as plot_cuckoo
from experiments import data_management as dm
import experiments.orchestrator as orchestrator
import numpy as np
import matplotlib.pyplot as plt
import lib

def factor_exp():
    #dont start lower than 1.7
    # factors = [1.7,1.8,1.9,2.0,2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,3.0,3.1,3.2,3.3]
    factors = [1.7,1.9,2.1,2.3,2.5,2.7,2.9,3.1,3.3]
    # factors = [2.3, 2.5, 2.7]
    config = lib.get_config()
    # factors = [3.0]
    clients = 400
    config['num_clients'] = clients
    config['workload'] = "ycsb-w"
    config["prime"]="true"
    config["prime_fill"]="40"
    config["max_fill"]="50"
    config["search_function"]="bfs"
    entry_size =8

    table_size = 1024 * 1024 * 10
    memory_size = entry_size * table_size
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)
    config["trials"] = 5


    orchestrator.boot(config)
    runs = []
    for f in factors:
        lconfig = config.copy()
        print("running factor", f)
        lconfig['hash_factor'] = str(f)
        results = orchestrator.run_trials(lconfig)
        if len(results) > 0:
            runs.append(results)
    directory = "factor"
    dm.save_statistics(runs, dirname=directory)

def plot_factor_exp():
    fig, ax = plt.subplots(1,1, figsize=(5,3))
    axt=ax.twinx()
    dirname = "factor"
    stats = dm.load_statistics(dirname=dirname)
    stats=stats[0]
    tputs = []
    print(tputs)

    throughputs = []
    std_errs = []
    for stat in stats:
        single_run_throughputs=[]
        for r in stat:
            s = plot_cuckoo.single_run_throughput(r)
            single_run_throughputs.append(s)
        print("single_run_throughput: ", single_run_throughputs)
        throughputs.append(np.mean(single_run_throughputs))
        std_errs.append(np.std(single_run_throughputs))

    x_axis = plot_cuckoo.get_x_axis(stats, "hash factor")
    x_axis = [float(x) for x in x_axis]
    tput = ax.errorbar(x_axis, throughputs, yerr=std_errs, label="insert tput", marker="o")
    ax.set_xlabel("hashing factor (f)")
    ax.set_ylabel("MOPS")


    # for i in range(len(stats)):
    #     tputs.append(plot_cuckoo.single_run_throughput(stats[i]))
    # print("done collecting tputs")
    # plot_cuckoo.throughput(ax, stats, decoration=False, x_axis="hash factor", label="rcuckoo")
    # print(x_axis)
    # print(tputs)
    # ax.plot()

    # ax.legend()
    # ax.set_xlabel("fill_factor")
    # ax.set_ylabel("MOPS")
    # ax.set_ylim(0,13)

    max_fill = [(1.7, 53.5),(1.9,79.5),(2.1, 93.1),(2.3,96.6),(2.5,97.3), (2.7, 97.5), (2.9, 97.7), (3.1, 97.9), (3.3, 98.2)]

    x = [ v[0] for v in max_fill]
    y = [ v[1] for v in max_fill]
    fill = axt.plot(x,y, label="max fill", color="black", linestyle="--")

    # labels = [tput[1].get_label(), fill[1].get_label()]
    ax.legend()
    axt.legend()
    axt.set_ylim(50,100)
    axt.set_ylabel("max fill (%)")


    plt.tight_layout()
    plt.savefig("factor.pdf")


def plot_factor_exp_static():
    fig, ax = plt.subplots(1,1, figsize=(5,3))
    axt=ax.twinx()
    dirname = "factor"
    stats = dm.load_statistics(dirname=dirname)
    stats=stats[0]
    tputs = []
    print(tputs)

    throughputs = []
    std_errs = []
    for stat in stats:
        single_run_throughputs=[]
        for r in stat:
            s = plot_cuckoo.single_run_throughput(r)
            single_run_throughputs.append(s)
        print("single_run_throughput: ", single_run_throughputs)
        throughputs.append(np.mean(single_run_throughputs))
        std_errs.append(np.std(single_run_throughputs))

    x_axis = plot_cuckoo.get_x_axis(stats, "hash factor")
    x_axis = [float(x) for x in x_axis]

    throughputs = [8.549023247211482, 7.81310996563574, 7.389588069433427, 6.989896907216491, 6.494618803418798, 5.690631135531132, 4.734080341880341, 3.903198506566869, 3.509013793103447]
    print(throughputs)
    tput = ax.plot(x_axis, throughputs,  label="insert tput", marker="o")
    ax.set_xlabel("hashing factor (f)")
    ax.set_ylabel("MOPS")


    max_fill = [(1.7, 53.5),(1.9,79.5),(2.1, 93.1),(2.3,96.6),(2.5,97.3), (2.7, 97.5), (2.9, 97.7), (3.1, 97.9), (3.3, 98.2)]

    x = [ v[0] for v in max_fill]
    y = [ v[1] for v in max_fill]
    fill = axt.plot(x,y, label="max fill", color="black", linestyle="--")


    ax.legend(tput+fill, ['throughput',"max fill"])

    # ax.legend()
    # axt.legend()
    axt.set_ylim(50,100)
    axt.set_ylabel("max fill (%)")


    plt.tight_layout()
    plt.savefig("factor.pdf")



# factor_exp()
# plot_factor_exp()

plot_factor_exp_static()