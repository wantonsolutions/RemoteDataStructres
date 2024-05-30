import lib

import experiments.plot_cuckoo as plot_cuckoo
# import run_experiments as re
import experiments.data_management as dm
import experiments.orchestrator as orchestrator
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


ycsb_location = {
    "ycsb-a": 2,
    "ycsb-b": 1,
    "ycsb-c": 0,
}

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
    runs = extract_multiple_fusee_ycsb_runs(2)

    workloads = ["workloada", "workloadb", "workloadc", "workloadupd100"]
    workload_labels = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-w"]

    i=0
    for load, label in zip(workloads, workload_labels):
        avg, err = average_runs(runs, load)
        avg = avg / 1000000
        err = err / 1000000
        print(label)
        print("fusee_threads=", runs[0]["clients"])
        print("fusee_avg_ops=", avg)
        # exit(0)
        # print(avg, err, runs[0]["clients"], label)
        axs[i].errorbar(runs[0]["clients"], avg, yerr=err, marker='x', label="fusee")
        i=i+1



def plot_sherman_ycsb(axs, lw):
    dir="zipf"
    # dir="uniform"
    # path="../../../experiments/systems/Sherman/data/sherman_ycsb_{}".format(dir)
    path="./sherman-data/sherman_ycsb_{}".format(dir)
    # data, dir = dm.load_statistics("../../../experiments/systems/Sherman/data/sherman_ycsb_uniform")
    data, dir = dm.load_statistics(path)
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

            axs[ycsb_location[label]].plot(clients,workload_data, marker='x', label="Sherman", color=lib.sherman_color, linewidth=lw)
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
    workloads = ["ycsb-a", "ycsb-b", "ycsb-c"]
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
    workloads = ["ycsb-a", "ycsb-b", "ycsb-c"]

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

def plot_hero_ycsb_throughput_static():

    lw=2
    #cuckoo
    # workloads = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-w"]
    workloads = ["ycsb-a", "ycsb-b", "ycsb-c"]
    # cuck_a_tput=[0.9,1.8538780691615686, 3.1858278318625897, 5.158820844419218, 9.909487030189728, 13.036238839513999, 13.443520512176052]

    # cuck_a_tput_uniform=[1.612634436401241, 3.1367891119939872, 6.300519374146803, 11.693492896979897, 21.28665615631139, 27.622141263157907, 28.08823142406725]

    # cuck_a_tput=[1.6169309908198795, 3.138653032153032, 5.261143450921882, 8.62887432024169, 15.765564428889595, 16.967970969414202, 16.928986495948775]
    cuck_a_tput_uniform=[0.9092282608695652, 1.8194756628787878, 4.275190168772477, 7.860019655559712, 13.808765315397203, 22.35341058026726]
    cuck_a_tput_50=[0.9253051089674884, 1.7989279977586852, 4.113733837817639, 7.600767727998496, 13.008543997724265, 20.862464981273405]
    cuck_a_tput_90=[0.9315004351610098, 1.8147325777444736, 4.193965156468937, 7.692508409283095, 10.56756695928278, 11.4431864630467568]
    cuck_a_tput_99=[0.9223187476423994, 1.827882668711656, 4.176799925065568, 6.385165081457765, 1.4740531378408663, 0.5553013911691006]
    cuck_a_tput=cuck_a_tput_90
    # cuck_b_tput_uniform=[1.8, 3.5, 8.67506880602754, 15.938655554644598, 29.301799386178676, 39.26305595324906, 39.53792829394712]
    cuck_b_tput=[2.7958561727143727, 5.432706901824854, 10.433385985247629, 19.842081785937822, 33.81278853084588, 34.72034606011372, 33.79788928103255]
    cuck_b_tput_99=[1.936371810680072, 3.7636963187527073, 8.914291681049361, 16.537861675456035, 27.736156310950133, 38.57030066460734]
    cuck_b_tput=cuck_b_tput_99

    cuck_c_tput=[2.99,5.952564563617636, 11.493884402194366, 22.578766368166765, 39.36893367295243, 46.49349910889845, 46.3064643682979]
    # cuck_w_tput=[0.5, 1.0992159380413737, 1.9709423507842736, 3.0512749083638777, 5.961405911330047, 6.9574283018867895, 7.115548453608248]
    clients=[10,20, 40, 80, 160, 320, 400]
    clients=[10,20, 40, 80, 160, 320]
    cuck_a_clients= [8,16,40,80,160,320]
    cuck_b_clients=[8,16,40,80,160,320]
    fig, axs = plt.subplots(1,len(workloads), figsize=(15,3))

    axs[ycsb_location["ycsb-a"]].plot(cuck_a_clients,cuck_a_tput,label="RCuckoo",marker="o",color=lib.rcuckoo_color, linewidth=lw)
    axs[ycsb_location["ycsb-b"]].plot(cuck_b_clients,cuck_b_tput,label="RCuckoo",marker="o",color=lib.rcuckoo_color, linewidth=lw)
    axs[ycsb_location["ycsb-c"]].plot(clients,cuck_c_tput[:len(clients)],label="RCuckoo",marker="o",color=lib.rcuckoo_color, linewidth=lw)
    # axs[3].plot(clients,cuck_w_tput,label="cuckoo",marker="o")

    #fusee

    fusee_a_tput_zipf=[ 0.0360255, 0.145604,   1.0589075,  7.5669515, 11.9509565, 11.9186975]
    fusee_b_tput_zipf=[ 0.049226,   0.190538,   1.4487765, 10.241494,  14.2780315, 14.2005985]
    fusee_c_tput_zipf=[ 0.069183,   0.2876015,  2.2447135, 12.025642,  16.322664,  16.160061 ]
    # fusee_w_tput=[0.03022,   0.123381,  0.868236,  6.1967235, 9.7452085, 9.7876635]
    fusee_clients = [8, 16, 32, 64, 128, 256]

    axs[ycsb_location["ycsb-a"]].plot(fusee_clients,fusee_a_tput_zipf,label="FUSEE",marker="s", color=lib.fusee_color, linewidth=lw)
    axs[ycsb_location["ycsb-b"]].plot(fusee_clients,fusee_b_tput_zipf,label="FUSEE",marker="s", color=lib.fusee_color, linewidth=lw)
    axs[ycsb_location["ycsb-c"]].plot(fusee_clients,fusee_c_tput_zipf,label="FUSEE",marker="s", color=lib.fusee_color, linewidth=lw)
    # axs[3].plot(fusee_clients,fusee_w_tput,label="fusee",marker="s")

    clover_c_tput=[1836632,3572209,7003510,13673092,25654763,34857976,39180515,39757292,39917704,40295512,]
    clover_b_tput=[1500840,2680607,4929508,7935485,12774234,14548308,14439633,14124372,13402903,13402903,]
    clover_a_tput=[815460,1331156,1858488,2343504,2639795,2386740,2098574,1471908,1830812,1602349,]
    clover_w_tput=[866916,1639656,3114720,5418308,9386624,12991886,14967071,17022698,17165892,17432716,] 
    # clover_clients=[7,14,28,56,112,168,224,280,336,392,]
    clover_clients=[7,14,28,56,112,168,224,280,336]

    clover_c_tput = [x / 1000000 for x in clover_c_tput]
    clover_b_tput = [x / 1000000 for x in clover_b_tput]
    clover_a_tput = [x / 1000000 for x in clover_a_tput]

    axs[ycsb_location["ycsb-a"]].plot(clover_clients,clover_a_tput[:len(clover_clients)],label="Clover",marker="^", color = lib.clover_color, linewidth=lw)
    axs[ycsb_location["ycsb-b"]].plot(clover_clients,clover_b_tput[:len(clover_clients)],label="Clover",marker="^", color = lib.clover_color, linewidth=lw)
    axs[ycsb_location["ycsb-c"]].plot(clover_clients,clover_c_tput[:len(clover_clients)],label="Clover",marker="^", color = lib.clover_color, linewidth=lw)




    #plot cuckoo
    for i in range(len(workloads)):
        # dirname="hero_ycsb/"+workloads[i]
        ax = axs[i]
        # stats = dm.load_statistics(dirname=dirname)
        # stats=stats[0]
        # plot_cuckoo.throughput(ax, stats, decoration=False)
        ax.set_xlabel("clients")
        #set workloads to uppercase
        # ax.set_title(workloads[i].upper())
        ax.set_ylabel("MOPS")


    #plot fusee
    # plot_fusee_ycsb_multi_run(axs)
    plot_sherman_ycsb(axs, lw)

    axs[0].legend()
    

    plt.tight_layout()
    plt.savefig("hero_ycsb_throughput.pdf")


    for ax in axs:
        ax.legend()
        # ax.set_xlabel('Threads')
        # ax.set_ylabel('MOPS')
        # ax.legend(loc='lower right', ncol=1, fontsize=12)
    names = ["c", "b", "a"]
    lib.save_figs(plt, fig, axs, names=names)

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
# plot_hero_ycsb_throughput()
plot_hero_ycsb_throughput_static()