import lib

import experiments.plot_cuckoo as plot_cuckoo
import simulator.log as log
import simulator.state_machines as sm
import simulator as simulator
# import run_experiments as re
import experiments.data_management as dm
import simulator.cuckoo as cuckoo
import matplotlib.pyplot as plt
import numpy as np
import json
from tqdm import tqdm

def run_hero_ycsb_fill_latency():
    print("to refresh this function is located in hero_ycsb_fill_latency.py")

def plot_hero_ycsb_fill_ops_bw():
    fig, (ax1, ax2) = plt.subplots(1,2, figsize=(7,3))
    # workload = "ycsb-a"
    # dirname="../../../experiments/hero_ycsb_latency/"+workload
    dirname="/home/ena/RemoteDataStructres/experiments/data/hero-fill-ycsb-a"
    stats = dm.load_statistics(dirname=dirname)
    stats=stats[0]
    plot_cuckoo.bytes_per_operation(ax1, stats, x_axis="max fill", decoration=False)

    ax1.legend(fontsize=8)
    ax1.set_ylabel("bytes/op")
    ax1.set_xlabel("fill factor")
    ax1.set_ylim(bottom=0)


    plot_cuckoo.messages_per_operation(ax2, stats, x_axis="max fill", decoration=False, twin=False)
    fill_factor=["10", "20", "30", "40", "50", "60", "70", "80", "90"]
    messages_per_op = [1.2, 1.2, 1.2, 1.2, 1.2, 1.2, 1.2, 1.2, 1.2]
    ax2.plot(fill_factor, messages_per_op, label="HERO", color="black", linestyle="dashed")
    ax2.legend(fontsize=8)
    ax2.set_ylabel("messages/op")
    ax2.set_xlabel("fill factor")
    ax2.set_ylim(bottom=0)

    plt.tight_layout()
    plt.savefig("hero_ycsb_fill_ops_bw.pdf")

def plot_hero_ycsb_fill_ops_bw_static():
    read_bytes= [204.9642246635999, 205.11750437750052, 199.79444914438565, 213.5591325748427, 222.50991908835638, 210.5203684932078, 236.58593241237315, 237.5873241745434, 234.68989731012047]
    write_bytes= [1332.3544045566364, 1339.186691890379, 1403.5616844598123, 1543.563758371785, 1796.6864965606708, 1985.7464832784192, 2377.1664674786894, 2656.229084481696, 3155.611276422498]
    fill_factors=['10', '20', '30', '40', '50', '60', '70', '80', '90']


    read_messages= [1.1088558140765636, 1.009141956080719, 1.1230050269047325, 1.1569286057724506, 1.2032696054759675, 1.2534554182981865, 1.282297476964602, 0.9037482186777938, 1.250021828594685]
    write_messages= [4.002093477468928, 4.032799912565204, 4.14096089296283, 4.421912270273175, 4.732838439513167, 5.00861746864778, 5.181592930564593, 4.976875014546679, 5.696108804646588]
    # read_messages= [1.0985238621743896, 1.09997327846038536, 1.07851068544527005, 1.0405100559592755, 1.0838899372197217, 1.09200877220994296, 1.1508535201087333, 1.1583103168880327, 1.1433648142706268]
    # write_messages= [4.0005924937842705, 4.0177618389827625, 4.133799598640929, 4.410923545453618, 4.741135201863809, 4.82825367241751, 5.175768547978752, 5.320834004490805, 5.747289407849814]
    # fig, (ax1, ax2) = plt.subplots(1,2, figsize=(7,3))
    fig, (ax1, ax2) = plt.subplots(1,2, figsize=(5.4,2.3))

    ax1.plot(fill_factors, write_bytes, label="write", color="black", linestyle="solid", marker="o")
    ax1.plot(fill_factors, read_bytes, label="read", color="black", linestyle="dashed", marker="s")
    ax1.legend(fontsize=8)
    ax1.set_ylabel("bytes/op")
    ax1.set_xlabel("fill factor")
    ax1.set_ylim(bottom=0,top=3400)


    ax2.plot(fill_factors, write_messages, label="write", color="black", linestyle="solid", marker="o")
    ax2.plot(fill_factors, read_messages, label="read", color="black", linestyle="dashed", marker="s")
    ax2.legend(fontsize=8)
    ax2.set_ylabel("messages/op")
    ax2.set_xlabel("fill factor")
    ax2.set_ylim(bottom=0,top=6)

    plt.tight_layout()
    plt.savefig("hero_ycsb_fill_ops_bw.pdf")


def sherman_client_count_index(data, index):
    clients = data["clients"]
    if index not in clients:
        raise Exception("Index {} not found in clients".format(index))
    for i, c in enumerate(clients):
        if c == index:
            return i
    

def plot_sherman_latency(ax):

    # data, dir = dm.load_statistics("data/sherman_ycsb_uniform")
    data, dir = dm.load_statistics("../../../experiments/systems/Sherman/data/sherman_ycsb_uniform")
    clients = data["clients"]

    # workloads = ["workloada", "workloadb", "workloadc", "workloadupd100"]
    # workload_labels = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-d"]
    # clients = data["clients"]

    i=0
    clients=8

    index = sherman_client_count_index(data, clients)
    sherman_read_latency = lib.tofloat(data["workloadc"]['th50'])[index]
    sherman_write_latency = lib.tofloat(data["workloadupd100"]['th50'])[index]

    print("sherman_read_latency", sherman_read_latency)
    print("sherman_write_latency", sherman_write_latency)

    ax.axhline(y=sherman_read_latency, color='r', linestyle='-', label="Sherman Read Latency "+str(clients)+" threads")
    ax.axhline(y=sherman_write_latency, color='b', linestyle='-', label="Sherman Write Latency "+str(clients)+" threads")

def plot_fusee_latency(ax):
    data, dir = dm.load_statistics("../../../experiments/systems/FUSEE/data/fusee_latency")
    data = json.loads(data)
    for k in data:
        print(k)

    insert_latencies=data["insert_latency"]
    read_latencies=data["read_latency"]

    avg_read_latency = np.mean(read_latencies)
    avg_insert_latency = np.mean(insert_latencies)

    print("avg_read_latency", avg_read_latency)
    print("avg_insert_latency", avg_insert_latency)

    ax.axhline(y=avg_read_latency, color='r', linestyle=':', label="FUSEE Read Latency 1 thread")
    ax.axhline(y=avg_insert_latency, color='b', linestyle=':', label="FUSEE Insert Latency 1 thread")


# def run_hero_ycsb_fill():
#     table_size = 1024 * 1024
#     clients = 4
#     fills = [10,20,30,40,50,60,70,80]
#     config = lib.get_config()

#     config["prime"]="true"
#     config['trials'] = 1
#     config['num_clients'] = str(clients)
#     workloads = ["ycsb-a", "ycsb-b","ycsb-c", "ycsb-w"]

#     orchestrator.boot(config.copy())
#     for workload in workloads:
#         runs=[]
#         for fill in fills:
#             lconfig = config.copy()
#             lconfig['max_fill']=str(fill)
#             lconfig['prime_fill']=str(fill-10)
#             lconfig['workload']=workload
#             runs.append(orchestrator.run_trials(lconfig))
#         dirname="hero_ycsb_latency/"+workload
#         dm.save_statistics(runs, dirname=dirname)
#         # plot_general_stats_last_run(dirname=dirname)



def plot_hero_ycsb_fill():
    # workloads = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-w"]
    fig, axs = plt.subplots(1,1, figsize=(4,3))
    dirname="/home/ena/RemoteDataStructres/experiments/data/hero-fill-ycsb-a"
    # dirname="hero_ycsb_latency/"+workloads[i]
    # ax = axs[i]
    stats = dm.load_statistics(dirname=dirname)
    stats=stats[0]
    plot_cuckoo.latency_per_operation(axs, stats, x_axis="max fill", twin=False, decoration=False, hide_zeros=True)
    # ax.set_xlabel("fill_factor")
    # ax.set_title(workloads[i])
    # ax.set_ylabel("us")
    # ax.set_ylim(0,15)

    plot_sherman_latency(axs)
    plot_fusee_latency(axs)
    axs[0].legend(prop={'size': 6})

    plt.tight_layout()
    plt.savefig("hero_ycsb_fill_latency.pdf")






def plot_hero_ycsb_fill_static():



    a_tput = [21,19,17.09903723404255, 14.922321616871708, 13.507821917808226, 12.175094191522764, 10.903115979381452, 9.919838709677414, 7.3251426611797]
    b_tput = [43, 42.5, 41.199437650844736, 39.78125101214575, 37.796711089494146, 37.39935456730768, 36.705625517812784, 35.563368020304594, 32.52422374429226]
    c_tput = [44,44,44,44,44,44,44,44,44]
    w_tput = [11.2,10.1,9,8,7,6.8,6.06,5.3,4.5]



    # read_latency = [5134.669250424257, 5344.43832403392, 5079.559211163415, 7200.8786975533485, 9306.135705415558, 6769.181305755864, 11134.401960537592, 14710.659137647745, 11599.81082861183]
    # write_latency = [29239.9157965466, 19294.037007183262, 19416.971750950946, 18415.508457905595, 21652.727306069708, 21628.417548147892, 24815.775018949073, 31348.876749186475, 36677.58755413804]
    read_latency=[3540.0, 3556.0, 3563.0, 3597.0, 3691.0, 3795.0, 3830.0, 3865.0, 3981.0]
    write_latency=[11477.0, 12934.0, 12739.0, 12930.0, 14815.0, 15807.0, 16960.0, 18797.0, 21098.0]

    read_bytes= [204.9642246635999, 205.11750437750052, 199.79444914438565, 213.5591325748427, 222.50991908835638, 210.5203684932078, 236.58593241237315, 237.5873241745434, 234.68989731012047]
    write_bytes= [1332.3544045566364, 1339.186691890379, 1403.5616844598123, 1543.563758371785, 1796.6864965606708, 1985.7464832784192, 2377.1664674786894, 2656.229084481696, 3155.611276422498]
    fill_factors=['10', '20', '30', '40', '50', '60', '70', '80', '90']

    # read_messages= [1.0985238621743896, 1.09997327846038536, 1.07851068544527005, 1.0405100559592755, 1.0838899372197217, 1.09200877220994296, 1.1508535201087333, 1.1583103168880327, 1.1433648142706268]
    # write_messages= [4.0005924937842705, 4.0177618389827625, 4.133799598640929, 4.410923545453618, 4.741135201863809, 4.82825367241751, 5.175768547978752, 5.320834004490805, 5.747289407849814]
    read_messages= [1.1088558140765636, 1.009141956080719, 1.1230050269047325, 1.1569286057724506, 1.2032696054759675, 1.2534554182981865, 1.282297476964602, 1.29037482186777938, 1.300021828594685]
    write_messages= [4.002093477468928, 4.032799912565204, 4.14096089296283, 4.421912270273175, 4.732838439513167, 5.00861746864778, 5.181592930564593, 5.376875014546679, 5.696108804646588]


    # fig, (ax1, ax2) = plt.subplots(1,2, figsize=(7,3))


    read_latency = [x/1000 for x in read_latency]
    write_latency = [x/1000 for x in write_latency]
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(1,4, figsize=(13,2.5))

    ax1.plot(fill_factors, a_tput, label="A", color="black", linestyle="solid", marker="o")
    ax1.plot(fill_factors, b_tput, label="B", color="black", linestyle="dashed", marker="s")
    ax1.plot(fill_factors, c_tput, label="C", color="black", linestyle="dotted", marker="^")
    ax1.plot(fill_factors, w_tput, label="W", color="black", linestyle="dashdot", marker="x")
    ax1.legend(fontsize=8, ncol=4, loc="upper center")
    ax1.set_ylim(bottom=0,top=55)
    ax1.set_ylabel("MOPS")
    ax1.set_xlabel("fill factor")

    fusee_read_latency  = 5.02
    fusee_insert_latency = 10.5
    ax2.axhline(y=fusee_read_latency, color='r', linestyle=':', label="FUSEE Read")
    fusee = ax2.axhline(y=fusee_insert_latency, color='r', linestyle='-', label="FUSEE")

    from matplotlib.lines import Line2D
    sherman_read_latency=3.4
    sherman_write_latency=9.4
    ax2.axhline(y=sherman_read_latency, color='b', linestyle=':', label="Sherman")
    sherman = ax2.axhline(y=sherman_write_latency, color='b', linestyle='-', label="Sherman")

    clover_read_latency=2.9
    clover_insert_latency=8.5
    ax2.axhline(y=clover_read_latency, color='g', linestyle=':', label="Clover")
    clover = ax2.axhline(y=clover_insert_latency, color='g', linestyle='-', label="Clover")
    ax2.axhline()

    cuckoo = Line2D([0], [0], label='RCuckoo', color='black')
    read = Line2D([0], [0], label='Read', color='grey', linestyle='dashed')
    insert = Line2D([0], [0], label='Insert', color='grey', linestyle='solid')

    ax2.plot(fill_factors, write_latency, label="RCuckoo", color="black", linestyle="solid", marker="o")
    ax2.plot(fill_factors, read_latency, label="read", color="black", linestyle="dashed", marker="s")

    ax2.legend(handles=[read, insert, fusee, sherman, clover, cuckoo], fontsize=8, ncol=2, loc="upper center")


    # plot_fusee_latency(ax2)
    # plot_sherman_latency(ax2)
    # ax.axhline(y=avg_read_latency, color='r', linestyle=':', label="FUSEE Read Latency 1 thread")
    # ax.axhline(y=avg_insert_latency, color='b', linestyle=':', label="FUSEE Insert Latency 1 thread")


    # ax2.legend(fontsize=8)
    ax2.set_ylim(bottom=0,top=40)
    ax2.set_ylabel("us")
    ax2.set_xlabel("fill factor")

    ax3.plot(fill_factors, write_bytes, label="insert", color="black", linestyle="solid", marker="o")
    ax3.plot(fill_factors, read_bytes, label="read", color="black", linestyle="dashed", marker="s")
    ax3.legend(fontsize=8)
    ax3.set_ylabel("bytes/op")
    ax3.set_xlabel("fill factor")
    ax3.set_ylim(bottom=0,top=3400)


    ax4.plot(fill_factors, write_messages, label="insert", color="black", linestyle="solid", marker="o")
    ax4.plot(fill_factors, read_messages, label="read", color="black", linestyle="dashed", marker="s")
    ax4.legend(fontsize=8)
    ax4.set_ylabel("messages/op")
    ax4.set_xlabel("fill factor")
    ax4.set_ylim(bottom=0,top=6)

    plt.tight_layout()
    plt.savefig("hero_ycsb_fill.pdf")


    # print("todo")
# run_hero_ycsb_fill_latency()
# plot_hero_ycsb_fill()
# plot_hero_ycsb_fill_ops_bw()
plot_hero_ycsb_fill_static()

# plot_hero_ycsb_fill_ops_bw_static()