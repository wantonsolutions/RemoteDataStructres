import numpy as np
import matplotlib.pyplot as plt
import json

from experiments import data_management as dm

def cdf(data):
    high = max(data)
    low = min(data)
    # norm = plt.colors.Normalize(low,high)

    #print(data)
    count, bins_count = np.histogram(data, bins = 100000 )
    pdf = count / sum(count)
    
    y = np.cumsum(pdf)
    x = bins_count[1:]

    y= np.insert(y,0,0)
    x= np.insert(x,0,x[0])
    return x, y

def mops(data):
    return [float(x) / 1000000 for x in data]

def ops(data):
    return [x * 1000000 for x in data]

def tofloat(data):
    return [float(x) for x in data]

def plot_latency():
    fig, ax = plt.subplots(1, 1, figsize=(5, 3))
    data, dir = dm.load_statistics("data/sherman_ycsb_uniform")
    clients = data["clients"]

    a99 = tofloat(data["workloada"]['th99'])
    b99 = tofloat(data["workloadb"]['th99'])
    c99 = tofloat(data["workloadc"]['th99'])
    d99 = tofloat(data["workloadupd100"]['th99'])
    ax.plot(clients,a99, marker='x', linestyle=":", label="ycsb-a-99th")
    ax.plot(clients,b99, marker='x', linestyle=":", label="ycsb-b-99th")
    ax.plot(clients,c99, marker='x', linestyle=":", label="ycsb-c-99th")
    ax.plot(clients,d99, marker='x', linestyle=":", label="ycsb-all_updates-99th")

    a50 = tofloat(data["workloada"]['th50'])
    b50 = tofloat(data["workloadb"]['th50'])
    c50 = tofloat(data["workloadc"]['th50'])
    d50 = tofloat(data["workloadupd100"]['th50'])
    ax.plot(clients,a50, marker='x', linestyle="-", label="ycsb-a-50th")
    ax.plot(clients,b50, marker='x', linestyle="-", label="ycsb-b-50th")
    ax.plot(clients,c50, marker='x', linestyle="-", label="ycsb-c-50th")
    ax.plot(clients,d50, marker='x', linestyle="-", label="ycsb-all_updates-50th")

    # ax.legend(ncol=2)
    l4 = ax.legend(bbox_to_anchor=(0, 1.02, 1, 0.2), loc="lower left",
                mode="expand", borderaxespad=0, ncol=2)
    ax.set_ylabel("microseconds (us)")
    ax.set_xlabel("Clients")
    # ax.set_title("Sherman YCSB latency")
    plt.tight_layout()
    plt.savefig("Sherman-ycsb-latency.pdf")




def plot_ycsb():
    fig, ax = plt.subplots(1, 1, figsize=(5, 3))
    data, dir = dm.load_statistics("data/sherman_ycsb_uniform")
    # data, dir = dm.load_statistics("data/sherman_ycsb_zipf")
    # data = json.loads(data)

    workloads = ["workloada", "workloadb", "workloadc", "workloadupd100"]
    workload_labels = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-d"]
    clients = data["clients"]

    for load, label in zip(workloads, workload_labels):
        try:
            # print("tring to plot", load, label)
            workload_data = tofloat(data[load]['tput'])
            print(label)
            print("avg_ops=", ops(workload_data))
            print("threads=", clients)

            ax.plot(clients,workload_data, marker='x', label=label)
        except:
            continue

    

    rcuckoo_insert=[0.3148210354061908, 0.6199394105755479, 1.1946543752999568, 2.3096083082187455, 4.1296484533547115, 5.993299988618042, 7.583539988986855]
    rcuckoo_clients=[4, 8, 16, 32, 64, 128, 160]
    # rcuckoo_insert = [x / 1000000 for x in rcuckoo_insert]
    ax.plot(rcuckoo_clients,rcuckoo_insert, marker='x', label="rcuckoo insert")

    ax.legend(ncol=2)
    ax.set_ylabel("MOPS")
    ax.set_xlabel("Clients")
    ax.set_title("Sherman YCSB throughput")
    plt.tight_layout()
    # plt.savefig("Sherman-ycsb-throughput-zipf.pdf")
    plt.savefig("Sherman-ycsb-throughput-uniform.pdf")

# plot_ycsb()
# plot_latency()

