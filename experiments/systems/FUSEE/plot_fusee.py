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
    return [x / 1000000 if x is not None else 0 for x in data]
    # return [float(x) if x is not None else 0 for x in data]

def plot_latency():
    fig, ax = plt.subplots(1, 1, figsize=(5, 3))
    data, dir = dm.load_statistics("data/fusee_latency")
    data = json.loads(data)
    print(data)
    insert_data = data["insert_latency"]
    x, y = cdf(insert_data)
    ax.plot(x,y, label="insert")
    read_data = data["read_latency"]
    x, y = cdf(read_data)
    ax.plot(x,y, label="read")
    update_data = data["update_latency"]
    x, y = cdf(update_data)
    ax.plot(x,y, label="update")

    ax.legend()
    ax.set_xlim(left=1)
    ax.set_xscale('log')
    ax.set_ylabel("CDF")
    ax.set_xlabel("Latency (us)")
    ax.set_title("FUSEE operation latency")
    plt.tight_layout()
    plt.savefig("FUSEE-latency.pdf")

def plot_tput():
    fig, ax = plt.subplots(1, 1, figsize=(5, 3))
    data, dir = dm.load_statistics("data/fusee_throughput")
    # data = json.loads(data)

    insert_data = mops(data["insert"])
    search_data = mops(data["search"])
    update_data = mops(data["update"])
    delete_data = mops(data["delete"])
    clients = data["clients"]

    ax.plot(clients,insert_data, marker='x', label="insert")
    ax.plot(clients,search_data, marker='x', label="search")
    ax.plot(clients,update_data, marker='x', label="update")
    ax.plot(clients,delete_data, marker='x', label="delete")
    

    # rcuckoo_insert=[120271.283245,234694.346647,459445.070634,917088.666479] #,1707337.275191,2391186.794957,]
    # rcuckoo_clients=[1,2,4,8] #,16,24]
    rcuckoo_insert=[0.3148210354061908, 0.6199394105755479, 1.1946543752999568, 2.3096083082187455, 4.1296484533547115, 5.993299988618042, 7.583539988986855]
    rcuckoo_clients=[4, 8, 16, 32, 64, 128, 160]
    # rcuckoo_insert = [x / 1000000 for x in rcuckoo_insert]
    ax.plot(rcuckoo_clients,rcuckoo_insert, marker='x', label="rcuckoo insert")

    ax.legend(ncol=2)
    ax.set_ylabel("MOPS")
    ax.set_xlabel("Clients")
    ax.set_title("FUSEE operation throughput")
    plt.tight_layout()

    plt.savefig("FUSEE-initial-throughput.pdf")

def plot_ycsb():
    fig, ax = plt.subplots(1, 1, figsize=(5, 3))
    data, dir = dm.load_statistics("data/fusee_ycsb")
    # data = json.loads(data)
    workloads = ["workloada", "workloadb", "workloadc", "workloadd"]
    workload_labels = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-d"]

    for load, label in zip(workloads, workload_labels):
        try:
            workload_data = mops(data[load])
            clients = data["clients"]
            ax.plot(clients,workload_data, marker='x', label=label)
        except:
            continue
    

    # rcuckoo_insert=[120271.283245,234694.346647,459445.070634,917088.666479] #,1707337.275191,2391186.794957,]
    # rcuckoo_clients=[1,2,4,8] #,16,24]
    # rcuckoo_insert = [x / 1000000 for x in rcuckoo_insert]
    # ax.plot(rcuckoo_clients,rcuckoo_insert, marker='x', label="rcuckoo insert")

    ax.legend(ncol=2)
    ax.set_ylabel("MOPS")
    ax.set_xlabel("Clients")
    ax.set_title("FUSEE YCSB throughput")
    plt.tight_layout()

    plt.savefig("FUSEE-ycsb-throughput.pdf")


def extract_multiple_ycsb_runs(total_runs=10):
    runs = []
    print("Attempting to extract {} ycsb runs".format(total_runs))
    for i in range(total_runs):
        try :
            data, dir = dm.load_statistics("data/fusee_ycsb_{}".format(i))
            runs.append(data)
        except:
            print("Failed to load data for run {}".format(i))

    print("Successfully able to extract {} ycsb runs".format(len(runs)))
    return runs

def average_runs(runs, workload):
    workload_data = []
    for run in runs:
        workload_data.append(mops(run[workload]))
    workload_data = np.array(workload_data)
    workload_data[workload_data == 0] = np.nan
    avg = np.nanmean(workload_data, axis=0)
    err = np.nanstd(workload_data, axis=0)
    return avg, err


def plot_ycsb_multi_run():
    total_runs=2
    fig, ax = plt.subplots(1, 1, figsize=(5, 3))
    runs = extract_multiple_ycsb_runs(total_runs)

    workloads = ["workloada", "workloadb", "workloadc", "workloadupd100"]
    workload_labels = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-w"]

    for load, label in zip(workloads, workload_labels):
        avg, err = average_runs(runs, load)
        print("average",avg)
        # avg = avg / 1000000
        # err = err / 1000000
        print(label)
        print("threads=", runs[0]["clients"])
        print("avg_ops=", avg)
        # exit(0)
        # print(avg, err, runs[0]["clients"], label)
        ax.errorbar(runs[0]["clients"], avg, yerr=err, marker='x', label=label)

    rcuckoo_insert=[0.3148210354061908, 0.6199394105755479, 1.1946543752999568, 2.3096083082187455, 4.1296484533547115, 5.993299988618042, 7.583539988986855]
    rcuckoo_clients=[4, 8, 16, 32, 64, 128, 160]
    # rcuckoo_insert = [x / 1000000 for x in rcuckoo_insert]
    ax.plot(rcuckoo_clients,rcuckoo_insert, marker='x', label="rcuckoo insert")

    ax.legend(ncol=2)
    ax.set_ylabel("MOPS")
    ax.set_xlabel("Clients")
    ax.set_title("FUSEE YCSB throughput")
    plt.tight_layout()
    plt.savefig("FUSEE-ycsb-throughput-multi.pdf")

# def __main__():
plot_ycsb_multi_run()

