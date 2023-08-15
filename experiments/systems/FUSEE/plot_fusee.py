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
    return [x / 1000000 for x in data]

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
    

    rcuckoo_insert=[120271.283245,234694.346647,459445.070634,917088.666479] #,1707337.275191,2391186.794957,]
    rcuckoo_clients=[1,2,4,8] #,16,24]
    rcuckoo_insert = [x / 1000000 for x in rcuckoo_insert]
    ax.plot(rcuckoo_clients,rcuckoo_insert, marker='x', label="rcuckoo insert")

    ax.legend(ncol=2)
    ax.set_ylabel("MOPS")
    ax.set_xlabel("Clients")
    ax.set_title("FUSEE operation throughput")
    plt.tight_layout()

    plt.savefig("FUSEE-initial-throughput.pdf")
