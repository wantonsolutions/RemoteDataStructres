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
from tqdm import tqdm

def run_hero_ycsb_fill_latency():
    print("to refresh this function is located in hero_ycsb_fill_latency.py")

def plot_hero_ycsb_fill_ops_bw():
    workload = "ycsb-a"
    fig, (ax1, ax2) = plt.subplots(1,2, figsize=(7,3))
    dirname="hero_ycsb_latency/"+workload
    stats = dm.load_statistics(dirname=dirname)[0]
    plot_cuckoo.bytes_per_operation(ax1, stats, x_axis="max fill", decoration=False)

    ax1.legend(fontsize=8)
    ax1.set_ylabel("bytes/op")
    ax1.set_xlabel("fill factor")
    ax1.set_ylim(bottom=0)


    plot_cuckoo.messages_per_operation(ax2, stats, x_axis="max fill", decoration=False, twin=False)
    ax2.legend(fontsize=8)
    ax2.set_ylabel("messages/op")
    ax2.set_xlabel("fill factor")
    ax2.set_ylim(bottom=0)

    plt.tight_layout()
    plt.savefig("hero_ycsb_fill_ops_bw.pdf")

run_hero_ycsb_fill_latency()
plot_hero_ycsb_fill_ops_bw()