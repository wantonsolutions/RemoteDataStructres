import lib

import cuckoo.plot_cuckoo as plot_cuckoo
# import run_experiments as re
import cuckoo.data_management as dm
import cuckoo.orchestrator as orchestrator
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm




def plot_fault_rate():
    fig, axs = plt.subplots(1,1, figsize=(5,3))
    rows_per_lock = [1,2,4,8,16]
    # rows_per_lock = [1,2,4]
    for rpl in rows_per_lock:
        dirname="fault-rate/fault-rate-" + str(rpl) + "-rpl"
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        ax = axs
        plot_cuckoo.throughput(ax, stats, x_axis="fault rate", decoration=False, label=str(rpl)+"rows/lock")
    ax.legend()
    ax.set_xlabel("Faults/Second")
    ax.set_ylabel("MOPS")
    ax.set_title("Throughput as a function of failures rate")
    ax.set_xscale('log')



    # ax = axs[1]
    # # plot_cuckoo.get_x_axis(stats, "virtual_lock_scale_factor")
    # plot_cuckoo.retry_breakdown(ax, stats, x_axis="virtual lock scale factor", decoration=True, label="cuckoo")
    # ax.legend()
    # # ax.set_xlabel("virtual lock scale factor")
    # # ax.set_ylabel("throughput \n(ops/rtts)*clients")
    # ax.set_title("ycsb-w")

    plt.tight_layout()
    plt.savefig("fault_rate.pdf")


def plot_fault_rate_static():
    # rpl=16
    # throughputs=[round(x,2) for x in throughputs]
    # x_axis=[round(x,2) for x in x_axis]
    # print("tput_"+str(rpl)+"=",throughputs)
    # print("x_"+str(rpl)+"=",x_axis)
    tput_1= [17.23, 17.15, 17.1, 16.79, 16.86, 16.79, 16.02, 15.67, 15.8, 15.42, 14.41,14.05, 13.41,  13.06, 11.52, 10.35]
    x_1= [0.11, 1.09, 2.04, 4.05, 8.03, 14.81, 31.89, 63.53, 103.74, 242.89, 440.76, 637.34, 691.36,  934.61, 1120.95, 1302.7]
    tput_2= [17.2, 17.1, 17.15, 16.6, 16.42, 16.21, 16.31, 16.52, 15.48, 15.14, 14.88, 14.76, 14.35,  13.36, 13.18,  10.39]
    x_2= [0.11, 1.05, 2.05, 4.05, 7.74, 15.2, 22.24, 54.59, 125.25, 244.31, 364.94, 466.83, 600.08,  761.28, 875.04,  1295.4]
    tput_4= [17.18, 17.25, 17.21, 17.15, 16.89, 16.97, 17.19, 16.97, 16.93, 15.27, 14.1, 13.15, 11.65, 11.03, 10.86, 8.98]
    x_4= [0.11, 1.04, 2.05, 4.05, 8.01, 15.5, 28.69, 46.48, 84.14, 201.15, 440.1, 481.81, 970.63, 1075.73, 1130.44, 1293.71]
    tput_8= [17.0, 16.95, 17.1, 16.92, 15.98, 15.7, 16.15, 15.72, 16.09, 15.79, 14.99, 12.15, 10.06, 10.01, 6.15, 4.23]
    x_8= [0.12, 1.04, 2.06, 4.05, 8.04, 16.02, 31.9, 63.56, 105.96, 202.0, 373.79, 657.79, 797.15, 832.42, 1037.26, 1100.66]
    tput_16= [16.73, 16.80, 17.00, 16.56, 16.13, 16.37, 16.67, 16.23, 15.79, 15.73, 13.87,  8.87, 7.42, 7.23, 6.54, 4.88]
    x_16= [0.11, 1.05, 2.07, 4.05, 8.03, 16.01, 31.91, 63.57, 126.08, 166.52, 383.22,  507.24, 531.11, 545.88, 621.72, 714.51]

    vals=[(1,tput_1,x_1),(2,tput_2,x_2),(4,tput_4,x_4),(8,tput_8,x_8), (16,tput_16,x_16)]

    fig, ax = plt.subplots(1,1, figsize=(3,2.5))
    for rpl, y, x in vals:
        ax.plot(x, y, label=str(rpl), marker="s")
    # ax.set_xscale('log')
    ax.legend()
    ax.set_ylim(0,18)
    ax.set_xlabel("Failures per second (# locks)")
    ax.set_ylabel("MOPS")
    plt.tight_layout()
    plt.savefig("fault-rate.pdf")




    # x_axis=[1,2,4,8,16,32,64,128,256,512,1024]
    # one_row_per_lock=[30 - x/100 for x in x_axis]
    # sixteen_rows_per_lock=[30 - x/50 for x in x_axis]
    # x_axis=[str(x) for x in x_axis]
    # x_axis[len(x_axis)-1] = "1K"
    # ax.legend()
    # ax.set_ylim(0,33)
    # ax.set_xlabel("Failures per second (# locks)")
    # ax.set_ylabel("MOPS")

    # ax.text(2,15, "FAKE DATA", fontsize="xx-large", color="red")
    # plt.tight_layout()
    # plt.savefig("failure_throughput.pdf")

# plot_fault_rate()
plot_fault_rate_static()
# search_success_lock_size()
# plot_search_success_lock_size()
# plot_failure_throughput_static()