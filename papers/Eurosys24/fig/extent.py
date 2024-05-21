import matplotlib.pyplot as plt
import numpy as np

import lib as lb

def plot_value_size_static():
    fig, ax = plt.subplots(1,1, figsize=(3,2.5))
    # sizes = [64,128,256,512,1024]
    # workloads = ["ycsb-c"]
    # workloads = ["ycsb-c","ycsb-b"]
    a_z50=[8.25, 8.22, 8.20, 8.19, 8.16, 8.14, 7.55, 6.75]
    b_z50=[14.77, 14.65, 14.48, 14.44, 14.41, 13.47, 10.95, 7.85]
    c_z50=[20.98, 20.64, 20.53, 20.05, 18.42, 16.38, 12.75, 9.11]
    a_static_z50=9.91
    b_static_z50=17.77
    c_static_z50=22.33


    a_z99=[6.22, 6.12, 5.96, 6.01, 5.7, 5.6, 5.4, 5.06]
    b_z99=[13.62, 13.59, 13.19, 12.46, 11.64, 11.53, 10.3, 9.00]
    c_z99=[21.44, 21.53, 21.41, 21.38, 17.95, 16.75, 13.39, 10.71]
    #single_run_throughput:  [42.33045659875998, 39.47476923076925, 23.51871792618629]
    c_static_z99=22.5
    #single_run_throughput:  [11.241931762295083, 21.511844512917747, 19.016406874695264]
    b_static_z99=17.26
    #single_run_throughput:  [15.899646699621988, 9.627829251544979, 4.193810521622825]
    a_static_z99=9.91
    


    workloads = ["ycsb-c","ycsb-b","ycsb-a"]
    sizes = [8,16,32,64,128,256,512,1024]
    sizes = ["8","16","32","64","128","256","512","1K"]
    # dists = [a_z50,b_z50,c_z50]
    # baselines = [a_static_z50,b_static_z50,c_static_z50]
    dists = [c_z99, b_z99, a_z99]
    baselines = [c_static_z99, b_static_z99, a_static_z99]

    #convert sizes to string
    sizes = [str(s) for s in sizes]

    ax.axhline(y=baselines[0], color='grey', linestyle='--',label="inline")

    display_gap=True
    for i in range(len(workloads)):
        # for s in sizes:
        # dirname = "data/value_size_"+w
        # stats = dm.load_statistics(dirname=dirname)
        # stats=stats[0]
        # plot_cuckoo.throughput(ax, stats, decoration=False, x_axis="value size", label=w)
        p = ax.plot(sizes, dists[i], label=workloads[i], marker='o')

        #set the axhline to the color of the prior line
        ax.axhline(y=baselines[i], color=p[0].get_color(), linestyle='--')
        ax.plot(sizes[0], baselines[i], color=p[0].get_color(), marker='x')

        if display_gap:
            dist_x = [sizes[0], sizes[0]]
            dist_y = [baselines[i], dists[i][0]]
            ax.plot(dist_x, dist_y, color='k', linestyle='--')
            gap=((baselines[i] - dists[i][0])/baselines[i]) * 100
            gap = round(gap,1)
            ax.text(1.5, baselines[i]+0.5, str(gap)+"%", ha='right', va='bottom')
            print(gap)
        

    # ax.legend(loc='upper right', fontsize='small')
    ax.legend(loc='lower center', ncol=2,  fontsize=6)
    ax.set_xlabel("value size (bytes)")
    ax.set_ylabel("MOPS")
    ax.set_ylim(0,26)

    plt.tight_layout()
    plt.savefig("extent.pdf")

plot_value_size_static()