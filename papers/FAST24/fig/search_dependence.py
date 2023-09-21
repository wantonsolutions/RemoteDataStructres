from experiments import plot_cuckoo as plot_cuckoo
from experiments import data_management as dm
import experiments.orchestrator as orchestrator
import numpy as np
import matplotlib.pyplot as plt
import lib


def independent_search():

    config=lib.get_config()
    # clients = [1, 2, 4, 8, 16, 24]
    # clients = [4, 8, 16, 32, 64, 128, 160]
    locks_per_message = [1,2,4,8,16,32,64]
    # locks_per_message = [1]
    # locks_per_message = [1]
    buckets_per_lock = 1
    configs = [
        ("independent", "random", "ind_random"),
        ("independent", "bfs", "ind_bfs"),
        ("dependent", "random", "dep_random"),
        ("dependent", "bfs", "dep_bfs"),
    ]
    entry_size=8
    table_size = 1024 * 1024 * 100
    memory_size = entry_size * table_size
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)
    # clients = [16,32]
    # clients = [4]
    # clients = [40]
    # clients = [8]
    # clients = [160]
    config["trials"]=1
    config["prime"]="true"
    config["prime_fill"]="85"
    config["max_fill"]="90"
    config["workload"]="ycsb-w"
    config["num_clients"]=400
    # config["location_function"]="independent"
    # config["search_function"]="random"
    config["buckets_per_lock"] = str(buckets_per_lock)

    orchestrator.boot(config.copy())
    all_runs = []

    for c in configs:
        runs = []
        lconfig = config.copy()
        lconfig["location_function"] = c[0]
        lconfig["search_function"] = c[1]
        filename = c[2]
        lock_config = lconfig.copy()
        for l in locks_per_message:
            lock_config["locks_per_message"] = str(l)
            stats = orchestrator.run_trials(lock_config)
            runs.append(stats)
        dm.save_statistics(runs, "data/"+filename)
        all_runs.append(runs)
    dm.save_statistics(all_runs, "data/locks_per_search")

def plot_round_trips_per_insert_operation():
    static = True

    if not static:
        ind_dir="data/ind_random"
        dep_dir="data/dep_random"
        bfs_dir="data/dep_bfs"
        print("loading ind stats")
        ind_stats, dir = dm.load_statistics(dirname=ind_dir)
        print("loading dep stats")
        dep_stats, dir = dm.load_statistics(dirname=dep_dir)
        print("loading bfs stats")
        bfs_stats, dir = dm.load_statistics(dirname=bfs_dir)
        ind_99, ind_99_err = plot_cuckoo.client_stats_get_percentile_err_trials(ind_stats, 'insert_rtt', 99)
        ind_50, ind_50_err = plot_cuckoo.client_stats_get_percentile_err_trials(ind_stats, 'insert_rtt', 50)
        dep_99, dep_99_err = plot_cuckoo.client_stats_get_percentile_err_trials(dep_stats, 'insert_rtt', 99)
        dep_50, dep_50_err = plot_cuckoo.client_stats_get_percentile_err_trials(dep_stats, 'insert_rtt', 50)
        bfs_99, bfs_99_err = plot_cuckoo.client_stats_get_percentile_err_trials(bfs_stats, 'insert_rtt', 99)
        bfs_50, bfs_50_err = plot_cuckoo.client_stats_get_percentile_err_trials(bfs_stats, 'insert_rtt', 50)
        x_axis_vals = plot_cuckoo.get_x_axis(ind_stats, "locks per message")
    else:
        ind_99= [2547.0, 2442.2399999999907, 2335.0, 2354.0, 2361.570000000007, 2203.0, 2316.0]
        ind_99_err= [1.2079815366635334, 1.3611773804557508, 1.2221125037766256, 1.3955377252947077, 1.467728772203742, 0.968203768585276, 1.2301404227383754]
        ind_50= [12.0, 9.0, 7.0, 7.0, 7.0, 7.0, 7.0]
        ind_50_err= [1.2079815366635334, 1.3611773804557508, 1.2221125037766256, 1.3955377252947077, 1.467728772203742, 0.968203768585276, 1.2301404227383754]
        dep_99= [2022.5, 720.0, 672.0, 583.0, 497.0, 434.0, 439.0]
        dep_99_err= [1.6795664939458914, 0.4174385633905766, 0.3709775735580779, 0.3168660476608781, 0.2625183284137047, 0.23270803861983266, 0.24446079357693565]
        dep_50= [10.0, 8.0, 5.0, 5.0, 5.0, 5.0, 5.0]
        dep_50_err= [1.6795664939458914, 0.4174385633905766, 0.3709775735580779, 0.3168660476608781, 0.2625183284137047, 0.23270803861983266, 0.24446079357693565]
        bfs_99= [140.0, 59.0, 53.0, 53.0, 54.0, 57.0, 58.0]
        bfs_99_err= [0.05643571842321897, 0.021736940995661473, 0.015328496953842325, 0.016637542933797766, 0.017181553194536087, 0.019327593169027695, 0.01589452059167094]
        bfs_50= [8.0, 6.0, 4.0, 4.0, 4.0, 4.0, 4.0]
        bfs_50_err= [0.05643571842321897, 0.021736940995661473, 0.015328496953842325, 0.016637542933797766, 0.017181553194536087, 0.019327593169027695, 0.01589452059167094]
        x_axis_vals = [1,2,4,8,16,32,64]

    read_write_steering_color='#cf243cff'     #red
    write_steering_color='#ed7d31ff'          #orange
    default_clover_color='#8caff6ff'          #blueish
    connection_color='#7D31ED'              #dark purple
    fusee_color='#006400'                  #green
    sherman_color='#7D31ED'                  #purple

    # print(ind_stats)
    # print(dep_stats)
    # print(bfs_stats)
    fig, ax = plt.subplots(1,1, figsize=(5,3))
    percentile=99
    # ax.hlines(3,0,128)




    print("ind_99=",ind_99)
    print("ind_99_err=",ind_99_err)
    print("ind_50=",ind_50)
    print("ind_50_err=",ind_50_err)
    print("dep_99=",dep_99)
    print("dep_99_err=",dep_99_err)
    print("dep_50=",dep_50)
    print("dep_50_err=",dep_50_err)
    print("bfs_99=",bfs_99)
    print("bfs_99_err=",bfs_99_err)
    print("bfs_50=",bfs_50)
    print("bfs_50_err=",bfs_50_err)

    # ax.errorbar(x_axis_vals,ind_99,ind_99_err)
    # ax.errorbar(x_axis_vals,ind_50,ind_50_err)
    # ax.errorbar(x_axis_vals,dep_99,dep_99_err)
    # ax.errorbar(x_axis_vals,dep_50,dep_50_err)
    # ax.errorbar(x_axis_vals,bfs_99,bfs_99_err)
    # ax.errorbar(x_axis_vals,bfs_50,bfs_50_err)

    ax.plot(x_axis_vals,ind_99, color=write_steering_color, linestyle='--',marker='o')
    ax.plot(x_axis_vals,dep_99, color=default_clover_color, linestyle='--',marker='x')
    ax.plot(x_axis_vals,bfs_99, color=read_write_steering_color, linestyle='--',marker='s')
    di = ax.plot(x_axis_vals,ind_50, label="dfs (independent)", color=write_steering_color, marker='o')
    dd = ax.plot(x_axis_vals,dep_50, label="dfs (dependent)",  color=default_clover_color, marker='x')
    bd = ax.plot(x_axis_vals,bfs_50, label="bfs (dependent)",  color=read_write_steering_color, marker='s')

    ax.legend()



    # plot_cuckoo.rtt_per_operation_line(ax, None,ind_stats, label="dfs (independent)", x_axis="locks per message", twin=False, percentile=percentile)#), label="Independent Random")
    # plot_cuckoo.rtt_per_operation_line(ax, None,dep_stats, label="dfs (dependent)", x_axis="locks per message", twin=False, percentile=percentile)#), label="Independent Random")
    # plot_cuckoo.rtt_per_operation_line(ax, None,bfs_stats, label="bfs (dependent)", x_axis="locks per message", twin=False, percentile=percentile)#), label="Independent Random")
    # percentile=50
    # plot_cuckoo.rtt_per_operation_line(ax, None,ind_stats, label="random (independent)", x_axis="locks per message", twin=False, percentile=percentile)#), label="Independent Random")
    # plot_cuckoo.rtt_per_operation_line(ax, None,dep_stats, label="random (dependent)", x_axis="locks per message", twin=False, percentile=percentile)#), label="Independent Random")
    # plot_cuckoo.rtt_per_operation_line(ax, None,bfs_stats, label="bfs (dependent)", x_axis="locks per message", twin=False, percentile=percentile)#), label="Independent Random")
    from matplotlib.lines import Line2D
    dashed = Line2D([0], [0], linestyle='--', color='black')
    solid = Line2D([0], [0], linestyle='-', color='black')


    yvalues=[2,8,32,128,512,2048]
    ylabels=[str(l) for l in yvalues]
    xvalues=[1,2,4,8,16,32,64]
    xlabels=[str(l) for l in xvalues]
    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.set_ylim(0,4096)
    ax.minorticks_off()

    ax.set_yticks(yvalues)
    ax.set_yticklabels(ylabels)
    ax.set_xticks(xvalues)
    ax.set_xticklabels(xlabels)
    ax.set_xlabel("Locks per message")
    ax.set_xlim(0,70)



    ax.set_ylabel("Round Trips")
    legend_labels=["dfs (independent)","dfs (dependent)","bfs (dependent)","99th percentile","50th percentile"]
    legend_lines=[di[0],dd[0],bd[0],dashed,solid]
    ax.legend(legend_lines,legend_labels, ncol=2, bbox_to_anchor=(0.15,1.02,1,0.2), loc='lower left', fontsize=8)
    # plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

    # ax.legend()
    plt.tight_layout()
    plt.savefig("search_dependence.pdf")

independent_search()
# plot_round_trips_per_insert_operation()