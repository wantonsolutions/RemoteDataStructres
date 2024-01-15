import orchestrator
import data_management as dm
import plot_cuckoo as plot_cuckoo
import datetime
import git

import matplotlib.pyplot as plt
import numpy as np

# def write_config_to_json_file(config, filename):
#     with open(filename, 'w') as outfile:
#         json.dump(config, outfile)

def plot_general_stats_last_run(dirname=""):
    stats, directory = dm.load_statistics(dirname=dirname)
    print("plot general stats")
    plot_names = [
        "general_stats",
        "cas_success_rate",
        "read_write_ratio",
        # #dont uncomment this one ####"request_success_rate",
        "rtt_per_operation",
        "bytes_per_operation",
        "messages_per_operation",
        "fill_factor",
        # "throughput_approximation",
        "throughput",
        "bandwidth",
        "latency_per_operation",
        "retry_breakdown",
        ]
    plot_cuckoo.multi_plot_runs(stats, plot_names, directory)

config=dict()


table_size = 1024 * 1024 * 100
# table_size = 1024 * 1024 * 10
# table_size = 1024 * 1024
#int table_size = 256;
#int table_size = 1024;
#int table_size = 256;
#int table_size = 1024 * 2;
entry_size = 8
bucket_size = 8
memory_size = entry_size * table_size
buckets_per_lock = 16
locks_per_message = 64
read_threshold_bytes = 256


#maditory fields added to prevent breakage
config["description"] = "This is a test run and a manditory field"
config["name"] = "test_run"
config["state_machine"] = "cuckoo"
config['date']=datetime.datetime.now().strftime("%Y-%m-%d")
config['commit']=git.Repo(search_parent_directories=True).head.object.hexsha
# config['hash_factor']=str(999999999)

config["bucket_size"] = str(bucket_size)
config["entry_size"] = str(entry_size)

config["indexes"] = str(table_size)
config["memory_size"] = str(memory_size)
config["hash_factor"] = str(2.3)

config["read_threshold_bytes"] = str(read_threshold_bytes)
config["buckets_per_lock"] = str(buckets_per_lock)
config["locks_per_message"] = str(locks_per_message)
config["deterministic"]="True"
config["workload"]="ycsb-w"
config["id"]=str(0)
config["search_function"]="bfs"
config["location_function"]="dependent"

#Client State Machine Arguements
total_inserts = 1
max_fill = 50
prime_fill = 10
num_clients = 1
runtime = 10
#num_clinets = 1;
config["total_inserts"]=str(total_inserts)
config["total_requests"]=str(total_inserts)
config["max_fill"]=str(max_fill)
config["prime"]="true"
config["prime_fill"]=str(prime_fill)
config["num_clients"]=str(num_clients)
config["runtime"]=str(runtime)
config["use_mask"]="true"

#RDMA Engine Arguments
config["server_address"]="192.168.1.12"
config["base_port"] = "20886"

config["trials"] = 1


def debug_exp(config):
    clients = [1, 2, 4, 8, 16, 24]
    runs = []
    for c in clients:
        lconfig = config.copy()
        lconfig["num_clients"] = str(c)
        stats = orchestrator.run_trials(lconfig)
        runs.append(stats)
    # print(runs)
    dm.save_statistics(runs)

def fill_factor(config):
    # fill = [10,20,30,40,50,60,70,80,90]
    fill = [10,20,30,40,50,60,70,80,82,83]
    # fill = [80,82,84,86]
    # fill = [80,81,82,83]
    # fill = list(range(70,82))

    table_size = 1024 * 1024 * 1
    clients = 16

    runs = []
    config["buckets_per_lock"] = str(16)
    config["table_size"] = str(table_size)
    config["memory_size"] = str(table_size * int(config["entry_size"]))
    config["num_clients"]=str(clients)
    config["prime"]="true"
    for f in fill:
        print("filling to " + str(f) + "%")
        lconfig = config.copy()
        lconfig["prime_fill"] = str(f-10)
        lconfig["max_fill"] = str(f)
        stats = orchestrator.run_trials(lconfig)
        runs.append(stats)
    # print(runs)
    dm.save_statistics(runs)

def client_fill_to_50_exp(config):
    # clients = [1, 2, 4, 8, 16, 24]
    # clients = [4, 8, 16, 32, 64, 128, 160]
    # clients = [10,20,40,80,160]
    # clients = [400]
    # clients = [4, 8]
    # clients = [16,32]
    # clients = [4]
    clients = [10]
    # clients = [8]
    # clients = [160]
    table_size = 1024 * 1024 * 10
    memory_size = table_size * 8
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)
    config["hash_factor"] = str(2.3)

    runs = []
    buckets_per_lock = 8
    config["bucket_size"] = str(8)
    config["buckets_per_lock"] = str(buckets_per_lock)
    config['search_function'] = "bfs"
    config["trials"]=1
    config["prime"]="true"
    config["prime_fill"]="40"
    config["max_fill"]="50"
    config["workload"]="ycsb-w"
    orchestrator.boot(config.copy())


    for c in clients:
        lconfig = config.copy()
        print(lconfig["state_machine"])
        lconfig["num_clients"] = str(c)
        stats = orchestrator.run_trials(lconfig)
        runs.append(stats)
    # print(runs)
    dm.save_statistics(runs)

def table_size_contention(config):
    fill =50
    entry_size = 8
    config["prime_fill"] = str(fill-10)
    config["max_fill"] = str(fill)

    table_size = [1024 * 128, 1024 * 256, 1024 * 512, 1024 * 1024, 1024 * 1024 * 2, 1024 * 1024 * 4, 1024 * 1024 * 8]
    table_size = [1024 * 128, 1024 * 256,1024 * 512]
    clients = 24
    runs = []
    config["num_clients"]=str(clients)
    config["prime"]="true"
    for t in table_size:
        memory_size = entry_size * t
        lconfig = config.copy()
        print("table size: " + str(t))
        lconfig["table_size"] = str(t)
        lconfig["indexes"] = str(t)
        lconfig["memory_size"] = str(memory_size)
        stats = orchestrator.run_trials(lconfig)
        runs.append(stats)
    # print(runs)
    dm.save_statistics(runs)

def run_hero_ycsb():
    table_size = 1024 * 1024 * 100
    # clients = [4, 8, 16, 32, 64, 128, 160]
    clients = [10,20,40,80,160,320,400]
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)
    config["search_function"]="bfs"

    config["prime"]="true"
    config["prime_fill"]="40"
    config["max_fill"]="50"

    config['trials'] = 3
    workloads = ["ycsb-a", "ycsb-b","ycsb-c", "ycsb-w"]
    # workloads = ["ycsb-w"]

    orchestrator.boot(config.copy())
    for workload in workloads:
        runs=[]
        for c in clients:
            lconfig = config.copy()
            lconfig['num_clients'] = str(c)
            lconfig['workload']=workload
            runs.append(orchestrator.run_trials(lconfig))
        dirname="data/hero-"+workload
        dm.save_statistics(runs, dirname=dirname)
        # plot_general_stats_last_run(dirname=dirname)


def plot_hero_ycsb():
    workloads = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-w"]
    fig, axs = plt.subplots(1,len(workloads), figsize=(12,3))
    for i in range(len(workloads)):
        dirname="data/hero-"+workloads[i]
        ax = axs[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        plot_cuckoo.throughput(ax, stats, decoration=False)
        ax.legend()
        ax.set_xlabel("clients")
        ax.set_title(workloads[i])
        ax.set_ylabel("throughput \n(ops/rtts)*clients")

    plt.tight_layout()
    plt.savefig("hero_ycsb.pdf")


def run_hero_ycsb_fill():
    table_size = 1024 * 1024 * 10
    memory_size = entry_size * table_size

    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)

    clients = 40
    fills = [10,20,30,40,50,60,70,80,90]
    # fills = [10,20,30]
    # fills = [10,20,30,40,50]
    # fills=[10]
    # fills = [10,50,80]

    config['trials'] = 1
    config['num_clients'] = str(clients)
    # workloads = ["ycsb-a", "ycsb-b","ycsb-c", "ycsb-w"]
    workloads = ["ycsb-c", "ycsb-w"]
    config['search_function'] = "bfs"
    config["buckets_per_lock"] = str(16)
    config["locks_per_message"] = str(64)
    config["prime"]="true"
    config["deterministic"]="False"
    config["read_threshold_bytes"] = str(256)
    config["runtime"]=str(1)


    orchestrator.boot(config.copy())
    for workload in workloads:
        runs=[]
        for fill in fills:
            lconfig = config.copy()
            lconfig['max_fill']=str(fill)
            lconfig['prime_fill']=str(fill-6)
            lconfig['workload']=workload
            runs.append(orchestrator.run_trials(lconfig))
        dirname="data/hero-fill-"+workload
        dm.save_statistics(runs, dirname=dirname)
        # plot_general_stats_last_run(dirname=dirname)


def plot_hero_ycsb_fill():
    workloads = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-w"]
    fig, axs = plt.subplots(1,len(workloads), figsize=(12,3))
    for i in range(len(workloads)):
        dirname="data/hero-fill-"+workloads[i]
        ax = axs[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        plot_cuckoo.latency_per_operation(ax, stats, x_axis="max fill", twin=False, decoration=False, hide_zeros=True)
        ax.legend()
        ax.set_xlabel("fill_factor")
        ax.set_title(workloads[i])
        ax.set_ylabel("us")
        # ax.set_ylim(0,15)

    plt.tight_layout()
    plt.savefig("hero_ycsb_fill.pdf")


def locks_per_message_test(config):
    # clients = [1, 2, 4, 8, 16, 24]
    # locks_per_message = [1]
    locks_per_message = [1, 2, 4, 8, 16, 32, 64]
    buckets_per_lock = [1, 2, 4, 8, 16, 32, 64]

    # locks_per_message = [1, 2]
    # buckets_per_lock = [1, 2]
    # clients = [4, 8]
    # clients = [16,32]
    # clients = [4]
    # clients = [40]
    # clients = [8]
    # clients = [160]
    table_size = 1024 * 1024 * 1
    memory_size = entry_size * table_size
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)
    runs = []
    clients = 64
    config["trials"]=1
    config["prime"]="true"
    config["prime_fill"]="75"
    config["max_fill"]="80"
    config["workload"]="ycsb-w"
    config["buckets_per_lock"]="1"
    config["num_clients"]=str(clients)
    orchestrator.boot(config.copy())

    for buckets in buckets_per_lock:
        for lpm in locks_per_message:
            lconfig = config.copy()
            print(lconfig["state_machine"])
            lconfig["locks_per_message"] = str(lpm)
            lconfig["buckets_per_lock"] = str(buckets)
            stats = orchestrator.run_trials(lconfig)
            runs.append(stats)
    dm.save_statistics(runs,"data/locks_per_message")

def plot_buckets_per_lock_vs_locks_per_message_experiment():
    data_dir="data/locks_per_message"
    stats, dir = dm.load_statistics(dirname=data_dir)
    rtt_matrix=[]
    rtt_row=[]
    max_buckets_per_lock=2
    max_locks_per_message=64
    for stat in stats:
        for trials in stat:
            config = trials['config']
            tput = plot_cuckoo.single_run_throughput(trials) 
            buckets_per_lock = int(config['buckets_per_lock'])
            locks_per_message = int(config['locks_per_message'])
            tup=(buckets_per_lock,locks_per_message,tput)
            rtt_row.append(tup)
            print(tup)
            if locks_per_message == max_locks_per_message: #max number of buckets per lock
                rtt_matrix.append(rtt_row)
                rtt_row=[]
            # print(config)
            # print("buckets per lock: ", buckets_per_lock, " locks per message: ", locks_per_message)
            break

    # fig, ax = plt.subplots()

    print(rtt_matrix)

    x_axis_buckets_per_lock = []
    for i in range(len(rtt_matrix[0])):
        x_axis_buckets_per_lock.append(rtt_matrix[i][0][0])
    print(x_axis_buckets_per_lock)

    y_axis_locks_per_message = []
    for i in range(len(rtt_matrix)):
        y_axis_locks_per_message.append(rtt_matrix[0][i][1])
    print(y_axis_locks_per_message)

    z_axis_rtt = []
    for i in range(len(rtt_matrix)):
        row=[]
        for j in range(len(rtt_matrix[0])):
            row.append(rtt_matrix[i][j][2])
        z_axis_rtt.append(row)
    print(z_axis_rtt)



    # ax.imshow(z_axis_rtt, cmap='summer', interpolation='nearest')
    # for i in range(len(y_axis_locks_per_message)):
    #     for j in range(len(x_axis_buckets_per_lock)):
    #         v = int(round(z_axis_rtt[i][j],1))
    #         text = ax.text(j, i, v,
    #                     ha="center", va="center", color="w")

    factor = 1.5
    fig, ax = plt.subplots(1,1, figsize=(3*factor,2*factor))

    z_axis_rtt = np.array(z_axis_rtt)
    im, cbar = plot_cuckoo.heatmap(z_axis_rtt, y_axis_locks_per_message, x_axis_buckets_per_lock, ax=ax,
                    cmap="YlGn", cbarlabel="MOPS")
    texts = plot_cuckoo.annotate_heatmap(im, valfmt="{x:.1f}")

    # ax.colorbar()
    # plt.yticks(np.arange(len(y_axis_locks_per_message)), y_axis_locks_per_message)
    # plt.xticks(np.arange(len(x_axis_buckets_per_lock)), x_axis_buckets_per_lock)
    ax.set_xlabel("Buckets per lock")
    ax.set_ylabel("Locks per message")
    fig.tight_layout()
    plt.tight_layout()
    plt.savefig("buckets_per_lock_vs_locks_per_message.pdf")


def independent_search(config):
    # clients = [1, 2, 4, 8, 16, 24]
    # clients = [4, 8, 16, 32, 64, 128, 160]
    locks_per_message = [1,2,4,8,16,32,64]
    # locks_per_message = [1]
    # locks_per_message = [1]
    buckets_per_lock = 1
    configs = [
        ("independent", "random", "ind_random"),
        ("dependent", "random", "dep_random"),
        ("dependent", "bfs", "dep_bfs"),
    ]
    table_size = 1024 * 1024 * 10
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
        ind_50= [12.0, 9.0, 5.0, 5.0, 5.0, 5.0, 5.0]
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
    fig, ax = plt.subplots(1,1, figsize=(6,4))
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
    ax.set_yscale('log')
    ax.set_ylim(0,40000)

    ax.set_yticks(yvalues)
    ax.set_yticklabels(ylabels)
    ax.set_xlabel("Locks per message")
    ax.set_xlim(0,65)



    ax.set_ylabel("Round Trips")
    ax.legend([dashed,solid,di[0],dd[0],bd[0]],["99th percentile","50th percentile","dfs (independent)","dfs (dependent)","bfs (dependent)"],loc='upper left', ncol=2)

    # ax.legend()
    plt.tight_layout()
    plt.savefig("rtt_per_operation.pdf")


def search_fill_throughput():
    table_size = 1024 *64
    clients = 40
    fills = [10,20,30,40,50,60,70,80,90,91,92,93,94,95]
    # fills = [90,91,92]
    # fills = [10,50,70,90] #,93,94,95]
    # fills = [90]
    # fills = [95]
    # fills = [10,20]
    # fills = [90]
    # fills = [10,20,30,40,50]
    # fills=[10]
    config["deterministic"]="false"
    config["prime"]="true"
    config['trials'] = 1
    config['workload'] = 'ycsb-w'
    config['num_clients'] = str(clients)
    config['indexes'] = str(table_size)
    config['memory_size'] = str(entry_size * table_size)
    config['hash_factor'] = str(2.3)

    searches = ["a_star", "random", "bfs"]
    config['trials'] = 5
    # searches = ["a_star"]
    # searches = ["a_star"]
    
    # searches = ["a_star"]
    # searches = ["bfs"]

    orchestrator.boot(config.copy())
    for search in searches:
        runs=[]
        for fill in fills:
            lconfig = config.copy()
            lconfig['max_fill']=str(fill)
            lconfig['prime_fill']=str(fill-10)
            lconfig['search_function']=search
            results = orchestrator.run_trials(lconfig)
            if len(results) > 0:
                runs.append(results)
        dirname="data/search_fill_"+search
        dm.save_statistics(runs, dirname=dirname)
        # plot_general_stats_last_run(dirname=dirname)
def print_recursive_sizes(stats,depth=0):
    if isinstance(stats, dict):
        return False
    if isinstance(stats, list):
        for s in stats:
            print_recursive_sizes(s,depth+1)
            print("    "*depth, "len",len(s), "depth",depth)


def plot_search_fill_tput():
    # searches = ["a_star", "random", "bfs"]
    # searches = ["a_star", "random", "bfs"]
    # searches = ["a_star", "random"]
    # searches = ["a_star", "random", "bfs"]
    searches = ["random", "bfs", "a_star"]
    fig, ax = plt.subplots(1,1, figsize=(12,3))
    for i in range(len(searches)):
        dirname="data/search_fill_"+searches[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        # plot_cuckoo.latency_per_operation(ax, stats, x_axis="max fill", twin=False, decoration=False, hide_zeros=True)
        print(searches[i])
        # plot_cuckoo.throughput(ax, stats, decoration=False, x_axis="max fill", label=searches[i])
        
        print_recursive_sizes(stats)
        if searches[i] == "a_star":
            plot_cuckoo.throughput(ax, stats, decoration=False, x_axis="max fill", label="a_star")
        else:
            plot_cuckoo.throughput(ax, stats, decoration=False, x_axis="max fill", label=searches[i])
    ax.legend()
    ax.set_xlabel("fill_factor")
    ax.set_ylabel("MOPS")
    # ax.set_ylim(0,15)

    plt.tight_layout()
    plt.savefig("search_fill_tput.pdf")


def run_entry_size_exp():
    # entry_sizes = [8,16,32,64]
    table_size = 1024 * 1024 * 10
    read_threshold_bytes=1024
    entry_sizes = [8,16,32,64,128]


    config["prime"]="true"
    config["prime_fill"]="40"
    config["max_fill"]="50"
    config["search_function"]="bfs"

    workloads = ["ycsb-a", "ycsb-c"]
    value_sizes=[x-4 for x in entry_sizes]

    clients=400
    config["read_threshold_bytes"] = str(read_threshold_bytes)
    for workload in workloads:
        runs = []
        for i in range(len(entry_sizes)):
            lconfig = config.copy()
            lconfig['workload'] = workload
            if read_threshold_bytes < entry_sizes[i] * int(lconfig['bucket_size']):
                print("WARNING READ THRESHOLD BEING CHANGED TO MAKE RUN WORK")
                lconfig['read_threshold_bytes'] = entry_sizes[i] * int(lconfig['bucket_size'])

            lconfig['entry_size'] = str(entry_sizes[i])
            lconfig['value_size'] = str(value_sizes[i])

            memory_size = entry_sizes[i] * table_size
            lconfig["indexes"] = str(table_size)
            lconfig["memory_size"] = str(memory_size)
            lconfig['num_clients'] = str(clients)
            orchestrator.build(lconfig)
            orchestrator.boot(lconfig)
            results = orchestrator.run_trials(lconfig)
            if len(results) > 0:
                runs.append(results)
        dirname="data/entry_size_"+workload
        dm.save_statistics(runs, dirname=dirname)

def plot_entry_size_exp():
    print("plotting entry size exp")
    workloads = ["ycsb-a", "ycsb-c"]
    entry_size_order=[]
    entry_sizes = dict()

    fig, ax = plt.subplots(1,1, figsize=(5,3))
    for i in range(len(workloads)):
        print("plotting workload", workloads[i])
        dirname="data/entry_size_"+workloads[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        # print(stats)
        for run in stats:
            run = run[0] #only do a single run
            run_tput = plot_cuckoo.single_run_throughput(run)
            run_tput = round(run_tput, 2)
            entry_size = int(run['config']['entry_size'])
            print(entry_size, run_tput)
            if entry_size not in entry_sizes:
                entry_sizes[entry_size] = []
            entry_sizes[entry_size].append(run_tput)
            if i==0:
                entry_size_order.append(entry_size)
    hatches = ['/', 'o', '\\', '.']
    print(entry_size_order)
    print(entry_sizes)
    print(workloads)
    width = 0.2
    multiplier=0
    x=np.arange(len(workloads))
    for attribute, measurement in entry_sizes.items():
        offset = width * multiplier
        rects = ax.bar(x + offset, measurement, width,hatch=hatches[multiplier], label=str(attribute) + " KV", alpha=0.99, edgecolor="black", color="white")
        # ax.bar_label(rects, padding=3 )
        multiplier += 1
    
    ax.set_ylabel("MOPS")
    ax.set_title("Entry Size vs Throughput")

    capitalized_workloads = [x.upper() for x in workloads]
    size = len(entry_size_order)
    visual_offset = (size-1) * width / 2
    ax.set_xticks(x + visual_offset, capitalized_workloads)
    # ax.set_xticks(x, workloads)
    ax.set_ylim(0,50)
    ax.legend()
    ax.grid()
    plt.tight_layout()
    plt.savefig("entry_size.pdf")

        
def factor_exp():
    #dont start lower than 1.7
    # factors = [1.7,1.8,1.9,2.0,2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,3.0,3.1,3.2,3.3]
    factors = [1.7,1.9,2.1,2.3,2.5,2.7,2.9,3.1,3.3]
    # factors = [2.3, 2.5, 2.7]

    # factors = [3.0]
    clients = 400
    config['num_clients'] = clients
    config['workload'] = "ycsb-w"
    config["prime"]="false"
    config["prime_fill"]="40"
    config["max_fill"]="100"
    config["search_function"]="bfs"

    table_size = 1024 * 1024 * 10
    memory_size = entry_size * table_size
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)
    config["trials"] = 1


    orchestrator.boot(config)
    runs = []
    for f in factors:
        lconfig = config.copy()
        print("running factor", f)
        lconfig['hash_factor'] = str(f)
        results = orchestrator.run_trials(lconfig)
        if len(results) > 0:
            runs.append(results)
    directory = "data/factor"
    dm.save_statistics(runs, dirname=directory)

def plot_factor_exp():
    fig, ax = plt.subplots(1,1, figsize=(12,3))
    dirname = "data/factor"
    stats = dm.load_statistics(dirname=dirname)
    stats=stats[0]
    plot_cuckoo.throughput(ax, stats, decoration=False, x_axis="hash factor", label="rcuckoo")

    ax.legend()
    ax.set_xlabel("fill_factor")
    ax.set_ylabel("MOPS")
    # ax.set_ylim(0,15)

    plt.tight_layout()
    plt.savefig("factor.pdf")


def read_size_sensitivity():
    read_sizes = [64,128,256,512,1024,2048,4096] #8192,16384,32768]
    # read_sizes = [64, 12]
    # read_sizes = [64, 128]
    clients = 40
    config['num_clients'] = clients
    config['workload'] = "ycsb-c"
    config["prime"]="true"
    config["prime_fill"]="80"
    config["max_fill"]="85"
    config["search_function"]="bfs"
    config["runtime"]="10"

    table_size = 1024 * 1024 * 10
    memory_size = entry_size * table_size
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)
    config["trials"] = 10


    orchestrator.boot(config)
    runs = []
    for read_threshold in read_sizes:
        lconfig = config.copy()
        print("running read threshold", read_threshold)
        lconfig["read_threshold_bytes"] = str(read_threshold)
        results = orchestrator.run_trials(lconfig)
        if len(results) > 0:
            print("APPENDING RESULTS")
            runs.append(results)
    directory = "data/read_size"
    dm.save_statistics(runs, dirname=directory)

def plot_read_size_sensitivity():
    fig, ax = plt.subplots(1,1, figsize=(5,3))
    dirname = "data/read_size"
    stats = dm.load_statistics(dirname=dirname)
    stats=stats[0]
    plot_cuckoo.throughput(ax, stats, decoration=False, x_axis="read threshold bytes", label="rcuckoo")

    ax.legend()
    ax.set_xlabel("read threshold bytes")
    ax.set_ylabel("MOPS")
    # ax.set_ylim(0,15)

    plt.tight_layout()
    plt.savefig("read_size.pdf")

def masked_cas_sensitivity():
    use_masked_cas = ["true", "false"]
    table_size = 1024 * 1024 * 1
    memory_size = entry_size * table_size
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)

    config["prime"]="true"
    # config["prime_fill"]="88"
    # config["max_fill"]="91"

    clients = 400
    config['num_clients'] = clients
    config['workload'] = "ycsb-w"
    fills = [10,20,30,40,50,60,70,80,90]

    orchestrator.boot(config)
    for use_mask in use_masked_cas:
        runs = []
        for fill in fills:
            lconfig = config.copy()
            lconfig["max_fill"] = str(fill)
            lconfig["prime_fill"] = str(fill-8)
            lconfig["use_mask"] = str(use_mask)
            results = orchestrator.run_trials(lconfig)
            if len(results) > 0:
                runs.append(results)

        directory = "data/masked_cas_"+use_mask
        dm.save_statistics(runs, dirname=directory)
    


def plot_masked_cas_sensitivity():
    fig, ax = plt.subplots(1,1, figsize=(5,3))
    use_masked_cas = ["true", "false"]

    for use_mask in use_masked_cas:
        dirname = "data/masked_cas_"+use_mask
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        plot_cuckoo.throughput(ax, stats, decoration=False, x_axis="read threshold bytes", label="rcuckoo")

        ax.legend()
        ax.set_xlabel("read threshold bytes")
        ax.set_ylabel("MOPS")
        # ax.set_ylim(0,15)

    plt.tight_layout()
    plt.savefig("masked_cas.pdf")

def masked_cas_vs_lock_size():
    use_masked_cas = ["true", "false"]
    table_size = 1024 * 1024 * 1
    memory_size = entry_size * table_size
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)

    config["prime"]="true"
    config["prime_fill"]="88"
    config["max_fill"]="91"
    config["trials"]=4

    clients = 400
    config['num_clients'] = clients
    config['workload'] = "ycsb-w"
    buckets_per_lock = [1,2,4,8,16,32,64,128]

    orchestrator.boot(config)
    for use_mask in use_masked_cas:
        runs = []
        for bps in buckets_per_lock:
            lconfig = config.copy()
            lconfig["use_mask"] = str(use_mask)
            lconfig["buckets_per_lock"] = str(bps)
            results = orchestrator.run_trials(lconfig)
            if len(results) > 0:
                runs.append(results)

        directory = "data/lock_masked_cas_"+use_mask
        dm.save_statistics(runs, dirname=directory)
    


def plot_masked_cas_vs_lock_size():
    fig, ax = plt.subplots(1,1, figsize=(5,3))
    use_masked_cas = ["true", "false"]

    for use_mask in use_masked_cas:
        dirname = "data/lock_masked_cas_"+use_mask
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        plot_cuckoo.throughput(ax, stats, decoration=False, x_axis="buckets per lock", label="rcuckoo")

        ax.legend()
        ax.set_xlabel("read threshold bytes")
        ax.set_ylabel("MOPS")
        # ax.set_ylim(0,15)

    plt.tight_layout()
    plt.savefig("masked_cas_lock_size.pdf")


def run_hero_ycsb_fill_tput():
    table_size = 1024 * 1024 * 20
    memory_size = entry_size * table_size

    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)

    clients = 400
    fills = [20,30,40,50,60,70,80,90]
    # fills = [50]
    # fills = [90]
    # fills = [10,20,30]
    # fills = [10,20,30,40,50]
    # fills=[10]
    # fills = [10,50,80]

    config['trials'] = 1
    config['num_clients'] = str(clients)
    # workloads = ["ycsb-b"]
    workloads = ["ycsb-a", "ycsb-b","ycsb-c", "ycsb-w"]
    # workloads = ["ycsb-c", "ycsb-w"]
    config['search_function'] = "bfs"
    config["buckets_per_lock"] = str(16)
    config["locks_per_message"] = str(64)
    config["prime"]="true"
    config["deterministic"]="False"
    config["read_threshold_bytes"] = str(256)
    config["runtime"]=str(1)


    orchestrator.boot(config.copy())
    for workload in workloads:
        runs=[]
        for fill in fills:
            lconfig = config.copy()
            lconfig['max_fill']=str(fill)
            lconfig['prime_fill']=str(fill-15)
            lconfig['workload']=workload
            runs.append(orchestrator.run_trials(lconfig))
        dirname="data/hero-fill-tput"+workload
        dm.save_statistics(runs, dirname=dirname)
        # plot_general_stats_last_run(dirname=dirname)


def plot_hero_ycsb_fill_tput():
    workloads = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-w"]
    fig, axs = plt.subplots(1,len(workloads), figsize=(12,3))
    for i in range(len(workloads)):
        dirname="data/hero-fill-tput"+workloads[i]
        ax = axs[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        plot_cuckoo.throughput(ax, stats, x_axis="max fill", decoration=False )
        ax.legend()
        ax.set_xlabel("fill_factor")
        ax.set_title(workloads[i])
        ax.set_ylabel("us")
        # ax.set_ylim(0,15)

    plt.tight_layout()
    plt.savefig("hero_ycsb_fill_tput.pdf")


def search_success_lock_size():
    table_size = 1024 * 1024 * 1
    memory_size = entry_size * table_size
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)

    clients = 400
    # fills = [50]
    # fills = [90]
    # fills = [10,20,30]
    # fills = [10,20,30,40,50]
    # fills=[10]
    # fills = [10,50,80]

    config['trials'] = 1
    config['num_clients'] = str(clients)
    # workloads = ["ycsb-b"]
    config["workload"] = "ycsb-w"
    # workloads = ["ycsb-c", "ycsb-w"]
    config['search_function'] = "bfs"
    config["buckets_per_lock"] = str(16)
    config["locks_per_message"] = str(64)
    config["prime_fill"]="85"
    config["max_fill"]="90"
    config["prime"]="true"
    config["deterministic"]="False"
    config["read_threshold_bytes"] = str(256)
    config["runtime"]=str(1)


    # rows_per_lock = [1,2,4,8,16,32,64]
    rows_per_lock = [1,2,4,8,16,32,64,128]
    # rows_per_lock = [1]
    orchestrator.boot(config)
    runs=[]
    for rpl in rows_per_lock:
        lconfig=config.copy()
        lconfig["buckets_per_lock"] = str(rpl)
        runs.append(orchestrator.run_trials(lconfig))
        dirname="data/search_success_lock_size"
    dm.save_statistics(runs, dirname=dirname)
        # plot_general_stats_last_run(dirname=dirname)

def plot_search_success_lock_size():
    fig, ax = plt.subplots(1,1, figsize=(4,2.5))
    dirname="data/search_success_lock_size"
    stats = dm.load_statistics(dirname=dirname)
    stats=stats[0]


    first_search_fails, first_search_errors = plot_cuckoo.client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'failed_insert_first_search_count', 'completed_insert_count')
    second_search_fails, second_search_errors = plot_cuckoo.client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'failed_insert_second_search_count', 'completed_insert_count')
    x_axis_vals = plot_cuckoo.get_x_axis(stats, "buckets per lock")

    ax.errorbar(x_axis_vals, first_search_fails, yerr=first_search_errors, label="first search retries", marker="s")
    ax.errorbar(x_axis_vals, second_search_fails, yerr=second_search_errors, label="second search retries", marker="o")
    ax.legend()
    ax.set_xlabel("rows per lock")
    ax.set_ylabel("failures per insert")
    ax.set_ylim(0,4)
    plt.tight_layout()
    plt.savefig("search_success_lock_size.pdf")

    # p = ax.errorbar(x_axis_vals,first_search_fails,yerr=first_search_errors,label="first search retries", marker="s")
    # color = p[0].get_color()
    # ax.errorbar(x_axis_vals,lock_retries,yerr=lock_errors,label="failed lock aquisitions", marker="o", linestyle=":", color=color)

def run_hero_ycsb_update():
    table_size = 1024 * 1024 * 10
    memory_size = entry_size * table_size
    # clients = [4, 8, 16, 32, 64, 128, 160]
    clients = [10,20,40,80,160,320,400]
    # clients = [160]
    # clients = [400]
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)
    config["search_function"]="bfs"

    config["prime"]="true"
    config["prime_fill"]="80"
    config["max_fill"]="90"


    config["buckets_per_lock"] = str(1)
    config["locks_per_message"] = str(locks_per_message)
    config["read_threshold_bytes"] = str(128)

    config['trials'] = 1
    workloads = ["ycsb-a", "ycsb-b"] #,"ycsb-c", "ycsb-w"]
    # workloads = ["ycsb-w"]
    # workloads=["ycsb-a"]

    orchestrator.boot(config.copy())
    for workload in workloads:
        runs=[]
        for c in clients:
            lconfig = config.copy()
            lconfig['num_clients'] = str(c)
            lconfig['workload']=workload
            runs.append(orchestrator.run_trials(lconfig))
        dirname="data/hero-update-"+workload
        dm.save_statistics(runs, dirname=dirname)
        # plot_general_stats_last_run(dirname=dirname)


def plot_hero_ycsb_update():
    workloads = ["ycsb-a", "ycsb-b", "ycsb-c", "ycsb-w"]
    # workloads = ["ycsb-a"]
    fig, axs = plt.subplots(1,len(workloads), figsize=(12,3))

    for i in range(len(workloads)):
        dirname="data/hero-update-"+workloads[i]
        ax = axs[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        plot_cuckoo.throughput(ax, stats, decoration=False)
        ax.legend()
        ax.set_xlabel("clients")
        ax.set_title(workloads[i])
        ax.set_ylabel("throughput \n(ops/rtts)*clients")

    plt.tight_layout()
    plt.savefig("hero_ycsb-update.pdf")

run_hero_ycsb_update()
plot_hero_ycsb_update()


# search_success_lock_size()
# plot_search_success_lock_size()


# run_hero_ycsb_fill_tput()
# plot_hero_ycsb_fill_tput()


# masked_cas_vs_lock_size()
# plot_masked_cas_vs_lock_size()

# masked_cas_sensitivity()
# plot_masked_cas_sensitivity()


# read_size_sensitivity()
# plot_read_size_sensitivity()

# factor_exp()
# plot_factor_exp()

# run_entry_size_exp()
# plot_entry_size_exp()





# debug_exp(config)
# fill_factor(config)
# table_size_contention(config)
# client_fill_to_50_exp(config)
# run_hero_ycsb()
# plot_hero_ycsb()

# run_hero_ycsb_fill()
# plot_hero_ycsb_fill()
# locks_per_message_test(config)
# plot_buckets_per_lock_vs_locks_per_message_experiment()

# independent_search(config)
# plot_round_trips_per_insert_operation()
# plot_general_stats_last_run()

# search_fill_throughput()
# plot_search_fill_tput()