import matplotlib.pyplot as plt
import numpy as np


def multi_plot_runs(runs, plot_names):
    print("plotting ", len(runs), " runs: with", len(plot_names), " plots" + str(plot_names))

    fig_width = 8
    fig_height = 3
    total_height = fig_height * len(plot_names)
    fig, axs = plt.subplots(len(plot_names), 1, figsize=(fig_width, total_height))
    if len(plot_names) == 1:
        axs = [axs]
    i=0

    x_axis = detect_x_axis(runs)

    for plot_name in plot_names:
        print(plot_name)
        if plot_name == "general_stats":
            general_stats(axs[i],runs)
        if plot_name == "cas_success_rate":
            cas_success_rate(axs[i],runs, x_axis)
        elif plot_name == "read_write_ratio":
            read_write_ratio(axs[i],runs, x_axis)
        elif plot_name == "bytes_per_operation":
            bytes_per_operation(axs[i],runs, x_axis)
        elif plot_name == "request_success_rate":
            request_success_rate(axs[i],runs, x_axis)
        elif plot_name == "messages_per_operation":
            messages_per_operation(axs[i],runs, x_axis)
        elif plot_name == "fill_factor":
            fill_factor(axs[i],runs, x_axis)
        else:
            print("unknown plot name: ", plot_name)
        i+=1

    
    plt.tight_layout()
    plt.savefig("latest_multi_run.pdf")

def multi_plot_run(run, plot_names):
    print("plotting ", len(plot_names), " plots" + str(plot_names))

def stderr(data):
    return np.std(data) / np.sqrt(np.size(data))

def cas_success_rate(ax, stats, x_axis="clients"):
    print("CAS SUCCESS RATE")
    print("\ttodo - label graph with workload")
    print("\ttodo print locking strategy")
    success_rates = []
    std_errs = []
    x_axis_vals = get_x_axis(stats, x_axis)
    for stat in stats:
        per_client_success_rate = []
        for client in stat['clients']:
            total_cas = client['stats']['total_cas']
            total_cas_failure = client['stats']['total_cas_failures']
            success_rate = float(1.0 - float(total_cas_failure)/float(total_cas))
            per_client_success_rate.append(success_rate)

        success_rates.append(np.mean(per_client_success_rate))
        std_errs.append(stderr(per_client_success_rate))

    x_pos = np.arange(len(success_rates))
    ax.bar(x_pos,success_rates,yerr=std_errs,align="center", edgecolor='black')
    ax.set_ylabel("CAS success rate")
    ax.set_xlabel(x_axis)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(x_axis_vals)
    # ax.set_yscale('log')
    # ax.set_title("CAS success rate vs # of clients")

def read_write_ratio(ax, stats, x_axis="clients"):
    print("READ WRITE RATIO")
    #read write ratio should work for both single and multi run
    if isinstance(stats, dict):
        stats = [stats]

    read_rates=[]
    read_err=[]
    write_rates=[]
    write_err=[]

    x_axis_vals = get_x_axis(stats, x_axis)
    for stat in stats:
        read_percent = []
        write_percent = []
        for client in stat['clients']:
            reads = client['stats']['completed_read_count']
            writes = client['stats']['completed_insert_count']
            total_ops = reads + writes
            read_percent.append(float(reads)/float(total_ops))
            write_percent.append(float(writes)/float(total_ops))
        read_rates.append(np.mean(read_percent))
        read_err.append(stderr(read_percent))
        write_rates.append(np.mean(write_percent))
        write_err.append(stderr(write_percent))

    x_pos = np.arange(len(x_axis_vals))
    bar_width = 0.35
    ax.bar(x_pos,read_rates,bar_width,yerr=read_err,align="center", edgecolor='black', label="Read")
    ax.bar(x_pos+bar_width,write_rates,bar_width,yerr=write_err,align="center", edgecolor='black', label="Insert")
    ax.set_ylabel("Read/Write ratio")
    ax.set_xlabel(x_axis)
    ax.set_xticks(x_pos+bar_width/2)
    ax.set_xticklabels(x_axis_vals)
    ax.legend()

def bytes_per_operation(ax, stats, x_axis="clients"):
    print("BYTES PER OPERATION")
    if isinstance(stats, dict):
        stats = [stats]

    read_bytes, read_err = client_stats_x_per_y_get_mean_std_multi_run(stats, 'read_operation_bytes', 'completed_read_count')
    write_bytes, write_err = client_stats_x_per_y_get_mean_std_multi_run(stats, 'insert_operation_bytes', 'completed_insert_count')
    x_axis_vals = get_x_axis(stats, x_axis)
    x_pos = np.arange(len(x_axis_vals))

    bar_width = 0.35
    ax.bar(x_pos,read_bytes,bar_width,yerr=read_err,align="center", edgecolor='black', label="Read")
    ax.bar(x_pos+bar_width,write_bytes,bar_width,yerr=write_err,align="center", edgecolor='black', label="Insert")
    ax.legend()
    ax.set_yscale('log')
    ax.set_ylabel("Bytes per operation")
    ax.set_xlabel(x_axis)
    ax.set_xticks(x_pos+bar_width/2)
    ax.set_xticklabels(x_axis_vals)

def messages_per_operation(ax, stats, x_axis="clients"):
    print("MESSAGES PER OPERATION")

    read_messages, read_err = client_stats_x_per_y_get_mean_std_multi_run(stats, 'read_operation_messages', 'completed_read_count')
    write_messages, write_err = client_stats_x_per_y_get_mean_std_multi_run(stats, 'insert_operation_messages', 'completed_insert_count')
    x_axis_vals = get_x_axis(stats, x_axis)
    x_pos = np.arange(len(x_axis_vals))
    bar_width = 0.35

    h1 = ax.bar(x_pos,read_messages,bar_width,yerr=read_err,align="center", edgecolor='black', color='blue', label="Read")

    axt=ax.twinx()
    h2 = axt.bar(x_pos+bar_width,write_messages,bar_width,yerr=write_err,align="center", color='orange', edgecolor='black', label="Insert")

    print(type(h1))
    print(type(h2))
    bars=[h1]+[h2]
    labs=[h.get_label() for h in bars]

    axt.legend(bars, labs, loc="upper left")
    # ax.set_yscale('log')
    ax.set_ylabel("read - messages/op")
    axt.set_ylabel("insert - messages/op")
    ax.set_xlabel(x_axis)
    ax.set_xticks(x_pos+bar_width/2)
    ax.set_xticklabels(x_axis_vals)


def fill_factor(ax, stats, x_axis="table size"):
    print("FILL FACTOR")

    label = str("exp - " + str(stats[0]['hash']['factor']))
    fill_rates = []

    x_axis_vals = get_x_axis(stats, x_axis)
    for stat in stats:
        fill_rates.append(stat['memory']['fill'])

    ax.plot(x_axis_vals, fill_rates, label=str(label), marker='o')
    ax.set_xlabel(x_axis)
    ax.set_ylabel('Fill Rate')
    ax.set_title('Fill Rate vs Table Size')
    ax.legend()

def request_success_rate(ax, stats, x_axis="clients"):
    print("Request Success Rate")
    #read write ratio should work for both single and multi run
    if isinstance(stats, dict):
        stats = [stats]

    read_rates=[]
    read_err=[]
    write_rates=[]
    write_err=[]

    x_axis_vals = get_x_axis(stats, x_axis)
    for stat in stats:
        read_percent = []
        write_percent = []
        for client in stat['clients']:
            reads = client['stats']['completed_read_count']
            read_failures = client['stats']['failed_read_count']
            writes = client['stats']['completed_insert_count']
            write_failures = client['stats']['failed_insert_count']

            if reads == 0:
                reads_success_rate = 0
            else:
                reads_success_rate = float(reads)/(reads+read_failures)

            if writes == 0:
                writes_success_rate = 0
            else:
                writes_success_rate = float(writes)/(writes+write_failures)

            read_percent.append(reads_success_rate)
            write_percent.append(writes_success_rate)

        read_rates.append(np.mean(read_percent))
        read_err.append(stderr(read_percent))
        write_rates.append(np.mean(write_percent))
        write_err.append(stderr(write_percent))

    x_pos = np.arange(len(x_axis_vals))
    bar_width = 0.35
    ax.bar(x_pos,read_rates,bar_width,yerr=read_err,align="center", edgecolor='black', label="Read")
    ax.bar(x_pos+bar_width,write_rates,bar_width,yerr=write_err,align="center", edgecolor='black', label="Insert")
    ax.set_ylabel("Success Rate")
    ax.set_xlabel(x_axis)
    ax.set_xticks(x_pos+bar_width/2)
    ax.set_xticklabels(x_axis_vals)
    ax.legend()



def client_stats_x_per_y_get_mean_std(stat, x,y):
        vals=[]
        for client in stat['clients']:
            y_val = client['stats'][y]
            x_val = client['stats'][x]
            if y_val == 0:
                print("ERROR DIVIDE BY ZERO")
                vals.append(0)
            else:
                vals.append(float(x_val)/float(y_val))
        return np.mean(vals), stderr(vals)

def client_stats_x_per_y_get_mean_std_multi_run(stats, x, y):
    means, stds = [], []
    for stat in stats:
        m, s = (client_stats_x_per_y_get_mean_std(stat, x, y))
        means.append(m)
        stds.append(s)
    return means, stds

def detect_x_axis(stats):
    x_axis=[
        "clients",
        "read threshold bytes",
        "table size",
        "locks per message",
        "buckets per lock"
    ]
    for axis in x_axis:
        if len(set(get_x_axis(stats,axis))) > 1:
            print(axis)
            return axis

    return x_axis[0] #default is to return the first option

def get_x_axis(stats, name):
    if name == "clients":
        return get_client_x_axis(stats)
    elif name == "read threshold bytes":
        return get_read_threshold_x_axis(stats)
    elif name == "table size":
        return get_table_size_x_axis(stats)
    elif name == "locks per message":
        return get_locks_per_message_x_axis(stats)
    elif name == "buckets per lock":
        return get_buckets_per_lock_x_axis(stats)
    else:
        print("unknown x axis: ", name)
        exit(1)

def get_config_axis(stats,name):
    if isinstance(stats, dict):
        stats = [stats]
    clients = []
    for stat in stats:
        clients.append(stat['config'][name])
    return clients


def get_client_x_axis(stats):
    return get_config_axis(stats,'num_clients')

def get_read_threshold_x_axis(stats):
    return get_config_axis(stats,'read_threshold_bytes')

def get_table_size_x_axis(stats):
    return get_config_axis(stats,'indexes')

def get_locks_per_message_x_axis(stats):
    return get_config_axis(stats,'locks_per_message')

def get_buckets_per_lock_x_axis(stats):
    return get_config_axis(stats,'buckets_per_lock')
    


def calculate_total_runs(stats):
    if isinstance(stats, list):
        total = 0
        for s in stats:
            total += calculate_total_runs(s)
        return total
    return 1

def total_run_entry(stats):
    runs = calculate_total_runs(stats)
    return ("runs", runs)

def workload_entry(stats):
    workloads = dict()
    if not isinstance(stats, list):
        stats = [stats]
    for s in stats:
        for c in s['clients']:
            w = c['stats']['workload_stats']['workload']
            workloads[w] = True
    loads = ""
    for w in workloads:
        if loads != "":
            loads = loads + ", "
        loads= loads + w
    return ("workload", loads)

def get_config_list(stats, key):
    key_dict = dict()
    if not isinstance(stats, list):
        stats = [stats]

    for s in stats:
        keys = s["config"][key]
        key_dict[keys] = True

    klist=[] 
    for k in key_dict:
        klist.append(k)

    klist.sort()

    sklist = ""
    for k in klist:
        if sklist != "":
            sklist = sklist + ", "
        sklist = sklist + str(k)
    return sklist

def clients_entry(stats):
    return ("clients", get_config_list(stats, "num_clients"))

def hash_factors(stats):
    return ("hash factor", get_config_list(stats, "hash_factor"))

def table_sizes(stats):
    return ("table size", get_config_list(stats, "indexes"))

def read_thresholds(stats):
    return ("read thresholds", get_config_list(stats, "read_threshold_bytes"))

def buckets_per_lock(stats):
    return ("buckets_per_lock", get_config_list(stats, "buckets_per_lock"))

def locks_per_message(stats):
    return ("locks_per_message", get_config_list(stats, "locks_per_message"))


def general_stats(ax, stats):
    print("RUN STATISTICS")
    rows=[]
    values=[]
    staistic_functions=[
        total_run_entry,
        workload_entry,
        clients_entry,
        hash_factors,
        table_sizes,
        read_thresholds,
        buckets_per_lock,
        locks_per_message,
    ]
    print(len(stats))
    for f in staistic_functions:
        label, value = f(stats)
        rows.append(label)
        values.append([value])
    ax.axis('off')

    print(values)
    print(rows)

    ax.table(cellText=values, rowLabels = rows, loc='center')
