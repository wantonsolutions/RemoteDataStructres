import matplotlib.pyplot as plt
import numpy as np


def multi_plot_runs(runs, plot_names):
    print("plotting ", len(runs), " runs: with", len(plot_names), " plots" + str(plot_names))

    fig_width = 5
    fig_height = 2
    total_height = fig_height * len(plot_names)
    fig, axs = plt.subplots(len(plot_names), 1, figsize=(fig_width, total_height))
    if len(plot_names) == 1:
        axs = [axs]
    i=0

    for plot_name in plot_names:
        print(plot_name)
        if plot_name == "general_stats":
            general_stats(axs[i],runs)
        if plot_name == "cas_success_rate":
            cas_success_rate(axs[i],runs)
        elif plot_name == "read_write_ratio":
            read_write_ratio(axs[i],runs)
        elif plot_name == "bytes_per_operation":
            bytes_per_operation(axs[i],runs)
        elif plot_name == "messages_per_operation":
            messages_per_operation(axs[i],runs)
        elif plot_name == "fill_factor":
            fill_factor(axs[i],runs)
        else:
            print("unknown plot name: ", plot_name)
        i+=1

    
    plt.tight_layout()
    plt.savefig("latest_multi_run.pdf")

def multi_plot_run(run, plot_names):
    print("plotting ", len(plot_names), " plots" + str(plot_names))

def stderr(data):
    return np.std(data) / np.sqrt(np.size(data))

def cas_success_rate(ax, stats):
    print("CAS SUCCESS RATE")
    print("\ttodo - label graph with workload")
    print("\ttodo print locking strategy")
    success_rates = []
    std_errs = []
    clients = []
    for stat in stats:
        per_client_success_rate = []
        for client in stat['clients']:
            total_cas = client['stats']['total_cas']
            total_cas_failure = client['stats']['total_cas_failures']
            success_rate = float(1.0 - float(total_cas_failure)/float(total_cas))
            per_client_success_rate.append(success_rate)

        success_rates.append(np.mean(per_client_success_rate))
        std_errs.append(stderr(per_client_success_rate))
        clients.append(str(len(stat['clients'])))

    x_pos = np.arange(len(success_rates))
    ax.bar(x_pos,success_rates,yerr=std_errs,align="center", edgecolor='black')
    ax.set_ylabel("CAS success rate")
    ax.set_xlabel("# clients")
    ax.set_xticks(x_pos)
    ax.set_xticklabels(clients)
    # ax.set_yscale('log')
    ax.set_title("CAS success rate vs # of clients")

def read_write_ratio(ax, stats):
    print("READ WRITE RATIO")
    print("\tTODO make the x axis configurable not just clients")

    #read write ratio should work for both single and multi run
    if isinstance(stats, dict):
        stats = [stats]

    read_rates=[]
    read_err=[]
    write_rates=[]
    write_err=[]
    clients = []
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
        clients.append(str(len(stat['clients'])))

    x_pos = np.arange(len(read_rates))
    bar_width = 0.35
    ax.bar(x_pos,read_rates,bar_width,yerr=read_err,align="center", edgecolor='black', label="Read")
    ax.bar(x_pos+bar_width,write_rates,bar_width,yerr=write_err,align="center", edgecolor='black', label="Write")
    ax.set_ylabel("Read/Write ratio")
    ax.set_xlabel("# clients")
    ax.set_xticks(x_pos+bar_width/2)
    ax.set_xticklabels(clients)
    ax.legend()

def client_stats_x_per_y_get_mean_std(stat, x,y):
        vals=[]
        for client in stat['clients']:
            y_val = client['stats'][y]
            x_val = client['stats'][x]
            vals.append(float(x_val)/float(y_val))
        return np.mean(vals), stderr(vals)

def client_stats_x_per_y_get_mean_std_multi_run(stats, x, y):
    means, stds = [], []
    for stat in stats:
        m, s = (client_stats_x_per_y_get_mean_std(stat, x, y))
        means.append(m)
        stds.append(s)
    return means, stds

def get_x_axis(stats, name):
    if name == "clients":
        return get_client_x_axis(stats)
    elif name == "read threshold bytes":
        return get_read_threshold_x_axis(stats)
    else:
        print("unknown x axis: ", name)
        exit(1)

def get_client_x_axis(stats):
    if isinstance(stats, dict):
        stats = [stats]
    clients = []
    for stat in stats:
        clients.append(stat['config']['num_clients'])
    return clients

def get_read_threshold_x_axis(stats):
    if isinstance(stats, dict):
        stats = [stats]
    read_thresholds = []
    for stat in stats:
        read_thresholds.append(stat['config']['read_threshold_bytes'])
    return read_thresholds
    

def bytes_per_operation(ax, stats, x_axis="clients"):
    print("BYTES PER OPERATION")
    print("\tTODO make the x axis configurable not just clients")
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
    ax.set_xlabel("# clients")
    ax.set_xticks(x_pos+bar_width/2)
    ax.set_xticklabels(x_axis_vals)

def messages_per_operation(ax, stats, x_axis="clients"):
    print("MESSAGES PER OPERATION")
    print("\tTODO make the x axis configurable not just clients")

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


def fill_factor(ax, stats):
    print("FILL FACTOR")
    print("\tTODO make the x axis configurable not just table size")

    label = str("exp - " + str(stats[0]['hash']['factor']))
    table_sizes = []
    fill_rates = []
    for stat in stats:
        c = stat["config"]
        table_sizes.append(c['indexes'])
        fill_rates.append(stat['memory']['fill'])

    if len(set(table_sizes)) == 1:
        ax.text(0.5, 0.5, "Warning - table sizes are all the same", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, color='red')
    elif len(set(table_sizes)) != len(table_sizes):
        ax.text(0.5, 0.5, "Warning - table sizes are not unique ", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, color='red')


    ax.plot(table_sizes, fill_rates, label=str(label), marker='o')
    ax.set_xlabel('Table Size')
    ax.set_ylabel('Fill Rate')
    ax.set_title('Fill Rate vs Table Size')
    ax.legend()

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
