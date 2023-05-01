import matplotlib.pyplot as plt
import numpy as np


def multi_plot_runs(runs, plot_names, directory=""):
    print("plotting ", len(runs), " runs: with", len(plot_names), " plots" + str(plot_names))

    fig_width = 8
    fig_height = 3
    total_height = fig_height * len(plot_names)
    fig, axs = plt.subplots(len(plot_names), 1, figsize=(fig_width, total_height))
    if len(plot_names) == 1:
        axs = [axs]
    i=0

    x_axis = detect_x_axis(runs)
    print("detected x_axis: ", x_axis)
    # runs = get_flattened_stats(runs)

    for plot_name in plot_names:
        # print(runs)
        print(plot_name)
        if plot_name == "general_stats":
            general_stats(axs[i],runs)
        elif plot_name == "cas_success_rate":
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

    fig_name = directory + "/general.pdf"
    plt.savefig(fig_name)
    fig_name = "./general.pdf"
    plt.savefig(fig_name)

def single_plot(data, function, filename):
    fig_width = 8
    fig_height = 3
    fig, ax = plt.subplots(1, 1, figsize=(fig_width, fig_height))
    function(ax, data)
    plt.tight_layout()
    plt.savefig(filename)

def multi_plot_run(run, plot_names):
    print("plotting ", len(plot_names), " plots" + str(plot_names))

def stderr(data):
    return np.std(data) / np.sqrt(np.size(data))

def div_by_zero_to_zero(x, y):
    if y == 0:
        print("ERROR - div by zero")

        # raise Exception("Div by zero")
        return float(0)
    else:
        return float(x/y)

def single_run_client_average_cas_success(stat):
    per_client_success_rate = []
    for client in stat['clients']:
        total_cas = client['stats']['total_cas']
        total_cas_failure = client['stats']['total_cas_failures']
        ratio = div_by_zero_to_zero(total_cas_failure, total_cas)
        success_rate = float(1.0 - ratio)
        per_client_success_rate.append(success_rate)
    return (np.mean(per_client_success_rate), stderr(per_client_success_rate))

def cas_success_rate_line(ax,stats,label,x_axis="clients"):
    success_rates = []
    std_errs = []
    x_axis_vals = get_x_axis(stats, x_axis)
    for stat in stats:
        single_run_success_rate=[]
        single_run_errors=[]
        for r in stat:
            s, e = single_run_client_average_cas_success(r)
            single_run_success_rate.append(s)
            single_run_errors.append(e)
        success_rates.append(np.mean(single_run_success_rate))
        std_errs.append(np.mean(single_run_errors))
    # x_pos = np.arange(len(success_rates))
    ax.errorbar(x_axis_vals,success_rates,yerr=std_errs,label=label, marker='o', capsize=3)

def cas_success_rate(ax, stats, x_axis="clients"):
    stats = correct_stat_shape(stats)
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        cas_success_rate_line(ax, stat, label=state_machine_label, x_axis=x_axis)
    cas_success_rate_decoration(ax, x_axis)

def cas_success_rate_decoration(ax, x_axis):
    ax.set_ylabel("CAS success rate")
    ax.set_xlabel(x_axis)
    ax.set_ylim(bottom=0, top=1.1)


def get_client_total_ops(client):
    reads = client['stats']['completed_read_count']
    writes = client['stats']['completed_insert_count']
    total_ops = reads + writes
    return total_ops

def single_run_client_average_ops(stat, op):
    op_percents = []
    op_string = "completed_" + op + "_count"
    for client in stat['clients']:
        ops = client['stats'][op_string]
        total_ops = get_client_total_ops(client)
        op_percents.append(div_by_zero_to_zero(ops, total_ops))

    op_ratio = np.mean(op_percents)
    op_error = stderr(op_percents)
    return (op_ratio, op_error)

def read_write_ratio_line(ax, stats, label, x_axis="clients"):
    print("Operation RATIO")
    #read write ratio should work for both single and multi run
    if isinstance(stats, dict):
        stats = [stats]

    operations = ["read", "insert"]
    color=None
    for op in operations:
        avg_op_rates, avg_op_errors = [], []
        x_axis_vals = get_x_axis(stats, x_axis)
        for stat in stats:
            op_rates, op_errors = [], []
            for run in stat:
                o, oe, = single_run_client_average_ops(run, op)
                op_rates.append(o)
                op_errors.append(oe)
            avg_op_rates.append(np.mean(op_rates))
            avg_op_errors.append(np.mean(op_errors))
        if color==None:
            p=ax.errorbar(x_axis_vals,avg_op_rates,yerr=avg_op_errors,linestyle=op_linestyles[op],marker=op_markers[op],label=label+"-"+op)
            color=p[0].get_color()
        else:
            ax.errorbar(x_axis_vals,avg_op_rates,yerr=avg_op_errors,linestyle=op_linestyles[op],marker=op_markers[op],label=label+"-"+op, color=color)

def read_write_ratio_decoration(ax, x_axis):
    ax.set_ylabel("Read/Write ratio")
    ax.set_xlabel(x_axis)
    ax.legend()

def read_write_ratio(ax, stats, x_axis="clients"):
    stats = correct_stat_shape(stats)
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        read_write_ratio_line(ax, stat, label=state_machine_label, x_axis=x_axis)
    read_write_ratio_decoration(ax, x_axis)


def bytes_per_operation(ax, stats, x_axis="clients"):
    stats = correct_stat_shape(stats)
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        bytes_per_operation_line(ax, stat, label=state_machine_label, x_axis=x_axis)
    bytes_per_operation_decoration(ax, x_axis)

def bytes_per_operation_decoration(ax, x_axis):
    ax.set_ylim(bottom=100)
    ax.set_yscale('log')
    ax.set_ylabel("Bytes per operation")
    ax.set_xlabel(x_axis)
    ax.legend()

def bytes_per_operation_line(ax, stats, label, x_axis="clients"):

    print("BYTES PER OPERATION")
    if isinstance(stats, dict):
        stats = [stats]

    read_bytes, read_err = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'read_operation_bytes', 'completed_read_count')
    write_bytes, write_err = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'insert_operation_bytes', 'completed_insert_count')
    x_axis_vals = get_x_axis(stats, x_axis)

    p = ax.errorbar(x_axis_vals,read_bytes,yerr=read_err,label=label+"-read", marker="s")
    color = p[0].get_color()
    ax.errorbar(x_axis_vals,write_bytes,yerr=write_err,label=label+"-insert", marker="o", linestyle=":", color=color)


def messages_per_operation_decoration(ax, axt, x_axis, lines):
    ax.set_ylabel("insert - messages/op")
    axt.set_ylabel("read - messages/op")
    ax.set_xlabel(x_axis)
    ax.set_ylim(bottom=0)
    axt.set_ylim(bottom=0)
    labs = [l.get_label() for l in lines]
    ax.legend(lines, labs)
    # axt.legend()
    # ax.set_xticks(x_pos+bar_width/2)
    # ax.set_xticklabels(x_axis_vals)

op_markers={"read": "s", "insert": "o"}
op_linestyles={"read": "-", "insert": ":"}


def messages_per_operation_line(ax, axt, stats, label, x_axis="clients"):
    print("MESSAGES PER OPERATION")

    read_messages, read_err = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'read_operation_messages', 'completed_read_count')
    write_messages, write_err = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'insert_operation_messages', 'completed_insert_count')
    x_axis_vals = get_x_axis(stats, x_axis)

    h1 = ax.errorbar(x_axis_vals,write_messages,yerr=write_err, linestyle=op_linestyles['insert'], marker=op_markers['insert'], label=label+"-insert")
    h2 = axt.errorbar(x_axis_vals,read_messages,yerr=read_err, linestyle=op_linestyles['read'], label=label+"-read", marker=op_markers['read'])

    lines = [h1, h2] 
    # labs=[h.get_label() for h in bars]
    return lines
    # axt.legend(bars, labs, loc="upper left")


def messages_per_operation(ax, stats, x_axis="clients"):
    axt=ax.twinx()
    stats = correct_stat_shape(stats)
    lines = []
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        l = messages_per_operation_line(ax, axt, stat, label=state_machine_label, x_axis=x_axis)
        lines.extend(l)
    messages_per_operation_decoration(ax, axt, x_axis, lines)



def fill_factor_line(ax, stats, label, x_axis="table size"):
    fr_avg, fr_err = [], []
    x_axis_vals = get_x_axis(stats, x_axis)
    for stat in stats:
        fill_rates = []
        for r in stat:
            fill_rates.append(r['memory']['fill'])
        # print("fill-rates", fill_rates)
        fr_avg.append(np.mean(fill_rates))
        fr_err.append(np.std(fill_rates))

    fr_avg = [x * 100 for x in fr_avg]
    fr_err = [x * 100 for x in fr_err]
    ax.errorbar(x_axis_vals, fr_avg, yerr=fr_err, label=label, marker='^')

def fill_factor_decoration(ax, x_axis):
    ax.grid(True, axis='y', linestyle=':')
    ax.axhline(y=90, color='r', linestyle=':')
    ax.set_ylim(top=105, bottom=0)
    ax.set_xlabel(x_axis)
    ax.set_ylabel('Max Load Factor (%)')
    ax.set_title('Max Load Factor vs ' + x_axis)
    ax.legend()

def correct_stat_shape(stats):
    dim = np.shape(stats)
    if len(dim) == 1:
        stats = [[stats]]
    elif len(dim) == 2:
        stats = [stats]
    elif len(dim) == 3:
        pass
    return stats


def fill_factor(ax, stats, x_axis="bucket size"):
    print("fill factor x-axis: ", x_axis)
    stats = correct_stat_shape(stats)
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        fill_factor_line(ax, stat, label=state_machine_label, x_axis=x_axis)
    fill_factor_decoration(ax, x_axis)


def single_run_op_success_rate(stat, op_string):
    op_percent = []
    for client in stat['clients']:
        ops = client['stats']['completed_'+op_string+'_count']
        op_failures = client['stats']['failed_'+op_string+'_count']

        op_success_rate = 0
        if ops != 0:
            op_success_rate = float(ops)/(ops+op_failures)
        op_percent.append(op_success_rate)

    return op_percent

def request_success_rate_line(ax, stats, label, x_axis="clients"):
    #read write ratio should work for both single and multi run
    if isinstance(stats, dict):
        stats = [stats]

    ops = ['read', 'insert']
    color=None
    for op in ops:
        avg_op_rates=[]
        avg_op_err=[]
        x_axis_vals = get_x_axis(stats, x_axis)
        for stat in stats:
            op_percentages, op_errors = [], []
            for run in stat:
                op_percent = single_run_op_success_rate(run, op)
                op_percentages.append(np.mean(op_percent))
                op_errors.append(stderr(op_percent))
            avg_op_rates.append(np.mean(op_percentages))
            avg_op_err.append(np.mean(op_errors))

        if color==None:
            p=ax.errorbar(x_axis_vals,avg_op_rates,yerr=avg_op_err,linestyle=op_linestyles[op],marker=op_markers[op],label=label+"-"+op)
            color=p[0].get_color()
        else:
            ax.errorbar(x_axis_vals,avg_op_rates,yerr=avg_op_err,linestyle=op_linestyles[op],marker=op_markers[op],label=label+"-"+op, color=color)

def request_success_rate_decoration(ax, x_axis):
    ax.set_ylabel("Success Rate")
    ax.set_xlabel(x_axis)
    # ax.set_xticks(x_pos+bar_width/2)
    # ax.set_xticklabels(x_axis_vals)
    ax.legend()

def request_success_rate(ax, stats, x_axis="clients"):
    stats = correct_stat_shape(stats)
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        request_success_rate_line(ax, stat, label=state_machine_label, x_axis=x_axis)
    request_success_rate_decoration(ax, x_axis)

def client_stats_x_per_y_get_mean_std(stat, x,y):
        vals=[]
        for client in stat['clients']:
            y_val = client['stats'][y]
            x_val = client['stats'][x]
            vals.append(div_by_zero_to_zero(x_val, y_val))
        return np.mean(vals), stderr(vals)

def client_stats_x_per_y_get_mean_std_multi_run_trials(stats, x, y):
    means, stds = [], []
    for stat in stats:
        r_means, r_stds = [], []
        for run in stat:
            m, s = (client_stats_x_per_y_get_mean_std(run, x, y))
            r_means.append(m)
            r_stds.append(s)
        means.append(np.mean(r_means))
        stds.append(np.mean(r_stds))
    return means, stds

def detect_x_axis(stats):
    x_axis=[
        "clients",
        "table size",
        "max fill",
        "locks per message",
        "buckets per lock",
        "bucket size",
        "state machine",
        "read threshold bytes",
    ]
    for axis in x_axis:
        if len(set(get_x_axis(stats,axis))) > 1:
            print(axis)
            return axis

    return x_axis[0] #default is to return the first option

def get_x_axis(stats, name):
    if name == "clients":
        return get_client_x_axis(stats)
    elif name == "table size":
        return get_table_size_x_axis(stats)
    elif name == "locks per message":
        return get_locks_per_message_x_axis(stats)
    elif name == "buckets per lock":
        return get_buckets_per_lock_x_axis(stats)
    elif name == "bucket size":
        return get_bucket_size_x_axis(stats)
    elif name == "read threshold bytes":
        return get_read_threshold_x_axis(stats)
    elif name == "max fill":
        return get_max_fill_x_axis(stats)
    elif name == "state machine":
        return get_state_machine_x_axis(stats)
    else:
        print("unknown x axis: ", name)
        exit(1)

def get_flattened_stats(stats):
    # we can have a few different dimensions for the stat file, and the goal is to get the x axis.
    # if we have a single run, then we can just get the x axis from the config
    # if we have a multi run, then we need to get the x axis from the first run of each trial
    s = np.array(stats).shape
    if len(s) == 1:
        if isinstance(stats, dict):
            stats = [stats]
        else:
            return stats
    elif len(s) == 2:
        #flatten by getting the first run of each trial
        s_prime = []
        for stat in stats:
            s_prime.append(stat[0])
        stats = s_prime
    elif len(s) == 3:
        s_prime = []
        for stat in stats[0]:
            s_prime.append(stat[0])
        stats = s_prime
    else:
        print("error we have a multi run but the dimensions are wrong")
        print("shape: ", s)
        exit(1)
    return stats

def get_config_axis(stats,name):
    stats = get_flattened_stats(stats)
    axis = []
    for stat in stats:
        axis.append(stat['config'][name])
    return axis


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

def get_state_machine_x_axis(stats):
    return get_config_axis(stats,'state_machine')

def get_bucket_size_x_axis(stats):
    return get_config_axis(stats,'bucket_size')

def get_max_fill_x_axis(stats):
    return get_config_axis(stats,'max_fill')

def calculate_total_runs(stats):
    s = np.array(stats).shape
    b = 1
    for i in range(len(s)):
        b = b * s[i]
    return b

def total_run_entry(stats):
    runs = calculate_total_runs(stats)
    return ("runs", runs)

def workload_entry(stats):
    workloads = dict()
    stats = get_flattened_stats(stats)
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
    stats = get_flattened_stats(stats)

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

def experiment_description(stats):
    return ("description", get_config_list(stats, "description"))

def experiment_commit(stats):
    return ("commit", get_config_list(stats, "commit"))

def experiment_date(stats):
    return ("date", get_config_list(stats, "date"))

def experiment_name(stats):
    return ("name", get_config_list(stats, "name"))

def experiment_trials(stats):
    return ("trials", get_config_list(stats, "trials"))

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

def state_machines(stats):
    return ("state machine", get_config_list(stats, "state_machine"))

def bucket_size(stats):
    return ("bucket size", get_config_list(stats, "bucket_size"))

def max_fill(stats):
    return ("max fill", get_config_list(stats, "max_fill"))


def general_stats(ax, stats):
    print("RUN STATISTICS")
    rows=[]
    values=[]
    staistic_functions=[
        experiment_description,
        experiment_name,
        experiment_trials,
        experiment_commit,
        experiment_date,
        total_run_entry,
        workload_entry,
        clients_entry,
        hash_factors,
        table_sizes,
        read_thresholds,
        buckets_per_lock,
        locks_per_message,
        state_machines,
        bucket_size,
        max_fill
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
