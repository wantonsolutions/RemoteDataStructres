import matplotlib.pyplot as plt
import numpy as np
import matplotlib
import json


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
        elif plot_name == "rtt_per_operation":
            rtt_per_operation(axs[i],runs, x_axis)
        elif plot_name == "fill_factor":
            fill_factor(axs[i],runs, x_axis)
        elif plot_name == "throughput_approximation":
            throughput_approximation(axs[i],runs, x_axis)
        elif plot_name == "throughput":
            throughput(axs[i],runs, x_axis)
        elif plot_name == "bandwidth":
            bandwidth(axs[i],runs, x_axis)
        elif plot_name == "latency_per_operation":
            latency_per_operation(axs[i],runs, x_axis)
        elif plot_name == "retry_breakdown":
            retry_breakdown(axs[i],runs, x_axis)
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
    x = float(x)
    y = float(y)
    if y == 0:
        # print("ERROR - div by zero")

        # raise Exception("Div by zero")
        return float(0)
    else:
        return float(x/y)

def heatmap(data, row_labels, col_labels, ax=None,
            cbar_kw=None, cbarlabel="", **kwargs):
    """
    Create a heatmap from a numpy array and two lists of labels.

    Parameters
    ----------
    data
        A 2D numpy array of shape (M, N).
    row_labels
        A list or array of length M with the labels for the rows.
    col_labels
        A list or array of length N with the labels for the columns.
    ax
        A `matplotlib.axes.Axes` instance to which the heatmap is plotted.  If
        not provided, use current axes or create a new one.  Optional.
    cbar_kw
        A dictionary with arguments to `matplotlib.Figure.colorbar`.  Optional.
    cbarlabel
        The label for the colorbar.  Optional.
    **kwargs
        All other arguments are forwarded to `imshow`.
    """

    if ax is None:
        ax = plt.gca()

    if cbar_kw is None:
        cbar_kw = {}
    cbar_kw["fraction"]=0.03

    # Plot the heatmap
    im = ax.imshow(data, **kwargs)

    # Create colorbar
    cbar = ax.figure.colorbar(im, ax=ax, **cbar_kw)
    cbar.ax.set_ylabel(cbarlabel, rotation=-90, va="bottom")

    # Show all ticks and label them with the respective list entries.
    ax.set_xticks(np.arange(data.shape[1]), labels=col_labels)
    ax.set_yticks(np.arange(data.shape[0]), labels=row_labels)

    # Let the horizontal axes labeling appear on top.
    # ax.tick_params(top=False, bottom=True,
    #                labeltop=False, labelbottom=True)

    # Rotate the tick labels and set their alignment.
    # plt.setp(ax.get_xticklabels(), rotation=-30, ha="right",
    #          rotation_mode="anchor")

    # Turn spines off and create white grid.
    ax.spines[:].set_visible(False)

    ax.set_xticks(np.arange(data.shape[1]+1)-.5, minor=True)
    ax.set_yticks(np.arange(data.shape[0]+1)-.5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax.tick_params(which="minor", bottom=False, left=False)

    return im, cbar


def annotate_heatmap(im, data=None, valfmt="{x:.2f}",
                     textcolors=("black", "white"),
                     threshold=None, **textkw):
    """
    A function to annotate a heatmap.

    Parameters
    ----------
    im
        The AxesImage to be labeled.
    data
        Data used to annotate.  If None, the image's data is used.  Optional.
    valfmt
        The format of the annotations inside the heatmap.  This should either
        use the string format method, e.g. "$ {x:.2f}", or be a
        `matplotlib.ticker.Formatter`.  Optional.
    textcolors
        A pair of colors.  The first is used for values below a threshold,
        the second for those above.  Optional.
    threshold
        Value in data units according to which the colors from textcolors are
        applied.  If None (the default) uses the middle of the colormap as
        separation.  Optional.
    **kwargs
        All other arguments are forwarded to each call to `text` used to create
        the text labels.
    """

    if not isinstance(data, (list, np.ndarray)):
        data = im.get_array()

    # Normalize the threshold to the images color range.
    if threshold is not None:
        threshold = im.norm(threshold)
    else:
        threshold = im.norm(data.max())/2.

    # Set default alignment to center, but allow it to be
    # overwritten by textkw.
    kw = dict(horizontalalignment="center",
              verticalalignment="center")
    kw.update(textkw)

    # Get the formatter in case a string is supplied
    if isinstance(valfmt, str):
        valfmt = matplotlib.ticker.StrMethodFormatter(valfmt)

    # Loop over the data and create a `Text` for each "pixel".
    # Change the text's color depending on the data.
    texts = []
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            kw.update(color=textcolors[int(im.norm(data[i, j]) > threshold)])
            text = im.axes.text(j, i, valfmt(data[i, j], None), **kw)
            texts.append(text)

    return texts

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
    # ax.set_ylim(bottom=0, top=1.1)


def get_client_total_ops(client):
    reads = int(client['stats']['completed_read_count'])
    writes = int(client['stats']['completed_insert_count'])
    total_ops = reads + writes
    return total_ops

def single_run_client_average_ops(stat, op):
    op_percents = []
    op_string = "completed_" + op + "_count"
    for client in stat['clients']:
        ops = int(client['stats'][op_string])
        total_ops = get_client_total_ops(client)
        print("ops: ", ops, "total_ops: ", total_ops)
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
    ax.set_ylim(bottom=0, top=1.1)
    ax.legend()

def read_write_ratio(ax, stats, x_axis="clients"):
    stats = correct_stat_shape(stats)
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        read_write_ratio_line(ax, stat, label=state_machine_label, x_axis=x_axis)
    read_write_ratio_decoration(ax, x_axis)


def bytes_per_operation(ax, stats, x_axis="clients", decoration=True):
    stats = correct_stat_shape(stats)
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        bytes_per_operation_line(ax, stat, label=state_machine_label, x_axis=x_axis)
    if decoration:
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
    read_bytes = [x for x in read_bytes if str(x) != 'nan']
    write_bytes = [x for x in write_bytes if str(x) != 'nan']
    read_err = [x for x in read_err if str(x) != 'nan']
    write_err = [x for x in write_err if str(x) != 'nan']
    print("read_bytes=", read_bytes)
    print("write_bytes=", write_bytes)

    x_axis_vals = get_x_axis(stats, x_axis)
    print("bytes x axis", x_axis_vals)

    p = ax.errorbar(x_axis_vals,read_bytes,yerr=read_err,label=label+"-read", marker="s")
    color = p[0].get_color()
    ax.errorbar(x_axis_vals,write_bytes,yerr=write_err,label=label+"-insert", marker="o", linestyle=":", color=color)

def retry_breakdown_line(ax, stats, label, x_axis="clients"):

    print("Retries per insert")
    if isinstance(stats, dict):
        stats = [stats]

    insert_retries, insert_errors = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'failed_insert_second_search_count', 'completed_insert_count')
    lock_retries, lock_errors = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'failed_lock_aquisition_count', 'completed_insert_count')
    x_axis_vals = get_x_axis(stats, x_axis)

    p = ax.errorbar(x_axis_vals,insert_retries,yerr=insert_errors,label="second search retries", marker="s")
    color = p[0].get_color()
    ax.errorbar(x_axis_vals,lock_retries,yerr=lock_errors,label="failed lock aquisitions", marker="o", linestyle=":", color=color)


def retry_breakdown_decoration(ax, x_axis):
    ax.set_ylabel("Retries per operation")
    ax.set_xlabel(x_axis)
    ax.legend()

def retry_breakdown(ax, stats, x_axis="clients", decoration=True):
    stats = correct_stat_shape(stats)
    lines = []
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        retry_breakdown_line(ax, stat, label=state_machine_label, x_axis=x_axis)
    if decoration:
        retry_breakdown_decoration(ax, x_axis)



op_markers={"read": "s", "insert": "o"}
op_linestyles={"read": "-", "insert": ":"}


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

def messages_per_operation_line(axs, stats, label, x_axis="clients"):
    print("MESSAGES PER OPERATION")

    if len(axs) == 2:
        ax = axs[0]
        axt = axs[1]
    else:
        ax = axs[0]
        axt = axs[0]

    read_messages, read_err = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'read_operation_messages', 'completed_read_count')
    write_messages, write_err = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'insert_operation_messages', 'completed_insert_count')
    x_axis_vals = get_x_axis(stats, x_axis)

    read_messages = [x for x in read_messages if str(x) != 'nan']
    write_messages = [x for x in write_messages if str(x) != 'nan']
    read_err = [x for x in read_err if str(x) != 'nan']
    write_err = [x for x in write_err if str(x) != 'nan']

    print("read_messages=", read_messages)
    print("write_messages=", write_messages)


    h1 = ax.errorbar(x_axis_vals,write_messages,yerr=write_err, linestyle=op_linestyles['insert'], marker=op_markers['insert'], label=label+"-insert")
    color = h1[0].get_color()
    h2 = axt.errorbar(x_axis_vals,read_messages,yerr=read_err, linestyle=op_linestyles['read'], label=label+"-read", marker=op_markers['read'], color=color)

    lines = [h1, h2] 
    return lines

def messages_per_operation_decoration(ax, axt, x_axis, lines):
    ax.set_ylabel("insert - messages/op")
    axt.set_ylabel("read - messages/op")
    ax.set_xlabel(x_axis)
    ax.set_ylim(bottom=0)
    axt.set_ylim(bottom=0)
    labs = [l.get_label() for l in lines]
    ax.legend(lines, labs)

def messages_per_operation(ax, stats, x_axis="clients", decoration=True, twin=True):
    if twin:
        axt=ax.twinx()
        axs = [ax, axt]
    else:
        axs = [ax]

    stats = correct_stat_shape(stats)
    lines = []
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        l = messages_per_operation_line(axs, stat, label=state_machine_label, x_axis=x_axis)
        lines.extend(l)
    if decoration:
        messages_per_operation_decoration(axs[0], axs[1], x_axis, lines)

def line_is_zero(line):
    for v in line:
        if v != 0:
            return False
    return True

def latency_per_operation_line(axs, stats, label, x_axis="clients", hide_zeros=False):
    print("LATENCY PER OPERATION")

    if len(axs) == 2:
        ax = axs[0]
        axt = axs[1]
    else:
        ax = axs[0]
        axt = axs[0]

    percentile=50
    read_latency, read_err = client_stats_get_percentile_err_trials(stats, 'read_latency_ns', percentile)
    write_latency, write_err = client_stats_get_percentile_err_trials(stats, 'insert_latency_ns', percentile)

    # read_latency, read_err = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'sum_read_latency_ns','completed_read_count' )
    # write_latency, write_err = client_stats_x_per_y_get_mean_std_multi_run_trials(stats,  'sum_insert_latency_ns','completed_insert_count' )

    print("read_latency", read_latency)
    print("write_latency", write_latency)

    read_latency = [x/1000 for x in read_latency]
    write_latency = [x/1000 for x in write_latency]
    read_err = [x/1000 for x in read_err]
    write_err = [x/1000 for x in write_err]

    read_latency = [x for x in read_latency if str(x) != 'nan']
    write_latency = [x for x in write_latency if str(x) != 'nan']
    read_err = [x for x in read_err if str(x) != 'nan']
    write_err = [x for x in write_err if str(x) != 'nan']
    x_axis_vals = get_x_axis(stats, x_axis)

    print("hiding zeros:", hide_zeros)
    if not hide_zeros:
        h1 = ax.errorbar(x_axis_vals,write_latency,yerr=write_err, linestyle=op_linestyles['insert'], marker=op_markers['insert'], label=label+"-insert")
        color = h1[0].get_color()
        h2 = axt.errorbar(x_axis_vals,read_latency,yerr=read_err, linestyle=op_linestyles['read'], label=label+"-read", marker=op_markers['read'], color=color)
        lines = [h1, h2] 
        return lines
    else:
        lines = []
        color = None
        if not line_is_zero(write_latency):
            h1 = ax.errorbar(x_axis_vals,write_latency,yerr=write_err, linestyle=op_linestyles['insert'], marker=op_markers['insert'], label=label+"-insert")
            color = h1[0].get_color()
            lines.append(h1)
        if not line_is_zero(read_latency):
            if color is None:
                h2 = axt.errorbar(x_axis_vals,read_latency,yerr=read_err, linestyle=op_linestyles['read'], label=label+"-read", marker=op_markers['read'])
            else:
                h2 = axt.errorbar(x_axis_vals,read_latency,yerr=read_err, linestyle=op_linestyles['read'], label=label+"-read", marker=op_markers['read'], color=color)

            lines.append(h2)


def latency_per_operation_decoration(ax, axt, x_axis, lines):
    ax.set_ylabel("insert - latency/op (us)")
    axt.set_ylabel("read - latency/op (us)")
    ax.set_xlabel(x_axis)
    ax.set_ylim(bottom=0)
    axt.set_ylim(bottom=0)
    labs = [l.get_label() for l in lines]
    ax.legend(lines, labs)

def latency_per_operation(ax, stats, x_axis="clients", decoration=True, twin=True, hide_zeros=False):
    if twin:
        axt=ax.twinx()
        axs = [ax, axt]
    else:
        axs = [ax]

    print("hiding zeros:", hide_zeros)

    stats = correct_stat_shape(stats)
    lines = []
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        l = latency_per_operation_line(axs, stat, label=state_machine_label, x_axis=x_axis, hide_zeros=hide_zeros)
        if l is not None:
            lines.extend(l)
    if decoration:
        latency_per_operation_decoration(axs[0], axs[1], x_axis, lines)





def rtt_per_operation_decoration(ax, axt, x_axis, lines):
    ax.set_ylabel("insert - rtt/op")
    axt.set_ylabel("read - rtt/op")
    ax.set_xlabel(x_axis)
    ax.set_ylim(bottom=0)
    axt.set_ylim(bottom=0)
    labs = [l.get_label() for l in lines]
    ax.legend(lines, labs)

def calculate_rtt(stats, key):
    means, stds = [], []
    for stat in stats:
        r_means, r_stds = [], []
        for run in stat:
            c_means = []
            for client in run["clients"]:
                count_key = key + "rtt+count"
                op_key = "completed_" + key + "_count"
                rtts = client[count_key]
                ops = client[op_key]
                avg = float(rtts)/float(ops)
                c_means.append(avg)
            r_means.append(np.mean(c_means))
            r_stds.append(np.std(c_means))
        means.append(np.mean(r_means))
        stds.append(np.mean(r_stds))
    return means, stds


def rtt_per_operation_line(ax, axt, stats, label, x_axis="clients", twin=True, percentile=50):
    print("RTT PER OPERATION")


    # read_rtt, read_err = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'read_rtt_count', 'completed_read_count')
    # insert_rtt, insert_err = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'insert_rtt_count', 'completed_insert_count')
    read_rtt, read_err = client_stats_get_percentile_err_trials(stats, 'read_rtt', percentile)
    insert_rtt, insert_err = client_stats_get_percentile_err_trials(stats, 'insert_rtt', percentile)

    # read_rtt, read_err = calculate_rtt(stats, 'read')
    # insert_rtt, insert_err = calculate_rtt(stats, 'insert')

    x_axis_vals = get_x_axis(stats, x_axis)
    lines = []
    h1 = ax.errorbar(x_axis_vals,insert_rtt,yerr=insert_err, linestyle=op_linestyles['insert'], marker=op_markers['insert'], label=label+"-insert-p"+str(percentile))
    lines.append(h1)
    if twin:
        if axt is None:
            print("if using twin, must provide axt  ")
        h2 = axt.errorbar(x_axis_vals,read_rtt,yerr=read_err, linestyle=op_linestyles['read'], label=label+"-read-p"+str(percentile), marker=op_markers['read'])
        lines.append(h2)
    return lines

def rtt_per_operation(ax, stats, x_axis="clients", twin=True):
    if twin:
        axt=ax.twinx()
    stats = correct_stat_shape(stats)
    lines = []
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        l = rtt_per_operation_line(ax, axt, stat, label=state_machine_label, x_axis=x_axis, twin=twin)
        lines.extend(l)
    rtt_per_operation_decoration(ax, axt, x_axis, lines)

def get_run_bandwidth(stat):
    total_bytes = 0
    for client in stat["clients"]:
        client_total_bytes = int(client["stats"]["total_bytes"])
        total_bytes += client_total_bytes
    runtime = float(stat["system"]["runtime_s"])
    gigabytes = float(total_bytes)/float(1024*1024*1024)
    gigabits = gigabytes*8
    return float(gigabits)/float(runtime)

    
def get_bandwith_trials(stats):
    bandwidths = []
    bandwidths_error = []
    for stat in stats:
        run_bandwidths = []
        for run in stat:
            bandwidth = get_run_bandwidth(run)
            run_bandwidths.append(bandwidth)
        run_average = np.mean(run_bandwidths)
        run_std = np.std(run_bandwidths)
        bandwidths.append(run_average)
        bandwidths_error.append(run_std)
    return bandwidths, bandwidths_error

def bandwidth_line(ax, stats, label, x_axis="clients"):
    bandwidths, bandwidths_error = get_bandwith_trials(stats)
    x_axis_vals = get_x_axis(stats, x_axis)
    h = ax.errorbar(x_axis_vals, bandwidths, yerr=bandwidths_error, label=label, linestyle=op_linestyles['insert'], marker=op_markers['insert'])
    return h

def bandwidth_decoration(ax, x_axis):
    ax.set_ylabel("bandwidth (GBPS)")
    ax.set_xlabel(x_axis)
    ax.set_ylim(bottom=0)
    ax.legend()

def bandwidth(ax, stats, x_axis="clients"):
    print("bandwidth")
    state_machine_label = stats[0][0]['config']['state_machine']
    bandwidth_line(ax, stats, state_machine_label, x_axis)
    bandwidth_decoration(ax, x_axis)



def fill_factor_line(ax, stats, label, x_axis="table size"):
    fr_avg, fr_err = [], []
    x_axis_vals = get_x_axis(stats, x_axis)
    for stat in stats:
        fill_rates = []
        for r in stat:
            fill_rates.append(float(r['memory']['fill']))
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

def clean_stats(stats):
    i=0
    while i < len(stats):
        if len(stats[i]) == 0:
            stats.pop(i)
        else:
            i+=1
    return stats

def correct_stat_shape(stats):
    dim = np.shape(stats)
    print("dim", dim, "len", len(dim))
    str_dim = str(dim)
    #count the commas in the string
    commas = str_dim.count(',')
    commas += 1
    if commas == 1:
        stats = [[stats]]
    elif commas == 2:
        stats = [stats]
    elif commas == 3:
        pass
    return stats


def fill_factor(ax, stats, x_axis="bucket size"):
    print("fill factor x-axis: ", x_axis)
    stats = correct_stat_shape(stats)
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        fill_factor_line(ax, stat, label=state_machine_label, x_axis=x_axis)
    fill_factor_decoration(ax, x_axis)

def single_run_approx_throughput(stat):
    # per_client_steps = []
    # approx_throughput =  stat['config']['num_clients'] / (stat['simulator']['steps']/3)
    # approx_throughput = sim_steps/
    # memory_steps=stat['memory']['steps']
    # print(stat)
    normal_throughputs = []
    for client in stat['clients']:
        read_rtt = client['stats']['read_rtt_count']
        insert_rtt = client['stats']['insert_rtt_count']
        read_operations = client['stats']['completed_read_count']
        insert_operations = client['stats']['completed_insert_count']

        total_operations = read_operations + insert_operations
        total_rtt = read_rtt + insert_rtt

        normal_throughput = div_by_zero_to_zero(total_operations, total_rtt)
        normal_throughputs.append(normal_throughput)
    num_clients = int(stat['config']['num_clients'])
    mean, std = np.mean(normal_throughputs), np.std(normal_throughputs)
    mean = mean *num_clients
    std = std *num_clients
    return mean, std
    # return (np.mean(per_client_success_rate), stderr(per_client_success_rate))

def approximate_throughput_line(ax,stats,label,x_axis="clients"):
    throughputs = []
    std_errs = []
    x_axis_vals = get_x_axis(stats, x_axis)
    for stat in stats:
        single_run_throughput=[]
        single_run_errors=[]
        for r in stat:
            s, e = single_run_approx_throughput(r)
            single_run_throughput.append(s)
            single_run_errors.append(e)
        throughputs.append(np.mean(single_run_throughput))
        std_errs.append(np.mean(single_run_errors))
    # x_pos = np.arange(len(success_rates))
    ax.errorbar(x_axis_vals,throughputs,yerr=std_errs,label=label, marker='o', capsize=3)



def approximate_throughput_decoration(ax, x_axis):
    ax.set_xlabel(x_axis)
    ax.set_ylabel('Approx Throughput')
    ax.set_title('Approximate Throughput vs ' + x_axis)
    ax.legend()

def throughput_approximation(ax, stats, x_axis='clients', decoration=True):
    stats = correct_stat_shape(stats)
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        approximate_throughput_line(ax, stat, label=state_machine_label, x_axis=x_axis)
    if decoration:
        approximate_throughput_decoration(ax, x_axis)


def single_run_throughput(stat):
    # if isinstance(stat, list):
    #     tputs = []
    #     for s in stat:
    #         tputs.append(single_run_throughput(s))
    #     return np.mean(tputs)
    total_operations_mop = 0
    try:
        for client in stat['clients']:
            read_operations = int(client['stats']['completed_read_count'])
            # print("read_operations: ", read_operations)
            read_operations_mop =read_operations/1000000
            insert_operations = int(client['stats']['completed_insert_count'])
            # print("insert_operations: ", insert_operations)
            insert_operations_mop = insert_operations/1000000

            update_operations = int(client['stats']['completed_update_count'])
            # print("update_operations: ", update_operations)
            update_operations_mop = update_operations/1000000
            total_operations_mop += read_operations_mop + insert_operations_mop + update_operations_mop
            # print("total_operations_mop: ", total_operations_mop)
    except:
        print("error in stat: ", stat)
        exit(1)

    execution_time_ms = int(stat['system']['runtime_ms'])
    execution_time_s = execution_time_ms / 1000.0
    throughput = div_by_zero_to_zero(total_operations_mop, execution_time_s)
    return throughput

def throughput_line(ax,stats,label,x_axis="clients"):
    throughputs = []
    std_errs = []
    # print(json.dumps(stats, indent=4))
    # exit(0)
    x_axis_vals = get_x_axis(stats, x_axis)
    for stat in stats:
        single_run_throughputs=[]
        for r in stat:
            s = single_run_throughput(r)
            single_run_throughputs.append(s)
        print("single_run_throughput: ", single_run_throughputs)
        throughputs.append(np.mean(single_run_throughputs))
        std_errs.append(np.std(single_run_throughputs))
    # x_pos = np.arange(len(success_rates))
    print("throughputs: ", throughputs)
    print("table size: ", x_axis_vals)
    print("tput errs  : ", std_errs)
    ax.errorbar(x_axis_vals,throughputs,yerr=std_errs,label=label, marker='o', capsize=3)

def throughput_line_trials(ax,stats,label,x_axis="clients"):
    throughputs = []
    std_errs = []
    # print(json.dumps(stats, indent=4))
    # exit(0)
    # x_axis_vals = get_x_axis(stats, x_axis)
    print(len(stats))
    for stat in stats:
        for trial in stat:
            single_run_throughputs=[]
            for run in trial:
                s = single_run_throughput(run)
                single_run_throughputs.append(s)
            print("single_run_throughput: ", single_run_throughputs)
            throughputs.append(np.mean(single_run_throughputs))
            std_errs.append(np.std(single_run_throughputs))
        print(throughputs)


def throughput_decoration(ax, x_axis):
    ax.set_xlabel(x_axis)
    ax.set_ylabel('Throughput MOPS')
    ax.set_title('Throughput vs ' + x_axis)
    ax.legend()

def throughput(ax, stats, x_axis='clients', decoration=True, label=None):
    stats = clean_stats(stats)
    stats = correct_stat_shape(stats)
    for stat in stats:
        if label is None:
            label = stat[0][0]['config']['state_machine']
            # label = "cuckoo (double check though)"
        # throughput_line(ax, stat[0], label=label, x_axis=x_axis)
        throughput_line(ax, stat, label=label, x_axis=x_axis)
        # throughput_line_trials(ax, stat, label=label, x_axis=x_axis)
    if decoration:
        throughput_decoration(ax, x_axis)

    


def fill_vs_latency_line(ax, stats, label, x_axis="max fill"):
    print("RTT PER OPERATION")
    # read_rtt, read_err = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'read_rtt_count', 'completed_read_count')
    # insert_rtt, insert_err = client_stats_x_per_y_get_mean_std_multi_run_trials(stats, 'insert_rtt_count', 'completed_insert_count')
    # percentile = 50
    # read_rtt, read_err = client_stats_get_percentile_err_trials(stats, 'read_rtt', percentile)
    # insert_rtt, insert_err = client_stats_get_percentile_err_trials(stats, 'insert_rtt', percentile)
    read_rtt, read_err = client_stats_get_mean_err_trials(stats, 'read_rtt')
    insert_rtt, insert_err = client_stats_get_mean_err_trials(stats, 'insert_rtt')

    # average_latency = [a+b/2 for a,b in zip(read_rtt,insert_rtt)] 
    # average_err = [a+b/2 for a,b in zip(read_err,insert_err)] 
    x_axis_vals = get_x_axis(stats, x_axis)

    color=None
    if insert_rtt[0] != 0:
        h1 = ax.errorbar(x_axis_vals,insert_rtt,yerr=insert_err, linestyle=op_linestyles['insert'], marker=op_markers['insert'], label=label+"-insert")
        color = h1[0].get_color()
    if read_rtt[0] != 0:
        h2 = ax.errorbar(x_axis_vals,read_rtt,yerr=read_err, linestyle=op_linestyles['read'], label=label+"-read", marker=op_markers['read'], color=color)

    # ax.errorbar(x_axis_vals,average_latency,yerr=average_err, marker="o", label=label)


def fill_vs_latency_decoration(ax, x_axis):
    ax.set_xlabel(x_axis)
    ax.set_ylabel('RTT')
    ax.set_title('RTT vs ' + x_axis)
    ax.legend()

def fill_vs_latency(ax, stats, x_axis='max fill', decoration=True):
    stats = correct_stat_shape(stats)
    for stat in stats:
        state_machine_label = stat[0][0]['config']['state_machine']
        fill_vs_latency_line(ax, stat, label=state_machine_label, x_axis=x_axis)
    if decoration:
        fill_vs_latency_decoration(ax, x_axis)

def single_run_op_success_rate(stat, op_string):
    op_percent = []
    for client in stat['clients']:
        ops = client['stats']['completed_'+op_string+'_count']
        op_failures = client['stats']['failed_'+op_string+'_count']

        ops=int(ops)
        op_failures=int(op_failures)

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
        # print("vals: ", vals)
        mean, std = np.mean(vals), stderr(vals)
        print("mean: ", mean, "std: ", std , x,  "/", y)
        return mean, std
        # print("90th percentile: ", np.percentile(vals,99))
        # return np.percentile(vals,99), stderr(vals)


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

def client_stats_get_percentile_err(stat, key, percentile):
    vals = []
    # print("STAT: ", stat)
    # print("KEY: ", key)

    for client in stat['clients']:
        value = client['stats'][key]
        try:
            # print("VALUE: ", value)
            # value = int(value)
            if isinstance(value, list):
                vals.extend(value)
            elif isinstance(value, str):
                # print("value is a string", value)
                val_list = value.split(",")
                # print("val_list: ", val_list)
                if val_list[0] == '':
                    continue
                val_list = [int(x) for x in val_list]
                vals.extend(val_list)
            else:
                continue

        except:
            exit(0)
            continue
    if len(vals) == 0:
        return 0, 0
    # print("PERCENTILE: ",np.percentile(vals, percentile))
    return np.percentile(vals, percentile), stderr(vals)

def client_stats_get_percentile_err_trials(stats, key, percentile):
    means, stds = [], []
    for stat in stats:
        r_means, r_stds = [], []
        for run in stat:
            m, s = (client_stats_get_percentile_err(run, key, percentile))
            r_means.append(m)
            r_stds.append(s)
        # print("MEANS!!!: ", r_means)
        means.append(np.mean(r_means))
        stds.append(np.mean(r_stds))
    return means, stds

def client_stats_get_mean_err(stat, key):
    vals = []
    for client in stat['clients']:
        vals.extend(client['stats'][key])
    if len(vals) == 0:
        return 0, 0
    print("MEAN: ",np.mean(vals))
    return np.mean(vals), stderr(vals)

def client_stats_get_mean_err_trials(stats, key):
    means, stds = [], []
    for stat in stats:
        r_means, r_stds = [], []
        for run in stat:
            m, s = (client_stats_get_mean_err(run, key))
            r_means.append(m)
            r_stds.append(s)
        print("MEANS!!!: ", r_means)
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
        "hash factor"
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
    elif name == "hash factor":
        return get_hash_factor_x_axis(stats)
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
        try:
            axis.append(stat['config'][name])
        except:
            try:
                axis.append(stat[0]['config'][name])
            except:
                print("error getting config axis")
                print("name: ", name)
                # print("stat: ", stat)
                # exit(1)
    print("axis: ", axis)
    return axis


def get_client_x_axis(stats):
    return get_config_axis(stats,'num_clients')

def get_read_threshold_x_axis(stats):
    return get_config_axis(stats,'read_threshold_bytes')

def get_table_size_x_axis(stats):
    axis = get_config_axis(stats,'indexes')
    return [int(x) for x in axis]

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

def get_hash_factor_x_axis(stats):
    return get_config_axis(stats,'hash_factor')

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
            w = c['stats']['workload']
            # w = c['stats']['workload_stats']['workload']
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

def search_function(stats):
    return ("search function", get_config_list(stats, "search_function"))

def is_deterministic(stats):
    return ("deterministic", get_config_list(stats, "deterministic"))


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
        max_fill,
        search_function,
        is_deterministic,
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
