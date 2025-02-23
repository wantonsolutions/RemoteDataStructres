import sys
import os
import numpy as np
import datetime
import git
import matplotlib as matplotlib
from matplotlib.transforms import Bbox


#definitions of variables.
#https://sashamaps.net/docs/resources/20-colors/
rcuckoo_color='#4363d8'
fusee_color='#e6194B'
clover_color='#3cb44b'
sherman_color='#f58231'

cyan_color='#42d4f4'
teal_color='#469990'
navy_color='#000075'

def get_plot_name_from_filename():
    return os.path.splitext(sys.argv[0])[0]

def full_extent(ax, pad=0.0):
    """Get the full extent of an axes, including axes labels, tick labels, and
    titles."""
    # For text objects, we need to draw the figure first, otherwise the extents
    # are undefined.
    ax.figure.canvas.draw()
    items = ax.get_xticklabels() + ax.get_yticklabels() 
    items += [ax, ax.title, ax.xaxis.label, ax.yaxis.label]
    items += [ax, ax.title]
    bbox = Bbox.union([item.get_window_extent() for item in items])
    return bbox.expanded(1.0 + pad, 1.0 + pad)

def save_figs(plt, fig, axs, names=[], pad=0.0):
    name=get_plot_name_from_filename()
    if len(names) == 0:
        names=['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p']
    if len(axs) > len(names):
        print('too many subplots')
        exit(1)

    for ax in axs:
        # ax.axis('off')
        ax.set_visible(False)

    for i, ax in enumerate(axs):
        # extent = ax.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
        # fig.savefig(name+"-"+subplots[i]+'.pdf', bbox_inches=extent.expanded(1.20, 1.3))
        # ax.axis('on') 
        ax.set_visible(True)
        extent = full_extent(ax, pad).transformed(fig.dpi_scale_trans.inverted())
        fig.savefig(name+"-"+names[i]+'.pdf', bbox_inches=extent)
        ax.set_visible(False)
        # ax.axis('off') 

def mops(data):
    # return [x / 1000000 if x is not None else 0 for x in data]
    return [float(x) if x is not None else 0 for x in data]

def tofloat(data):
    return [float(x) for x in data]

def ops(data):
    return [x * 1000000 for x in data]

def get_config():
    config=dict()
    table_size = 1024 * 1024 * 10
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
    config["description"] = "ATC24 experiment"
    config["name"] = "no name given"
    config["state_machine"] = "cuckoo"
    config['date']=datetime.datetime.now().strftime("%Y-%m-%d")
    config['commit']=git.Repo(search_parent_directories=True).head.object.hexsha
    config['hash_factor']=str(999999999)

    config["bucket_size"] = str(bucket_size)
    config["entry_size"] = str(entry_size)

    table_size = 1024 * 1024 * 10
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)

    config["read_threshold_bytes"] = str(read_threshold_bytes)
    config["buckets_per_lock"] = str(buckets_per_lock)
    config["locks_per_message"] = str(locks_per_message)
    config["deterministic"]="True"
    config["workload"]="ycsb-w"
    config["id"]=str(0)
    config["search_function"]="a_star"
    config["location_function"]="dependent"

    config["virtual_lock_scale_factor"] = 1
    config["use_virtual_lock_table"] = "false"

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

    #RDMA Engine Arguments
    config["server_address"]="192.168.1.12"
    config["base_port"] = "20886"

    #failure config
    config["simulate_failures"]="false"
    config["lock_timeout_us"]="10000"
    config["lease_timeout_us"]="10000"
    config["delay_between_failures_us"]="1000000000"

    config["trials"] = 1
    return config


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
            kw.update(color=textcolors[int(im.norm(data[i, j]) < threshold)])
            text = im.axes.text(j, i, valfmt(data[i, j], None), **kw)
            texts.append(text)

    return texts
