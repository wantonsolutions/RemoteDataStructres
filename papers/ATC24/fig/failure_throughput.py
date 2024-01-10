import lib

import experiments.plot_cuckoo as plot_cuckoo
# import run_experiments as re
import experiments.data_management as dm
import experiments.orchestrator as orchestrator
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


def plot_failure_throughput_static():
    fig, ax = plt.subplots(1,1, figsize=(4,2.5))

    x_axis=[1,2,4,8,16,32,64,128,256,512,1024]
    one_row_per_lock=[30 - x/100 for x in x_axis]
    sixteen_rows_per_lock=[30 - x/50 for x in x_axis]
    x_axis=[str(x) for x in x_axis]
    x_axis[len(x_axis)-1] = "1K"
    ax.plot(x_axis, one_row_per_lock, label="1 row per lock", marker="s")
    ax.plot(x_axis, sixteen_rows_per_lock, label="16 rows per lock", marker="o")
    ax.legend()
    ax.set_ylim(0,33)
    ax.set_xlabel("Failures per second (# locks)")
    ax.set_ylabel("MOPS")

    ax.text(2,15, "FAKE DATA", fontsize="xx-large", color="red")
    plt.tight_layout()
    plt.savefig("failure_throughput.pdf")


# search_success_lock_size()
# plot_search_success_lock_size()
plot_failure_throughput_static()