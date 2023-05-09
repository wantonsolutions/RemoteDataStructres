import lib
lib.import_rmem_simulator()

import plot_cuckoo as plot_cuckoo
import log as log
import state_machines as sm
import simulator as simulator
# import run_experiments as re
import data_management as dm
import cuckoo as cuckoo
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


data_dir = "insertion_span"

def run_insertion_range_protocol_cdf():
    table_size = 1024 * 1
    configs = [ 
                ("a_star", "independent"),
                ("a_star", "dependent"),
            ]
    runs = []
    for c in configs:
        config = lib.get_config()
        search = c[0]
        dependent = c[1]
        config['indexes'] = table_size
        config['search_function'] = search
        config['location_function'] = dependent
        config['read_threshold_bytes'] = 128
        config['max_fill'] = 90
        config['bucket_size']=8
        config['num_steps'] = 100000000
        config['state_machine'] = cuckoo.rcuckoobatch

        sim = simulator.Simulator(config)
        log.set_off()
        runs.append(simulator.run_trials(config))
    dm.save_statistics(runs,dirname=data_dir)



def plot_insertion_range_protocol_cdf():
    import matplotlib.ticker as mticker
    stats = dm.load_statistics(data_dir)
    fig, ax1 = plt.subplots(1,1, figsize=(6,3))
    buckets=True
    for stat in stats[0]:
        # exit(0)
        c0_stats = stat[0]['clients'][0]
        config = stat[0]['config']

        ranges = c0_stats['stats']['index_range_per_insert']

        if not buckets:
            bucket_size = config['bucket_size']
            entry_size = config['entry_size']
            bucket_bytes = bucket_size * entry_size
            ranges = [ (r * bucket_bytes) + bucket_bytes for r in ranges]
        else:
            ranges = [ (r + 1) for r in ranges]



        # print(ranges)
        x, y = plot_cuckoo.cdf(ranges)

        search = config['search_function']
        dependent = config['location_function']
        ax1.plot(x,y, label=str(dependent))
        if dependent == "dependent":
            nf=0
            nn=0
            for j in y:
                if j < 0.95:
                    nf+=1
                if j < 0.99:
                    nn+=1

            ax1.vlines(x[nf], 0, 1, color='red', linestyle='--', label="95% dependent")
            ax1.vlines(x[nn], 0, 1, color='black', linestyle='--', label="99% dependent")

    
    ax1.set_xscale('log')
    # ax1.xaxis.set_minor_formatter(mticker.ScalarFormatter())
    if not buckets:
        ax1.set_xlabel('Insertion Span (bytes)')
    else:
        ax1.set_xlabel('Insertion Span (buckets)')
    # ax1.set_title('Insertion Range CDF')
    ax1.legend()


    plt.tight_layout()
    plt.savefig("insertion_span.pdf")

run_insertion_range_protocol_cdf()
plot_insertion_range_protocol_cdf()