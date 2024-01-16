import lib

import experiments.plot_cuckoo as plot_cuckoo
import simulator.log as log
import simulator.simulation_runtime as sim
# import run_experiments as re
import experiments.data_management as dm
import simulator.cuckoo as cuckoo
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


data_dir = "insertion_span"

def run_insertion_range_protocol_cdf():
    table_size = 1024 * 512
    configs = [ 
                ("a_star", "dependent"),
                ("a_star", "independent"),
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
        config['state_machine'] = cuckoo.rcuckoo

        log.set_off()
        runs.append(sim.run_trials(config))
    dm.save_statistics(runs,dirname=data_dir)



def plot_insertion_range_protocol_cdf():
    import matplotlib.ticker as mticker
    stats = dm.load_statistics(data_dir)
    fig, ax1 = plt.subplots(1,1, figsize=(4,2.5))
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
        if dependent == "dependent":
            ax1.plot(x,y, label=str(dependent), linewidth=2, color=lib.rcuckoo_color)
        elif dependent == "independent":
            ax1.plot(x,y, label=str(dependent), linewidth=2, color=lib.sherman_color)
        
        # ax1.plot(x,y, label=str(dependent), linewidth=2)
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

    
    ax1.set_xscale('log', base=2)
    # ax1.xaxis.set_minor_formatter(mticker.ScalarFormatter())
    if not buckets:
        ax1.set_xlabel('Insertion Span (bytes)')
    else:
        ax1.set_xlabel('Insertion Span (rows)')
    # ax1.set_title('Insertion Range CDF')
    ax1.legend(prop={'size': 8})
    ax1.set_ylim(0,1.05)
    ax1.set_ylabel('CDF')
    plt.grid()


    plt.tight_layout()
    plt.savefig("insertion_span.pdf")

# run_insertion_range_protocol_cdf()
plot_insertion_range_protocol_cdf()