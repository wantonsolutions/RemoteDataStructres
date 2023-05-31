import simulator.simulation_runtime as sim
import simulator.cuckoo
import simulator.race as race
import simulator.log as log

import simulator.rcuckoo_basic as rcuckoo_basic
import simulator.cuckoo as cuckoo
import simulator.race as race

import matplotlib.pyplot as plt
import matplotlib as matplotlib
import numpy as np
from tqdm import tqdm
import data_management as dm
import plot_cuckoo as plot_cuckoo

import argparse
parser = argparse.ArgumentParser(description='Experimental Parameters')
parser.add_argument('-n', '--exp_name',    type=str, default="", help='name of experiment. will set output file names')
parser.add_argument('-d', '--description', type=str, default="", help='description of experiment will be embedded in general output')



def get_config():
    args = parser.parse_args()
    print(args)
    config = sim.default_config()
    config['description'] = args.description
    config['name'] = args.exp_name
    return config


def plot_fills(runs):

    fig, ax = plt.subplots()
    ax.set_xlabel('Table Size')
    ax.set_ylabel('Fill Rate')
    ax.set_title('Fill Rate vs Table Size')

    table_sizes = []
    fill_rates = []
    for run in runs:
        print(run)
        print(type(run))
        c = run["config"]
        table_sizes.append(c['indexes'])
        fill_rates.append(run['memory']['fill'])

    ax.plot(table_sizes, fill_rates)
    plt.savefig("fill_rate.pdf")


def pow_2_table_sizes(count):
    table_sizes = []
    i=5
    while i < count:
        table_sizes.append(2**i)
        i+=1
    return table_sizes


def table_size_experiment():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_sizes = pow_2_table_sizes(10)
    print("largest table size: ", table_sizes[-1])

    runs = []
    for table_size in table_sizes:
        config = get_config()
        s = sim.Simulator(config)
        print("table size: ", table_size)
        config['indexes'] = table_size
        s = sim.Simulator(config)
        log.set_off()
        s.run()
        stats = s.collect_stats()
        runs.append(stats)

    dm.save_statistics(runs)


def plot_table_size_experiment():
    runs = dm.load_statistics()
    plot_fills(runs)

def factor_table_size_experiments():
    table_sizes = [1024, 2048, 4096, 8192, 16384, 32768, 65536]
    factors = [2.5, 2.6, 2.7, 2.8, 2.9, 3.0]

    factor_runs = []
    for f in factors:
        runs=[]
        for t in table_sizes:
            config = get_config()
            config['indexes'] = t
            config['hash_factor'] = f
            log.set_off()
            stats = sim.run_trials(config)
            runs.append(stats)
        factor_runs.append(runs)
    dm.save_statistics(factor_runs)

def plot_factor_table_size_experiments():
    factor_runs = dm.load_statistics()

    fig, ax = plt.subplots()
    ax.set_xlabel('Table Size')
    ax.set_ylabel('Fill Rate')
    ax.set_title('Fill Rate vs Table Size')

    for factor in factor_runs:
        table_sizes = []
        fill_rates = []
        label = factor[0]['hash']['factor']
        for run in factor:
            print(run)
            print(type(run))
            c = run["config"]
            table_sizes.append(c['indexes'])
            fill_rates.append(run['memory']['fill'])

        ax.plot(table_sizes, fill_rates, label=str(label))
    ax.legend()
    plt.savefig("factor_fill_rate.pdf")



def plot_insertion_range_cdf():
    table_size = 1024
    config = get_config()
    config['indexes'] = table_size
    log.set_off()
    stats = sim.run_trials(config)
    c0_stats = stats[0]['clients'][0]
    print(c0_stats)
    ranges = c0_stats['stats']['index_range_per_insert']
    x, y = plot_cuckoo.cdf(ranges)
    fig, (ax1, ax2) = plt.subplots(2,1)

    ax1.plot(x,y)
    ax1.set_xlabel('Insertion Range')
    ax1.set_title('Insertion Range CDF')

    nf=0
    nn=0
    for j in y:
        if j < 0.95:
            nf+=1
        if j < 0.99:
            nn+=1

    ax1.vlines(x[nf], 0, 1, color='red', linestyle='--', label="95%")
    ax1.vlines(x[nn], 0, 1, color='black', linestyle='--', label="99%")
    ax1.legend()
    

    ax2.plot(ranges)
    ax2.set_xlabel("Insertion number")
    ax2.set_ylabel("insertion range")
    plt.tight_layout()
    plt.savefig("insertion_range_cdf.pdf")




def plot_hash_distribution():
    import simulator.hash as hash

    fig, ax = plt.subplots()
    ax.set_xlabel('Hash Value')
    ax.set_ylabel('Count')
    ax.set_title('Hash Value Distribution')

    max_value = 1000
    bins=128
    primary = []
    secondary = []
    for i in range(max_value):
        v1, v2 = hash.rcuckoo_hash_locations(i, bins)
        primary.append(v1)
        secondary.append(v2)

    ax.hist(primary, bins=bins)
    ax.hist(secondary, bins=bins)
    plt.savefig("hash_distribution.pdf")


def plot_hash_factor_distance_cdf():
    import chash as hash
    factors = [1.8, 1.9, 2.0, 2.1, 2.2, 2.3]
    samples = 1000
    table_size = 512
    bucket_size = 8
    fig, ax = plt.subplots()
    ax.set_xlabel('Read Size (Bytes)')
    ax.set_ylabel('CDF')
    ax.set_title('Hash Factor Distance CDF')
    for f in factors:
        print("factor: " + str(f))
        hash.set_factor(f)
        distances = []
        for i in range(samples):
            v1, v2 = hash.rcuckoo_hash_locations(i, table_size)
            distances.append(hash.distance_to_bytes(v1, v2, bucket_size, 8))
        x, y = plot_cuckoo.cdf(distances)
        ax.plot(x,y, label=str(f))
    ax.set_xlim(0,4096)
    ax.legend()
    plt.tight_layout()
    plt.savefig("hash_factor_distance_cdf.pdf")


def todos():
    print("Write a function to CAS a single lock in the lock table. The CAS operation here is just for a global lock")
    print("Write a multi cas function to grab multiple locks in the lock table")
    print("Implement A lock aquisition function that grabs a few locks at a time.")
    print("move the table and a* search to a shared file so I can do millions of inserts on the table just like the prior tests")

def plot_general_stats_last_run(dirname=""):
    stats, directory = dm.load_statistics(dirname=dirname)
    print("plot general stats")
    plot_names = [
        "general_stats",
        "cas_success_rate",
        "read_write_ratio",
        "request_success_rate",
        "rtt_per_operation",
        "bytes_per_operation",
        "messages_per_operation",
        "fill_factor",
        "throughput_approximation"
        ]
    plot_cuckoo.multi_plot_runs(stats, plot_names, directory)



def plot_read_threshold_experiment():
    stats = dm.load_statistics()
    fig, ax = plt.subplots()
    plot_cuckoo.messages_per_operation(ax, stats, "read threshold bytes")
    ax.set_title("Messages per operation for different read thresholds")
    plt.tight_layout()
    plt.savefig("messages_per_operation_read_threshold.pdf")

def insertion_debug():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 4096
    runs = []
    print("table size: ", table_size)

    config = get_config()
    config['indexes'] = table_size
    config['num_clients'] = 8
    config['bucket_size'] = 8
    config['num_steps'] = 5000000
    config['read_threshold_bytes'] = 256
    config["buckets_per_lock"] = 1
    config["locks_per_message"] = 64
    config["trials"] = 1
    
    config['max_fill']= 50
    # config['deterministic']=True
    # config["state_machine"]=race.race
    config["state_machine"]=cuckoo.rcuckoo
    config['workload']='ycsb-w'
    # log.set_debug()
    log.set_off()

    runs.append(sim.run_trials(config))

    dm.save_statistics(runs)



def avg_run_debug():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 20 * 128
    runs=[]
    bucket_sizes = [4,5]
    trials = 8
    for bucket_size in bucket_sizes:
        config = sim.default_config()
        config['num_clients'] = 1
        config['num_steps'] = 10000000000
        config['bucket_size'] = bucket_size
        config['read_threshold_bytes'] = config['entry_size'] * bucket_size
        config['indexes'] = table_size
        config["state_machine"]=race.race
        log.set_off()
        runs.append(sim.run_trials(config, trials))
        
    dm.save_statistics(runs)




def locks_per_message_experiment():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 2048
    clients=8
    runs=[]
    read_threshold=128
    locks_per_message_arr=[1, 2, 4, 8, 16, 32, 64]
    # locks_per_message_arr=[1, 2]
    for locks_per_message in locks_per_message_arr:
        print("read threshold: ", read_threshold)
        config = simulator.default_config()
        config['indexes'] = table_size
        config['num_clients'] = clients
        config['num_steps'] = 1000000
        config['read_threshold_bytes'] = read_threshold

        config["buckets_per_lock"] = 1
        config["locks_per_message"] = locks_per_message
        log.set_off()
        runs.append(simulator.run_trials(config))
    dm.save_statistics(runs)



def read_threshold_experiment():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 2048
    clients=8
    runs=[]
    read_thresholds=[32, 64, 128, 256, 512, 1024, 2048, 4096]
    for read_threshold in read_thresholds:
        print("read threshold: ", read_threshold)
        config = simulator.default_config()
        config['indexes'] = table_size
        config['num_clients'] = clients
        config['num_steps'] = 1000000
        config['read_threshold_bytes'] = read_threshold

        config["buckets_per_lock"] = 16
        config["locks_per_message"] = 4
        log.set_off()
        runs.append(simulator.run_trials(config))
    dm.save_statistics(runs)

def basic_contention():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 2048
    clients= [1,2,4,8,16,32]
    runs=[]
    for client_count in clients:
        config = get_config()
        config['indexes'] = table_size
        config['num_clients'] = client_count
        config['num_steps'] = 1000000
        config['read_threshold_bytes'] = 128

        config["buckets_per_lock"] = 16
        config["locks_per_message"] = 4
        config["state_machine"]=cuckoo.rcuckoo
        config["max_fill"]=90
        log.set_off()
        runs.append(sim.run_trials(config))
    dm.save_statistics(runs)


def client_scalability():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 2048
    runs=[]
    state_machines = [rcuckoo_basic.rcuckoo_basic, race.race, cuckoo.rcuckoo]
    for state_machine in state_machines:
        config = get_config()
        config['indexes'] = table_size
        config['num_clients'] = 8
        config['num_steps'] = 1000000
        config['read_threshold_bytes'] = 128

        config["buckets_per_lock"] = 16
        config["locks_per_message"] = 4
        config["state_machine"]=state_machine
        config["max_fill"]=90
        log.set_off()
        runs.append(sim.run_trials(config))
    dm.save_statistics(runs)

def race_bucket_size_fill_factor():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 1680  * 128 #lcm of 3,4,5,6,7,8,10,12,14,16
    runs=[]
    bucket_sizes = [3,4,5,6,7,8,10,12,14,16]
    # bucket_sizes = [3,4,5,6,7]
    # bucket_sizes = [3,4]

    # bucket_sizes = [8]
    log.set_off()
    for bucket_size in bucket_sizes:
        config = get_config()
        config['num_clients'] = 1
        config['num_steps'] = 100000000000
        config['bucket_size'] = bucket_size
        config['read_threshold_bytes'] = config['entry_size'] * bucket_size
        config['indexes'] = table_size
        config['trials'] = 3
        config['state_machine']=race.race
        runs.append(sim.run_trials(config))
    dm.save_statistics(runs)

def race_vs_rcuckoo_fill_factor():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 1680  * 2 #lcm of 3,4,5,6,7,8,10,12,14,16
    # bucket_sizes = [3,4,5,6,7,8,10,12,14,16]
    # bucket_sizes = [3,4,5,6,7]
    bucket_sizes = [3,4]

    # bucket_sizes = [8]
    log.set_off()
    state_machines = [cuckoo.rcuckoo, race.race]
    multi_runs = []
    for state_machine in state_machines:
        runs=[]
        for bucket_size in bucket_sizes:
            config = get_config()
            config['num_clients'] = 1
            config['num_steps'] = 10000000000
            config['bucket_size'] = bucket_size
            config['read_threshold_bytes'] = config['entry_size'] * bucket_size
            config['indexes'] = table_size
            config['trials'] = 1
            config['state_machine']=state_machine
            runs.append(sim.run_trials(config))
        dm.save_statistics(runs)
        plot_general_stats_last_run()
        multi_runs.append(runs)

    plot_cuckoo.single_plot(multi_runs,plot_cuckoo.fill_factor, "race_vs_rcuckoo_fill_factor.pdf")

def success_rate_contention_machines():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")
    multi_runs=[]
    # table_size = 1680  * 4 #lcm of 3,4,5,6,7,8,10,12,14,16
    table_size = 1680  * 1 #lcm of 3,4,5,6,7,8,10,12,14,16
    # clients=[1,2,4,8,16,32]
    clients=[1,2,4,8]
    # clients=[32]
    bucket_size=8
    # state_machines = [cuckoo.rcuckoobatch,sm.rcuckoo,sm.race]
    # state_machines = [sm.race]
    state_machines = [cuckoo.rcuckoo]
    log.set_off()
    for s in state_machines:
        runs=[]
        for c in clients:
            config = get_config()
            config['num_clients'] = c
            config['num_steps'] = 100000000000
            config['bucket_size'] = bucket_size
            config['read_threshold_bytes'] = config['entry_size'] * bucket_size
            config['indexes'] = table_size
            config['trials'] = 1
            config['state_machine']=s
            config['max_fill']= 100
            runs.append(sim.run_trials(config))
        dm.save_statistics(runs)
        plot_general_stats_last_run()
        multi_runs.append(runs)
    dm.save_statistics(multi_runs)
    plot_general_stats_last_run()
    # plot_cuckoo.single_plot(multi_runs,plot_cuckoo.success_rate_multi, "race_vs_rcuckoo_fill_factor.pdf")

def success_rate_contention():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 2048
    # clients=[1,2,4,8]
    # clients=[32,64]
    clients=[1]
    bucket_size=8
    runs=[]
    log.set_off()
    for c in clients:
        config = get_config()
        config['num_clients'] = c
        config['num_steps'] = 100000000000
        config['bucket_size'] = bucket_size
        config['read_threshold_bytes'] = config['entry_size'] * bucket_size
        config['indexes'] = table_size
        config['trials'] = 1
        # config['state_machine']=cuckoo.rcuckoobatch
        config['state_machine']=race.race
        runs.append(sim.run_trials(config))
    dm.save_statistics(runs)


def fill_factor_limit_experiment():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")
    multi_runs=[]
    table_size = 1680  * 4 #lcm of 3,4,5,6,7,8,10,12,14,16
    clients = 8
    bucket_size=8
    fill_factors = [10, 20, 30, 40, 50, 60, 70, 80, 90]
    state_machines = [cuckoo.rcuckoo,race.race]
    # state_machines = [sm.race]
    log.set_off()
    for s in state_machines:
        runs=[]
        for f in fill_factors:
            config = get_config()
            config['num_clients'] = clients
            config['num_steps'] = 100000000000
            config['bucket_size'] = bucket_size
            config['read_threshold_bytes'] = config['entry_size'] * bucket_size
            config['indexes'] = table_size
            config['trials'] = 1
            config['state_machine']=s
            config['max_fill']= f
            runs.append(sim.run_trials(config))
        dm.save_statistics(runs)
        plot_general_stats_last_run()
        multi_runs.append(runs)
    dm.save_statistics(multi_runs)
    plot_general_stats_last_run()

def locks_per_message_experiment():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    runs=[]
    locks_per_message_arr=[1, 2, 4, 8, 16, 32, 64]
    log.set_off()
    # locks_per_message_arr=[1, 2]
    for locks_per_message in locks_per_message_arr:
        config = get_config()
        config["locks_per_message"] = locks_per_message
        config['indexes'] = 4096 * 4
        config['num_clients'] = 1
        config['num_steps'] = 100000000
        config['read_threshold_bytes'] = 128
        config['trials'] = 1
        config["buckets_per_lock"] = 1
        config["state_machine"] = cuckoo.rcuckoo
        config['max_fill']=100
        config['bucket_size']=8
        config['search_function']="a_star"
        runs.append(sim.run_trials(config))
    dm.save_statistics(runs)

def buckets_per_lock_experiment():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 4096
    clients=8
    runs=[]
    read_threshold=128
    # buckets_per_lock_arr=[1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    buckets_per_lock_arr=[1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    # buckets_per_lock_arr=[8]
    locks_per_message=64
    log.set_off()
    # locks_per_message_arr=[1, 2]
    for buckets_per_lock in buckets_per_lock_arr:
        config = get_config()
        config['indexes'] = table_size
        config['num_clients'] = clients
        config['num_steps'] = 1000000
        config['read_threshold_bytes'] = read_threshold

        config['bucket_size']=8
        config["buckets_per_lock"] = buckets_per_lock
        config["locks_per_message"] = locks_per_message
        config["state_machine"] = cuckoo.rcuckoo
        config["max_fill"] = 90
        runs.append(sim.run_trials(config))
    dm.save_statistics(runs)

def buckets_per_lock_vs_locks_per_message_experiment():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 4096
    clients=8
    all_runs=[]
    read_threshold=128
    # buckets_per_lock_arr=[1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    buckets_per_lock_arr=[1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    # buckets_per_lock_arr=[8]
    locks_per_message=[1,2,4,8,16,32,64]
    log.set_off()
    # locks_per_message_arr=[1, 2]
    for lpm in locks_per_message:
        runs=[]
        for buckets_per_lock in buckets_per_lock_arr:
            config = get_config()
            config['indexes'] = table_size
            config['num_clients'] = clients
            config['num_steps'] = 1000000
            config['read_threshold_bytes'] = read_threshold

            config['bucket_size']=8
            config["buckets_per_lock"] = buckets_per_lock
            config["locks_per_message"] = lpm
            config["state_machine"] = cuckoo.rcuckoo
            config["max_fill"] = 90
            runs.append(sim.run_trials(config))
        all_runs.append(runs)
    dm.save_statistics(all_runs)

def plot_buckets_per_lock_vs_locks_per_message_experiment():
    stats = dm.load_statistics()
    # fig, ax = plt.subplots()

    rtt_matrix=[]
    stats = stats[0]
    for stat in stats:
        rtt_row=[]
        for s_full in stat:
            s = s_full[0]
            print(s)
            config=s['config']
            buckets_per_lock=config['buckets_per_lock']
            locks_per_message=config['locks_per_message']

            percentile = 99
            insert_rtt, insert_err = plot_cuckoo.client_stats_get_percentile_err(s, 'insert_rtt', percentile)

            # print("buckets per lock: ", buckets_per_lock, " locks per message: ", locks_per_message, " fill: ", fill)
            rtt_row.append((buckets_per_lock, locks_per_message,insert_rtt))
        rtt_matrix.append(rtt_row)
    print(rtt_matrix)

    x_axis_buckets_per_lock = []
    for i in range(len(rtt_matrix[0])):
        x_axis_buckets_per_lock.append(rtt_matrix[0][i][0])
    print(x_axis_buckets_per_lock)

    y_axis_locks_per_message = []
    for i in range(len(rtt_matrix)):
        y_axis_locks_per_message.append(rtt_matrix[i][0][1])
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
                    cmap="YlGn", cbarlabel="Lock Aquire RTT")
    texts = plot_cuckoo.annotate_heatmap(im, valfmt="{x:.0f}")

    # ax.colorbar()
    # plt.yticks(np.arange(len(y_axis_locks_per_message)), y_axis_locks_per_message)
    # plt.xticks(np.arange(len(x_axis_buckets_per_lock)), x_axis_buckets_per_lock)
    ax.set_xlabel("Buckets per lock")
    ax.set_ylabel("Locks per message")
    fig.tight_layout()
    plt.tight_layout()
    plt.savefig("buckets_per_lock_vs_locks_per_message.pdf")




def run_insertion_range_protocol_cdf():
    table_size = 1024 * 128
    configs = [ 
                ("a_star", "independent"),
                ("a_star", "dependent"),
            ]
    runs = []
    for c in configs:
        config = get_config()
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
    dm.save_statistics(runs)



def plot_insertion_range_protocol_cdf():
    import matplotlib.ticker as mticker
    stats = dm.load_statistics()
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

    fig.tight_layout()
    # plt.tight_layout()
    plt.savefig("insertion_span.pdf")


def plot_race_bucket_fill_factor():
    print("not implemented race bucket fill factor")


def run_hero_ycsb():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator Hero YCSB")
    # table_size = 1680
    table_size = 1680 * 10
    # table_size=420
    clients = [1,2,4,8,16,32,64,128]
    # clients = [2]
    state_machines = [cuckoo.rcuckoo]

    master_config = get_config()
    master_config["bucket_size"]=8
    master_config['num_steps'] = 10000000
    master_config['bucket_size'] = 8
    master_config['read_threshold_bytes'] = 512
    master_config['indexes'] = table_size
    master_config['trials'] = 1
    master_config['max_fill']= 90
    master_config['deterministic'] = False
    # workloads = ["ycsb-a", "ycsb-b","ycsb-c", "ycsb-w"]
    # workloads = ["ycsb-a", "ycsb-b"]
    workloads = ["ycsb-w"]
    # workloads = ["ycsb-a", "ycsb-a"]
    log.set_off()
    # log.set_debug()

    #ycsb-a
    for workload in workloads:
        multi_runs=[]
        for s in state_machines:
            runs=[]
            for c in clients:
                config = master_config.copy()
                config['num_clients'] = c
                config['state_machine']=s
                config['workload']=workload
                runs.append(sim.run_trials(config))
            dm.save_statistics(runs)
            plot_general_stats_last_run()
            multi_runs.append(runs)
        dirname="data/hero-"+workload
        dm.save_statistics(multi_runs, dirname=dirname)
        plot_general_stats_last_run(dirname=dirname)

def plot_hero_ycsb():
    workloads = ["ycsb-a", "ycsb-b", "ycsb-w"]

    fig, axs = plt.subplots(1,len(workloads), figsize=(12,3))
    for i in range(len(workloads)):
        dirname="data/hero-"+workloads[i]
        ax = axs[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        plot_cuckoo.throughput_approximation(ax, stats, decoration=False)
        ax.legend()
        ax.set_xlabel("clients")
        ax.set_title(workloads[i])
        ax.set_ylabel("throughput \n(ops/rtts)*clients")

    plt.tight_layout()
    plt.savefig("hero_ycsb.pdf")




def fill_then_measure():
    print("START HERE TOMORROW!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!, you need to be able to stop and start simulations mid run.")
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")
    multi_runs=[]
    table_size = 1680 # * 4 #lcm of 3,4,5,6,7,8,10,12,14,16
    clients = 8
    bucket_size=8
    fill_factors = [10, 20, 30, 40, 50, 60, 70, 80, 90]
    # fill_factors=[10]
    # state_machines = [cuckoo.rcuckoobatch]
    # state_machines = [sm.race]
    log.set_off()
    runs=[]
    for f in fill_factors:
        config = get_config()
        config['num_clients'] = clients
        config['num_steps'] = 100000000000
        config['bucket_size'] = bucket_size
        config['read_threshold_bytes'] = config['entry_size'] * bucket_size
        config['indexes'] = table_size
        config['locks_per_message'] = 64
        config['buckets_per_lock'] = 1
        config['trials'] = 1
        # config['deterministic']=True
        config['state_machine']=cuckoo.rcuckoo
        config['max_fill']= f
        run = sim.fill_then_run_trials(config, f-10, f)
        runs.append(run)

    dm.save_statistics(runs)
    plot_general_stats_last_run()


def run_hero_ycsb_fill_latency():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")
    # table_size = 1680  * 512
    table_size=1680
    clients = 128
    fills = [10, 20, 30, 40, 50, 60, 70, 80, 90]
    # fills = [10, 20]
    # clients = [100]
    # state_machines = [cuckoo.rcuckoo,race.race]
    state_machines = [rcuckoo_basic.rcuckoo_basic]

    master_config = get_config()
    master_config["bucket_size"]=8
    master_config['num_steps'] = 100000000000
    master_config['bucket_size'] = 8
    master_config['read_threshold_bytes'] = 512
    master_config['locks_per_message'] = 64
    master_config['indexes'] = table_size
    master_config['trials'] = 1
    workloads = ["ycsb-a", "ycsb-b","ycsb-c", "ycsb-w"]
    # workloads = ["ycsb-a", "ycsb-b", "ycsb-w"]
    # workloads = ["ycsb-c"]
    log.set_off()

    #ycsb-a
    for workload in workloads:
        multi_runs=[]
        for s in state_machines:
            runs=[]
            for fill in fills:
                config = master_config.copy()
                config['num_clients'] = clients
                config['state_machine']=s
                config['workload']=workload
                config['max_fill']=fill
                steps = 100000
                r = sim.fill_then_run_trials(config, fill_to=config['max_fill'], max_fill=config['max_fill']+5, max_steps=steps)
                runs.append(r)
            # dm.save_statistics(runs)
            # plot_cuckoo.plot_general_stats_last_run()
            multi_runs.append(runs)
        dirname="data/hero_ycsb_latency/"+workload
        dm.save_statistics(multi_runs, dirname=dirname)
        # plot_cuckoo.plot_general_stats_last_run(dirname=dirname)

def plot_hero_ycsb_fill_latency():
    # workloads = ["ycsb-w", "ycsb-a", "ycsb-b", "ycsb-c"]
    workloads = [ "ycsb-w", "ycsb-a", "ycsb-b", "ycsb-c"]
    # workloads = [ "ycsb-w"]
    # workloads = ["ycsb-c"]

    fig, axs = plt.subplots(1,len(workloads), figsize=(12,3))
    if len(workloads) == 1:
        axs = [axs]
    for i in range(len(workloads)):
        dirname="data/hero_ycsb_latency/"+workloads[i]
        ax = axs[i]
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        # import json
        # print(json.dumps(stats, indent=4))
        # exit(1)
        plot_cuckoo.fill_vs_latency(ax, stats, decoration=False)
        ax.legend()
        # ax.set_xlabel("clients")
        ax.set_title(workloads[i])
        ax.set_ylabel("average rtt")

    plt.tight_layout()
    plt.savefig("hero_ycsb_fill_latency.pdf")

# table_size_experiment()
# plot_table_size_experiment()

# factor_table_size_experiments()
# plot_factor_table_size_experiments()

# run_hero_ycsb_fill_latency()
# plot_hero_ycsb_fill_latency()
# plot_hero_ycsb_throughput()




# fill_then_measure()


# buckets_per_lock_vs_locks_per_message_experiment()
# plot_buckets_per_lock_vs_locks_per_message_experiment()

run_hero_ycsb()
plot_hero_ycsb()
# 

# plot_insertion_range_cdf()
# run_insertion_range_protocol_cdf()
# plot_insertion_range_protocol_cdf()

# locks_per_message_experiment()
# global_lock_success_rate()
# plot_global_lock_success_rate()
# buckets_per_lock_experiment()

# todos()

# insertion_debug()
# plot_general_stats_last_run()
# plot_hash_factor_distance_cdf()

# success_rate_contention_machines()
# success_rate_contention()
# race_bucket_size_fill_factor()
# fill_factor_limit_experiment()
# buckets_per_lock_experiment()
# basic_contention()
# client_scalability()

# read_threshold_experiment()


# race_vs_rcuckoo_fill_factor()
# avg_run_debug()

# plot_read_threshold_experiment()




# plot_hash_distribution()
# plot_hash_factor_distance_cdf()
# plot_insertion_range_cdf()
# plot_hash_factor_distance_cdf()


# plot_general_stats_last_run()
