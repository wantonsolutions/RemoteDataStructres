import simulator
import json
import log
import logging
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm

import plot_cuckoo


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

def save_statistics(statistics, filename="latest_statistics.json"):
    print(statistics)
    stats = json.dumps(statistics, indent=4)
    with open(filename, "w") as f:
        f.write(stats)

def load_statistics(filename="latest_statistics.json"):
    with open(filename, "r") as f:
        stats = json.load(f)
    return stats

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
        config = simulator.default_config()
        sim = simulator.Simulator(config)
        print("table size: ", table_size)
        config['indexes'] = table_size
        sim = simulator.Simulator(config)
        log.set_off()
        sim.run()
        stats = sim.collect_stats()
        runs.append(stats)

    save_statistics(runs)


def plot_table_size_experiment():
    runs = load_statistics()
    plot_fills(runs)

def factor_table_size_experiments():
    table_sizes = [1024, 2048, 4096, 8192, 16384, 32768, 65536]
    factors = [2.5, 2.6, 2.7, 2.8, 2.9, 3.0]

    factor_runs = []
    for f in factors:
        runs=[]
        for t in table_sizes:
            config = simulator.default_config()
            config['indexes'] = t
            config['hash_factor'] = f
            sim = simulator.Simulator(config)
            log.set_off()
            sim.run()
            stats = sim.collect_stats()
            runs.append(stats)
        factor_runs.append(runs)
    save_statistics(factor_runs)

def plot_factor_table_size_experiments():
    factor_runs = load_statistics()

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

def plot_insertion_range_cdf():
    table_size = 1024
    config = simulator.default_config()
    sim = simulator.Simulator(config)
    config['indexes'] = table_size
    sim = simulator.Simulator(config)
    log.set_off()
    sim.run()
    stats = sim.collect_stats()
    c0_stats = stats['clients'][0]
    print(c0_stats)
    ranges = c0_stats['stats']['index_range_per_insert']
    x, y = cdf(ranges)
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
    import hash

    fig, ax = plt.subplots()
    ax.set_xlabel('Hash Value')
    ax.set_ylabel('Count')
    ax.set_title('Hash Value Distribution')

    max_value = 1000
    bins=128
    primary = []
    secondary = []
    for i in range(max_value):
        v1, v2 = hash.hash_locations(i, bins)
        primary.append(v1)
        secondary.append(v2)

    ax.hist(primary, bins=bins)
    ax.hist(secondary, bins=bins)
    plt.savefig("hash_distribution.pdf")


def plot_hash_factor_distance_cdf():
    import hash
    factors = [1.8, 1.9, 2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0]
    samples = 10000
    table_size = 512
    fig, ax = plt.subplots()
    ax.set_xlabel('Read Size (Bytes)')
    ax.set_ylabel('CDF')
    ax.set_title('Hash Factor Distance CDF')
    for f in factors:
        print("factor: " + str(f))
        hash.set_factor(f)
        distances = []
        for i in range(samples):
            v1, v2 = hash.hash_locations(i, table_size)
            distances.append(8 + (abs(v1-v2) * 8))
        x, y = cdf(distances)
        ax.plot(x,y, label=str(f))
    ax.set_xlim(0,1500)
    ax.legend()
    plt.tight_layout()
    plt.savefig("hash_factor_distance_cdf.pdf")


def todos():
    print("Write a function to CAS a single lock in the lock table. The CAS operation here is just for a global lock")
    print("Write a multi cas function to grab multiple locks in the lock table")
    print("Implement A lock aquisition function that grabs a few locks at a time.")
    print("move the table and a* search to a shared file so I can do millions of inserts on the table just like the prior tests")

def global_lock_success_rate():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator for global locking")
    table_size = 1024
    # client_counts = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
    # client_counts = [1, 2, 4, 8, 16, 32]
    client_counts = [1, 2, 4]


    runs=[]
    for client_count in client_counts:
        print("client count: ", client_count)
        config = simulator.default_config()
        sim = simulator.Simulator(config)
        # print("table size: ", table_size)
        config['indexes'] = table_size
        config['num_clients'] = client_count
        config["num_steps"] = 100000
        #global lock config
        config['buckets_per_lock'] = config['indexes'] / config["bucket_size"]
        sim = simulator.Simulator(config)
        log.set_off()
        sim.run()
        stats = sim.collect_stats()
        runs.append(stats)
    save_statistics(runs)

def plot_general_stats_last_run():
    stats = load_statistics()
    plot_names = [
        "general_stats",
        "cas_success_rate",
        "read_write_ratio",
        "bytes_per_operation",
        "messages_per_operation",
        "fill_factor"
        ]
    plot_cuckoo.multi_plot_runs(stats, plot_names)

def plot_global_lock_success_rate():
    stats = load_statistics()
    print(stats)
    success_rates = []
    std_errs = []
    clients = []
    for stat in stats:
        per_client_success_rate = []
        for client in stat['clients']:
            print(client)
            total_cas = client['stats']['total_cas']
            total_cas_failure = client['stats']['total_cas_failures']
            success_rate = float(1.0 - float(total_cas_failure)/float(total_cas))
            print(total_cas,total_cas_failure,success_rate)
            per_client_success_rate.append(success_rate)
        mean =np.mean(per_client_success_rate)
        std_err = np.std(per_client_success_rate) / np.sqrt(np.size(per_client_success_rate))
        success_rates.append(mean)
        std_errs.append(std_err)
        # print(len(stat['clients']))
        clients.append(str(len(stat['clients'])))


    x_pos = np.arange(len(success_rates))
    fig, ax = plt.subplots()
    ax.bar(x_pos,success_rates,yerr=std_errs,align="center", edgecolor='black')
    ax.set_ylabel("success rate")
    ax.set_xlabel("# clients")
    ax.set_xticks(x_pos)
    ax.set_xticklabels(clients)
    # ax.set_yscale('log')
    ax.set_title("Success Rate for global lock aquires")
    plt.tight_layout()
    plt.savefig("global_lock_aquire.pdf")


def insertion_debug():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 256
    clients=8
    runs=[]
    config = simulator.default_config()
    sim = simulator.Simulator(config)
    print("table size: ", table_size)
    config['indexes'] = table_size
    config['num_clients'] = clients
    config['num_steps'] = 100000
    config['read_threshold_bytes'] = 256

    #global lock config
    config['buckets_per_lock'] = config['indexes'] / config["bucket_size"]
    sim = simulator.Simulator(config)
    # log.set_debug()
    # log.set_off()
    try:
        sim.run()
    except Exception as e:
        print(e)
        stats = sim.collect_stats()
        sim.validate_run()
    sim.validate_run()
    stats = sim.collect_stats()
    runs.append(stats)
    save_statistics(runs)

def read_threshold_experiment():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 128
    clients=8
    runs=[]
    read_thresholds=[32, 64, 128, 256, 512, 1024]
    for read_threshold in read_thresholds:
        print("read threshold: ", read_threshold)
        config = simulator.default_config()
        sim = simulator.Simulator(config)
        config['indexes'] = table_size
        config['num_clients'] = clients
        config['num_steps'] = 100000
        config['read_threshold_bytes'] = read_threshold
        sim = simulator.Simulator(config)
        # log.set_off()
        try:
            sim.run()
        except Exception as e:
            print(e)
            stats = sim.collect_stats()
            sim.validate_run()
        sim.validate_run()
        stats = sim.collect_stats()
        runs.append(stats)
    save_statistics(runs)

def plot_read_threshold_experiment():
    stats = load_statistics()
    fig, ax = plt.subplots()
    plot_cuckoo.messages_per_operation(ax, stats, "read threshold bytes")
    ax.set_title("Messages per operation for different read thresholds")
    plt.tight_layout()
    plt.savefig("messages_per_operation_read_threshold.pdf")




    
global_lock_success_rate()
# plot_global_lock_success_rate()

# todos()

# insertion_debug()
# plot_general_stats_last_run()

# read_threshold_experiment()
plot_general_stats_last_run()
# plot_read_threshold_experiment()

# factor_table_size_experiments()
# plot_factor_table_size_experiments()


# table_size_experiment()
# plot_table_size_experiment()

# plot_hash_distribution()
# plot_insertion_range_cdf()
# plot_hash_factor_distance_cdf()


