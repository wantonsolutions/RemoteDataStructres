import simulator
import log
import logging
import matplotlib.pyplot as plt
import numpy as np


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

def save_statistics(statistics):
    print(statistics)
    import json
    stats = json.dumps(statistics, indent=4)
    with open("latest_statistics.json", "w") as f:
        f.write(stats)

def load_statistics():
    import json
    with open("latest_statistics.json", "r") as f:
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
    table_sizes = [64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]
    factors = [1.8, 1.9, 2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0]

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
    

# factor_table_size_experiments()
# plot_factor_table_size_experiments()


# table_size_experiment()
# plot_table_size_experiment()

# plot_hash_distribution()

plot_insertion_range_cdf()


