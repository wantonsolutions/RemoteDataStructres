import simulator
import log
import logging


def plot_fills(runs):
    import matplotlib.pyplot as plt
    import numpy as np

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
    #test_hashes()
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")
    # log.set_off()

    table_sizes = pow_2_table_sizes(12)
    # table_sizes = [65536]
    print("largest table size: ", table_sizes[-1])
    # table_sizes = [32, 64, 128]

    runs = []

    for table_size in table_sizes:
        config = simulator.default_config()
        sim = simulator.Simulator(config)
        print("table size: ", table_size)
        config['indexes'] = table_size
        sim = simulator.Simulator(config)
        # log.set_off()
        sim.run()
        stats = sim.collect_stats()
        runs.append(stats)

    save_statistics(runs)

def plot_table_size_experiment():
    runs = load_statistics()
    plot_fills(runs)

table_size_experiment()
plot_table_size_experiment()


