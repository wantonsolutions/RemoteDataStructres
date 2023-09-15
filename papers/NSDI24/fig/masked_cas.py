from experiments import plot_cuckoo as plot_cuckoo
from experiments import data_management as dm
import experiments.orchestrator as orchestrator
import numpy as np
import matplotlib.pyplot as plt
import lib

def masked_cas_sensitivity():
    config = lib.get_config()
    use_masked_cas = ["true", "false"]
    table_size = 1024 * 1024 * 1
    entry_size =8
    memory_size = entry_size * table_size
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)

    config["prime"]="true"
    # config["prime_fill"]="88"
    # config["max_fill"]="91"

    clients = 400
    config['num_clients'] = clients
    config['workload'] = "ycsb-w"
    fills = [10,20,30,40,50,60,70,80,90]

    orchestrator.boot(config)
    for use_mask in use_masked_cas:
        runs = []
        for fill in fills:
            lconfig = config.copy()
            lconfig["max_fill"] = str(fill)
            lconfig["prime_fill"] = str(fill-8)
            lconfig["use_mask"] = str(use_mask)
            results = orchestrator.run_trials(lconfig)
            if len(results) > 0:
                runs.append(results)

        directory = "masked_cas/masked_cas_"+use_mask
        dm.save_statistics(runs, dirname=directory)
    


def plot_masked_cas_sensitivity():
    fig, ax = plt.subplots(1,1, figsize=(5,3))
    use_masked_cas = ["true", "false"]

    for use_mask in use_masked_cas:
        dirname = "masked_cas/masked_cas_"+use_mask
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        plot_cuckoo.throughput(ax, stats, decoration=False, x_axis="max fill", label="rcuckoo")

        ax.legend()
        ax.set_xlabel("fill percentage")
        ax.set_ylabel("MOPS")
        # ax.set_ylim(0,15)

    plt.tight_layout()
    plt.savefig("masked_cas.pdf")


def plot_masked_cas_sensitivity_static():
    with_mask=[9.04877777777779, 8.87, 8.510999999999992, 6.548140845070415, 5.9683, 5.360578947368423, 4.8703157894736915, 3.227105263157893, 1.7973913043478253]
    without_mask=[3.345576923076917, 3.197037037037043, 2.7532592592592624, 1.7924130434782615, 1.4269491525423725, 1.1805142857142845, 0.9505842696629208, 0.7095862068965514, 0.27647666666666665]
    fill_factor=['10', '20', '30', '40', '50', '60', '70', '80', '90']
    fill_factor = [int(x) for x in fill_factor]

    fig, ax = plt.subplots(1,1, figsize=(5,3))
    ax.plot(fill_factor, with_mask, label="with masked CAS", marker="o")
    ax.plot(fill_factor, without_mask, label="without masked CAS", marker="o")
    ax.legend()
    ax.set_xlabel("fill percentage")
    ax.set_ylabel("MOPS")
    plt.tight_layout()
    plt.savefig("masked_cas.pdf")


# masked_cas_sensitivity()
plot_masked_cas_sensitivity()
plot_masked_cas_sensitivity_static()
