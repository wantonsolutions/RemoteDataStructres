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





def masked_cas_vs_lock_size():
    use_masked_cas = ["true", "false"]
    entry_size=8
    config=lib.get_config()
    table_size = 1024 * 1024 * 1
    memory_size = entry_size * table_size
    config["indexes"] = str(table_size)
    config["memory_size"] = str(memory_size)

    config["prime"]="true"
    config["prime_fill"]="88"
    config["max_fill"]="91"
    config["trials"]=4

    clients = 400
    config['num_clients'] = clients
    config['workload'] = "ycsb-w"
    buckets_per_lock = [1,2,4,8,16,32,64,128]

    orchestrator.boot(config)
    for use_mask in use_masked_cas:
        runs = []
        for bps in buckets_per_lock:
            lconfig = config.copy()
            lconfig["use_mask"] = str(use_mask)
            lconfig["buckets_per_lock"] = str(bps)
            results = orchestrator.run_trials(lconfig)
            if len(results) > 0:
                runs.append(results)

        directory = "masked_cas/lock_masked_cas_"+use_mask
        dm.save_statistics(runs, dirname=directory)
    


def plot_masked_cas_vs_lock_size():
    fig, ax = plt.subplots(1,1, figsize=(5,3))
    use_masked_cas = ["true", "false"]

    for use_mask in use_masked_cas:
        dirname = "masked_cas/lock_masked_cas_"+use_mask
        stats = dm.load_statistics(dirname=dirname)
        stats=stats[0]
        plot_cuckoo.throughput(ax, stats, decoration=False, x_axis="buckets per lock", label="rcuckoo")

        ax.legend()
        ax.set_xlabel("read threshold bytes")
        ax.set_ylabel("MOPS")
        # ax.set_ylim(0,15)

    plt.tight_layout()
    plt.savefig("masked_cas_lock_size.pdf")

def plot_masked_cas_vs_lock_size_static():
    print("implement masked cas lock size")
    mask_throughputs=[2.4190555555555552, 2.9133750000000003, 2.510516339869281, 2.236777777777778, 1.8561458333333336, 1.5773345588235285, 1.1309074074074081, 0.5238192073475094]
    mask_errors=[0.3453123152994412, 0.262330709982952, 0.37494516118314763, 0.27919253058741, 0.1352029615365582, 0.24852834262191723, 0.018278265594451046, 0.006648065288488561]

    non_mask_throughputs=[2.198333333333332, 1.709834150326798, 0.9843730098877155, 0.5999596960167713, 0.4128516285301999, 0.16073440748738965, 0.07186497034258635, 0.024098609873506486]
    non_mask_errs=[0.32196820447267566, 0.20181637770667726, 0.14523715202499893, 0.07238581927342275, 0.13359068523373258, 0.02986919971236892, 0.004763669522751848, 0.0019695280878782623]
    lock_size=[1, 2, 4, 8, 16, 32, 64, 128]
    x_labels=[str(l) for l in lock_size]
    fig, ax = plt.subplots(1,1, figsize=(5,3))
    ax.errorbar(lock_size,mask_throughputs,mask_errors,label="Masked CAS",marker="o")
    ax.errorbar(lock_size,non_mask_throughputs,non_mask_errs,label="CAS",marker='x')
    ax.set_xscale("log")
    ax.set_xticks(lock_size)
    ax.set_xticklabels(x_labels)
    ax.minorticks_off()
    ax.set_xlabel("rows per lock")
    ax.set_ylabel("MOPS")
    ax.legend()
    plt.tight_layout()
    plt.savefig("masked_cas_lock_size.pdf")






# masked_cas_sensitivity()
# plot_masked_cas_sensitivity()
# plot_masked_cas_sensitivity_static()

# plot_masked_cas_vs_lock_size()
plot_masked_cas_vs_lock_size_static()