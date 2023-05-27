import sys
# caution: path[0] is reserved for script path (or '' in REPL)
# sys.path.insert(1, '/home/ena/RemoteDataStructres/rmem_simulator')
from experiments import plot_cuckoo as pc
from experiments import data_management as dm
import simulator.search as search
# import simulator.tables as tables
import ctables as tables

# import simulator.hash as hash
import chash as hash
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm
import random

# factors = [1.8, 1.9, 2.0, 2.1, 2.2, 2.3]
# factors = [1.9, 2.1, 2.3, 2.5, 2.7, 2.9, 3.1, 3.3]
factors = [3.3]
memory_size = 1024
bucket_size = 8
buckets_per_lock = 1
trials = 1
data_dir="hash_fill"

def insert_cuckoo_path(path, table):
    assert(len(path) >=2)
    for i in reversed(range(0,len(path)-1)):
        table.set_entry(path[i+1].bucket_index, path[i+1].bucket_offset, path[i].key)

def random_inserts(size):
    inserts=[]
    random_mult = int(random.random() * 100)
    for i in range(0, size):
        inserts.append(i * random_mult)
    return inserts

def stderr(data):
    return np.std(data) / np.sqrt(np.size(data))


def factor_fill(memory_size, bucket_size, buckets_per_lock, factor):
        entry_size = 8
        table = tables.Table(memory_size * entry_size, bucket_size, buckets_per_lock)
        inserts = random_inserts(memory_size)
        hash.set_factor(factor)
        print("filling with factor " + str(factor) + " ...")
        for i in tqdm(range(len(inserts))):
            search_path=search.bucket_cuckoo_a_star_insert(table, hash.rcuckoo_hash_locations, inserts[i])
            # print(search_path)
            if len(search_path) == 0:
                print("Search Failed: " + str(inserts[i]), hash.rcuckoo_hash_locations(inserts[i],(int((memory_size/bucket_size)/8))))
                break
            insert_cuckoo_path(search_path, table)
            # table.print_table()
        return table.get_fill_percentage()


def run_fill_factor_experiment():
    global factors
    global memory_size
    global bucket_size
    global buckets_per_lock
    global trials
    global data_dir
    experiments=[]
    for f in factors:
        fill_exp=[]
        for t in range(trials):
            fill = factor_fill(memory_size,bucket_size,buckets_per_lock, f)
            fill_exp.append(fill)
        experiments.append((f,fill_exp))
    dm.save_statistics(experiments,dirname=data_dir)
    print("all distance prior to save: ", len(experiments))

def plot_hash_fill():
    global factors
    fig, ax = plt.subplots(1,1,figsize=(4,2.5))
    experiments = dm.load_statistics(dirname=data_dir)
    experiments=experiments[0]

    print(experiments)

    y = []
    y_err = []
    x = []
    for e in experiments:
        factor = e[0]
        fills = e[1]

        fill = np.mean(fills) * 100
        standard_err = stderr(fills) * 100
        y.append(fill)
        y_err.append(standard_err)
        x.append(factor)

    print(x)
    x_labels = [str(f) for f in factors]
    ax.bar(x,y,yerr=y_err, width=0.15, edgecolor='black')
    ax.set_xticks(x, x_labels)
    ax.grid(True, axis='y', linestyle=':')
    ax.axhline(y=90, color='r', linestyle=':')

    ax.set_xlabel('factor')
    ax.set_ylabel('max fill')

    # ax.legend()
    plt.tight_layout()
    plt.savefig("hash_fill.pdf")


run_fill_factor_experiment()
plot_hash_fill()

