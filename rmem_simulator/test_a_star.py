from cuckoo import *
import hash
import numpy as np
import matplotlib.pyplot as plt
import random


def insert_cuckoo_path(path, table):
    assert(len(path) >=2)
    for i in reversed(range(0,len(path)-1)):
        table.set_entry(path[i+1].bucket_index, path[i+1].bucket_offset, path[i].key)

def random_inserts(size):
    inserts=[]
    random_mult = int(random.random() * 100)
    for i in range(0, TOTAL_INSERTS):
        inserts.append(i * random_mult)
    return inserts

def deterministic_inserts(size):
    inserts=[]
    random_mult = 1
    for i in range(0, TOTAL_INSERTS):
        inserts.append(i * random_mult)
    return inserts

def stderr(data):
    return np.std(data) / np.sqrt(np.size(data))

memory_size = 1024 * 1024
bucket_size = 4
buckets_per_lock = 1
trials = 5


TOTAL_INSERTS = memory_size

bucket_sizes=[1,2,4,8,16,32]
factors=[1.8,1.9,2.0,2.1,2.2,2.3,2.4]

def bucket_sizes_test(memory_size, bucket_sizes, buckets_per_lock):
    fills=[]
    for bucket_size in bucket_sizes:
        table = Table(memory_size, bucket_size, buckets_per_lock)
        inserts = random_inserts(TOTAL_INSERTS)
        for v in inserts:
            search_path=bucket_cuckoo_a_star_insert(table, hash.rcuckoo_hash_locations, v)
            if len(search_path) == 0:
                print("Search Failed: " + str(v))
                break
            insert_cuckoo_path(search_path, table)
        table.print_table()
        fills.append(table.get_fill_percentage())
    print(bucket_sizes)
    print(fills)

def factors_test(memory_size, bucket_size, buckets_per_lock, factors, trials):
        fills=[]
        for f in factors:
            hash.set_factor(f)
            trial_fills=[]
            for t in range(trials):
                table = Table(memory_size, bucket_size, buckets_per_lock)
                inserts = random_inserts(TOTAL_INSERTS)
                for i in tqdm(range(len(inserts))):
                    search_path=bucket_cuckoo_a_star_insert(table, hash.rcuckoo_hash_locations, inserts[i])
                    if len(search_path) == 0:
                        # print("Search Failed: " + str(v))
                        break
                    insert_cuckoo_path(search_path, table)
                # table.print_table()
                trial_fills.append(table.get_fill_percentage())
            fills.append(trial_fills)

        #plot results
        avg, std = [], []
        for f in fills:
            avg.append(np.mean(f))
            std.append(stderr(f))

        fig, ax = plt.subplots(1, 1, figsize=(5,3))
        ax.errorbar(factors, avg, yerr=std, marker='o')
        ax.set_xlabel("Factor")
        ax.set_ylabel("Fill Percentage")
        # plt.show()
        plt.tight_layout()
        plt.savefig("factor_test.pdf")
        # print(factors)
        # print(fills)



# bucket_sizes_test(memory_size, bucket_sizes, buckets_per_lock)
# factors_test(memory_size, bucket_size, buckets_per_lock, factors, trials)

def factor_debug(memory_size, bucket_size, buckets_per_lock):
        table = Table(memory_size, bucket_size, buckets_per_lock)
        inserts = deterministic_inserts(memory_size)
        factor=1.8
        hash.set_factor(factor)
        for i in tqdm(range(len(inserts))):
            search_path=bucket_cuckoo_a_star_insert(table, hash.rcuckoo_hash_locations, inserts[i])
            print(search_path)
            if len(search_path) == 0:
                print("Search Failed: " + str(inserts[i]), hash.rcuckoo_hash_locations(inserts[i],(int((memory_size/bucket_size)/8))))
                break
            insert_cuckoo_path(search_path, table)
            table.print_table()
        # table.print_table()


def random_search_debug(memory_size, bucket_size, buckets_per_lock):
    table = Table(memory_size, bucket_size, buckets_per_lock)
    inserts = deterministic_inserts(memory_size)
    factor=1.8
    hash.set_factor(factor)
    for i in tqdm(range(len(inserts))):
        search_path=bucket_cuckoo_random_insert(table, hash.rcuckoo_hash_locations, inserts[i])
        print(search_path)
        if len(search_path) == 0:
            print("Search Failed: " + str(inserts[i]), hash.rcuckoo_hash_locations(inserts[i],(int((memory_size/bucket_size)/8))))
            break
        insert_cuckoo_path(search_path, table)
        table.print_table()
    # table.print_table()

memory_size=1024
# factor_debug(memory_size, bucket_size, buckets_per_lock)
random_search_debug(memory_size, bucket_size, buckets_per_lock)

