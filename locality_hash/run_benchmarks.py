from cuckoo import *
from locality_hash_collisions import *

from doctest import master
from symbol import return_stmt
import matplotlib.pyplot as plt
import matplotlib
import numpy as np


def cdf(data):
    high = max(data)
    low = min(data)
    norm = matplotlib.colors.Normalize(low,high)

    #print(data)
    count, bins_count = np.histogram(data, bins = 100000 )
    pdf = count / sum(count)
    
    y = np.cumsum(pdf)
    x = bins_count[1:]

    print('inserting')
    y= np.insert(y,0,0)
    x= np.insert(x,0,x[0])
    print(y[0])
    print(x)
    return x, y

def plot_cdf(data, label):
    x,y = cdf(data)
    plt.plot(x,y, label=label)

def cdf(data):
    high = max(data)
    low = min(data)
    norm = matplotlib.colors.Normalize(low,high)

    #print(data)
    count, bins_count = np.histogram(data, bins = 100000 )
    pdf = count / sum(count)
    
    y = np.cumsum(pdf)
    x = bins_count[1:]

    print('inserting')
    y= np.insert(y,0,0)
    x= np.insert(x,0,x[0])
    print(y[0])
    print(x)
    return x, y

def plot_cdf(data, label):
    x,y = cdf(data)
    plt.plot(x,y, label=label)


def run_trials(table_size, trials, suffix, location_func, decision_func):
    master_table = []
    for i in range(trials):
        table = [0] * table_size
        for i in range(0, table_size):
            rand=random.randint(0,table_size)
            primary, secondary = location_func(rand, table_size, suffix)
            decision_func(table, primary, secondary)
        master_table.extend(table)
    return master_table


def run_insertion_tests(table_size, suffix, trials):
    master_table = run_trials(table_size, trials, suffix, get_locations, single)
    plot_cdf(master_table, "Primary Only")
    master_table = run_trials(table_size, trials, suffix, get_locations, blind)
    plot_cdf(master_table, "Blind Insertion")
    master_table = run_trials(table_size, trials, suffix, get_locations, less_crowded)
    plot_cdf(master_table, "Less Crowded")
    master_table = run_trials(table_size, trials, suffix, get_locations_bounded, blind)
    plot_cdf(master_table, "Blind Insertion Bounded")
    master_table = run_trials(table_size, trials, suffix, get_locations_bounded, less_crowded)
    plot_cdf(master_table, "Less Crowded Bounded")

    #print all the tables
    plt.legend()
    plt.xlabel("Collisions Per Bucket")
    plt.savefig('locality_hash.pdf')
    plt.clf()

def run_suffix_trials(table_size, trials):
    suffix=[1,2,4,8,16,32,64,128,256,512,1024]

    #print all the suffexes
    for s in suffix:
        master_table = run_trials(table_size, trials, s, get_locations_bounded, less_crowded)
        plot_cdf(master_table, "Suffix Size: " + str(s))
    plt.title("Suffix Size vs. Collisions Less Crowded")
    plt.legend()
    plt.xlabel("Collisions Per Bucket")
    plt.savefig('suffix_size_less_crowded.pdf')
    plt.clf()

    for s in suffix:
        master_table = run_trials(table_size, trials, s, get_locations_bounded, blind)
        plot_cdf(master_table, "Suffix Size: " + str(s))
    plt.title("Suffix Size vs. Collisions Blind")
    plt.legend()
    plt.xlabel("Collisions Per Bucket")
    plt.savefig('suffix_size_blind.pdf')
    plt.clf()

def run_cuckoo_trials_50(table_size, trials):

    inserts=int(table_size/2)
    master_table=[]
    for i in range(trials):
        collisions = []
        while len(collisions) != inserts:
            collisions = cuckoo(table_size, inserts, get_locations, suffix=0)
        master_table.extend(collisions)
    plot_cdf(master_table, "Cuckoo Hash Collisions")
    for i in range(trials):
        collisions = []
        while len(collisions) != inserts:
            collisions = cuckoo(table_size, inserts, get_locations_bounded, suffix=32)
        master_table.extend(collisions)
    plot_cdf(master_table, "Bounded Cuckoo Hash Collisions")
    plt.title("Cuckoo Hash Collisions")
    plt.legend()
    plt.savefig("cuckoo_hash.pdf")
    plt.clf()

def run_cuckoo_bounded_loops(table_size, trials):
    suffix=[1,2,4,8,16,32,64,128,256,512,1024]
    inserts=table_size
    for s in suffix:
        master_table=[]
        for i in range(trials):
            collisions = cuckoo(table_size, inserts, get_locations_bounded, suffix=s)
            master_table.append(len(collisions))
        plot_cdf(master_table, str(s))
    plt.xlim(0,1500)
    plt.title("Cuckoo Hash Loop Capacity By Suffix Size")
    plt.legend()
    plt.savefig("bounded_cuckoo_hash_loops.pdf")
    plt.clf()

def run_bucket_cuckoo_trials_50(table_size, bucket_size, trials):
    total_entries=table_size*bucket_size*2
    inserts=int(total_entries * 0.95)
    master_table=[]
    for i in range(trials):
        collisions = bucket_cuckoo(table_size, bucket_size, inserts, get_locations, suffix=0)
        master_table.extend(collisions)
    plot_cdf(master_table, "Bucket Cuckoo Hash - No Bound")
    for i in range(trials):
        collisions = bucket_cuckoo(table_size, bucket_size, inserts, get_locations_bounded, suffix=8)
        master_table.extend(collisions)
    plot_cdf(master_table, "Bucket Cuckoo Hash - 8")

    plt.title("Bucket Cuckoo Hash Collisions")
    plt.legend()
    plt.savefig("bucket_cuckoo_hash.pdf")
    plt.clf

def run_bucket_cuckoo_bounded_loops(table_size, bucket_size, trials):
    suffix=[1,2,4,8,16,32,64,128,256,512,1024]
    total_locations = (table_size * bucket_size * 2)
    inserts=int(total_locations * 0.95)

    for s in suffix:
        master_table=[]
        for i in range(trials):
            collisions = bucket_cuckoo(table_size, bucket_size, inserts, get_locations_bounded, s)
            master_table.append(len(collisions))
        plot_cdf(master_table, str(s))
    #plt.xlim(0,1500)
    plt.title("Bucket Cuckoo Loop Capacity By Suffix Size")
    plt.legend()
    plt.savefig("bounded_bucket_cuckoo_hash_loops.pdf")
    plt.clf()

def normalize_to_memory(master_table, memory):
    print(memory)
    print(master_table)


    return [x/memory for x in master_table]

def run_bucket_cuckoo_bounded_fill_size(memory, bucket_size, suffix, trials):
    table_size=int((memory/2)/bucket_size)
    ratios=[0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]
    ratios.reverse()

    for r in ratios:
        inserts = int(memory * r)
        master_table=[]
        for i in range(trials):
            collisions = bucket_cuckoo(table_size, bucket_size, inserts, get_locations_bounded, suffix)
            master_table.append(len(collisions))

        master_table = normalize_to_memory(master_table, memory)
        plot_cdf(master_table, str(r))
    #plt.xlim(0,1500)
    plt.xlim(0.4,1)
    plt.title("Bucket Cuckoo Loop Capacity By Fill Ratio (suffix " + str(suffix) + ") \n(Bucket Size " + str(bucket_size) + ") (Memory "+ str(memory) + ")")
    plt.legend()
    plt.savefig("bounded_bucket_cuckoo_hash_fill_ratio.pdf")
    plt.clf()

def run_bucket_cuckoo_bucket_size(memory, suffix, trials):
    bucket_size=[1,2,4,8,16,32,64,128,256,512,1024]
    suffix=16
    fill=0.95
    #inserts=int(memory * 0.95)
    inserts=int(memory * fill)

    for b in bucket_size:
        table_size=int((memory/2)/b)
        assert(table_size*b*2 == memory)

        master_table=[]
        for i in range(trials):
            collisions = bucket_cuckoo(table_size, b, inserts, get_locations_bounded, suffix)
            master_table.append(len(collisions))
        plot_cdf(master_table, str(b))
    #plt.xlim(0,1500)
    plt.title("Bucket Size Bound = "+str(suffix)+", ("+str(fill)+ "% fill), ")
    plt.legend()
    plt.savefig("bounded_bucket_cuckoo_hash_bucket_size.pdf")

def bucket_size_vs_bound(memory, trials):
    # bucket_size=[1,2,4,8,16,32,64,128,256,512,1024]
    # suffix=[1,2,4,8,16,32,64,128,256,512,1024]
    bucket_size=[1,2,4,8,16,32,64]
    suffix=[1,2,4,8,16,32,64]
    results=[None] * len(bucket_size)
    for i in range(len(bucket_size)):
        results[i]=[None] * len(suffix)
    print(results)
    fill=0.95
    inserts=int(memory * fill)

    bucket_id=0
    for b in bucket_size:
        table_size=int((memory/2)/b)
        assert(table_size*b*2 == memory)
        suffix_id=0
        for s in suffix:
            master_table=[]
            for i in range(trials):
                collisions = bucket_cuckoo_insert_only(table_size, b, inserts, get_locations_bounded, s)
                master_table.append(len(collisions))
            master_table.sort()
            master_table=normalize_to_memory(master_table, memory)
            #collect the 50th percentile
            result_value=master_table[int(len(master_table)*0.50)]
            result_value=round(result_value,2)
            print(suffix_id, bucket_id, result_value)
            results[bucket_id][suffix_id]=result_value
            suffix_id+=1
        bucket_id+=1
    print(results)
    im = plt.imshow(results, cmap='hot', interpolation='nearest')
    for i in range(len(bucket_size)):
        for j in range(len(suffix)):
            text = plt.text(j, i, str(results[i][j]) + "\n" + str(bucket_size[i]*suffix[j]),
                        ha="center", va="center", color="b")

    plt.yticks(np.arange(len(bucket_size)),labels=[ str(x) for x in bucket_size])
    plt.ylabel("Bucket Size")
    plt.xticks(np.arange(len(suffix)),labels=[str(x) for x in suffix])
    plt.xlabel("Suffix Size")

    plt.colorbar(im)

    #plt.xlim(0,1500)
    plt.title("Bucket Size vs Bound")
    plt.savefig("bucket_vs_bound.pdf")

#todo imortalize some of these that produced good results.

# collsions=cuckoo(table_size, table_size*2)
# print(collsions)

#run_cuckoo_trials_50(table_size, trials)
#run_cuckoo_bounded_loops(table_size, trials)
memory=8
table_size=8
bucket_size=4
location_func=get_locations_bounded
suffix=2


#collisions = bucket_cuckoo(table_size, bucket_size, 50, location_func, suffix)
#exit(1)
# print(collisions)

#run_bucket_cuckoo_trials_50(table_size, bucket_size, trials)
#run_bucket_cuckoo_bounded_loops(table_size, bucket_size, trials)

#run_bucket_cuckoo_bucket_size(memory, suffix, trials)
memory=1024
trials=5
bucket_size_vs_bound(memory, trials)

# memory=1024
# bucket_size=8
# suffix=8
# trials=1024
# run_bucket_cuckoo_bounded_fill_size(memory, bucket_size, suffix, trials)