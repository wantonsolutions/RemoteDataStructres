import random
import hashlib
import matplotlib.pyplot as plt
import matplotlib
import numpy as np

def h1(key):
    return hashlib.md5(str(key).encode('utf-8')).hexdigest()

def h2(key):
    val = str(key) + "salt"
    return hashlib.md5(val.encode('utf-8')).hexdigest()

def primary_location(key, table_size):
    return int(h1(key),16) % table_size

def secondary_location(key, table_size):
    return int(h2(key),16) % table_size

def secondary_bounded_location(key, table_size, suffix_size):
    primary = primary_location(key, table_size)
    secondary = (int(h2(key),16)) % suffix_size
    return (primary + secondary) % table_size

def get_locations(key, table_size, suffix):
    return (primary_location(key, table_size), secondary_location(key, table_size))

def get_locations_bounded(key, table_size, suffix_size):
    return (primary_location(key, table_size), secondary_bounded_location(key, table_size, suffix_size))

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

def single(table, primary, secondary):
    table[primary]+=1

def blind(table, primary, secondary):
    if table[primary] == 0:
        table[secondary]+=1
    else:
        table[secondary]+=1

def less_crowded(table, primary, secondary):
    if table[primary] < table[secondary]:
        table[primary]+=1
    else:
        table[secondary]+=1

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


def cuckoo_insert(table, location_func, location_index, value, suffix, table_indexes, collisions):
    index = location_func(value, table_size, suffix)
    index = index[location_index]

    success = False
    loop=False
    if index in table_indexes:
        loop=True
    elif table[index] == None:
        table[index] = value
        success = True
    else:
        collisions+=1
        table_indexes.append(index)
        temp = table[index]
        table[index] = value
        value = temp

    return success, value, loop
            
    

def cuckoo(table_size, insertions, location_func, suffix):
    table_1 =[None] * table_size
    table_2 =[None] * table_size
    collisions_per_insert=[]

    loop = False

    rand_start=random.randint(0,table_size)

    for i in range(rand_start, insertions+rand_start):
        if loop:
            break

        finished_hashing = False
        value = i
        collisions=0
        table_1_indexs=[]
        table_2_indexs=[]
        while not finished_hashing:
            #base case
            success, value, loop = cuckoo_insert(table_1, location_func, 0, value, suffix, table_1_indexs, collisions)
            if success or loop:
                break

            success, value, loop = cuckoo_insert(table_2, location_func, 1, value, suffix, table_2_indexs, collisions)
            if success or loop:
                break

        collisions_per_insert.append(collisions)
        # for i in range(0, table_size):
        #     print("Table 1: " + str(table_1[i]) + "\tTable 2: " + str(table_2[i]))
        # print("-"*30)
    return collisions_per_insert
    




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

    

    




table_size=1024
suffix=32
trials=64
#run_insertion_tests(table_size,suffix,trials)
#run_suffix_trials(table_size, trials)
# collsions=cuckoo(table_size, table_size*2)
# print(collsions)

#run_cuckoo_trials_50(table_size, trials)
run_cuckoo_bounded_loops(table_size, trials)



