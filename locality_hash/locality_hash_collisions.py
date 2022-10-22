from doctest import master
import random
import hashlib
from symbol import return_stmt
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


def cuckoo_insert(table, table_size, location_func, location_index, value, suffix, table_indexes, collisions):
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

        success = False
        value = i
        collisions=0
        table_1_indexs=[]
        table_2_indexs=[]
        while not success:
            #base case
            success, value, loop = cuckoo_insert(table_1, table_size, location_func, 0, value, suffix, table_1_indexs, collisions)
            if success or loop:
                break

            success, value, loop = cuckoo_insert(table_2, table_size, location_func, 1, value, suffix, table_2_indexs, collisions)
            if success or loop:
                break

        collisions_per_insert.append(collisions)
        # for i in range(0, table_size):
        #     print("Table 1: " + str(table_1[i]) + "\tTable 2: " + str(table_2[i]))
        # print("-"*30)
    return collisions_per_insert

def bucket_cuckoo_insert(table, table_size, location_func, location_index, value, bucket_size, suffix, table_values, collisions):
    index = location_func(value, table_size, suffix)
    index = index[location_index]

    #print("value: " + str(value) + "\tindex: " + str(index))

    success = False
    loop=False
    #loop case
    if value in table_values:
        #print("loop exiting")
        success=False
        value=value
        loop=True
        return success, value, loop, collisions

    #search for an empty slot in this bucket
    for i in range(0, bucket_size):
        if table[index][i] == None:
            #print("found candidate index: " + '{0: <5}'.format(str(index)) + "\tvalue: " + str(value))
            table[index][i] = value
            success = True
            value=value
            loop=False
            return success, value, loop, collisions
    
    #here we have a full bucket we need to evict a candidate
    collisions+=1
    #randomly select an eviction candidate
    table_values.append(value)
    evict_index = random.randint(0, bucket_size-1)
    evict_value = table[index][evict_index]
    table[index][evict_index] = value
    value = evict_value
    return success, value, loop, collisions

def print_bucket_table(table, table_size, bucket_size):
    for i in range(0, table_size):
        for j in range(0, bucket_size):
            print("[" + '{0: <5}'.format(str(table[i][j])) + "] ", end='')
            #print("Table: " + str(i) + "\tBucket: " + str(j) + "\tValue: " + str(table[i][j]))
        print("")

def bucket_cuckoo(table_size, bucket_size, insertions, location_func, suffix):
    table_1 = [] * table_size
    for i in range(0, table_size):
        table_1.append([None] * bucket_size)
    table_2 = [] * table_size
    for i in range(0, table_size):
        table_2.append([None] * bucket_size)

    # table_1 =[[None]*bucket_size] * table_size
    # table_2 =[[None]*bucket_size] * table_size
    collisions_per_insert=[]
    loop = False
    #values=["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]
    #rand_start=random.randint(0,table_size)
    values=np.arange(insertions)
    #print(values)
    for v in values:
        if loop:
            break
        collisions=0
        success=False
        table_1_values=[]
        table_2_values=[]
        while not success:
            success, v, loop, collisions = bucket_cuckoo_insert(table_1, table_size, location_func, 0, v, bucket_size, suffix, table_1_values, collisions)
            if success or loop:
                break
            success, v, loop, collisions = bucket_cuckoo_insert(table_2, table_size, location_func, 1, v, bucket_size, suffix, table_2_values, collisions)
            if success or loop:
                break
        collisions_per_insert.append(collisions)
        # print(table_1_values)
        # print_bucket_table(table_1, table_size, bucket_size)
        # print("-"*30)
        # print_bucket_table(table_2, table_size, bucket_size)
        # print("-"*30)
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
    bucket_size=[1,2,4,8,16,32,64,128]
    suffix=[1,2,4,8,16,32,64,128]
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
                collisions = bucket_cuckoo(table_size, b, inserts, get_locations_bounded, s)
                master_table.append(len(collisions))
            master_table.sort()
            #collect the 50th percentile
            result_value=master_table[int(len(master_table)*0.50)]
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



table_size=1024
suffix=32
trials=32
#run_insertion_tests(table_size,suffix,trials)
#run_suffix_trials(table_size, trials)
# collsions=cuckoo(table_size, table_size*2)
# print(collsions)

#run_cuckoo_trials_50(table_size, trials)
#run_cuckoo_bounded_loops(table_size, trials)
memory=4096
table_size=1024
bucket_size=4
location_func=get_locations_bounded
suffix=2


# collisions = bucket_cuckoo(table_size, bucket_size, insertions, location_func, suffix)
# print(collisions)

#run_bucket_cuckoo_trials_50(table_size, bucket_size, trials)
#run_bucket_cuckoo_bounded_loops(table_size, bucket_size, trials)

#run_bucket_cuckoo_bucket_size(memory, suffix, trials)
#bucket_size_vs_bound(memory, trials)

memory=1024
bucket_size=8
suffix=8
trials=1024
run_bucket_cuckoo_bounded_fill_size(table_size, bucket_size, suffix, trials)



