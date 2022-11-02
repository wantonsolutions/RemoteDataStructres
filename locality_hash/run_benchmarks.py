from cuckoo import *
from locality_hash_collisions import *

from doctest import master
from symbol import return_stmt
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.pyplot import cm
import numpy as np

def save_figure(filename):
    plt.savefig(filename + ".pdf")
    plt.savefig(filename + ".png")
    plt.clf()

def plot_cdf(data, label, color='none'):
    x,y = cdf(data)
    if color == 'none':
        plt.plot(x,y, label=label)
    else:
        plt.plot(x,y, label=label, color=color)

def cdf(data):
    high = max(data)
    low = min(data)
    norm = matplotlib.colors.Normalize(low,high)

    #print(data)
    count, bins_count = np.histogram(data, bins = 100000 )
    pdf = count / sum(count)
    
    y = np.cumsum(pdf)
    x = bins_count[1:]

    y= np.insert(y,0,0)
    x= np.insert(x,0,x[0])
    return x, y

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
        collisions = bucket_cuckoo_insert_only(table_size, bucket_size, inserts, get_locations, suffix=0)
        master_table.extend(collisions)
    plot_cdf(master_table, "Bucket Cuckoo Hash - No Bound")
    for i in range(trials):
        collisions = bucket_cuckoo_insert_only(table_size, bucket_size, inserts, get_locations_bounded, suffix=8)
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
            collisions = bucket_cuckoo_insert_only(table_size, bucket_size, inserts, get_locations_bounded, s)
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
            collisions = bucket_cuckoo_insert_only(table_size, bucket_size, inserts, get_locations_bounded, suffix)
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
            collisions = bucket_cuckoo_insert_only(table_size, b, inserts, get_locations_bounded, suffix)
            master_table.append(len(collisions))
        plot_cdf(master_table, str(b))
    #plt.xlim(0,1500)
    plt.title("Bucket Size Bound = "+str(suffix)+", ("+str(fill)+ "% fill), ")
    plt.legend()
    plt.savefig("bounded_bucket_cuckoo_hash_bucket_size.pdf")

def bucket_size_vs_bound(memory, trials):
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
            
            print("master table----------------------")
            print(master_table)
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




def size_vs_bound(memory, trials, insertion_func, title, figname):
    bucket_size=[1,2,4,8]
    suffix=[1,2,4,8,16]
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
                collisions, paths = insertion_func(table_size, b, inserts, get_locations_bounded, s)
                print("collisions")
                print(collisions)
                master_table.append(len(collisions))

            master_table=normalize_to_memory(master_table, memory)
            master_table.sort()
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
    plt.title(title)
    save_figure(figname)

def size_vs_bound_bucket_cuckoo(memory,trials):
    size_vs_bound(memory, trials, bucket_cuckoo_insert_only, "Bucket Size vs Bound", "bucket_vs_bound")

def size_vs_bound_bfs_bucket_cuckoo(memory,trials):
    size_vs_bound(memory, trials, bucket_cuckoo_bfs_insert_only, "Bucket Size vs Bound BFS", "bucket_vs_bound_bfs")

def calculate_read_size(distance, bucket_size, entry_size):
    return (bucket_size * entry_size * 2) * (1 + distance)

def cuckoo_measure_average_read_size(memory, trials, insertion_func, title, figname):
    #bucket_size=[1,2,4,8,16,32,64]
    bucket_size=[1,2,4,8]
    #bucket_size=[4]
    suffix=[1,2,4,8,16]
    #suffix=[1]

    results=[None] * len(bucket_size)
    for i in range(len(bucket_size)):
        results[i]=[None] * len(suffix)

    fill=0.95
    entry_size=8
    inserts=int(memory * fill)
    bucket_id=0
    for b in bucket_size:
        suffix_id=0
        for s in suffix:
            table_size=int((memory/2)/b)
            master_read_sizes=[]
            for i in range(trials):
                read_sizes = cuckoo_insert_then_measure_reads(insertion_func, table_size, b, inserts, get_locations_bounded, s)
                read_sizes = [calculate_read_size(x, b, entry_size) for x in read_sizes]
                master_read_sizes.extend(read_sizes)
            master_read_sizes.sort()
            color=(0.5,0.01*s,0.01*b)
            plot_cdf(master_read_sizes, "b="+str(b)+", s="+str(s), color)
            results[bucket_id][suffix_id]=master_read_sizes[int(len(master_read_sizes)/2)]
            suffix_id+=1
        bucket_id+=1
    plt.title(title + "cdf")
    plt.legend(ncol=2, prop={'size': 6})
    plt.xlabel("Read Size (bytes)")
    save_figure(figname+"_cdf")
    plt.clf()

    #cmap_reversed = matplotlib.cm.get_cmap('autumn_r')
    im = plt.imshow(results, cmap='hot_r', interpolation='nearest')
    for i in range(len(bucket_size)):
        for j in range(len(suffix)):
            text = plt.text(j, i, str(results[i][j]),ha="center", va="center", color="b")

    plt.yticks(np.arange(len(bucket_size)),labels=[ str(x) for x in bucket_size])
    plt.ylabel("Bucket Size")
    plt.xticks(np.arange(len(suffix)),labels=[str(x) for x in suffix])
    plt.xlabel("Suffix Size")
    plt.colorbar(im)
    #plt.xlim(0,1500)
    plt.title(title)
    save_figure(figname + "_heatmap")

def bucket_cuckoo_measure_average_read_size(memory, trials):
    cuckoo_measure_average_read_size(memory, trials, bucket_cuckoo_insert, "Bucket Cuckoo Average Read Size", "bucket_cuckoo_average_read_size")

def bfs_cuckoo_measure_average_read_size(memory, trials):
    cuckoo_measure_average_read_size(memory, trials, bucket_cuckoo_bfs_insert, "Bucket Cuckoo BFS Average Read Size", "bucket_cuckoo_bfs_average_read_size")


#this function attempts to remove the modulo distance from hash table insertion
#paths.  if a wrap around occured, then this function will attempt to normalize
#the wraped around index.  for example if a path on table [0,1,2,3,4] tried to
#insert at index 4, then 0, this function would change the 0 index to 5. The
#reason I'm doing this is to not have the wrap around dammage my measurements of
#how far away indexs are from one another.
def normalize_path(path, suffix_size, table_size):
    t = path
    #skip the first index because its (-1,-1,-1), and don't go to the last index
    for i in range(1,len(path)-1,1):
        distance = path[i].bucket_index - path[i+1].bucket_index
        if distance < 0 and abs(distance) > suffix_size:
            for j in range(i+1, len(path)):
                path[j].bucket_index-=table_size
        if distance > 0 and abs(distance) > suffix_size:
            for j in range(i+1, len(path)):
                path[j].bucket_index+=table_size
    return path


def paths_to_ranges(paths, suffix_size, table_size):
    ranges=[]
    for path in paths:
        path=normalize_path(path, suffix_size, table_size)
        #todo this is a weird patterns, that overloads the path_element class, consider revising.
        #trim the first element from the path
        path=path[1:]
        r = max(path) - min(path)
        #print("max:", max(path), "min:", min(path), "range:", r)
        if r >= 0:
            ranges.append(r)
    #print(ranges)
    return ranges
        
def bucket_cuckoo_insert_range(memory, trials):
    cuckoo_insert_range(memory, trials, bucket_cuckoo_insert_only, "Insert Difference Bucket Cuckoo", "bucket_cuckoo_insert_range")

def bfs_bucket_cuckoo_insert_range(memory, trials):
    cuckoo_insert_range(memory, trials, bucket_cuckoo_bfs_insert_only, "Insert Difference BFS Bucket Cuckoo", "bucket_cuckoo_bfs_insert_range")

def get_sized_result_array(bucket_size, suffix):
    results=[None] * len(bucket_size)
    for i in range(len(bucket_size)):
        results[i]=[None] * len(suffix)
    return results

def run_insertion_trials(trials, insertion_func, table_size, bucket_size, inserts, get_locations_func, suffix_size):
    all_results=[]
    entry_size=8
    for i in range(trials):
        collisions, paths = insertion_func(table_size, bucket_size, inserts, get_locations_bounded, suffix_size)
        ranges= paths_to_ranges(paths, suffix_size,table_size)
        ranges = [calculate_read_size(x, bucket_size, entry_size) for x in ranges]
        all_results.extend(ranges)
    all_results.sort()
    return all_results

def bucket_suffix_cdf(title, figname, results, bucket, suffix):
    color_pallets=[cm.Greys, cm.Purples, cm.Blues, cm.Greens, cm.Oranges, cm.Reds, cm.summer, cm.YlOrRd, cm.OrRd, cm.PuRd, cm.RdPu, cm.BuPu, cm.GnBu, cm.PuBu, cm.YlGnBu, cm.PuBuGn, cm.BuGn, cm.YlGn]
    for bucket_id in range(len(bucket)):
        color = iter(color_pallets[bucket_id%len(color_pallets)](np.linspace(0.5, 1, len(suffix))))
        for suffix_id in range(len(suffix)):
            c=next(color)
            plot_cdf(results[bucket_id][suffix_id], "b="+str(bucket[bucket_id])+", s="+str(suffix[suffix_id]),color=c)

    plt.title(title + " cdf")
    plt.ylim(0.5,1.01)
    plt.ylim(0.85,1.01)
    plt.legend(ncol=4, prop={'size': 6})
    plt.xlabel("Distance (Bytes)")
    plt.tight_layout()
    plt.xscale("log")
    save_figure(figname+ "_cdf")
    plt.clf()

def get_percentile_heatmap_results(results, bucket, suffix, percentile):
    heatmap_results=get_sized_result_array(bucket, suffix)
    for bucket_id in range(len(bucket)):
        for suffix_id in range(len(suffix)):
            heatmap_results[bucket_id][suffix_id]=results[bucket_id][suffix_id][int(len(results[bucket_id][suffix_id])*percentile)]
    return heatmap_results

def label_percentile_heatmap(heatmap_results, bucket, suffix, percentile):
    for i in range(len(bucket)):
        for j in range(len(suffix)):
            result_value=heatmap_results[i][j]
            result_value=round(result_value,2)
            text = plt.text(j, i, str(result_value),ha="center", va="center", color="b")

def percentile_heatmaps(title, figname, percentiles, results, bucket, suffix):
    for percentile in percentiles:
        percentile_heatmap = get_percentile_heatmap_results(results, bucket, suffix, percentile)
        im = plt.imshow(percentile_heatmap, cmap='hot_r', interpolation='nearest')
        plt.colorbar(im)
        label_percentile_heatmap(percentile_heatmap, bucket, suffix, percentile)

        plt.yticks(np.arange(len(bucket)),labels=[ str(x) for x in bucket])
        plt.ylabel("Bucket Size")
        plt.xticks(np.arange(len(suffix)),labels=[str(x) for x in suffix])
        plt.xlabel("Suffix Size")

        percent_string=str(int(round(percentile*100,2)))
        plt.title(title+"_"+percent_string+"th percentile")
        plt.tight_layout()
        save_figure(figname+"_"+percent_string)
        plt.clf()

def cuckoo_insert_range(memory, trials, insertion_func, title, figname):
    bucket=[1,2,4,8,16]
    suffix=[1,2,4,8,16,32,64]
    fill=0.95
    inserts=int(memory * fill)
    percentiles=[0.5,0.9,0.99]
    results = get_sized_result_array(bucket, suffix)

    for bucket_id in range(len(bucket)):
        table_size=int((memory/2)/bucket[bucket_id])
        assert(table_size*bucket[bucket_id]*2 == memory)
        for suffix_id in range(len(suffix)):
            print("bucket size", bucket[bucket_id], "suffix size", suffix[suffix_id])
            results[bucket_id][suffix_id]=run_insertion_trials(trials, insertion_func, table_size, bucket[bucket_id], inserts, get_locations_bounded, suffix[suffix_id])

    bucket_suffix_cdf(title, figname, results, bucket, suffix)
    percentile_heatmaps(title, figname, percentiles, results, bucket, suffix)



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
suffix=8
bucket_size=8
trials=16
#size_vs_bound_bucket_cuckoo(memory, trials)
#size_vs_bound_bfs_bucket_cuckoo(memory, trials)

#bucket_cuckoo_measure_average_read_size(memory, trials)
# bfs_cuckoo_measure_average_read_size(memory, trials)
bucket_cuckoo_insert_range(memory, trials)
bfs_bucket_cuckoo_insert_range(memory, trials)





# memory=1024
# bucket_size=8
# suffix=8
# trials=1024
#run_bucket_cuckoo_bounded_fill_size(memory, bucket_size, suffix, trials)