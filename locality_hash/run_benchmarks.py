from cuckoo import *
from locality_hash_collisions import *

from doctest import master
from symbol import return_stmt
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.pyplot import cm
import numpy as np

def save_figure(filename):
    plt.savefig("fig/" + filename + ".pdf")
    plt.savefig("fig/" + filename + ".png")
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

def bucket_suffix_cdf(title, figname, results, bucket, suffix):
    color_pallets=[cm.Greys, cm.Purples, cm.Blues, cm.Greens, cm.Oranges, cm.Reds, cm.summer, cm.YlOrRd, cm.OrRd, cm.PuRd, cm.RdPu, cm.BuPu, cm.GnBu, cm.PuBu, cm.YlGnBu, cm.PuBuGn, cm.BuGn, cm.YlGn]
    for bucket_id in range(len(bucket)):
        color = iter(color_pallets[bucket_id%len(color_pallets)](np.linspace(0.5, 1, len(suffix))))
        for suffix_id in range(len(suffix)):
            c=next(color)
            plot_cdf(results[bucket_id][suffix_id], "b="+str(bucket[bucket_id])+", s="+str(suffix[suffix_id]),color=c)

    plt.title(title + " cdf")
    # plt.ylim(0.5,1.01)
    # plt.ylim(0.85,1.01)
    plt.xlim(0.9)
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

def percentile_heatmaps(title, figname, percentiles, results, bucket, suffix, reversed=False):
    for percentile in percentiles:
        percentile_heatmap = get_percentile_heatmap_results(results, bucket, suffix, percentile)
        cmap='hot'
        if reversed:
            cmap='hot_r'
        im = plt.imshow(percentile_heatmap, cmap=cmap, interpolation='nearest')
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

def normalize_to_memory(master_table, memory):
    return [x/memory for x in master_table]

def get_sized_result_array(bucket_size, suffix):
    results=[None] * len(bucket_size)
    for i in range(len(bucket_size)):
        results[i]=[None] * len(suffix)
    return results

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

def get_table_size(memory, bucket_size):
    assert memory > bucket_size, "Memory must be larger than bucket size" + " memory: " + str(memory) + " bucket_size: " + str(bucket_size)
    table_size=int((memory/2)/bucket_size)
    assert table_size*bucket_size*2 == memory, "table size: " + str(table_size) + " bucket size: " + str(bucket_size) + " memory: " + str(memory)
    return table_size

def run_insertion_fill_trials(trials, insertion_func, table_size, bucket_size, inserts, suffix_size):
    all_results=[]
    for i in range(trials):
        collisions, paths = insertion_func(table_size, bucket_size, inserts, get_locations_bounded, suffix_size)
        all_results.append(paths)
    return all_results

def all_paths_to_path_length(all_paths):
    all_paths_lengths=[]
    for paths in all_paths:
        all_paths_lengths.extend(paths_to_collisions(paths))
    all_paths_lengths.sort()
    return all_paths_lengths

def fill_and_paths(bucket, suffix, memory, fill, trials, insertion_func, title):
    inserts=int(memory * fill)
    fill_results = get_sized_result_array(bucket, suffix)
    path_results = get_sized_result_array(bucket, suffix)
    for bucket_id in range(len(bucket)):
        table_size = get_table_size(memory, bucket[bucket_id])
        for suffix_id in range(len(suffix)):
            print("[fill and paths ("+title+")] bucket size", bucket[bucket_id], "suffix size", suffix[suffix_id])
            all_paths=run_insertion_fill_trials(trials, insertion_func, table_size, bucket[bucket_id], inserts, suffix[suffix_id])

            # Measure how full each table got before it broke
            fill=[len(paths) for paths in all_paths]
            fill.sort()
            fill=normalize_to_memory(fill, memory)
            fill_results[bucket_id][suffix_id]=fill

            #measure the path length
            path_results[bucket_id][suffix_id]=all_paths_to_path_length(all_paths)
    return fill_results, path_results

def size_vs_bound(memory, trials, insertion_func, title, figname):
    bucket=[1,2,4,8,16]
    suffix=[1,2,4,8,16]
    percentiles=[0.5, 0.90, 0.99]
    fill_results = get_sized_result_array(bucket, suffix)
    path_results = get_sized_result_array(bucket, suffix)
    fill=0.95

    fill_results, path_results = fill_and_paths(bucket, suffix, memory, fill, trials, insertion_func, title)
    percentile_heatmaps(title+"_fill", figname+"_fill", percentiles, fill_results, bucket, suffix)
    bucket_suffix_cdf(title+"_fill", figname+"_fill", fill_results, bucket, suffix)
    percentile_heatmaps(title+"_path", figname+"_path", percentiles, path_results, bucket, suffix, reversed=True)
    bucket_suffix_cdf(title+"_path", figname+"_path", path_results, bucket, suffix)


def size_vs_bound_bucket_cuckoo(memory,trials):
    size_vs_bound(memory, trials, bucket_cuckoo_insert_only, "Bucket Size vs Bound", "bucket_vs_bound")

def size_vs_bound_bfs_bucket_cuckoo(memory,trials):
    size_vs_bound(memory, trials, bucket_cuckoo_bfs_insert_only, "Bucket Size vs Bound BFS", "bucket_vs_bound_bfs")

def calculate_read_size(distance, bucket_size, entry_size):
    return (bucket_size * entry_size * 2) * (1 + distance)

def run_read_size_trials(trials, insertion_func, table_size, bucket_size, inserts, suffix_size):
    entry_size=8
    master_read_sizes=[]
    for i in range(trials):
        read_sizes = cuckoo_insert_then_measure_reads(insertion_func, table_size, bucket_size, inserts, get_locations_bounded, suffix_size)
        read_sizes = [calculate_read_size(x, bucket_size, entry_size) for x in read_sizes]
        master_read_sizes.extend(read_sizes)
    master_read_sizes.sort()
    return master_read_sizes


def cuckoo_measure_read_size(memory, trials, insertion_func, title, figname):
    bucket=[1,2,4,8]
    suffix=[1,2,4,8,16]
    percentiles=[0.5]
    results = get_sized_result_array(bucket, suffix)
    fill=0.95

    inserts=int(memory * fill)
    for bucket_id in range(len(bucket)):
        table_size = get_table_size(memory, bucket[bucket_id])
        for suffix_id in range(len(suffix)):
            print("[cuckoo read size ("+title+")] bucket size", bucket[bucket_id], "suffix size", suffix[suffix_id])
            results[bucket_id][suffix_id]=run_read_size_trials(trials, insertion_func, table_size, bucket[bucket_id], inserts, suffix[suffix_id])

    bucket_suffix_cdf(title, figname, results, bucket, suffix)
    percentile_heatmaps(title, figname, percentiles, results, bucket, suffix, reversed=True)

def bucket_cuckoo_measure_average_read_size(memory, trials):
    cuckoo_measure_read_size(memory, trials, bucket_cuckoo_insert, "Bucket Cuckoo Read Size", "bucket_cuckoo_read_size")

def bfs_cuckoo_measure_average_read_size(memory, trials):
    cuckoo_measure_read_size(memory, trials, bucket_cuckoo_bfs_insert, "Bucket Cuckoo BFS Read Size", "bucket_cuckoo_bfs_read_size")



def paths_to_ranges(paths, suffix_size, table_size):
    ranges=[]
    for path in paths:
        path=normalize_path(path, suffix_size, table_size)
        #todo this is a weird patterns, that overloads the path_element class, consider revising.
        #trim the first element from the path
        path=path[1:]
        r = max(path) - min(path)
        if r >= 0:
            ranges.append(r)
    return ranges

def run_insertion_range_trials(trials, insertion_func, table_size, bucket_size, inserts, suffix_size):
    all_results=[]
    entry_size=8
    for i in range(trials):
        collisions, paths = insertion_func(table_size, bucket_size, inserts, get_locations_bounded, suffix_size)
        ranges= paths_to_ranges(paths, suffix_size,table_size)
        ranges = [calculate_read_size(x, bucket_size, entry_size) for x in ranges]
        all_results.extend(ranges)
    all_results.sort()
    return all_results

def cuckoo_insert_range(memory, trials, insertion_func, title, figname):
    bucket=[1,2,4,8,16]
    suffix=[1,2,4,8,16,32,64]
    fill=0.80

    inserts=int(memory * fill)
    percentiles=[0.5,0.9,0.99]
    results = get_sized_result_array(bucket, suffix)

    for bucket_id in range(len(bucket)):
        table_size = get_table_size(memory, bucket[bucket_id])
        for suffix_id in range(len(suffix)):
            print("[cuckoo insert range ("+title+")] bucket size", bucket[bucket_id], "suffix size", suffix[suffix_id])
            results[bucket_id][suffix_id]=run_insertion_range_trials(trials, insertion_func, table_size, bucket[bucket_id], inserts, suffix[suffix_id])

    bucket_suffix_cdf(title, figname, results, bucket, suffix)
    percentile_heatmaps(title, figname, percentiles, results, bucket, suffix, reversed=True)

def bucket_cuckoo_insert_range(memory, trials):
    cuckoo_insert_range(memory, trials, bucket_cuckoo_insert_only, "Insert Difference Bucket Cuckoo", "bucket_cuckoo_insert_range")

def bfs_bucket_cuckoo_insert_range(memory, trials):
    cuckoo_insert_range(memory, trials, bucket_cuckoo_bfs_insert_only, "Insert Difference BFS Bucket Cuckoo", "bucket_cuckoo_bfs_insert_range")

def plot_memory_results(title, figname, memory, mem_results, bucket, suffix):
    fig, ax = plt.subplots()
    for bucket_id in range(len(bucket)):
        for suffix_id in range(len(suffix)):
            results = [x[bucket_id][suffix_id] for x in mem_results]
            ax.plot(memory, results, label="bucket size "+str(bucket[bucket_id])+" suffix size "+str(suffix[suffix_id]))

    ax.set_xlabel("Memory (8 Byte entries)")
    ax.set_ylabel("Fill")
    ax.set_title(title)
    ax.legend()
    plt.tight_layout()
    save_figure(figname)
    plt.clf()

def cuckoo_memory_inserts(trials, insertion_func, title, figname):
    bucket=[4,8]
    suffix=[4,8]
    fill=0.95
    percentiles=[0.5,0.9,0.99]
    results = get_sized_result_array(bucket, suffix)

    memory = [ 1 << i for i in range(4, 10)]
    print(memory)
    mem_results=[]
    for m in memory:
        fill_results, path_results = fill_and_paths(bucket, suffix, m, fill, trials, insertion_func, title)
        mem_results.append(fill_results)

    plot_memory_results(title, figname, memory, mem_results, bucket, suffix)

def bucket_cuckoo_memory_inserts(trials):
    cuckoo_memory_inserts(trials, bucket_cuckoo_insert_only, "Bucket Cuckoo Memory", "bucket_cuckoo_memory")

def bfs_bucket_cuckoo_memory_inserts(trials):
    cuckoo_memory_inserts(trials, bucket_cuckoo_bfs_insert_only, "Bucket Cuckoo BFS Memory", "bucket_cuckoo_bfs_memory")    





memory=1024
trials=1

#size_vs_bound_bucket_cuckoo(memory, trials)
# size_vs_bound_bfs_bucket_cuckoo(memory, trials)

# bucket_cuckoo_measure_average_read_size(memory, trials)
# bfs_cuckoo_measure_average_read_size(memory, trials)

# bucket_cuckoo_insert_range(memory, trials)
# bfs_bucket_cuckoo_insert_range(memory, trials)

bucket_cuckoo_memory_inserts(trials)
#bfs_bucket_cuckoo_memory_inserts(trials)