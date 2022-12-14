from cuckoo import *
from locality_hash_collisions import *
import hash
from hash import secondary_bounded_location_exp_extern

from doctest import master
from symbol import return_stmt
from tqdm import tqdm
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
    plt.grid()
    save_figure(figname+ "_cdf")
    plt.clf()

def plot_memory_results_2(title, figname, memory, mem_resuts, ids):
    fig, ax = plt.subplots()
    print(memory)
    print(mem_resuts)
    for id, exp in zip(ids, mem_resuts):
        results = [np.average(x) for x in exp]
        std = [np.std(x) for x in exp]
        ax.errorbar(memory, results, std, label=id, marker='o', capsize=3)

    ax.set_xlabel("Memory (8 Byte entries)")
    ax.set_ylabel("Fill")
    ax.set_title(title)
    ax.set_xscale('log')
    ax.legend()
    plt.tight_layout()
    save_figure(figname)
    plt.clf()

def plot_memory_results(title, figname, memory, mem_results, bucket, suffix):
    fig, ax = plt.subplots()
    for bucket_id in range(len(bucket)):
        for suffix_id in range(len(suffix)):
            for x in mem_results:
                print(x)
            results = [np.average(x[bucket_id][suffix_id]) for x in mem_results]
            label_str="b-"+str(bucket[bucket_id])+"_s-"+str(suffix[suffix_id])
            std = [np.std(x[bucket_id][suffix_id]) for x in mem_results]
            print(label_str + "_fill=" + str(results))
            print(label_str + "_x=" + str(memory))
            print(label_str + "_std=" + str(std))
            ax.errorbar(memory, results, std, label=label_str, marker='o', capsize=3)

    ax.set_xlabel("Memory (8 Byte entries)")
    ax.set_ylabel("Fill")
    ax.set_title(title)
    ax.set_xscale('log')
    ax.legend()
    plt.tight_layout()
    save_figure(figname)
    plt.clf()

def plot_table_fills(title, figname, memory, table_results, bucket, suffix):
    print(len(table_results))
    fig, axs = plt.subplots(len(table_results)+1)
    print(len(axs))
    counter=0
    for bucket_id in range(len(bucket)):
        for suffix_id in range(len(suffix)):

            table_refs = table_results[bucket_id][suffix_id]
            table_refs = table_refs[0]
            print(table_refs)
            axs[counter].bar(np.arange(len(table_refs)),table_refs)
            axs[counter].set_xlabel("Memory (8 Byte entries)")
            axs[counter].set_ylabel("Fill")
            axs[counter].set_title(title)
            # axs[counter].set_xscale('log')
            axs[counter].legend()
            counter+=1
            # results = [np.average(x[bucket_id][suffix_id]) for x in mem_results]
            # std = [np.std(x[bucket_id][suffix_id]) for x in mem_results]
            # ax.errorbar(memory, results, std, label="b-"+str(bucket[bucket_id])+" s-"+str(suffix[suffix_id]), marker='o', capsize=3)

    
    plt.tight_layout()
    save_figure(figname)
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


def visualize_distance(tables, suffix_size, d_func):
    dtables=[]
    zero = -5
    minimum=1 << 32
    maximum=zero
    for table_index in range(len(tables)):
        distance=copy.deepcopy(tables[table_index])
        for i in range(len(distance)):
            for j in range(len(distance[i])):
                if distance[i][j] == None:
                    distance[i][j] = zero
                else:
                    d = d_func(distance[i][j], len(distance), suffix_size)
                    d = normalize_distance(d[0], d[1], len(distance))
                    distance[i][j] = d
                if distance[i][j] < minimum:
                    minimum = distance[i][j]
                elif distance[i][j] > maximum:
                    maximum = distance[i][j]
        dtables.append(distance)
    
    fig, axs = plt.subplots(nrows=1, ncols=len(dtables), figsize=(5,6))
    for i in range(len(dtables)):
        im = axs[i].imshow(dtables[i], cmap="hot_r", vmin=minimum, vmax=maximum)
        #if i == len(dtables)-1:
        fig.colorbar(im, ax=axs[i], label="Distance")
        axs[i].set_title("Table "+str(i))
        axs[i].set_xlabel("bucket id")
        axs[i].set_ylabel("index")

    fig.tight_layout(pad=3)
    save_figure("debug_distance")
    plt.clf()


def visualize_table(tables, suffix_size, options):
    if options["print_table"]:    
        print_styled_table(tables, len(tables[0]), len(tables[0][0]))
    if options["distance"]:
        if "distance_func" in options:
            d_func = options["distance_func"]
        else:
            print("using default distance function (get_locations_bounded) for visualization")
            d_func = get_locations_bounded
        visualize_distance(tables, suffix_size, d_func)

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

def total_inserts(memory, r):
    return int(memory * r)

def normalize_to_memory(master_table, memory, fill):
    inserts=total_inserts(memory, fill)
    table=[(x/inserts)*fill for x in master_table]
    return table

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
        inserts = total_inserts(memory,r)
        master_table=[]
        for i in range(trials):
            collisions = bucket_cuckoo_insert_only(table_size, bucket_size, inserts, get_locations_bounded, suffix)
            master_table.append(len(collisions))

        master_table = normalize_to_memory(master_table, memory, r)
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

def run_insertion_fill_trials(trials, insertion_func, location_func, table_size, bucket_size, inserts, suffix_size):
    all_results=[]
    for i in range(trials):
        collisions, paths, _ = insertion_func(table_size, bucket_size, inserts, location_func, suffix_size)
        all_results.append(paths)
    return all_results

def run_table_density_trials(trials, insertion_func, location_func, table_size, bucket_size, inserts, suffix_size):
    all_results=[]
    for i in range(trials):
        _, paths, table = insertion_func(table_size, bucket_size, inserts, location_func, suffix_size)
        all_results.append(table)
    return all_results

def fill_table(bucket, suffix, memory, fill, trials, insertion_func, location_func, title):
    inserts=total_inserts(memory, fill)
    table_results = get_sized_result_array(bucket, suffix)
    for bucket_id in range(len(bucket)):
        table_size = get_table_size(memory, bucket[bucket_id])
        for suffix_id in range(len(suffix)):
            print("[table fill ("+title+")] memory: "+str(memory)+" bucket size:", bucket[bucket_id], " suffix size: ", suffix[suffix_id])
            all_tables=run_table_density_trials(trials, insertion_func, location_func, table_size, bucket[bucket_id], inserts, suffix[suffix_id])
            #todo average aross tables?
            assert len(all_tables) == trials, "all tables: " + str(len(all_tables)) + " trials: " + str(trials) + "mismatch here something is wrong"
            tables = all_tables[0]

            #print_styled_table(tables, table_size, suffix[suffix_id])
            #print_styled_table(tables, table_size, suffix[suffix_id])
            options={"print_table":False, "distance":True, "distance_func":location_func}
            visualize_table(tables,suffix[suffix_id],options)

            table_ref = [0] * len(tables[0])
            for bucket in tables[0]:
                for value in bucket:
                    search_index = get_locations_bounded(value, table_size, suffix[suffix_id])
                    # table_index = 1
                    # search_index = search_index[table_index]
                    table_ref[search_index[1]] += 1
                    table_ref[search_index[0]] += 1
            table_results[bucket_id][suffix_id]=table_ref
    return table_results



def all_paths_to_path_length(all_paths):
    all_paths_lengths=[]
    for paths in all_paths:
        all_paths_lengths.extend(paths_to_collisions(paths))
    all_paths_lengths.sort()
    return all_paths_lengths

def fill_and_paths(bucket, suffix, memory, fill, trials, insertion_func, location_func, title):
    inserts=total_inserts(memory, fill)
    fill_results = get_sized_result_array(bucket, suffix)
    path_results = get_sized_result_array(bucket, suffix)
    for bucket_id in range(len(bucket)):
        table_size = get_table_size(memory, bucket[bucket_id])
        for suffix_id in range(len(suffix)):
            print("[fill and paths ("+title+")] memory: "+str(memory)+" bucket size:", bucket[bucket_id], " suffix size: ", suffix[suffix_id])
            all_paths=run_insertion_fill_trials(trials, insertion_func, location_func, table_size, bucket[bucket_id], inserts, suffix[suffix_id])

            # Measure how full each table got before it broke
            fills=[len(paths) for paths in all_paths]
            fills.sort()
            fills=normalize_to_memory(fills, memory, fill)
            fill_results[bucket_id][suffix_id]=fills

            #measure the path length
            path_results[bucket_id][suffix_id]=all_paths_to_path_length(all_paths)
    return fill_results, path_results

def size_vs_bound(memory, trials, insertion_func, title, figname):
    bucket=[1,2,4,8]
    suffix=[1,2,3,4]
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

def size_vs_bound_a_star_bucket_cuckoo(memory,trials):
    size_vs_bound(memory, trials, bucket_cuckoo_a_star_insert_only, "Bucket Cuckoo A* Bound", "bucket_vs_bound_a_star")

def calculate_read_size(distance, bucket_size, entry_size):
    return (bucket_size * entry_size * 2) * (1 + distance)

def run_read_size_trials(trials, insertion_func, location_func, table_size, bucket_size, inserts, suffix_size):
    entry_size=8
    master_read_sizes=[]
    for i in range(trials):
        read_sizes = cuckoo_insert_then_measure_reads(insertion_func, table_size, bucket_size, inserts, location_func, suffix_size)
        read_sizes = [calculate_read_size(x, bucket_size, entry_size) for x in read_sizes]
        master_read_sizes.extend(read_sizes)
    master_read_sizes.sort()
    return master_read_sizes


def cuckoo_measure_read_size(memory, trials, insertion_func, location_func, title, figname):
    bucket=[8]
    suffix=[2]
    percentiles=[0.5, 0.9, 0.99]
    results = get_sized_result_array(bucket, suffix)
    fill=0.95

    inserts=total_inserts(memory, fill)
    for bucket_id in range(len(bucket)):
        table_size = get_table_size(memory, bucket[bucket_id])
        for suffix_id in range(len(suffix)):
            print("[cuckoo read size ("+title+")] bucket size", bucket[bucket_id], "suffix size", suffix[suffix_id])
            results[bucket_id][suffix_id]=run_read_size_trials(trials, insertion_func, location_func, table_size, bucket[bucket_id], inserts, suffix[suffix_id])

    bucket_suffix_cdf(title, figname, results, bucket, suffix)
    percentile_heatmaps(title, figname, percentiles, results, bucket, suffix, reversed=True)

def bucket_cuckoo_measure_average_read_size(memory, trials):
    cuckoo_measure_read_size(memory, trials, bucket_cuckoo_insert, get_locations_bounded, "Bucket Cuckoo Read Size", "bucket_cuckoo_read_size")

def bfs_cuckoo_measure_average_read_size(memory, trials):
    cuckoo_measure_read_size(memory, trials, bucket_cuckoo_bfs_insert, get_locations_bounded, "Bucket Cuckoo BFS Read Size", "bucket_cuckoo_bfs_read_size")

def a_star_cuckoo_measure_average_read_size(memory, trials):
    cuckoo_measure_read_size(memory, trials, bucket_cuckoo_a_star_insert, get_locations_bounded, "Bucket Cuckoo A Star Read Size", "bucket_cuckoo_a_star_read_size")

def a_star_cuckoo_measure_read_hash_comp(memory, trials):
    # cuckoo_measure_read_size(memory, trials, bucket_cuckoo_a_star_insert, get_locations_bounded, "A Star Bounded", "read_a_star_bounded")
    cuckoo_measure_read_size(memory, trials, bucket_cuckoo_a_star_insert, get_locations_bounded_exp, "A Star exp", "read_a_star_exp")
    #cuckoo_measure_read_size(memory, trials, bucket_cuckoo_a_star_insert, get_locations_bounded_phi, "A Star phi", "read_a_star_phi")
    #cuckoo_measure_read_size(memory, trials, bucket_cuckoo_a_star_insert, get_locations_bounded_fixed, "A Star fixed", "read_a_star_fixed")




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
        collisions, paths, _ = insertion_func(table_size, bucket_size, inserts, get_locations_bounded, suffix_size)
        ranges= paths_to_ranges(paths, suffix_size,table_size)
        ranges = [calculate_read_size(x, bucket_size, entry_size) for x in ranges]
        all_results.extend(ranges)
    all_results.sort()
    return all_results

def cuckoo_insert_range(memory, trials, insertion_func, title, figname):
    bucket=[1,2,4,8,16]
    suffix=[1,2,4,8,16,32,64]
    fill=0.80

    inserts=total_inserts(memory, fill)
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


def cuckoo_memory_inserts(trials, insertion_func, location_func, title, figname):
    bucket=[8]
    suffix=[2]

    fill=0.95
    percentiles=[0.5,0.9,0.99]
    results = get_sized_result_array(bucket, suffix)

    #24 is 8 million entries
    #21 is 1 million
    maximum=23
    minimum=5
    memory = [ 1 << i for i in range(minimum, maximum)]
    #memory = [128]
    print(memory)
    mem_results=[]
    for m in memory:
        fill_results, path_results = fill_and_paths(bucket, suffix, m, fill, trials, insertion_func, location_func, title)
        mem_results.append(fill_results)
    plot_memory_results(title, figname, memory, mem_results, bucket, suffix)

def bucket_cuckoo_memory_inserts(trials):
    cuckoo_memory_inserts(trials, bucket_cuckoo_insert_only, get_locations_bounded_exp, "Bucket Cuckoo Memory", "bucket_cuckoo_memory")

def bfs_bucket_cuckoo_memory_inserts(trials):
    cuckoo_memory_inserts(trials, bucket_cuckoo_bfs_insert_only, get_locations_bounded_exp, "Bucket Cuckoo BFS Memory", "bucket_cuckoo_bfs_memory")    

def a_star_bucket_cuckoo_memory_inserts(trials):
    cuckoo_memory_inserts(trials, bucket_cuckoo_a_star_insert_only, get_locations_bounded_exp, "Bucket Cuckoo A* Memory", "bucket_cuckoo_a_star_memory")

def a_star_bucket_cuckoo_memory_hashes(trials):
    cuckoo_memory_inserts(trials, bucket_cuckoo_a_star_insert_only, get_locations_bounded_exp, "Bucket Cuckoo A* Memory exp", "bucket_cuckoo_a_star_memory_exp")
    cuckoo_memory_inserts(trials, bucket_cuckoo_a_star_insert_only, get_locations_bounded_phi, "Bucket Cuckoo A* Memory phi", "bucket_cuckoo_a_star_memory_phi")
    #cuckoo_memory_inserts(trials, bucket_cuckoo_a_star_insert_only, get_locations_bounded_fixed, "Bucket Cuckoo A* Memory fixed", "bucket_cuckoo_a_star_memory_fixed")

def exp_hash_memory(trials, insertion_func, location_fun, title, figname):
    #from hash import global_exp
    exps=[1.7,1.8,1.9,2.0]
    ids=[str(x) for x in exps]
    bucket=[8]
    suffix=[2]

    fill=0.95
    #24 is 8 million entries
    #21 is 1 million
    maximum=22
    minimum=19
    memory = [ 1 << i for i in range(minimum, maximum)]
    results=[]
    for exp in exps:
        #global_exp = exp

        #secondary_bounded_location_exp_extern.global_exp=exp
        hash.global_exp=exp
        m_results=[]
        for m in memory:
            fill_results, path_results = fill_and_paths(bucket, suffix, m, fill, trials, insertion_func, location_fun, title + " " + str(exp))
            fill_results = [item for sublist in fill_results for item in sublist]
            fill_results = [item for sublist in fill_results for item in sublist]

            m_results.append(fill_results)
        results.append(m_results)
    plot_memory_results_2(title, figname, memory, results, ids)

def a_star_exp_hash(trials):
    exp_hash_memory(trials, bucket_cuckoo_a_star_insert_only, get_locations_bounded_exp_extern, "A* exp tests", "exp_hash_exp")


def cuckoo_table_density(trials, insertion_func, location_func,title, figname):
    bucket=[16]
    suffix=[8]
    percentiles=[0.5,0.9,0.99]
    results = get_sized_result_array(bucket, suffix)

    #24 is 8 million entries
    #20 is 1 million
    # maximum=18
    # memory = [ 1 << i for i in range(6, maximum)]

    memory = [1024* 4]
    print(memory)
    table_results=[]
    for m in memory:
        table_fills = fill_table(bucket, suffix, m, 0.95, trials, insertion_func, location_func, title)
        table_results.append(table_fills)

    

    plot_table_fills(title, figname, memory, table_results, bucket, suffix)

def a_star_bucket_cuckoo_table_density(trials):
    #cuckoo_table_density(trials, bucket_cuckoo_a_star_insert_only, get_locations_bounded_exp, "Bucket Cuckoo A* Table Density", "bucket_cuckoo_a_star_table_density")
    cuckoo_table_density(trials, bucket_cuckoo_a_star_insert_only, get_locations_bounded, "Bucket Cuckoo A* Table Density", "bucket_cuckoo_a_star_table_density")

def bfs_bucket_cuckoo_table_density(trials):
    cuckoo_table_density(trials, bucket_cuckoo_bfs_insert_only, get_locations_bounded_exp, "Bucket Cuckoo A* Table Density", "bucket_cuckoo_a_star_table_density")

def plot_hash_distribution(title, figname, ids, results):
    fig, ax = plt.subplots()
    for r, id in zip(reversed(results), reversed(ids)):
        hist = dict()
        print("id: "+str(id)+" Average: "+str(np.average(r)), " Median: "+str(np.median(r)))
        for x in r:
            hist[x] = hist.get(x, 0) + 1
        hist = sorted(hist.items())
        #print("suffix", s, "x", [x[0] for x in hist], "y", [x[1] for x in hist])
        x, y = zip(*hist)
        y = list(y)
        sy= sum(y)
        y = [x / sy for x in y]
        ax.plot(x, y, label=id)
        #ax.hist(r, bins=1000, label="suffix size "+str(s))
    ax.set(xlabel='hash distance', ylabel='count (normalized)', title=title)
    ax.set_xscale('log', base=2)
    ax.set_yscale('log')
    ax.legend()
    plt.tight_layout()
    save_figure(figname)



def test_hash_distribution(trials, title, figname):
    #suffix=[1,2,4,8,16,32,64]
    suffix=[2,3,4,5,6]
    base=[2]
    exp=[2]
    results=[]
    ids=[]
    insertions = 100000
    table_size = 100000
    for s in suffix:
        for b in base:
            for e in exp:
                run_results=[]
                print("suffix", s, "base", b, "exp", e)
                id="s"+str(s)+":b"+str(b)+":e"+str(e)
                for v in tqdm(np.arange(insertions), leave=False):
                    index=get_locations_bounded_test(v,table_size,s,b,e)
                    distance = normalize_distance(index[0],index[1],table_size)
                    run_results.append(distance)
                results.append(run_results)
                ids.append(id)
    plot_hash_distribution(title, figname, ids, results)
            



memory=1024 * 64
#memory=1024 * 1024
trials=3

#size_vs_bound_bucket_cuckoo(memory, trials)
#size_vs_bound_bfs_bucket_cuckoo(memory, trials)
#size_vs_bound_a_star_bucket_cuckoo(memory,trials)

# bucket_cuckoo_measure_average_read_size(memory, trials)
# bfs_cuckoo_measure_average_read_size(memory, trials)
# a_star_cuckoo_measure_average_read_size(memory, trials)

# bucket_cuckoo_insert_range(memory, trials)
# bfs_bucket_cuckoo_insert_range(memory, trials)

# bucket_cuckoo_memory_inserts(trials)
# bfs_bucket_cuckoo_memory_inserts(trials)
#a_star_bucket_cuckoo_memory_inserts(trials)

#a_star_bucket_cuckoo_table_density(trials)
#bfs_bucket_cuckoo_table_density(trials)

#test_hash_distribution(trials, "Hash Distribution", "hash_distribution")

#a_star_cuckoo_measure_read_hash_comp(memory, trials)
# a_star_bucket_cuckoo_memory_hashes(trials)

a_star_exp_hash(trials)
