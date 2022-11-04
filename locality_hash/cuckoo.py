import random
from hash import *
import numpy as np
import copy

class entry:
    def __init__(self, key):
        self.key = key
    def __str__(self):
        return("Key: " + str(self.key))


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
            
def generate_insertions(insertions,deterministic=True):
    if deterministic:
        return np.arange(insertions)
    else:
        values=[None] * insertions
        for i in range(0,insertions):
            values[i]=random.randint(0,1<<32)
        return values
            


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

########################## Bucket Cuckoo Hash ###############################
########################## Bucket Cuckoo Hash ###############################
########################## Bucket Cuckoo Hash ###############################

def print_bucket_table(table, table_size, bucket_size):
    for i in range(0, table_size):
        for j in range(0, bucket_size):
            print("[" + '{0: <5}'.format(str(table[i][j])) + "] ", end='')
            #print("Table: " + str(i) + "\tBucket: " + str(j) + "\tValue: " + str(table[i][j]))
        print("")

def print_styled_table(table_1,table_2, table_size, bucket_size):
    print_bucket_table(table_1, table_size, bucket_size)
    print("-"*30)
    print_bucket_table(table_2, table_size, bucket_size)
    print("-"*30)
    print("-"*30)

def generate_cuckoo_table(table_size, bucket_size):
    table = [] * table_size
    for i in range(0, table_size):
        table.append([None] * bucket_size)
    return table

def generate_cuckoo_tables(table_size, bucket_size):
    table_1 = generate_cuckoo_table(table_size, bucket_size)
    table_2 = generate_cuckoo_table(table_size, bucket_size)
    return table_1, table_2

class path_element:
    def __init__(self, key, table_index, bucket_index, bucket_offset):
        self.key = key
        self.table_index = table_index
        self.bucket_index = bucket_index
        self.bucket_offset = bucket_offset

    def __str__(self):
        return "(" + str(self.key) + "," + str(self.table_index) + "," + str(self.bucket_index) + "," + str(self.bucket_offset) + ")"

    def __gt__(self, other):
        return self.bucket_index > other.bucket_index

    def __sub__(self, other):
        return self.bucket_index - other.bucket_index


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
        if r >= 0:
            ranges.append(r)
    return ranges

def min_range_index(paths, suffix_size, table_size):
    p2 = copy.deepcopy(paths)
    ranges = paths_to_ranges(p2, suffix_size, table_size)
    min_index=0
    min_range=ranges[0]
    for i in range(1, len(ranges)-1):
        if ranges[i] < min_range:
            min_index=i
            min_range=ranges[i]
    return min_index


def key_causes_a_path_loop(path, key):
    for pe in path:
        if pe.key == key:
            return True
    return False

def next_table_index(table_index):
    if table_index == 0:
        return 1
    else:
        return 0

def bucket_cuckoo_insert_key(tables, table_size, location_func, pe, bucket_size, suffix, path):
    bucket, table_index, index = next_search_location(pe, table_size, location_func, suffix, tables)
    #search for an empty slot in this bucket
    if None in bucket:
        i = bucket.index(None)
        bucket[i] = entry(pe.key)
        pe = path_element(None, table_index, index, i)
        path.append(pe)
        return True, path
    
    #here we have a full bucket we need to evict a candidate
    #randomly select an eviction candidate
    evict_index = random.randint(0, bucket_size-1)
    evict_value = bucket[evict_index]
    bucket[evict_index] = entry(pe.key)

    pe = path_element(evict_value.key, table_index, index, evict_index)
    if key_causes_a_path_loop(path, pe.key):
        path = []
    else:
        path.append(pe)
    
    return False, path


def bucket_cuckoo_insert(tables, table_size, location_func, value, bucket_size, suffix):
    success=False
    pe = path_element(value.key, -1,-1,-1)
    path=[pe]
    while not success:
        success, path = bucket_cuckoo_insert_key(tables, table_size, location_func, path[-1], bucket_size, suffix, path)
        if path == []:
            break
    return path


def next_search_location(pe, table_size, location_func, suffix, tables):
        table_index=next_table_index(pe.table_index)
        index = location_func(pe.key, table_size, suffix)
        index = index[table_index]
        bucket = tables[table_index][index]
        return bucket, table_index, index

def key_causes_a_path_loop(path, key):
    for pe in path:
        if pe.key == key:
            return True
    return False

def print_whole_path_queue(path_queue):
        for path in path_queue:
            print_path(path)


def print_path(path):
    print("[", end='')
    for e in path:
        print(str(e), end='')
    print("]")

def find_closest_target(tables, table_size, location_func, value, bucket_size, suffix):
    index = location_func(value, table_size, suffix)
    closest_target = -1
    index = index[0]
    while closest_target == -1:
        for table in tables:
            bucket = table[index]
            for slot in bucket:
                if slot == None:
                    closest_target = index
                    break
        index = index+1
    print("Closest Target:" + str(closest_target))



def bucket_cuckoo_a_star(tables, table_size, location_func, value, bucket_size, suffix):
    closest_target = find_closest_target(tables, table_size, location_func, value, bucket_size, suffix)

def bucket_cuckoo_a_star_insert(tables, table_size, location_func, value, bucket_size, suffix):
    insert_paths=bucket_cuckoo_a_star(tables, table_size, location_func, value, bucket_size, suffix)

    if len(insert_paths) == 0:
        print("Failed Insert: " + str(value))
        print_styled_table(tables[0],tables[1], table_size, bucket_size)
        return []
    #select a random path from a star
    insert_path = insert_paths[random.randint(0, len(insert_paths)-1)]
    insert_cuckoo_path(insert_path, tables)
    return insert_path


def bucket_cuckoo_bfs(tables, table_size, location_func, value, bucket_size, suffix, max_depth):
    path_queue = []
    pe = path_element(value, -1,-1,-1)
    search_item = [pe]
    path_queue.append(search_item)

    insert_paths = []

    solution_depth = max_depth
    while True:
        if len(path_queue) == 0:
            return insert_paths
        
        search_path = path_queue.pop(0)
        if len(search_path) >= solution_depth:
            # print("max len: " + str(solution_depth))
            # print_path(search_path)
            continue

        current_pe = search_path[-1]
        bucket, table_index, index = next_search_location(current_pe, table_size, location_func, suffix, tables)

        for i in range(bucket_size):
            if bucket[i] == None:
                #found an empty slot
                #print("found empty slot")
                pe = path_element(bucket[i], table_index, index, i)
                search_path.append(pe)
                solution_depth = len(search_path)
                insert_paths.append(copy.deepcopy(search_path))
                #todo we need to set the depth of the path here
                break
            else:
                #found a non empty slot, add to queue
                pe = path_element(bucket[i], table_index, index, i)
                tsp = copy.deepcopy(search_path)
                #don't put loops in the bfs tree
                if not key_causes_a_path_loop(tsp, pe.key):
                    tsp.append(pe)
                    path_queue.append(tsp)
        #print_whole_path_queue(path_queue)   

def insert_cuckoo_path(path, tables):
    assert(len(path) >=2)
    for i in reversed(range(0,len(path)-1)):
        tables[path[i+1].table_index][path[i+1].bucket_index][path[i+1].bucket_offset] = path[i].key


def bucket_cuckoo_bfs_insert(tables, table_size, location_func, value, bucket_size, suffix):
    #begin by performing bfs
    #path queue contains all active search paths. This is useful, but not optimal for reconstructing paths.

    insert_paths=bucket_cuckoo_bfs(tables, table_size, location_func, value, bucket_size, suffix, max_depth=8)
    if len(insert_paths) == 0:
        print("Failed Insert")
        print_styled_table(tables[0],tables[1], table_size, bucket_size)
        return []

    #choose a random insert path from the available options 
    insert_path = insert_paths[random.randint(0, len(insert_paths)-1)]
    #min insert range
    #insert_path = insert_paths[min_range_index(insert_paths, suffix, table_size)]
    #print("inserting: path:", end='')
    #print_path(insert_path)

    #now we have a path to insert the value
    insert_cuckoo_path(insert_path, tables)
    #print_styled_table(table_1, table_2, table_size, bucket_size)

    return insert_path



def paths_to_collisions(paths):
    collisions = [None] * len(paths)
    for i in range(len(paths)):
        collisions[i] = len(paths[i]) -2
    return collisions

def cuckoo_insert_only(insert_func, table_size, bucket_size, insertions, location_func, suffix):
    tables = generate_cuckoo_tables(table_size, bucket_size)
    paths = []
    values=generate_insertions(insertions)

    for v in values:
        v=entry(v)
        path = insert_func(tables, table_size, location_func, v, bucket_size, suffix)
        if path == []:
            break
        paths.append(path)
    #print(len(collisions_per_insert))
    #print_styled_table(table_1, table_2, table_size, bucket_size)
    return paths_to_collisions(paths), paths

        
def bucket_cuckoo_bfs_insert_only(table_size, bucket_size, insertions, location_func, suffix):
    return cuckoo_insert_only(bucket_cuckoo_bfs_insert, table_size, bucket_size, insertions, location_func, suffix)

def bucket_cuckoo_insert_only(table_size, bucket_size, insertions, location_func, suffix):
    return cuckoo_insert_only(bucket_cuckoo_insert, table_size, bucket_size, insertions, location_func, suffix)

def bucket_cuckoo_a_star_insert_only(table_size, bucket_size, insertions, location_func, suffix):
    return cuckoo_insert_only(bucket_cuckoo_a_star_insert, table_size, bucket_size, insertions, location_func, suffix)

def cuckoo_insert_then_measure_reads(insert_func, table_size, bucket_size, insertions, location_func, suffix):
    tables = generate_cuckoo_tables(table_size, bucket_size)
    values=generate_insertions(insertions)
    inserted_values=[]
    read_size=[]
    for v in values:
        v=entry(v)
        path = insert_func(tables, table_size, location_func, v, bucket_size, suffix)
        if path == []:
            break
        inserted_values.append(v)
    #measure reads

    #print_styled_table(table_1, table_2, table_size, bucket_size)
    for v in inserted_values:
        location_1 = location_func(v.key, table_size, suffix)[0]
        location_2 = location_func(v.key, table_size, suffix)[1]
        #print("L1: "+str(location_1)+"\tL2: "+str(location_2))
        diff = location_2 - location_1
        if diff < 0:
            continue
        else:
            read_size.append(diff)

    #print_styled_table(table_1, table_2, table_size, bucket_size)
    return read_size


def bucket_cuckoo_insert_then_measure_reads(table_size, bucket_size, insertions, location_func, suffix):
    return cuckoo_insert_then_measure_reads(bucket_cuckoo_insert, table_size, bucket_size, insertions, location_func, suffix)