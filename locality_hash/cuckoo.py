import random
from hash import *
import numpy as np

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


def bucket_cuckoo_insert_key(table, table_size, location_func, location_index, value, bucket_size, suffix, table_values, collisions, path):
    index = location_func(value.key, table_size, suffix)
    index = index[location_index]
    path.append(index)

    #print("value: " + str(value) + "\tindex: " + str(index))

    success = False
    loop=False
    #loop case
    for v in table_values:
        if v.key == value.key:
            success=False
            value=value
            loop=True
            return success, value, loop, collisions, path

    #search for an empty slot in this bucket
    for i in range(0, bucket_size):
        if table[index][i] == None:
            #print("found candidate index: " + '{0: <5}'.format(str(index)) + "\tvalue: " + str(value))
            table[index][i] = value
            success = True
            value=value
            loop=False
            return success, value, loop, collisions, path
    
    #here we have a full bucket we need to evict a candidate
    collisions+=1
    #randomly select an eviction candidate
    table_values.append(value)
    evict_index = random.randint(0, bucket_size-1)

    evict_value = table[index][evict_index]
    table[index][evict_index] = value
    value = evict_value
    return success, value, loop, collisions, path

def bucket_cuckoo_insert(tables, table_size, location_func, value, bucket_size, suffix):
    table_1 = tables[0]
    table_2 = tables[1]
    collisions=0
    success=False
    table_1_values=[]
    table_2_values=[]
    path=[]
    v = value
    while not success:
        success, v, loop, collisions, path = bucket_cuckoo_insert_key(table_1, table_size, location_func, 0, v, bucket_size, suffix, table_1_values, collisions, path)
        if success or loop:
            break
        success, v, loop, collisions, path = bucket_cuckoo_insert_key(table_2, table_size, location_func, 1, v, bucket_size, suffix, table_2_values, collisions, path)
        if success or loop:
            break
    if loop:
        success = False

    return success, collisions, path

class path_element:
    def __init__(self, key, table_index, bucket_index, bucket_offset):
        self.key = key
        self.table_index = table_index
        self.bucket_index = bucket_index
        self.bucket_offset = bucket_offset

    def __str__(self):
        return "(" + str(self.key) + "," + str(self.table_index) + "," + str(self.bucket_index) + "," + str(self.bucket_offset) + ")"

def next_search_location(pe, table_size, location_func, suffix, table_1, table_2):
        table_index=0
        if pe.table_index == 0:
            table_index=1

        index = location_func(pe.key, table_size, suffix)
        index = index[table_index]
        if table_index == 0:
            bucket = table_1[index]
        else:
            bucket = table_2[index]

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

def insert_cuckoo_path(path, tables):
    assert(len(path) >=2)
    for i in reversed(range(0,len(path)-1)):
        tables[path[i+1].table_index][path[i+1].bucket_index][path[i+1].bucket_offset] = path[i].key

def bucket_cuckoo_bfs(table_1,table_2, table_size, location_func, value, bucket_size, suffix, max_depth=5):
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
            continue

        current_pe = search_path[-1]
        bucket, table_index, index = next_search_location(current_pe, table_size, location_func, suffix, table_1, table_2)

        for i in range(bucket_size):
            if bucket[i] == None:
                #found an empty slot
                #print("found empty slot")
                pe = path_element(bucket[i], table_index, index, i)
                search_path.append(pe)
                solution_depth = len(search_path)
                insert_paths.append(search_path.copy())
                #todo we need to set the depth of the path here
                break
            else:
                #found a non empty slot, add to queue
                pe = path_element(bucket[i], table_index, index, i)
                tsp = search_path.copy()
                #don't put loops in the bfs tree
                if not key_causes_a_path_loop(tsp, pe.key):
                    tsp.append(pe)
                    path_queue.append(tsp)
        #print_whole_path_queue(path_queue)   


def bucket_cuckoo_bfs_insert(table_1, table_2, table_size, location_func, value, bucket_size, suffix):
    #begin by performing bfs
    #path queue contains all active search paths. This is useful, but not optimal for reconstructing paths.

    insert_paths=bucket_cuckoo_bfs(table_1, table_2, table_size, location_func, value, bucket_size, suffix, max_depth=5)
    if len(insert_paths) == 0:
        return []

    #choose a random insert path from the available options 
    insert_path = insert_paths[random.randint(0, len(insert_paths)-1)]
    #print("inserting: path:", end='')
    #print_path(insert_path)

    #now we have a path to insert the value
    insert_cuckoo_path(insert_path, [table_1, table_2])
    #print_styled_table(table_1, table_2, table_size, bucket_size)

    return insert_path

        
def bucket_cuckoo_bfs_insert_only(table_size, bucket_size, insertions, location_func, suffix):
    table_1, table_2 = generate_cuckoo_tables(table_size, bucket_size)
    collisions_per_insert=[]
    paths = []
    values=np.arange(insertions)
    for v in values:
        v=entry(v)
        path = bucket_cuckoo_bfs_insert(table_1, table_2, table_size, location_func, v, bucket_size, suffix)
        #print_path(path)
        if path == []:
            break
        collisions = len(path)-2
        collisions_per_insert.append(collisions)
        paths.append(path)
    #print_styled_table(table_1, table_2, table_size, bucket_size)
    return collisions_per_insert, paths




def bucket_cuckoo_insert_only(table_size, bucket_size, insertions, location_func, suffix):
    tables = generate_cuckoo_tables(table_size, bucket_size)
    collisions_per_insert=[]
    paths = []
    values=np.arange(insertions)
    for v in values:
        v=entry(v)
        success, collisions, path = bucket_cuckoo_insert(tables, table_size, location_func, v, bucket_size, suffix)
        if not success:
            break
        collisions_per_insert.append(collisions)
        paths.append(path)
    #print(len(collisions_per_insert))
    #print_styled_table(table_1, table_2, table_size, bucket_size)
    return collisions_per_insert, paths


def bucket_cuckoo_insert_then_measure_reads(table_size, bucket_size, insertions, location_func, suffix):
    table_1, table_2 = generate_cuckoo_tables(table_size, bucket_size)
    collisions_per_insert=[]
    values=np.arange(insertions)
    inserted_values=[]
    read_size=[]
    for v in values:
        v=entry(v)
        success, collisions, path = bucket_cuckoo_insert(tables, table_size, location_func, v, bucket_size, suffix)
        if not success:
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
        #print(diff)

    #print_styled_table(table_1, table_2, table_size, bucket_size)
    return read_size