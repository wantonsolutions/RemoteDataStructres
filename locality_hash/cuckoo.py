import random
from hash import *
import numpy as np
import copy
import heapq
from tqdm import tqdm

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
            
def generate_insertions(insertions,deterministic=False):
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

def print_styled_table(tables, table_size, bucket_size):
    for table in tables:
        print_bucket_table(table, table_size, bucket_size)
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

def bucket_has_empty(bucket):
    for slot in bucket:
        if slot == None:
            return True

def find_closest_target_n(tables, table_size, location_func, value, bucket_size, suffix, n):
    index = location_func(value.key, table_size, suffix)
    index = index[0]
    results=[]
    while len(results) < n:
        for table_index in range(len(tables)):
            bucket = tables[table_index][index]
            if bucket_has_empty(bucket):
                results.append((table_index, index))
        index = (index+1) % len(tables[table_index])
        assert index < table_size, "TODO Implement modulo wrap around in find closest target"
    return results

def find_closest_target_n_bi(tables, table_size, location_func, value, bucket_size, suffix, n):
    index = location_func(value.key, table_size, suffix)
    index_1 = index[0]
    index_2 = index[0] - 1
    if index_2 < 0:
        index_2 = table_size - 1
    
    results=[]
    while len(results) < n:
        for table_index in range(len(tables)):
            bucket = tables[table_index][index_1]
            if bucket_has_empty(bucket):
                results.append((table_index, index_1))
            bucket = tables[table_index][index_2]
            if bucket_has_empty(bucket):
                results.append((table_index, index_2))
        index_1 = (index_1+1) % len(tables[table_index])
        index_2 = (index_2-1)
        if index_2 < 0:
            index_2 = table_size - 1
        assert index_1 < table_size, "TODO Implement modulo wrap around in find closest target"
        assert index_2 < table_size and index_2 >= 0, "TODO something wrong with look back index"
    return results

#todo fix heuristic
def heuristic(current_index, current_table, target_index, target_table, table_size, suffix_size):
    if (target_index < current_index):
        target_index += table_size
    distance = abs(current_index - target_index)
    if current_table != target_table:
        distance += 1
    return distance

def heuristic_2(current_index, current_table, target_index, target_table,table_size, suffix_size):
    if (target_index == current_index and current_table == target_table):
        return 0
    # if we are on the first table

    #distance between indexes is target - current max travel distance per hash
    #step is suffix size - 1 Given that we already handled the 0 distance case
    #we add 1 because if we have to move, it is at least one step multiply by
    #two because if we stay in the same table it takes 2 steps. For each shuffle
    distance = (((target_index - current_index) / (suffix_size - 1))+1) * 2

    #we only need to make a correction if the tables are not the same, otherwise the step direction does not matter
    if (target_table != current_table):
        #the table we are in determines the number of shuffles.
        #table 1 moves backwards more easily, while table 0 moves forward more easily (by one step)
        #travel direction is the -1 or +1 move depending on what table we are in
        travel_direction = 1
        if (current_table == 1):
            travel_direction *= -1

        #we need to know what direction we are traveling in, as stated before, if we
        #are moving forward distance > 0 than table 1 will have a -1 step advantage
        #over table 2 and vice versa.
        if (distance > 0):
            travel_direction *= -1
        
        distance += travel_direction
    
    return distance

#designed for exp
def heuristic_3(current_index, current_table, target_index, target_table,table_size, suffix_size):
    if (target_index == current_index and current_table == target_table):
        return 0

    if suffix_size > 1:
        distance = (((target_index - current_index) / ((suffix_size*suffix_size) - 1))+1) * 2
    else:
        distance = (target_index - current_index)*2

    #we only need to make a correction if the tables are not the same, otherwise the step direction does not matter
    if (target_table != current_table):
        #the table we are in determines the number of shuffles.
        #table 1 moves backwards more easily, while table 0 moves forward more easily (by one step)
        #travel direction is the -1 or +1 move depending on what table we are in
        travel_direction = 1
        if (current_table == 1):
            travel_direction *= -1

        #we need to know what direction we are traveling in, as stated before, if we
        #are moving forward distance > 0 than table 1 will have a -1 step advantage
        #over table 2 and vice versa.
        if (distance > 0):
            travel_direction *= -1
        
        distance += travel_direction
    
    return distance




def fscore(element, target_index, target_table, table_size, suffix_size):
        g_score = element.distance
        #h_score = heuristic(element.pe.bucket_index,element.pe.table_index, target_index, target_table,table_size, suffix_size)
        #h_score = heuristic_2(element.pe.bucket_index,element.pe.table_index, target_index, target_table,table_size, suffix_size)
        h_score = heuristic_3(element.pe.bucket_index,element.pe.table_index, target_index, target_table,table_size, suffix_size)
        f_score = g_score + h_score
        return f_score

class a_star_pe:
    def __init__ (self, pe, prior, distance, score):
        self.pe = pe
        self.prior = prior
        self.distance = distance
        self.fscore = score
    def __lt__(self, other):
        return self.fscore < other.fscore
    def __str__(self) -> str:
        return "pe:" + str(self.pe) + " prior:" + str(self.prior) + " distance:" + str(self.distance) + "fscore: " + str(self.fscore)

def a_star_pe_in_list(aspe, list):
    for e in list:
        if e.pe.key == aspe.pe.key:
            return True
    return False

def pop_open_list(open_list, open_list_map):
    val = heapq.heappop(open_list)
    del open_list_map[val.pe.key]
    return val

def push_open_list(open_list, open_list_map, aspe):
    heapq.heappush(open_list,aspe)
    open_list_map[aspe.pe.key] = aspe

def open_list_contains(open_list_map, aspe):
    return aspe.pe.key in open_list_map

def push_closed_list(closed_list_map, aspe):
    closed_list_map[aspe.pe.key] = aspe

def closed_list_contains(closed_list_map, aspe):
    return aspe.pe.key in closed_list_map

def bucket_cuckoo_a_star(tables, table_size, location_func, value, bucket_size, suffix):
    # print("-"*80)
    #targets = find_closest_target_n(tables, table_size, location_func, value, bucket_size, suffix, 10)
    targets = find_closest_target_n_bi(tables, table_size, location_func, value, bucket_size, suffix, 20)
    #print(targets)

    while (len(targets) > 0):
        target_table, target_index = targets.pop(0)
        starting_pe = path_element(value.key, -1, -1, -1)
        search_element = a_star_pe(starting_pe, prior=None, distance=0, score=0)

        open_list = []
        open_list_map=dict()
        push_open_list(open_list, open_list_map, search_element)

        closed_list_map=dict()

        found = False
        while len(open_list) > 0:
            # min_index, min_fscore = find_lowest_f_score(open_list, target_table, target_index, table_size)
            # search_element = open_list.pop(min_index)#[min_index]
            search_element = pop_open_list(open_list, open_list_map)
            push_closed_list(closed_list_map, search_element)

            search_index = location_func(search_element.pe.key, table_size, suffix)
            table_index = next_table_index(search_element.pe.table_index)
            search_index = search_index[table_index]

            # print("Search: ("+str(table_index)+","+str(search_index) + ")"+ " Closest Target:("+ str(target_table) + "," + str(target_index)+")")
            #Check if any slots are open for the search element
            for i in range(bucket_size):
                if tables[table_index][search_index][i] == None:
                    # print("Found Slot:" + str(search_element.pe))
                    search_element = a_star_pe(pe = path_element(tables[table_index][search_index][i],table_index,search_index,i), prior=search_element, distance=search_element.distance+1, score=0)
                    f_score = fscore(search_element, target_index, target_table, table_size, suffix)
                    search_element.fscore = f_score

                    found = True
                    break

            if found:
                break


            #add all the available neighbors to the open list
            child_list=[]
            for i in range(bucket_size):
                neighbor = a_star_pe(pe = path_element(tables[table_index][search_index][i],table_index, search_index, i),prior=search_element, distance=search_element.distance+1, score=0)
                f_score = fscore(neighbor, target_index, target_table, table_size, suffix)
                neighbor.fscore = f_score
                child_list.append(neighbor)
            
            for child in child_list:
                if closed_list_contains(closed_list_map, child):
                    continue
                #update existing open list elements if the new one is better
                if open_list_contains(open_list_map, child):
                    existing_element = open_list_map[child.pe.key]
                    if child.distance < existing_element.distance:
                        existing_element.distance = child.distance
                        existing_element.prior = child.prior
                        f_score = fscore(existing_element, target_index, target_table, table_size, suffix)
                        existing_element.fscore = f_score

                    continue
                push_open_list(open_list, open_list_map, child)
            
            
        if found:
            break

    
    #construct the path
    if found:
        # print("found")
        path = []
        while search_element != None:
            path.append(search_element.pe)
            search_element = search_element.prior
        path=path[::-1]


        # print_path(path)
        return [path]
    else:
        # print("not found")
        # print("closed list")
        # for e in closed_list:
        #     print(e)
        # print("open list")
        # for e in open_list:
        #     print(e)
        search_index = location_func(value, table_size, suffix)[0]
        #print("FAILED TO INSERT VALUE " + str(value.key) + " into index " + str(search_index))

        return[]

    



def bucket_cuckoo_a_star_insert(tables, table_size, location_func, value, bucket_size, suffix):
    insert_paths=bucket_cuckoo_a_star(tables, table_size, location_func, value, bucket_size, suffix)

    if len(insert_paths) == 0:
        return []
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

    insert_paths=bucket_cuckoo_bfs(tables, table_size, location_func, value, bucket_size, suffix, max_depth=5)
    if len(insert_paths) == 0:
        #print("Failed Insert")
        #print_styled_table(tables, table_size, bucket_size)
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

    for v in tqdm(values, leave=False):
        v=entry(v)
        path = insert_func(tables, table_size, location_func, v, bucket_size, suffix)
        if path == []:
            break
        paths.append(path)
    #print(len(collisions_per_insert))
    # print_styled_table(tables, table_size, bucket_size)
    # print("len paths: " + str(len(paths)))
    print("Fill: " + str(round(len(paths)/len(values), 2)))
    return paths_to_collisions(paths), paths, tables

        
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
    for v in tqdm(values, leave=False):
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