import logging
import heapq
import random
from . import tables
from . import hash
logger = logging.getLogger('root')

#paths are built from path elements. A path element is a tuple of (key, table_index, bucket_index, bucket_offset)
#that defines a cuckoo path
class path_element:
    def __init__(self, key, table_index, bucket_index, bucket_offset):
        self.key = key
        self.table_index = table_index
        self.bucket_index = bucket_index
        self.bucket_offset = bucket_offset

    def __str__(self):
        return "(key: " + str(self.key) + ", table: " + str(self.table_index) + ", bucket: " + str(self.bucket_index) + ", offset: " + str(self.bucket_offset) + ")"

    def __repr__(self):
        return str(self)

    def __gt__(self, other):
        return self.bucket_index > other.bucket_index

    def __sub__(self, other):
        return self.bucket_index - other.bucket_index


def search_path_to_buckets(search_path):
    buckets = list(set([pe.bucket_index for pe in search_path]))
    buckets.remove(-1)
    buckets.sort()
    return buckets

DEPTH_LIMIT = 100
def random_dfs_search(table, location_func, path, open_buckets, visited):
    if len(path) > DEPTH_LIMIT:
        return False, path

    pe = path[-1]
    if pe.key in visited:
        return False, path
    else:
        visited[pe.key] = True

    table_index, index = next_search_location(pe, location_func, table)
    #search for an empty slot in this bucket

    if open_buckets != None:
        if not index in open_buckets:
            return False, path

    if table.bucket_has_empty(index):
        # print("Found Slot:" + str(search_element.pe))
        bucket_index = table.get_first_empty_index(index)
        pe = path_element(table.get_entry(index,bucket_index),table_index,index,bucket_index)
        path.append(pe)
        return True, path
    
    #here we have a full bucket we need to evict a candidate
    #randomly select an eviction candidate
    indicies = list(range(0, table.get_buckets_per_row()))
    random.shuffle(indicies)
    for evict_index in indicies:
        pe = path_element(table.get_entry(index,evict_index), table_index, index, evict_index)
        if not key_causes_a_path_loop(path, pe.key):
            path.append(pe)
        else:
            continue
        
        success, path = random_dfs_search(table, location_func, path, open_buckets, visited)

        if success:
            return True, path
        else:
            path.pop()
        
    return False, path


def bucket_cuckoo_insert(table, location_func, value, open_buckets=None):
    success=False
    pe = path_element(value, -1,-1,-1)
    path=[pe]
    visited = dict()
    success, path = random_dfs_search(table, location_func, path, open_buckets, visited)
    if not success:
        print("failed insert")
        path=[]
    return path


def next_search_location(pe, location_func, table):
    locations = location_func(pe.key, table.get_row_count())
    table_index = next_table_index(pe.table_index)
    #here the index is the row in the table
    index = locations[table_index]
    return table_index, index

def key_causes_a_path_loop(path, key):
    for pe in path:
        if pe.key == key:
            return True
    return False


#print a list of path elements
#todo refactor to create a class called path
def print_path(path):
    print(path_to_string(path))

def path_to_string(path):
    if path == None:
        return("No path")
    string=""
    for i in range(len(path)):
        value = path[i]
        prefix = str(i)
        string += prefix + " - " + str(value) + ", "
    return string

def path_index_range(path):
    max_index = -1
    min_index = 1 << 32 #very large
    # print_path(path)
    for i in range(len(path)):
        value = path[i]
        # print(value)
        if value.bucket_index > max_index:
            max_index = value.bucket_index
        if value.bucket_index >= 0 and value.bucket_index < min_index:
            min_index = value.bucket_index
    # print("max,min index",max_index,min_index )
    return max_index - min_index

#locations for insertion are found in both directions from
#the hash location.  this function searches in both
#directions to produce a list of n locations which are close
#to the original hash. There is no guarantee that these
#entries are reachable. They are merely the closest n open
#entries.
def find_closest_target_n_bi_directional(table, location_func, value, n):
    locations = location_func(value, table.get_row_count())
    index_1 = locations[0]
    index_2 = locations[0] - 1
    if index_2 < 0:
        index_2 = table.get_row_count() - 1
    
    results=[]
    while len(results) < n:
        if table.bucket_has_empty(index_1):
            results.append(index_1)
        if table.bucket_has_empty(index_2):
            results.append(index_2)
        index_1 = (index_1+1) % table.get_row_count()
        index_2 = (index_2-1)
        if index_2 < 0:
            index_2 = table.get_row_count() - 1
        assert index_1 < table.get_row_count(), "TODO Implement modulo wrap around in find closest target"
        assert index_2 < table.get_row_count() and index_2 >= 0, "TODO something wrong with look back index"
    return results

#an a star path element is a path element which we search
#for using a star. These elements have a prior, a score, and
#a distance from the destination.
class a_star_pe:
    def __init__ (self, pe, prior, distance, score):
        self.pe = pe
        self.prior = prior
        self.distance = distance
        self.fscore = score
    def __lt__(self, other):
        return self.fscore < other.fscore
    def __str__(self):
        return "pe:" + str(self.pe) + " prior:" + str(self.prior) + " distance:" + str(self.distance) + "fscore: " + str(self.fscore)


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

def next_table_index(table_index):
    if table_index == 0:
        return 1
    else:
        return 0

#designed for exp
def heuristic_3(current_index, target_index, table_size):

    #todo deal with wrap around
    #todo the first thing we should do it figure out if we want to move forward or backwards
    #if the forward direction is closer we should go that way,
    #and if the backwards direction is closer we should go thy.
    # print("current_index: " + str(current_index) + " target_index: " + str(target_index))

    median = 4
    target_index = target_index % table_size
    if  (target_index == current_index):
        return 0

    half_table = table_size/2
    #todo start here after lunch.


    if (target_index > current_index):
        if (target_index - current_index <= table_size/2):
            distance = (target_index - current_index)
        else:
            distance = (table_size + (current_index - target_index))
    else:
        if (current_index - target_index > table_size/2):
            distance = (current_index - target_index)
        else:
            distance = (target_index - current_index) * -1

    distance = int(distance / median)

    current_table = hash.get_table_id_from_index(current_index)
    target_table = hash.get_table_id_from_index(target_index)

    if current_table == target_table: 
        distance = distance + 1
    return distance

def heuristic_4(current_index, target_index, table_size):
    median = 4 ## This value is the median number of buckets we hop per step
    distance = 0
    target_index = target_index % table_size
    if  (target_index == current_index):
        return distance

    current_table = hash.get_table_id_from_index(current_index)
    target_table = hash.get_table_id_from_index(target_index)
    if current_table == target_table:
        distance = distance + 1

    # if (target_index < current_index):
    #     target_index = target_index + table_size

    # distance = distance + int((target_index - current_index) / median)
    return distance

def fscore(element, target_index, table_size):
        #TODO start here tomorrow, make Fscore work on a merged table
        g_score = element.distance
        h_score = heuristic_4(element.pe.bucket_index, target_index, table_size)
        # print(h_score)
        f_score = g_score + h_score
        return f_score



def a_star_search(table, location_func, value, open_buckets=None):
    table_size = table.get_row_count()
    bucket_size = table.get_buckets_per_row()
    targets = find_closest_target_n_bi_directional(table, location_func, value, 20)
    # targets = list(set(targets))
    # print(targets)

    while (len(targets) > 0):
        target_index = targets.pop(0)
        starting_pe = path_element(value, -1, -1, -1)
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

            locations = location_func(search_element.pe.key, table_size)
            table_index = next_table_index(search_element.pe.table_index)
            index = locations[table_index]

            if open_buckets != None:
                if not index in open_buckets:
                    continue


            # print("Search: ("+str(table_index)+","+str(search_index) + ")"+ " Closest Target:("+ str(target_table) + "," + str(target_index)+")")
            #Check if any slots are open for the search element

            if table.bucket_has_empty(index):
                # print("Found Slot:" + str(search_element.pe))
                bucket_index = table.get_first_empty_index(index)
                search_element = a_star_pe(pe = path_element(table.get_entry(index,bucket_index),table_index,index,bucket_index), prior=search_element, distance=search_element.distance+1, score=0)

                f_score = fscore(search_element, target_index, table_size)
                search_element.fscore = f_score

                found = True
                break

            if found:
                break


            #add all the available neighbors to the open list
            child_list=[]
            for i in range(bucket_size):
                neighbor = a_star_pe(pe = path_element(table.get_entry(index,i),table_index, index, i),prior=search_element, distance=search_element.distance+1, score=0)
                f_score = fscore(neighbor, target_index, table_size)
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
                        f_score = fscore(existing_element, target_index, table_size)
                        existing_element.fscore = f_score

                    continue
                push_open_list(open_list, open_list_map, child)
            
            
        if found:
            break

    
    #construct the path
    if found:
        path = []
        while search_element != None:
            path.append(search_element.pe)
            search_element = search_element.prior
        path=path[::-1]


        return [path]
    else:
        return []

def bucket_cuckoo_a_star_insert(table, location_func, value, open_buckets=None):
    if table.full():
        return []
    insert_paths=a_star_search(table, location_func, value, open_buckets)
    if len(insert_paths) == 0:
        return []
    insert_path = insert_paths[random.randint(0, len(insert_paths)-1)]
    return insert_path

def bucket_cuckoo_random_insert(table, location_func, value, open_buckets=None):
    if table.full():
        return []
    insert_path=bucket_cuckoo_insert(table, location_func, value, open_buckets)
    if len(insert_path) == 0:
        return []
    return insert_path