import hash
import logging
import heapq
from tqdm import tqdm
import random
import json

logger = logging.getLogger('root')

TABLE_ENTRY_SIZE = 8
CAS_SIZE = 64

#########A star search

#an entry is a physical entry in the hash table index.
class entry:
    def __init__(self, key):
        self.key = key
    def __str__(self):
        return("Key: " + str(self.key))

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

#print a list of path elements
#todo refactor to create a class called path
def print_path(path):
    print("Printing Path")
    if path == None:
        print("No path")
        return
    for i in range(len(path)):
        value = path[i]
        prefix = str(i)
        print(prefix + ") " + str(value))

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
    locations = location_func(value, table.table_size)
    index_1 = locations[0]
    index_2 = locations[0] - 1
    if index_2 < 0:
        index_2 = table.table_size - 1
    
    results=[]
    while len(results) < n:
        if table.bucket_has_empty(index_1):
            results.append(index_1)
        if table.bucket_has_empty(index_2):
            results.append(index_2)
        index_1 = (index_1+1) % table.table_size
        index_2 = (index_2-1)
        if index_2 < 0:
            index_2 = table.table_size - 1
        assert index_1 < table.table_size, "TODO Implement modulo wrap around in find closest target"
        assert index_2 < table.table_size and index_2 >= 0, "TODO something wrong with look back index"
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
    table_size = table.table_size
    bucket_size = table.bucket_size
    targets = find_closest_target_n_bi_directional(table, location_func, value, 20)

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

class Message:
    def __init__(self, config):
        self.payload = dict()
        for key, value in config.items():
            self.payload[key] = value
    
    def __str__(self):
        v = ""
        for key, value in self.payload.items():
            v += str(key) + ":" + str(value) + "\n"
        return v[:len(v)-1]

class Lock:
    def __init__(self):
        self.lock_state = 0

    def lock(self):
        self.lock_state = 1

    def unlock(self):
        self.lock_state = 0

    def is_locked(self):
        return self.lock_state == 1

    def equals(self, other):
        return self.lock_state == other

    def set_bit(self, bit):
        self.lock_state = bit

    def __str__(self):
        if self.is_locked():
            return "L"
        else:
            return "U"

    def __repr__(self):
        return self.__str__()

class LockTable:
    def __init__(self, memory_size, entry_size, bucket_size, buckets_per_lock):
        self.memory_size = memory_size
        self.entry_size = entry_size
        self.bucket_size = bucket_size
        self.buckets_per_lock = buckets_per_lock
        rows = int(((memory_size/entry_size))/bucket_size)
        assert rows % buckets_per_lock == 0, "Number of buckets per lock must be a factor of the number of rows"
        self.total_locks = int(rows/buckets_per_lock)
        # print("Lock Table Locks:" + str(self.total_locks))
        self.locks = [Lock() for i in range(self.total_locks)]


        # print(self)

    def set_lock(self, lock_index):
        if self.locks[lock_index].is_locked():
            return False
        self.locks[lock_index].lock()
        return True
    
    def unlock_lock(self, lock_index):
        if not self.locks[lock_index].is_locked():
            return False
        self.locks[lock_index].unlock()
        return True

    
    def multi_lock(self, lock_indexes):
        for i in lock_indexes:
            if self.locks[i].is_locked():
                return False
        for i in lock_indexes:
            self.locks[i].lock()
        return True

    def multi_unlock(self, lock_indexes):
        for i in lock_indexes:
            if not self.locks[i].is_locked():
                return False
        for i in lock_indexes:
            self.locks[i].unlock()
        return True


    def get_cas_range(self, starting_index):
        locks=[Lock()] * CAS_SIZE
        index = starting_index
        for i in range(CAS_SIZE):
            if index >= len(self.locks):
                break
            locks[i] = self.locks[index]
            index += 1
        return locks




    def __str__(self):
        s = ""
        for i in range(len(self.locks)):
            bottom_bucket = i*self.buckets_per_lock
            top_bucket = bottom_bucket + self.buckets_per_lock - 1
            s += str(bottom_bucket) + "-" + str(top_bucket) + ":" + str(self.locks[i]) + "\n"
        return s[:len(s)-1]

class Table:
    def __init__(self, memory_size, bucket_size, buckets_per_lock):
        # self.number_tables=2
        self.entry_size=8
        self.memory_size=memory_size
        self.bucket_size=bucket_size
        self.buckets_per_lock=buckets_per_lock
        self.table=self.generate_bucket_cuckoo_hash_index(memory_size, bucket_size)
        self.table_size = len(self.table)

        self.lock_table = LockTable(memory_size, self.entry_size, bucket_size, self.buckets_per_lock)
        self.fill = 0


    def print_table(self):
        for i in range(0, self.table_size):
            print('{0: <5}'.format(str(i)+")"), end='')
            for j in range(0, self.bucket_size):
                print("[" + '{0: <5}'.format(str(self.table[i][j])) + "] ", end='')
            print("")

    def get_bucket_size(self):
        return len(self.table[0]) * TABLE_ENTRY_SIZE
        
    def row_size_bytes(self):
        return self.n_buckets_size(self.bucket_size)

    def row_size_in_indexes(self):
        return len(self.table[0])

    def n_buckets_size(self, n_buckets):
        return n_buckets * TABLE_ENTRY_SIZE

    def get_entry(self, bucket, offset):
        return self.table[bucket][offset]

    def set_entry(self, bucket, offset, entry):
        #maintain fill counter
        old = self.table[bucket][offset]
        if old == None:
            self.fill += 1
        if entry == None:
            self.fill -=1
            # print("Fill: " + str(self.fill))
        self.table[bucket][offset] = entry


    def bucket_has_empty(self, bucket_index):
        return None in self.table[bucket_index]

    def get_first_empty_index(self, bucket_index):
        for i in range(len(self.table[bucket_index])):
            if self.table[bucket_index][i] == None:
                return i
        return len(self.table[bucket_index])


        

    def bucket_contains(self, bucket_index, value):
        return value in self.table[bucket_index]

    def contains(self, value):
        locations = hash.rcuckoo_hash_locations(value, self.table_size)
        for bucket_index in locations:
            if self.bucket_contains(bucket_index, value):
                return True
        return False

    def get_fill_percentage(self):
        print("fill: ",self.fill)
        print("table size: ", self.table_size)
        print("bucket size: ", self.bucket_size)
        return float(self.fill)/float(self.table_size * self.bucket_size)

    def full(self):
        return self.fill == self.table_size * self.bucket_size


    def generate_bucket_cuckoo_hash_index(self, memory_size, bucket_size):

        total_rows = int(memory_size/bucket_size)/self.entry_size
        logger.debug("Memory Size: " + str(memory_size))
        logger.debug("Bucket Size: " + str(bucket_size))
        logger.debug("Total Rows: " + str(total_rows))

        #we must have more than one row. I'm not worried about
        #the pessimal case here, the point is to test hash
        #tables
        row_err = "We must have more than 1 row"
        assert total_rows > 1, row_err

        #for cuckoo hashing we have two tables, therefore memory size must be a power of two

        # table_div_error = "Memory must divide evenly across tables. Memory:" + str(memory_size) + " Tables:" + str(self.number_tables)
        # assert memory_size % self.number_tables == 0, table_div_error

        #We also need the number of buckets to fit into memory correctly
        bucket_div_error = "Memory must divide evenly across buckets. Memory:" + str(memory_size) + " Buckets:" + str(bucket_size)
        assert (memory_size) % bucket_size == 0, bucket_div_error

        #finally each entry in the bucket is 8 bytes
        entry_div_error = "Memory must divide evenly across entries. Memory:" + str(memory_size) + " Entries:" + str(self.entry_size)
        assert ((memory_size)/bucket_size % self.entry_size == 0), entry_div_error

        table = []
        rows = int((memory_size/self.entry_size)/bucket_size)

        logger.debug("Generating table with " + str(rows) + " rows and " + str(bucket_size) + " buckets per row")
        for i in range(rows):
            table.append([None]*bucket_size)
        return table


    def find_empty_index(self, bucket_index):
        bucket = self.table[bucket_index]
        empty_index = -1
        for i in range(len(bucket)):
            if bucket[i] == None:
                empty_index = i
                break
        return empty_index


    def absolute_index_to_bucket_index(self, index):
        bucket_offset = index % self.bucket_size
        bucket = int(index/self.bucket_size)
        return (bucket,bucket_offset)


    def assert_operation_in_table_bound(self, bucket_id, bucket_offset, size):
        self.assert_read_table_properties(size)
        total_indexs = size/8
        max_read=bucket_id * bucket_offset + total_indexs
        table_bound=self.table_size * self.bucket_size
        assert max_read <= table_bound, "read is out of bounds " + str(max_read) + " " + str(table_bound)

    def assert_read_table_properties(self, read_size):
        assert self.table != None, "table is not initalized"
        assert self.table[0] != None, "table must have dimensions"
        assert read_size % TABLE_ENTRY_SIZE == 0, "size must be a multiple of 8"

    #check if the table has any duplicates in it
    def contains_duplicates(self):
        check_dict = dict()
        for i in range(len(self.table)):
            for j in range(len(self.table[i])):
                if self.table[i][j] != None:
                    if self.table[i][j] in check_dict:
                        return True
                    else:
                        check_dict[self.table[i][j]] = 1

    def get_duplicates(self):
        check_dict = dict()
        duplicates = []
        for i in range(len(self.table)):
            for j in range(len(self.table[i])):
                if self.table[i][j] != None:
                    if self.table[i][j] in check_dict:
                        tup = (self.table[i][j], check_dict[self.table[i][j]], (i,j))
                        duplicates.append(tup)
                    else:
                        check_dict[self.table[i][j]] = (i,j)
        return duplicates


#RDMA operations on the table
def fill_table_with_cas(table, bucket_id, bucket_offset, success, value):
    table.assert_operation_in_table_bound(bucket_id, bucket_offset, TABLE_ENTRY_SIZE)
    table.set_entry(bucket_id,bucket_offset,value)

    if not success:
        logger.warning("returned value is not the same as the value in the table, inserted it anyways")

def cas_table_entry(table, bucket_id, bucket_offset, old, new):
    v = table.get_entry(bucket_id,bucket_offset)
    if v == old:
        table.set_entry(bucket_id,bucket_offset,new)
        return (True, new)
    else:
        return (False, v)

def masked_cas_lock_table(lock_table, lock_index, old, new, mask):
    #sanity check
    assert len(old) == CAS_SIZE, "old must be 64 bytes"
    assert len(old) == len(new), "old and new must be the same length"
    assert len(new) == len(mask), "new and mask must be the same length"

    assert lock_table != None, "lock table is not initalized"
    assert lock_index < lock_table.total_locks, "lock index is out of bounds"

    index = lock_index

    #XOR check that the old value in the cas matches the existing lock table.
    for o, m in zip(old,mask):
        #if the index steps out of the range of the lock table break, we have not learned anything by this
        if index >= lock_table.total_locks:
            break
        if m == 1:
            #we actually want to check the lock
            if not lock_table.locks[index].equals(o):
                #if the old cas value is not the same as the submitted value return the current state of the table
                current = lock_table.get_cas_range(lock_index)
                return (False,current)
        index+=1

    #now we just apply. At this point we know that the old value is the same as the current so we can lock and unlock accordingly.
    index = lock_index
    for n, m in zip(new,mask):
        if index >= lock_table.total_locks:
            break
        #If this value is part of the mask, set it to the cas value
        if m == 1:
            lock_table.locks[index].set_bit(n)
        index += 1
    current = lock_table.get_cas_range(lock_index)
    return (True, current)

def fill_lock_table_masked_cas(lock_table, lock_index, success, value, mask):
    #sanity check
    assert len(value) == CAS_SIZE, "value must be 64 bytes"
    assert len(value) == len(mask), "value and mask must be the same length"

    assert lock_table != None, "lock table is not initalized"
    assert lock_index < len(lock_table), "lock index is out of bounds"

    index = lock_index
    for v, m in zip(value,mask):
        if index >= len(lock_table):
            break
        #If this value is part of the mask, set it to the cas value
        if m == True:
            lock_table[index] = v
        index += 1

    if not success:
        logger.warning("returned value is not the same as the value in the table, inserted it anyways")


def read_table_entry(table, bucket_id, bucket_offset, size):
    table.assert_operation_in_table_bound(bucket_id, bucket_offset, size)
    total_indexs = int(size/TABLE_ENTRY_SIZE)

    #collect the read
    read = []
    base = bucket_id * table.bucket_size + bucket_offset
    for i in range(total_indexs):
        bucket, offset = table.absolute_index_to_bucket_index(base + i)
        read.append(table.get_entry(bucket,offset))
    return read

def fill_table_with_read(table, bucket_id, bucket_offset, size, read):
    table.assert_operation_in_table_bound(bucket_id, bucket_offset, size)
    total_indexs = int(size/TABLE_ENTRY_SIZE)

    #write remote read to the table
    base = bucket_id * table.bucket_size + bucket_offset
    for i in range(total_indexs):
        bucket, offset = table.absolute_index_to_bucket_index(base + i)
        table.set_entry(bucket, offset, read[i])

def fill_local_table_with_cas_response(table, args):
    args_copy = args.copy()
    args_copy["read"]=[args["value"]]
    args_copy["size"]=TABLE_ENTRY_SIZE
    del(args_copy["value"])
    del(args_copy["success"])
    fill_table_with_read(table, **args_copy)

def fill_local_table_with_read_response(table, args):
    fill_table_with_read(table, **args)



#message helper functions
function_to_type_map = {
    cas_table_entry: "cas",
    fill_table_with_cas: "cas_response",
    masked_cas_lock_table: "masked_cas",
    fill_lock_table_masked_cas: "masked_cas_response",
    read_table_entry: "read",
    fill_table_with_read: "read_response",
}

def message_type(message):
    if message == None:
        return "None"

    payload = message.payload
    function = payload["function"]
    if function in function_to_type_map:
        return function_to_type_map[function]
    print("Unknown message type! (message type function)", message)
    exit(1)

ethernet_size = 18 #14 header 4 crc
ipv4_size = 20 #20 header
udp_size = 8 #8 header
roce_size = 16 #12 header 4 crc
base_roce_size = ethernet_size + ipv4_size + udp_size + roce_size

read_header = 16
read_response_header = 4
cas_header = 28
cas_response_header = 8

header_size = {
    "cas": base_roce_size + cas_header,
    "cas_response": base_roce_size + cas_response_header,
    "masked_cas": base_roce_size + cas_header + CAS_SIZE,
    "masked_cas_response": base_roce_size + cas_response_header,
    "read": base_roce_size + read_header,
    "read_response": base_roce_size + read_response_header,
}

def message_to_bytes(message):
    t = message_type(message)
    size = header_size[t]
    payload = message.payload
    if "size" in payload["function_args"]:
        size += payload["function_args"]["size"]
    return size


def messages_append_or_extend(messages, message):
    if message != None:
        if isinstance(message, list):
            messages.extend(message)
        else:
            messages.append(message)
    return messages

def cas_table_entry_message(bucket_index, bucket_offset, old, new):
        message = Message({})
        message.payload["function"] = cas_table_entry
        message.payload["function_args"] = {'bucket_id':bucket_index, 'bucket_offset':bucket_offset, 'old':old, 'new':new}
        return message

def unpack_cas_response(message):
    assert message.payload["function"] == fill_table_with_cas, "client is in inserting state but message is not a cas " + str(message)
    args = message.payload["function_args"]
    logger.debug("Insert Response: " +  "Success: " + str(args["success"]) + " Value: " + str(args["value"])) if __debug__ else None
    return args

def unpack_read_response(message):
    assert message.payload["function"] == fill_table_with_read, "client is reading but function was not a read response " + str(message)
    args = message.payload["function_args"]
    logger.debug("Read Response " + str(args["read"])) if __debug__ else None
    return args
    

def masked_cas_lock_table_message(lock_index, old, new, mask):
        message = Message({})
        message.payload["function"] = masked_cas_lock_table
        message.payload["function_args"] = {'lock_index':lock_index, 'old':old, 'new':new, 'mask':mask}
        return message

def masked_cas_lock_table_messages(lock_messages):
    messages = []
    for l in lock_messages:
        index, old, new, mask = l[0], l[1], l[2], l[3]
        messages.append(masked_cas_lock_table_message(index,old,new,mask))
    return messages


def unpack_masked_cas_response(message):
        assert message.payload["function"] == fill_lock_table_masked_cas, "client is in inserting state but message is not a cas " + str(message)
        args = message.payload["function_args"]
        logger.debug("Insert Response: " +  "Success: " + str(args["success"]) + " Value: " + str(args["value"]) + str(args["mask"]))  if __debug__ else None
        return args

def get_state_machine_name(state_machine_class_pointer):
    str_name = str(state_machine_class_pointer)
    back = str_name.split(".")[1]
    front = back.split("'")[0]
    return front

#cuckoo protocols
class state_machine:
    def __init__(self, config):
        self.logger = logging.getLogger("root")
        self.config = config
        #spiritually these are the high level states of the state machine
        #these are considered less configurable than the self.state variable I use in the children
        self.complete = False
        self.inserting = False
        self.reading = False

        #state machine statistics
        self.total_bytes = 0
        self.read_bytes = 0
        self.write_bytes = 0
        self.cas_bytes = 0
        self.total_reads = 0
        self.total_writes = 0
        self.total_cas = 0
        self.total_cas_failures = 0

        #todo add to a subclass
        #insert stats
        self.insert_path_lengths = []
        self.index_range_per_insert = []
        self.current_insert_messages = 0
        self.messages_per_insert = []
        self.completed_inserts = []
        self.completed_insert_count = 0
        self.failed_inserts = []
        self.failed_insert_count = 0
        self.insert_operation_bytes = 0
        self.insert_operation_messages = 0

        #read stats
        self.current_read_messages = 0
        self.messages_per_read = []
        self.completed_reads = []
        self.completed_read_count = 0
        self.failed_reads = []
        self.failed_read_count = 0
        self.read_operation_bytes = 0
        self.read_operation_messages = 0

    def complete_read_stats(self, success, read_value):
        if success:
            self.completed_read_count += 1
            self.completed_reads.append(read_value)
        else:
            self.failed_read_count += 1
            self.failed_reads.append(read_value)
        self.messages_per_read.append(self.current_read_messages)

        #clear for next time
        self.current_read_messages = 0

    def complete_insert_stats(self, success):
        if success:
            self.completed_inserts.append(self.current_insert_value)
            self.completed_insert_count += 1
        else:
            self.failed_inserts.append(self.current_insert_value)
            self.failed_insert_count += 1


    def get_stats(self):
        stats = dict()
        stats["total_bytes"] = self.total_bytes
        stats["read_bytes"] = self.read_bytes
        stats["write_bytes"] = self.write_bytes
        stats["cas_bytes"] = self.cas_bytes
        stats["total_reads"] = self.total_reads
        stats["total_writes"] = self.total_writes
        stats["total_cas"] = self.total_cas
        stats["total_cas_failures"] = self.total_cas_failures

        #todo add to a subclass 
        stats["insert_path_lengths"] = self.insert_path_lengths
        stats["index_range_per_insert"] = self.index_range_per_insert
        stats["messages_per_insert"] = self.messages_per_insert
        stats["completed_inserts"] = self.completed_inserts
        stats["completed_insert_count"] = self.completed_insert_count
        stats["failed_inserts"] = self.failed_inserts
        stats["failed_insert_count"] = self.failed_insert_count
        stats["insert_operation_bytes"] = self.insert_operation_bytes
        stats["insert_operation_messages"] = self.insert_operation_messages


        stats["messages_per_read"] = self.messages_per_read
        stats["completed_reads"] = self.completed_reads
        stats["completed_read_count"] = self.completed_read_count
        stats["failed_reads"] = self.failed_reads
        stats["failed_read_count"] = self.failed_read_count
        stats["read_operation_bytes"] = self.read_operation_bytes
        stats["read_operation_messages"] = self.read_operation_messages
        return stats

    def update_message_stats(self, messages):

        if messages == None:
            return

        if not isinstance(messages, list):
            messages = [messages]

        for message in messages:

            size = message_to_bytes(message)
            if self.inserting:
                self.current_insert_messages += 1
                self.insert_operation_bytes += size
                self.insert_operation_messages +=1
            elif self.reading:
                self.current_read_messages +=1
                self.read_operation_bytes += size
                self.read_operation_messages +=1

            self.total_bytes += size
            t = message_type(message)
            payload = message.payload
            if t == "read":
                self.total_reads += 1
                self.read_bytes += size
            elif t == "read_response":
                self.read_bytes += size
            elif t == "cas" or t == "masked_cas":
                self.total_cas += 1
                self.cas_bytes += size
            elif t == "cas_response" or t == "masked_cas_response":
                self.cas_bytes += size
                if payload["function_args"]["success"] == False:
                    self.total_cas_failures += 1
            elif t == "write":
                self.total_writes += 1
                self.write_bytes += size
            elif t == "write_response":
                self.write_bytes += size
            else:
                print("Unknown message type! " + str(message))
                exit(1)

    
    def fsm(self, message = None):
        #caclulate statistics
        self.update_message_stats(message)
        #return fsm_wapper
        output_message = self.fsm_logic(message)
        if __debug__:
            if isinstance(output_message, list):
                for m in output_message:
                    self.warning("FSM: " + str(message) + " -> " + str(m))
            else:
                self.warning("FSM: " + str(message) + " -> " + str(output_message))

        self.update_message_stats(output_message)
        return output_message

    def fsm_logic(self, messages=None):
        print("state machine top level overload this")


    def log_prefix(self):
        return "{:<9}".format(str(self))

    def info(self, message):
        self.logger.info("[" + self.log_prefix() + "] " + message)

    def debug(self, message):
        self.logger.debug("[" + self.log_prefix() + "] " + message)

    def warning(self, message):
        self.logger.warning("[" + self.log_prefix() + "] " + message)

    def critical(self, message):
        self.logger.critical("[" + self.log_prefix() + "] " + message)


class request():
    def __init__(self, request_type, key, value=None):
        self.request_type = request_type
        self.key = key
        self.value = value

    def __str__(self):
        return "Request: " + self.request_type + " Key: " + str(self.key) + " Value: " + str(self.value)

    def __repr__(self):
        return self.__str__()

workload_write_percentage={
    "ycsb-a": 50,
    "ycsb-b": 5,
    "ycsb-c": 0,
    "ycsb-w": 100,
}

class client_workload_driver():
    def __init__(self, config):
        self.workload=config["workload"]
        self.total_requests=config["total_requests"]
        self.client_id=config['id']
        self.num_clients=config['num_clients']
        self.non_deterministic=config['non_deterministic']

        if self.non_deterministic:
            self.random_factor = int(random.random() * 100) + 1
        else:
            self.random_factor = 1
        #todo add a line for having a workload passed in as a file
        self.completed_requests=0
        self.completed_puts=0
        self.completed_gets=0
        self.last_request=None

    def get_stats(self):
        stats = dict()
        stats["completed_requests"] = self.completed_requests
        stats["completed_puts"] = self.completed_puts
        stats["completed_gets"] = self.completed_gets
        stats["workload"] = self.workload
        stats["total_requests"] = self.total_requests
        stats["client_id"] = self.client_id
        stats["num_clients"] = self.num_clients
        return stats

    def record_last_request(self):
        request = self.last_request
        self.completed_requests += 1
        if request == None:
            return
        if request.request_type == "put":
            self.completed_puts += 1
        elif request.request_type == "get":
            self.completed_gets += 1
        else:
            print("Unknown request type! " + str(request))
            exit(1)

    def unique_insert(self, insert, client_id, total_clients, factor):
        return (insert * total_clients * factor) + client_id

    def unique_get(self, get, client_id, total_clients, factor):
        return (get * total_clients * factor) + client_id

    def next_put(self):
        next_value = self.unique_insert(self.completed_puts, self.client_id, self.num_clients, self.random_factor)
        req = request("put", next_value, next_value)
        self.last_request = req
        return req

    def next_get(self):
        index = self.completed_puts - 1

        next_value = self.unique_get(index, self.client_id, self.num_clients, self.random_factor)
        req = request("get", next_value)
        self.last_request = req
        return req


    def gen_next_operation(self, workload):
        if not workload in workload_write_percentage:
            print("Unknown workload! " + workload)
            exit(1)
        percentage = workload_write_percentage[workload]
        # if int(random.random() * 100) < percentage:
        if ((self.completed_requests * 1337) % 100) < percentage:
            return "put"
        else:
            return "get"

    def next(self):
        self.record_last_request()
        if self.completed_requests >= self.total_requests:
            return None
        op = self.gen_next_operation(self.workload)
        if op == "put":
            return self.next_put()
        elif op == "get":
            return self.next_get()




class client_state_machine(state_machine):
    def __init__(self, config):
        super().__init__(config)
        self.total_inserts = config["total_inserts"]
        self.id = config["id"]
        self.table = config["table"]
        self.state="idle"


        #read state machine
        self.current_read_key = None
        self.outstanding_read_requests = 0
        self.read_values_found = 0
        self.read_values = []
        self.duplicates_found = 0

        workload_config = {
            "workload": "ycsb-w",
            "total_requests": self.total_inserts,
            "id": self.id,
            "num_clients": config["num_clients"],
            "non_deterministic": False,
        }
        self.workload_config=workload_config
        self.workload_driver = client_workload_driver(workload_config)


    def begin_read(self, messages):
        self.outstanding_read_requests = len(messages)
        self.read_values_found = 0
        self.read_values = []
        self.state = "reading"
        self.reading=True
        return messages

    def read_complete(self):
        return self.outstanding_read_requests ==  0

    def read_successful(self, key):
        if self.read_values_found == 0:
            success = False
            self.info("Read Failed: " + str(key)) if __debug__ else None
        elif self.read_values_found == 1:
            success = True
            self.info("Read Complete: " + str(self.read_values)) if __debug__ else None
        elif self.read_values_found > 1:
            success = True
            self.duplicates_found = self.duplicates_found + 1
            self.info("Read Complete: " + str(self.read_values) + " Duplicate Found") if __debug__ else None
            #todo we likely need a tie breaker here
        return success


    #return true if the read is complete
    def wait_for_read_messages_fsm(self, message, key):
        if message != None and message_type(message) == "read_response":

            #unpack and check the response for a valid read
            args = unpack_read_response(message)
            fill_local_table_with_read_response(self.table, args)
            read = args["read"]

            keys_found = keys_contained_in_read_response(key, read)
            self.read_values_found = self.read_values_found + keys_found
            self.read_values.extend(get_entries_from_read(key, read))
            self.outstanding_read_requests = self.outstanding_read_requests - 1


        complete = self.read_complete()
        success = self.read_successful(key)
        return complete, success


    def general_idle_fsm(self, message):
        if message != None:
            self.critical("Idle state machine received a message" + str(message))
            return None
        #get the next operations
        req = self.workload_driver.next()
        if req == None:
            self.complete=True
            return None
        elif req.request_type == "put":

            self.current_insert_value = req.key
            self.inserting=True
            return self.put()

        elif req.request_type == "get":
            if req.key < 0:
                return None
            self.current_read_key = req.key
            self.reading=True
            return self.get()
        else:
            raise Exception("No generator for request : " + str(req.request_type))

    def __str__(self):
        return "Client " + str(self.id)

    def get_stats(self):
        stats = super().get_stats()
        stats["workload_stats"] = self.workload_driver.get_stats()
        return stats

    def get(self):
        self.critical("Get method should be overwritten")

    def put(self):
        self.critical("put method should be overwritten")

def get_lock_index(buckets, buckets_per_lock):
    locks = list(set([int(b/buckets_per_lock) for b in buckets]))
    if len(locks) > 0:
        locks.sort()
    return locks

def lock_message_to_buckets(message, buckets_per_lock):
    lock_indexes = lock_message_to_lock_indexes(message)
    buckets = lock_indexes_to_buckets(lock_indexes, buckets_per_lock)
    return buckets

def lock_indexes_to_buckets(lock_indexes, buckets_per_lock):
    return [int(buckets_per_lock * l) for l in lock_indexes]

def lock_message_to_lock_indexes(message):
    base=message.payload["function_args"]['lock_index']
    lock_list=message.payload["function_args"]["mask"]
    lock_indexes=[]
    # print(lock_list)
    for i in range(len(lock_list)):
        if lock_list[i] == 1:
            lock_indexes.append(i)
    lock_indexes = [int((base + l)) for l in lock_indexes]
    return lock_indexes

def lock_array_to_bits(lock_array):
    min_lock_index = min(lock_array)
    lock_array = [l - min_lock_index for l in lock_array]
    old = [0] * CAS_SIZE
    new = [0] * CAS_SIZE
    for l in lock_array:
        new[l] = 1
    mask = new #the mask is the same as the locks we want to aquire
    return (min_lock_index, old, new, mask)

def unlock_array_to_bits(lock_array):
    # swap old and new
    min_lock_index, old, new, mask = lock_array_to_bits(lock_array)
    return (min_lock_index, new, old, mask)

def break_list_to_chunks(lock_array, locks_per_message):
    chunks = []
    current_chunk = []
    for i in range(len(lock_array)):
        if len(current_chunk) == 0:
            min_lock_index = lock_array[i]

        if (lock_array[i] - min_lock_index) < CAS_SIZE and len(current_chunk) < locks_per_message:
            current_chunk.append(lock_array[i])
        else:
            chunks.append(current_chunk)
            current_chunk = [lock_array[i]]
            min_lock_index = lock_array[i]
    chunks.append(current_chunk)
    return chunks

def get_lock_or_unlock_messages(buckets, buckets_per_lock, locks_per_message, lock=True):
    assert locks_per_message <= CAS_SIZE, "locks_per_message must be <= CAS_SIZE: locks/message-"+str(locks_per_message)+" CAS_SIZE-"+str(CAS_SIZE)
    lock_array = get_lock_index(buckets, buckets_per_lock)
    # print("lock array!: " + str(lock_array))
    lock_chunks = break_list_to_chunks(lock_array, locks_per_message)
    # print("lock_chunks: " + str(lock_chunks))
    if lock:
        lock_messages = [lock_array_to_bits(l) for l in lock_chunks]
    else:
        lock_messages = [unlock_array_to_bits(l) for l in lock_chunks]
    return lock_messages

def get_unlock_messages(buckets, buckets_per_lock, locks_per_message):
    return get_lock_or_unlock_messages(buckets, buckets_per_lock, locks_per_message, lock=False)

def get_lock_messages(buckets, buckets_per_lock, locks_per_message):
    return get_lock_or_unlock_messages(buckets, buckets_per_lock, locks_per_message, lock=True)

def aquire_global_lock_masked_cas():
    return get_lock_messages([0], 1, 1)[0]

def release_global_lock_masked_cas():
    return get_unlock_messages([0], 1, 1)[0]

#insert functions
def next_cas_message(search_path, search_path_index):
    insert_pe = search_path[search_path_index] 
    copy_pe = search_path[search_path_index-1]
    return cas_table_entry_message(insert_pe.bucket_index, insert_pe.bucket_offset, insert_pe.key, copy_pe.key)

def undo_last_cas_message(search_path, search_path_index):
    last_pe = search_path[search_path_index+1]
    current_pe = search_path[search_path_index]
    return cas_table_entry_message(last_pe.bucket_index, last_pe.bucket_offset, current_pe.key, None)

## Read operations
def keys_contained_in_read_response(key, read):
    count=0
    for k in read:
        if k == None:
            continue
        if k == key:
            count=count+1
    return count

def get_entries_from_read(key, read):
    return [k for k in read if k == key]

def single_bucket_read_message(bucket, row_size_bytes):
    message = Message({})
    message.payload["function"] = read_table_entry
    message.payload["function_args"] = {'bucket_id':bucket, 'bucket_offset':0, 'size':row_size_bytes}
    return message

#single smaller reads for each bucket
def single_bucket_read_messages(buckets, row_size_bytes):
    messages = []
    for bucket in buckets:
        messages.append(single_bucket_read_message(bucket, row_size_bytes))
    return messages

def single_read_size_bytes(buckets, row_size_bytes):
    min_bucket = min(buckets)
    max_bucket = max(buckets)
    distance = abs(max_bucket - min_bucket)
    indexes = (distance + 1) * row_size_bytes
    return indexes

#one big read for all the buckets    
def multi_bucket_read_message(buckets, row_size_bytes):
    min_bucket = min(buckets)
    size = single_read_size_bytes(buckets, row_size_bytes)
    message = Message({})
    message.payload["function"] = read_table_entry
    message.payload["function_args"] = {'bucket_id':min_bucket, 'bucket_offset':0, 'size':size}
    return [message]

def read_threshold_message(key, read_threshold_bytes, table_size, row_size_bytes):
    buckets = hash.rcuckoo_hash_locations(key, table_size)
    if single_read_size_bytes(buckets, row_size_bytes) <= read_threshold_bytes:
        messages = multi_bucket_read_message(buckets, row_size_bytes)
    else:
        messages = single_bucket_read_messages(buckets, row_size_bytes)
    return messages

def get_covering_read_from_lock_message(lock_message, buckets_per_lock, row_size_bytes):
    # print(lock_message.payload)
    base_index = lock_message.payload['function_args']['lock_index'] * buckets_per_lock
    #calculate the max and min mask index
    mask = lock_message.payload['function_args']['mask']
    min_index = -1
    max_index = -1
    for i in range(0, len(mask)):
        if mask[i] == 1:
            max_index = i
            if min_index == -1:
                min_index = i
    # print("min_index: " + str(min_index), "max_index: " + str(max_index))
    min_index = (min_index * buckets_per_lock) + base_index
    max_index = (max_index * buckets_per_lock) + base_index
    # print("min_bucket: " + str(min_index), "max_bucket: " + str(max_index))
    read_message = multi_bucket_read_message([min_index, max_index], row_size_bytes)
    # print(read_message[0].payload)
    assert len(read_message) == 1, "read message should be length 1"
    #todo start here after chat, we have just gotten the spanning read message for a lock range
    return read_message[0]

def race_messages(key, table_size, row_size_bytes):
    locations = hash.race_hash_locations(key, table_size)
    #get the min of the overflow and index buckets
    l1 = min(locations[0])
    l2 = min(locations[1])
    double_size = int(row_size_bytes * 2)
    locations = [l1, l2]
    messages = single_bucket_read_messages(locations, double_size)
    return messages




def search_path_to_buckets(search_path):
    buckets = list(set([pe.bucket_index for pe in search_path]))
    buckets.remove(-1)
    buckets.sort()
    return buckets


def gen_cas_messages(search_path):
    i = len(search_path)-1
    # print(search_path)
    messages = []
    while i > 0:
        messages.append(next_cas_message(search_path,i))
        i-=1
    return messages

class rcuckoobatch(client_state_machine):
    def __init__(self, config):
        super().__init__(config)

        #inserting and locking
        self.current_insert_value = None
        self.search_path = []
        self.search_path_index = 0

        self.buckets_per_lock = config['buckets_per_lock']
        self.locks_per_message = config['locks_per_message']

        self.read_threshold_bytes = config['read_threshold_bytes']
        assert self.read_threshold_bytes >= self.table.row_size_bytes(), "read threshold is smaller than a single row in the table (this is not allowed)"
        self.locks_held = []
        self.current_locking_messages = []
        self.current_lock_read_messages = []
        self.locking_message_index = 0


    def get(self):
        messages = read_threshold_message(self.current_read_key, self.read_threshold_bytes, self.table.table_size, self.table.row_size_bytes())
        return self.begin_read(messages)

    def put(self):
        return self.search()

    def all_locks_aquired(self):
        return self.locking_message_index == len(self.current_locking_messages) and len(self.locks_held) > 0

    def all_locks_released(self):
        return len(self.locks_held) == 0

    def get_current_locking_message(self):
        return self.current_locking_messages[self.locking_message_index]

    def get_current_locking_message_with_covering_read(self):
        lock_message = self.current_locking_messages[self.locking_message_index]
        read_message = get_covering_read_from_lock_message(lock_message, self.buckets_per_lock, self.table.row_size_bytes())
        self.outstanding_read_requests += 1
        return [lock_message, read_message]

    def receieve_successful_locking_message(self, message):
        lock_indexes = lock_message_to_lock_indexes(message)
        self.info("aquired locks: " + str(lock_indexes)) if __debug__ else None
        self.locks_held.extend(lock_indexes)
        #make unique
        self.locks_held = list(set(self.locks_held))
        self.locking_message_index = self.locking_message_index + 1

    def receive_successful_unlocking_message(self, message):
        unlocked_locks = lock_message_to_lock_indexes(message)
        self.info("released locks: " + str(unlocked_locks)) if __debug__ else None
        for lock in unlocked_locks:
            self.locks_held.remove(lock)
        self.locking_message_index = self.locking_message_index + 1


    def aquire_locks(self):
        #get the unique set of buckets and remove -1 (the root) from the search path
        self.locking_message_index = 0

        buckets = search_path_to_buckets(self.search_path)
        self.info("gather locks for buckets: " + str(buckets)) if __debug__ else None
        lock_messages = get_lock_messages(buckets, self.buckets_per_lock, self.locks_per_message)
        self.current_locking_messages = masked_cas_lock_table_messages(lock_messages)
        return self.get_current_locking_message_with_covering_read()

    def release_locks_batched(self):
        self.locking_message_index = 0
        buckets = lock_indexes_to_buckets(self.locks_held, self.buckets_per_lock)
        buckets.sort(reverse=True)
        unlock_messages = get_unlock_messages(buckets, self.buckets_per_lock, self.locks_per_message)
        self.current_locking_messages = masked_cas_lock_table_messages(unlock_messages)
        return self.current_locking_messages

    def aquire_locks_with_reads_fsm(self, message):
        if message_type(message) == "masked_cas_response":
            if message.payload["function_args"]["success"] == True:
                self.receieve_successful_locking_message(message)
            #1) retransmit if we did not make process
            #2) or just select the next message
            if not self.all_locks_aquired():
                return self.get_current_locking_message_with_covering_read()

        if message_type(message) == "read_response":
            #unpack and check the response for a valid read
            args = unpack_read_response(message)
            fill_local_table_with_read_response(self.table, args)
            self.outstanding_read_requests = self.outstanding_read_requests - 1
            #enter the critical section if we have all of the locks
            #we also want to have all of the reads completed (for now)
            if self.all_locks_aquired() and self.read_complete():
                self.state = "critical_section"
        return None

    def release_locks_fsm(self, message):
        if message_type(message) == "masked_cas_response":
            if message.payload["function_args"]["success"] == False:
                self.critical("What the fuck is happening I failed to release a lock")
                exit(1)
            
            #successful response
            self.receive_successful_unlocking_message(message)
            if self.all_locks_released():
                if self.state == "inserting":
                    return self.complete_insert()
                if self.state == "release_locks_try_again":
                    return self.search()
            return None


    def search(self, message=None):
        assert message == None, "there should be no message passed to search"

        self.search_path=bucket_cuckoo_a_star_insert(self.table, hash.rcuckoo_hash_locations, self.current_insert_value)
        if len(self.search_path) == 0:
            self.info("Search Failed: " + str(self.current_insert_value) + "| unable to continue, client " + str(self.id) + " is done")
            self.complete=True
            self.state = "idle"
            return None

        self.info("Search complete aquiring locks") if __debug__ else None
        self.state="aquire_locks"
        return self.aquire_locks()


    def begin_insert(self):
        self.state = "inserting"
        #todo there are going to be cases where this fails because

        search_buckets=lock_indexes_to_buckets(self.locks_held, self.buckets_per_lock)
        self.search_path=bucket_cuckoo_a_star_insert(self.table, hash.rcuckoo_hash_locations, self.current_insert_value, search_buckets)
        if len(self.search_path) == 0:
            self.info("Search Failed: " + str(self.current_insert_value) + "| unable to continue, client " + str(self.id) + " is done") if __debug__ else None
            return self.retry_insert()
        self.search_path_index = len(self.search_path) - 1

        insert_messages = gen_cas_messages(self.search_path)
        unlock_messages = self.release_locks_batched()

        insert_messages.extend(unlock_messages)

        #I can add the unlock messages here as well because I know that I'm going to succeed at this point
        return insert_messages

    def complete_insert_stats(self, success):
        self.insert_path_lengths.append(len(self.search_path))
        self.index_range_per_insert.append(path_index_range(self.search_path))
        self.messages_per_insert.append(self.current_insert_messages)

        #clear for next insert
        self.current_insert_messages = 0
        return super().complete_insert_stats(success)

    def complete_insert(self):
        self.info("Insert Complete: " + str(self.current_insert_value)) if __debug__ else None
        self.state= "idle"
        self.inserting=False
        self.complete_insert_stats(success=True)
        return None

    def retry_insert(self):
        self.state="release_locks_try_again"
        return self.release_locks_batched()


    def insert_cas_fsm(self, message):
        args = unpack_cas_response(message)
        fill_local_table_with_cas_response(self.table, args)

        #If CAS failed, try the insert a second time.
        success = args["success"]
        if not success:
            raise Exception("Failed Cas on Insert path -- we should not have failures here")

        #Step down the search path a single index
        self.search_path_index -= 1
        if self.search_path_index == 0:
            self.debug("Insertion Path Complete") if __debug__ else None
        elif self.search_path_index < 0:
            raise Exception("Error we have gone too far down the search path, too many received CAS messages")
        
        #just return None we are only using this method to receieve messages
        return None

    def insert_and_release_fsm(self, message):
        #there should be a message, otherwise don't do anything
        if message == None:
            return None
        
        if message_type(message) == "cas_response":
            return self.insert_cas_fsm(message)

        if message_type(message) == "masked_cas_response":
            return self.release_locks_fsm(message)


    def idle_fsm(self,message):
        return self.general_idle_fsm(message)

    def read_fsm(self,message):
        complete, success = self.wait_for_read_messages_fsm(message, self.current_read_key)
        if complete:
            self.state="idle"
            self.complete_read_stats(success, self.current_read_key)
            self.reading=False
        return None

    def fsm_logic(self, message = None):

        if self.complete and self.state == "idle":
            return None

        if self.state== "idle":
            return self.idle_fsm(message)

        if self.state == "reading":
            return self.read_fsm(message)

        if self.state == "aquire_locks":
            return self.aquire_locks_with_reads_fsm(message)

        if self.state == "release_locks_try_again":
            return self.release_locks_fsm(message)

        if self.state == "critical_section":
            return self.begin_insert()

        if self.state == "inserting":
            return self.insert_and_release_fsm(message)


    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)