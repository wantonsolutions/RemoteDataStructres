import hash
import logging
import heapq
from tqdm import tqdm
import random

logger = logging.getLogger('root')

TABLE_ENTRY_SIZE = 8

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
        return "(" + str(self.key) + "," + str(self.table_index) + "," + str(self.bucket_index) + "," + str(self.bucket_offset) + ")"

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

#locations for insertion are found in both directions from
#the hash location.  this function searches in both
#directions to produce a list of n locations which are close
#to the original hash. There is no guarantee that these
#entries are reachable. They are merely the closest n open
#entries.
def find_closest_target_n_bi_directional(table, location_func,value, n):
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
    def __str__(self) -> str:
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



def fscore(element, target_index, table_size):
        #TODO start here tomorrow, make Fscore work on a merged table
        g_score = element.distance
        h_score = heuristic_3(element.pe.bucket_index, target_index, table_size)
        f_score = g_score + h_score
        return f_score

def a_star_search(table, location_func, value):
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
            print("a*", locations)
            table_index = next_table_index(search_element.pe.table_index)
            index = locations[table_index]
            print("a*", index)


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
        search_index = location_func(value, table_size, suffix)[0]
        return[]

def bucket_cuckoo_a_star_insert(table, location_func, value):
    insert_paths=a_star_search(table, location_func, value)
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


def messages_append_or_extend(messages, message):
    if message != None:
        if isinstance(message, list):
            messages.extend(message)
        else:
            messages.append(message)
    return messages


class Table:
    def __init__(self, memory_size, bucket_size):
        self.number_tables=2
        self.entry_size=8
        self.memory_size=memory_size
        self.bucket_size=bucket_size
        print(memory_size)
        print(bucket_size)
        self.table=self.generate_bucket_cuckoo_hash_index(memory_size, bucket_size)
        self.table_size = len(self.table)


    def print(self):
        for i in range(0, self.table_size):
            for j in range(0, self.bucket_size):
                print("[" + '{0: <5}'.format(str(self.table[i][j])) + "] ", end='')
            print("")

    def get_bucket_size(self):
        return len(self.table[0]) * TABLE_ENTRY_SIZE
        
    def row_size(self):
        return self.n_buckets_size(self.bucket_size)

    def n_buckets_size(self, n_buckets):
        return n_buckets * TABLE_ENTRY_SIZE

    def get_entry(self, bucket, offset):
        return self.table[bucket][offset]

    def set_entry(self, bucket, offset, entry):
        self.table[bucket][offset]=entry


    def bucket_has_empty(self, bucket_index):
        return None in self.table[bucket_index]

    def get_first_empty_index(self, bucket_index):
        for i in range(len(self.table[bucket_index])):
            if self.table[bucket_index][i] == None:
                return i

    def bucket_contains(self, bucket_index, value):
        return value in self.table[bucket_index]


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
        table_div_error = "Memory must divide evenly across tables. Memory:" + str(memory_size) + " Tables:" + str(self.number_tables)
        assert memory_size % self.number_tables == 0, table_div_error

        #We also need the number of buckets to fit into memory correctly
        bucket_div_error = "Memory must divide evenly across buckets. Memory:" + str(memory_size) + " Buckets:" + str(bucket_size)
        assert (memory_size/self.number_tables) % bucket_size == 0, bucket_div_error

        #finally each entry in the bucket is 8 bytes
        entry_div_error = "Memory must divide evenly across entries. Memory:" + str(memory_size) + " Entries:" + str(self.entry_size)
        assert ((memory_size/self.number_tables)/bucket_size % self.entry_size == 0), entry_div_error

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
        assert bucket_id * bucket_offset + total_indexs <= self.table_size * self.bucket_size, "operation is outside of the table"

    def assert_read_table_properties(self, read_size):
        assert self.table != None, "table is not initalized"
        assert self.table[0] != None, "table must have dimensions"
        assert read_size % TABLE_ENTRY_SIZE == 0, "size must be a multiple of 8"

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
        return (False, old)

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

#cuckoo protocols

class state_machine:
    def __init__(self, config):
        self.logger = logging.getLogger("root")
        self.config = config

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

    def fsm(self, message=None):
        self.logger.debug("state machine top level overload this")

class basic_memory_state_machine(state_machine):
    def __init__(self,config):
        super().__init__(config)
        self.table = config["table"]
        self.state = "memory... state not being used"

    def __str__(self):
        return "Memory"
    
    def fsm(self, message=None):
        if message == None:
            return None
        args = message.payload["function_args"]
        if message.payload["function"] == read_table_entry:
            self.info("Read: " + "Bucket: " + str(args["bucket_id"]) + " Offset: " + str(args["bucket_offset"]) + " Size: " + str(args["size"]))
            read = read_table_entry(self.table, **args)
            response = Message({"function":fill_table_with_read, "function_args":{"read":read, "bucket_id":args["bucket_id"], "bucket_offset":args["bucket_offset"], "size":args["size"]}})
            self.info("Read Response: " +  str(response.payload["function_args"]["read"]))
            return response

        if message.payload["function"] == cas_table_entry:
            self.info("CAS: " + "Bucket: " + str(args["bucket_id"]) + " Offset: " + str(args["bucket_offset"]) + " Old: " + str(args["old"]) + " New: " + str(args["new"]))
            success, value = cas_table_entry(self.table, **args)
            response = Message({"function":fill_table_with_cas, "function_args":{"bucket_id":args["bucket_id"], "bucket_offset":args["bucket_offset"], "value":value, "success":success}})

            self.table.print()

            rargs=response.payload["function_args"]
            self.info("Read Response: " +  "Success: " + str(rargs["success"]) + " Value: " + str(rargs["value"]))
            return response

        else:
            self.logger.warning("unknown message type " + str(message))

class basic_insert_state_machine(state_machine):
    def __init__(self, config):
        super().__init__(config)
        self.total_inserts = config["total_inserts"]
        self.id = config["id"]
        self.table = config["table"]

        self.current_insert = 0
        self.search_path = []
        self.search_path_index = 0
        self.state="idle"

    def __str__(self):
        return "Client " + str(self.id)

    def read_current(self):
        locations = hash.hash_locations(self.current_insert, self.table.table_size)
        self.info("Inserting: " + str(self.current_insert) + " Locations: " + str(locations))
        bucket = locations[0]
        message = Message({})
        message.payload["function"] = read_table_entry
        message.payload["function_args"] = {'bucket_id':bucket, 'bucket_offset':0, 'size':self.table.row_size()}
        return message



    def read_path_element(self, pe):
        message = Message({})
        message.payload["function"] = read_table_entry
        message.payload["function_args"] = {'bucket_id':pe.bucket_index, 'bucket_offset':0, 'size':self.table.row_size()}
        return message

    def table_contains_value(self, value):
        #todo this does not actually work because their may be duplicates while inserting
        #todo asynchronusly
        locations = hash.hash_locations(value, self.table.table_size)
        for bucket_index in locations:
            if self.table.bucket_contains(bucket_index, value):
                return True
        return False


    def fsm(self, message = None):

        if self.state == "idle":
            assert message == None, "idle state should not have a message being returned, message is old " + str(message)
            self.current_insert += 1

            self.state = "reading"
            #only perform an insert to the first location.
            self.search_path=bucket_cuckoo_a_star_insert(self.table, hash.hash_locations, self.current_insert)
            print_path(self.search_path)
            self.search_index=1
            return self.read_path_element(self.search_path[self.search_index])

        if self.state == "reading":
            if message == None:
                self.debug("client is in reading state, and no message was provided, returning")
                return None

            #insert the table which was read from remote memory
            assert message.payload["function"] == fill_table_with_read, "client is in reading state but message is not a read " + str(message)
            self.info("Read Response: " +  str(message.payload["function_args"]["read"]))
            args = message.payload["function_args"]
            fill_table_with_read(self.table, **args)

            #at this point we have done the read on the remote node, it's time to actually do the insert
            #find the first empty bucket
            
            #check to see if the bucket contains the insert value
            if self.table_contains_value(self.current_insert):
                self.warning("Insert value " + str(self.current_insert) + " already exists in bucket, not inserting, and going idle")
                self.state = "idle"
                return

            bucket_cas_index = self.table.find_empty_index(args["bucket_id"])
            if (bucket_cas_index == -1):
                self.warning("Bucket is full, not inserting and exiting for now. Need to Cuckoo Search")
                exit(1)
            
            #at this point we can actually attempt an insert
            message = Message({})
            message.payload["function"] = cas_table_entry
            message.payload["function_args"] = {'bucket_id':args["bucket_id"], 'bucket_offset':bucket_cas_index, 'old':None, 'new':self.current_insert}
            self.state = "inserting"
            return message

        if self.state == "inserting":
            if message == None:
                self.debug("State: Inserting, no message provided")
                return None
            assert message.payload["function"] == fill_table_with_cas, "client is in inserting state but message is not a cas " + str(message)
            args = message.payload["function_args"]
            self.info("Insert Response: " +  "Success: " + str(args["success"]) + " Value: " + str(args["value"]))
            fill_table_with_cas(self.table, **args)

            if args["success"]:
                self.state = "idle"
                return None
            else:
                self.warning("cas failed, retrying read")
                self.state = "reading"
                return self.read_current()

            # generate read
        if self.state == "done":
            self.logger.debug(str(self) + " done")


    
    




    