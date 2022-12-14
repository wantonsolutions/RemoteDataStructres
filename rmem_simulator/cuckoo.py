import hash
import logging

logger = logging.getLogger('root')

TABLE_ENTRY_SIZE = 8

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

def print_bucket_table(table, table_size, bucket_size):
    for i in range(0, table_size):
        for j in range(0, bucket_size):
            print("[" + '{0: <5}'.format(str(table[i][j])) + "] ", end='')
        print("")

def messages_append_or_extend(messages, message):
    if message != None:
        if isinstance(message, list):
            messages.extend(message)
        else:
            messages.append(message)
    return messages


def get_bucket_size(table):
    return len(table[0]) * TABLE_ENTRY_SIZE

def n_buckets_size(n_buckets):
    return n_buckets * TABLE_ENTRY_SIZE


def generate_bucket_cuckoo_hash_index(memory_size, bucket_size):
    number_tables=2
    entry_size=8
    #for cuckoo hashing we have two tables, therefore memory size must be a power of two
    assert(memory_size % number_tables == 0)
    #We also need the number of buckets to fit into memory correctly
    assert((memory_size/number_tables) % bucket_size == 0)
    #finally each entry in the bucket is 8 bytes
    assert(((memory_size/number_tables)/bucket_size % entry_size == 0))
    table = []
    rows = int((memory_size/entry_size)/bucket_size)
    for i in range(rows):
        table.append([None]*bucket_size)
    return table

def absolute_index_to_bucket_index(index, bucket_size):
    bucket_offset = index % bucket_size
    bucket = int(index/bucket_size)
    return (bucket,bucket_offset)

def cas_table_entry(table, bucket_id, bucket_offset, old, new):
    v = table[bucket_id][bucket_offset]
    if v == old:
        table[bucket_id][bucket_offset] = new
        return (True, new)
    else:
        return (False, old)

def fill_table_with_cas(table, bucket_id, bucket_offset, success, value):
    assert_operation_in_table_bound(table, bucket_id, bucket_offset, TABLE_ENTRY_SIZE)
    table[bucket_id][bucket_offset] = value

    if not success:
        logger.warning("returned value is not the same as the value in the table, inserted it anyways")

def assert_read_table_properties(table, read_size):
    assert table != None, "table is not initalized"
    assert table[0] != None, "table must have dimensions"
    assert read_size % TABLE_ENTRY_SIZE == 0, "size must be a multiple of 8"

def assert_operation_in_table_bound(table, bucket_id, bucket_offset, size):
    assert_read_table_properties(table, size)

    bucket_size = len(table[0])
    total_indexs = size/8
    total_buckets = len(table)
    assert bucket_id * bucket_offset + total_indexs <= total_buckets * bucket_size, "operation is outside of the table"

def read_table_entry(table, bucket_id, bucket_offset, size):

    assert_operation_in_table_bound(table, bucket_id, bucket_offset, size)
    bucket_size = len(table[0])
    total_indexs = int(size/TABLE_ENTRY_SIZE)

    #collect the read
    read = []
    base = bucket_id * bucket_size + bucket_offset
    for i in range(total_indexs):
        bucket, offset = absolute_index_to_bucket_index(base + i, bucket_size)
        read.append(table[bucket][offset])
    return read

def fill_table_with_read(table, bucket_id, bucket_offset, size, read):
    assert_operation_in_table_bound(table, bucket_id, bucket_offset, size)

    bucket_size = len(table[0])
    total_indexs = int(size/TABLE_ENTRY_SIZE)

    #write remote read to the table
    base = bucket_id * bucket_size + bucket_offset
    for i in range(total_indexs):
        bucket, offset = absolute_index_to_bucket_index(base + i, bucket_size)
        table[bucket][offset] = read[i]

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
        self.bucket_size = len(self.table[0])
        self.table_size = len(self.table)
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

            print_bucket_table(self.table, self.table_size, self.bucket_size)

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
        self.table_size = len(self.table)
        self.bucket_size = len(self.table[0])

        self.current_insert = 0
        self.state="idle"

    def __str__(self):
        return "Client " + str(self.id)

    def read_current(self):
        locations = hash.hash_locations(self.current_insert, self.table_size)
        self.info("Inserting: " + str(self.current_insert) + " Locations: " + str(locations))
        bucket = locations[0]
        message = Message({})
        message.payload["function"] = read_table_entry
        message.payload["function_args"] = {'bucket_id':bucket, 'bucket_offset':0, 'size':n_buckets_size(self.bucket_size)}
        return message

    def table_contains_value(self, value):
        #todo this does not actually work because their may be duplicates while inserting
        #todo asynchronusly
        locations = hash.hash_locations(value, self.table_size)
        for location in locations:
            bucket = self.table[location]
            if value in bucket:
                return True
        return False

    def find_empty_index(self, bucket_index):
        bucket = self.table[bucket_index]
        empty_index = -1
        for i in range(len(bucket)):
            if bucket[i] == None:
                empty_index = i
                break
        return empty_index


    def fsm(self, message = None):
        if self.state == "idle":
            assert message == None, "idle state should not have a message being returned, message is old " + str(message)
            self.current_insert += 1

            self.state = "reading"
            #only perform an insert to the first location.
            return self.read_current()

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

            bucket_cas_index = self.find_empty_index(args["bucket_id"])
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


    
    




    