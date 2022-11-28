import hash

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
        print("returned value is not the same as the value in the table, inserted it anyways")

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
        self.config = config

    def fsm(self, message=None):
        print("state machine top level overload this")

class basic_memory_state_machine(state_machine):
    def __init__(self,config):
        super().__init__(config)
        self.table = config["table"]
        self.bucket_size = len(self.table)
        self.table_size = len(self.table[0])
        self.state = "memory... state not being used"
    
    def fsm(self, message=None):
        print(message.payload)
        if message == None:
            return None
        if message.payload["function"] == read_table_entry:
            print("running read table entry")
            bucket_id=message.payload["function_args"]['bucket_id']
            bucket_offset=message.payload["function_args"]['bucket_offset']
            size=message.payload["function_args"]['size']

            read = read_table_entry(self.table, bucket_id, bucket_offset, size)
            m = Message({"function":fill_table_with_read, "function_args":{"read":read}})
            m.payload["function_args"]["bucket_id"] = bucket_id
            m.payload["function_args"]["bucket_offset"] = bucket_offset
            m.payload["function_args"]["size"] = size

            print(m)

            return m

        if message.payload["function"] == cas_table_entry:
            print("running cas table entry")
            bucket_id=message.payload["function_args"]['bucket_id']
            bucket_offset=message.payload["function_args"]['bucket_offset']
            old=message.payload["function_args"]['old']
            new=message.payload["function_args"]['new']
            success, value = cas_table_entry(self.table, bucket_id, bucket_offset, old, new)

            m = Message({"function":fill_table_with_cas, "function_args":{"value":value, "success":success}})
            m.payload["function_args"]["bucket_id"] = bucket_id
            m.payload["function_args"]["bucket_offset"] = bucket_offset
            print(m)
            return m


        else:
            print("unknown message type " + str(message))

class basic_insert_state_machine(state_machine):
    def __init__(self, config):
        self.total_inserts = config["total_inserts"]
        self.table = config["table"]
        self.table_size = len(self.table)
        self.bucket_size = len(self.table[0])

        self.current_insert = 0
        self.state="idle"
    def __str__(self):
        print("Basic_Insert_State_Machine")

    def fsm(self, message = None):
        if self.state == "idle":
            assert message == None, "idle state should not have a message being returned, message is old " + str(message)
            print("generating insert")
            self.current_insert += 1
            self.state = "reading"
            locations = hash.hash_locations(self.current_insert, self.table_size)
            print(locations)

            #only perform an insert to the first location.
            location = locations[0]
            message = Message({})
            message.payload["function"] = read_table_entry
            message.payload["function_args"] = {'bucket_id':location, 'bucket_offset':0, 'size':n_buckets_size(self.bucket_size)}
            return message

        if self.state == "reading":
            if message == None:
                print("client is in reading state, and no message was provided, returning")
                return None

            #insert the table which was read from remote memory
            assert message.payload["function"] == fill_table_with_read, "client is in reading state but message is not a read " + str(message)
            print("inserting response")
            bucket_id = message.payload["function_args"]["bucket_id"]
            bucket_offset = message.payload["function_args"]["bucket_offset"]
            size = message.payload["function_args"]["size"]
            fill_table_with_read(self.table, bucket_id, bucket_offset, size, message.payload["function_args"]["read"])

            #at this point we have done the read on the remote node, it's time to actually do the insert
            #find the first empty bucket
            bucket=self.table[bucket_id]
            bucket_cas_index = -1
            for i in range(self.bucket_size):
                if bucket[i] == None:
                    bucket_cas_index = i
                    break
            if (bucket_cas_index == -1):
                print("bucket is full, evicting")
                print("Todo now we need to cuckoo search")
                print("exiting for now")
                exit(1)
            
            #at this point we can actually attempt an insert
            message = Message({})
            message.payload["function"] = cas_table_entry
            message.payload["function_args"] = {'bucket_id':bucket_id, 'bucket_offset':bucket_cas_index, 'old':None, 'new':self.current_insert}
            self.state = "inserting"
            return message

        if self.state == "inserting":
            if message == None:
                print("client is in inserting state, and no message was provided, returning")
                return None
            assert message.payload["function"] == fill_table_with_cas, "client is in inserting state but message is not a cas " + str(message)
            print("inserting response")
            bucket_id = message.payload["function_args"]["bucket_id"]
            bucket_offset = message.payload["function_args"]["bucket_offset"]
            success = message.payload["function_args"]["success"]
            value = message.payload["function_args"]["value"]
            fill_table_with_cas(self.table, bucket_id, bucket_offset, success, value)

            if success:
                self.state = "idle"
                return None
            else:
                print("cas failed, evicting")
                print("Todo now we need to cuckoo search")
                print("exiting for now")
                exit(1)





            # generate read
        if self.state == "done":
            print(str(self) + " done")


    
    




    