import logging
logger = logging.getLogger('root')

from . import hash

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

CAS_SIZE=64
TABLE_ENTRY_SIZE=8

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

inverted_mask_index_global = [0] * CAS_SIZE
def masked_cas_lock_table(lock_table, lock_index, old, new, mask):
    #sanity check
    assert len(old) == CAS_SIZE, "old must be 64 bytes"
    assert len(old) == len(new), "old and new must be the same length"
    assert len(new) == len(mask), "new and mask must be the same length"

    assert lock_table != None, "lock table is not initalized"
    assert lock_index < lock_table.total_locks, "lock index is out of bounds"


    #create an inverted index of the mask, to reduce the indexes we needto check.
    #i've pre
    global inverted_mask_index_global
    c=0
    for i in range(len(mask)):
        if mask[i] == 1:
            inverted_mask_index_global[0]=i
            c+=1

    inverted_mask_index = inverted_mask_index_global[0:c]
    #XOR check that the old value in the cas matches the existing lock table.
    for v in inverted_mask_index:
        if not lock_table.locks[lock_index+v].equals(old[v]):
            return (False, old)
    
    #now we just apply. At this point we know that the old value is the same as the current so we can lock and unlock accordingly.
    for v in inverted_mask_index:
        lock_table.locks[lock_index+v].set_bit(new[v])
    return (True, new)

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
    base_indicies = [int(buckets_per_lock * l) for l in lock_indexes]
    indicies = []
    for base in base_indicies:
        indicies.extend(list(range(base, base + buckets_per_lock)))
    return indicies


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
    base_index = lock_message.payload['function_args']['lock_index']
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

    # min_index = (min_index * buckets_per_lock) + base_index
    # max_index = (max_index * buckets_per_lock) + base_index

    min_index = (min_index + base_index) * buckets_per_lock
    max_index = ((max_index + base_index) * buckets_per_lock) + (buckets_per_lock - 1)

    # print("base_index: " + str(base_index) + " min_index: " + str(min_index), "max_index: " + str(max_index))

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

def race_message_read_key_location(key, table_size, row_size_bytes, location):
    locations = hash.race_hash_locations(key, table_size)
    #get the min of the overflow and index buckets
    l = min(locations[location])
    double_size = int(row_size_bytes * 2)
    locations = [l]
    messages = single_bucket_read_messages(locations, double_size)
    return messages

def gen_cas_messages(search_path):
    i = len(search_path)-1
    # print(search_path)
    messages = []
    while i > 0:
        messages.append(next_cas_message(search_path,i))
        i-=1
    return messages