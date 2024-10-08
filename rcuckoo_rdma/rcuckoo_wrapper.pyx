cimport rcuckoo_wrapper as rw
# cimport rcuckoo_wrapper as t
from libcpp.string cimport string

from simulator import virtual_rdma as vrdma


def set_factor(x):
    return rw.set_factor(x)

def get_factor():
    return rw.get_factor()

def get_table_id_from_index(index):
    return rw.get_table_id_from_index(index)

def distance_to_bytes(a, b, bucket_size, entry_size):
    return rw.distance_to_bytes(a, b, bucket_size, entry_size)

def h1(key):
    return rw.h1(key)

def h2(key):
    return rw.h2(key)

def h3(key):
    return rw.h3(key)

def rcuckoo_primary_location(key, table_size):
    return rw.rcuckoo_primary_location(key, table_size)

def h3_suffix_base_two(key):
    return rw.h3_suffix_base_two(key)

def rcuckoo_secondary_location(key, factor, table_size):
    return rw.rcuckoo_secondary_location(key, factor, table_size)

def rcuckoo_secondary_location_independent(key, table_size):
    return rw.rcuckoo_secondary_location_independent(key, table_size)

def int_str_converter(orig_int):
    output = ""
    for i in range(4):
        ret = int(orig_int % 256)
        output += str(ret)
        orig_int = int(orig_int / 256)
    return output

def rcuckoo_hash_locations(key, table_size):

    output_string = int_str_converter(key)
    cdef string string_key = output_string.encode('utf-8')
    # c_key.bytes = key.to_bytes(4, byteorder='little')
    # print("do the ckey bytes at least look right? ", string_key)
    # for i in range(4):
    #     c_key.bytes[i] = key.to_bytes(4, byteorder='little')[i]



    # tmp_string_key = str(c_key.bytes)
    # string_key = tmp_string_key.encode('utf-8')

    locations = rw.rcuckoo_hash_locations(string_key, table_size)
    return (locations.primary, locations.secondary)

def rcuckoo_hash_locations_independent(key, table_size):
    bytes_key = str(key).encode('utf-8')
    locations = rw.rcuckoo_hash_locations_independent(bytes_key, table_size)
    return (locations.primary, locations.secondary)

def to_race_index_math(index,table_size):
    locations = rw.to_race_index_math(index, table_size)
    return (locations.bucket, locations.overflow)

def race_primary_location(key, table_size):
    bytes_key = str(key).encode('utf-8')
    locations = rw.race_primary_location(bytes_key, table_size)
    return (locations.bucket, locations.overflow)

def race_secondary_location(key, table_size):
    bytes_key = str(key).encode('utf-8')
    locations = rw.race_secondary_location(bytes_key, table_size)
    return (locations.bucket, locations.overflow)

def race_hash_locations(key, table_size):
    bytes_key = str(key).encode('utf-8')
    hash_buckets = rw.race_hash_locations(bytes_key, table_size)
    return ((hash_buckets.primary.bucket,hash_buckets.primary.overflow), (hash_buckets.secondary.bucket,hash_buckets.secondary.overflow))

def ten_k_hashes():
    return rw.ten_k_hashes()

# distutils: language = c++
# from tables_wrapper cimport Table as t
# from tables_wrapper import Table as t
# cimport tables_wrapper_def as t
# cimport tables_wrapper as t
# from tables_wrapper cimport PyTable
# import tables_wrapper_def as t
# cimport tables_wrapper_forward_def as tab
# from .tables_wrapper_forward_def cimport Table
# cimport Table from tables_wrapper_forward_def
# cimport tables_wrapper_forward_def as tab
# t=h
from libcpp cimport bool

def key_to_c_key(key):
    cdef rw.Key c_key
    c_key = str(key)
    return c_key

cdef class PyLock_Table:
    cdef rw.Lock_Table c_lock_table

    def __init__(self, unsigned int memory_size=-1, unsigned int bucket_size=-1, unsigned int buckets_per_lock=-1):
        if memory_size is not -1 and bucket_size is not -1 and buckets_per_lock is not -1:
            self.c_lock_table = rw.Lock_Table(memory_size, bucket_size, buckets_per_lock)
        else:
            self.c_lock_table = rw.Lock_Table()

    def unlock_all(self):
        return self.c_lock_table.unlock_all()

    def masked_cas(self, unsigned int index, old, new_value, mask):
        #todo: these are getting called with arrays convert them to uin64_t
        response = self.c_lock_table.masked_cas(index, old, new_value, mask)
        return (response.success, response.new_value)

    def fill_masked_cas(self, unsigned int index, bool success, new_value, mask):
        #todo: these are getting called with arrays convert them to uin64_t
        self.c_lock_table.fill_masked_cas(index, success, new_value, mask)
        return

cdef class PyTable:
    cdef rw.Table *c_table

    # def __cinit__(self, unsigned int memory_size=-1, unsigned int bucket_size=-1, unsigned int buckets_per_lock=-1):
    #     if memory_size is not -1 and bucket_size is not -1 and buckets_per_lock is not -1:
    #         self.c_table = new t.Table(memory_size, bucket_size, buckets_per_lock)
    #     else:
    #         self.c_table = new t.Table()

    def __init__(self, unsigned int memory_size=-1, unsigned int bucket_size=-1, unsigned int buckets_per_lock=-1):
        if memory_size is not -1 and bucket_size is not -1 and buckets_per_lock is not -1:
            self.c_table = new rw.Table(memory_size, bucket_size, buckets_per_lock)
        else:
            self.c_table = new rw.Table()

    def __dealloc__(self):
        del self.c_table

    def unlock_all(self):
        return self.c_table.unlock_all()

    def print_table(self):
        return self.c_table.print_table()

    def lock_table_masked_cas(self, unsigned int lock_index, old, new_value, mask):
        cdef unsigned long long c_old =0 
        cdef unsigned long long c_new_value =0
        cdef unsigned long long c_mask =0

        for i in range(0, 64):
            # print("old[" + str(i) + "]: ", old[i])
            c_old = c_old | (old[i] << i)
            c_new_value = c_new_value | (new_value[i] << i)
            c_mask = c_mask | (mask[i] << i )
        # print("c_old: ", c_old)
        # print("c_new_value: ", c_new_value)
        # print("c_mask: ", c_mask)

        # return self.c_table.lock_table_masked_cas(lock_index, old, new_value, mask)
        atomic_response  = self.c_table.lock_table_masked_cas(lock_index, c_old, c_new_value, c_mask)
        # print(atomic_response.success, atomic_response.original_value)
        return (atomic_response.success, atomic_response.original_value)


    def fill_lock_table_masked_cas(self, unsigned int lock_index, bool success, value, mask):
        self.c_table.fill_lock_table_masked_cas(lock_index, success, value, mask)

    def get_buckets_per_row(self):
        return self.c_table.get_buckets_per_row()

    def get_row_count(self):
        return self.c_table.get_row_count()

    def get_bucket_size(self):
        return self.c_table.get_bucket_size()

    def row_size_bytes(self):
        return self.c_table.row_size_bytes()

    def n_buckets_size(self, unsigned int n_buckets):
        return self.c_table.n_buckets_size(n_buckets)

    def get_entry(self, unsigned int bucket_index, unsigned int offset):
        e = self.c_table.get_entry(bucket_index, offset)
        bytes = e.key.bytes
        # print("sanity checking get entries bytes: ", bytes)
        key = int.from_bytes(bytes, "little")
        return key

    def set_entry(self, unsigned int bucket_index, unsigned int offset, entry):
        cdef rw.Entry c_entry

        c_entry.key.bytes = entry.to_bytes(4, byteorder='little')
        # print("sanity checking set entries bytes: ", c_entry.key.bytes)
        self.c_table.set_entry(bucket_index, offset, c_entry)

    
    def bucket_has_empty(self, unsigned int bucket_index):
        return self.c_table.bucket_has_empty(bucket_index)

    def get_first_empty_index(self, unsigned int bucket_index):
        return self.c_table.get_first_empty_index(bucket_index)

    def bucket_contains(self, unsigned int bucket_index, key):
        c_key = key_to_c_key(key)
        return self.c_table.bucket_contains(bucket_index, c_key)

    def get_fill_percentage(self):
        return self.c_table.get_fill_percentage()

    def full(self):
        return self.c_table.full()

    def generate_bucket_cuckoo_hash_index(self, unsigned int memory_size, unsigned int bucket_size):
        #todo: this is a crazy pointer thing watch out
        print("WARTING WARNTING WARING generate_bucket_cuckoo_hash_index not implemented")
        return None
        # return self.c_table.generate_bucket_cuckoo_hash_index(memory_size, bucket_size)

    def absolute_index_to_bucket_index(self, unsigned int absolute_index):
        return self.c_table.absolute_index_to_bucket_index(absolute_index)
    
    def absolute_index_to_bucket_offset(self, unsigned int absolute_index):
        return self.c_table.absolute_index_to_bucket_offset(absolute_index)
    
    def assert_operation_in_table_bound(self, unsigned int bucket_index, unsigned int offset, unsigned int memory_size):
        return self.c_table.assert_operation_in_table_bound(bucket_index, offset, memory_size)

    def contains_duplicates(self):
        return self.c_table.contains_duplicates()

    def get_duplicates(self):
        print("WARTING WARNTING WARING get_duplicates not implemented")
        # duplicates = self.c_table.get_duplicates()
        # #todo do something with the duplicates
        # print(duplicates)
        return None


from cython.operator cimport dereference as deref
from libcpp.vector cimport vector


def search_path_to_buckets(path):
    cdef vector[rw.path_element] c_path
    cdef rw.path_element pe
    for p in path:
        pe.key.bytes = p.key.to_bytes(4, byteorder='little')
        pe.table_index = p.table_index
        pe.bucket_index = p.bucket_index
        pe.offset = p.bucket_offset
        c_path.push_back(pe)
    return rw.search_path_to_buckets(c_path)

def path_to_string(path):
    cdef vector[rw.path_element] c_path
    cdef rw.path_element pe
    for p in path:
        pe.key.bytes = p.key.to_bytes(4, byteorder='little')
        pe.table_index = p.table_index
        pe.bucket_index = p.bucket_index
        pe.offset = p.bucket_offset
        c_path.push_back(pe)
    path_string = rw.path_to_string(c_path)
    return str(path_string)

def path_index_range(path):
    cdef vector[rw.path_element] c_path
    cdef rw.path_element pe
    for p in path:
        pe.key.bytes = p.key.to_bytes(4, byteorder='little')
        pe.table_index = p.table_index
        pe.bucket_index = p.bucket_index
        pe.offset = p.bucket_offset
        c_path.push_back(pe)
    return rw.path_index_range(c_path)

def bucket_cuckoo_a_star_insert(PyTable table, location_func, key, open_buckets=None):
    cdef vector[unsigned int] empty_buckets
    cdef rw.Key c_key
    #copy the key over
    c_key.bytes = key.to_bytes(4, byteorder='little')

    #determine which hash function to use
    if location_func.__name__ == "rcuckoo_hash_locations":
        sub_function = rw.rcuckoo_hash_locations
    elif location_func.__name__ == "rcuckoo_hash_locations_independent":
        sub_function = rw.rcuckoo_hash_locations_independent
    else :
        print("ERROR: location_func not recognized returning defualt func")
        return None

    #call the actuall insert function
    import simulator.search
    if open_buckets is None:
        dict_path =  rw.bucket_cuckoo_a_star_insert(deref(table.c_table), sub_function, c_key, empty_buckets)
    else:
        dict_path = rw.bucket_cuckoo_a_star_insert(deref(table.c_table), sub_function, c_key, open_buckets)

    #reconstruct a python path_element path from the c++ path
    ret_path = []
    for d in dict_path:
        bytes = d.key.bytes
        key = int.from_bytes(bytes, "little")
        table_index = d.table_index
        bucket_index = d.bucket_index
        bucket_offset = d.offset
        ret_path.append(simulator.search.path_element(key=key,table_index=table_index,bucket_index=bucket_index,bucket_offset=bucket_offset))
    ret_path.reverse()
    return ret_path

def bucket_cuckoo_random_insert(PyTable table, location_func, rw.Key key, open_buckets=None):
    cdef vector[unsigned int] empty_buckets
    if location_func.__name__ == "rcuckoo_hash_locations":
        sub_function = rw.rcuckoo_hash_locations
    elif location_func.__name__ == "rcuckoo_hash_locations_independent":
        sub_function = rw.rcuckoo_hash_locations_independent
    else :
        print("ERROR: location_func not recognized returning defualt func")
        return None

    if open_buckets is None:
        return rw.bucket_cuckoo_random_insert(deref(table.c_table), sub_function, key, empty_buckets)
    else:
        return rw.bucket_cuckoo_random_insert(deref(table.c_table), sub_function, key, open_buckets)


from libcpp.unordered_map cimport unordered_map

# cdef class PyState_Machine:
#     cdef rw.State_Machine *c_state_machine

#     def __init__(self, config=None):
#         cdef unordered_map[string,string] c_config
#         if config is not None:
#             for k, v in config.items():
#                 c_config[k] = v
#         self.c_state_machine = new rw.State_Machine(c_config)


#     # def get_state_machine_name(self):
#     #     return self.c_state_machine.get_state_machine_name()

# cdef class PyClient_State_Machine(PyState_Machine):
#     cdef rw.Client_State_Machine *c_client_state_machine

#     def __init__(self, config=None):
#         cdef unordered_map[string,string] c_config
#         if config is not None:
#             for k, v in config:
#                 c_config[k] = v
#         self.c_client_state_machine = new rw.Client_State_Machine(c_config)
    
#     # def get_state_machine_name(self):
#     #     return self.c_client_state_machine.get_state_machine_name()

def tryeval(val):
    import ast
    try:
        val = ast.literal_eval(val)
    except:
        return None
    return val

def decode_cpp_stats(client_stats):
    decoded_stats = dict()
    for key in client_stats:
        value = client_stats[key]
        # print("key: ", key, " value: ", client_stats[key])
        dec_key = key.decode('utf-8')
        dec_str_value = value.decode('utf-8')
        # print("dec_key: ", dec_key, " dec_str_value: ", dec_str_value)
        #I think the rule here is that if the value is nothign, then it's an empty array.
        #if it was a simple type, I should be able to cast it.
        #I believe that I'm only returning ints from the cpp code
        try_evaled = tryeval(dec_str_value)
        if try_evaled != None:
            decoded_stats[dec_key] = try_evaled
        else:
            if dec_str_value == "":
                decoded_stats[dec_key] = []
            elif "," in dec_str_value:
                decoded_stats[dec_key] = dec_str_value.split(",")
            else:
                print("ERROR: I don't know how to decode this value: ", value)
                exit(1)
    return decoded_stats

cdef class PyRDMA_Engine:
    cdef rw.RDMA_Engine *c_rdma_engine

    def __init__(self, config=None):
        print("---------------------Init PyRDMA_Engine------------------")
        cdef unordered_map[string,string] c_config
        if config is not None:
            for k, v in config.items():
                c_config[k.encode('utf8')] = v.encode('utf8')
        self.c_rdma_engine = new rw.RDMA_Engine(c_config)
        print("---------------------End PyRDMA_Engine------------------")

    def start(self):
        return self.c_rdma_engine.start()

    def get_stats(self):
        cdef unordered_map[string,string] c_stats
        c_stats = self.c_rcuckoo.get_stats()
        ret_stats = {}
        for k, v in c_stats:
            ret_stats[k] = v
        return decode_cpp_stats(ret_stats)

cdef class PyRCuckoo:
    cdef rw.RCuckoo *c_rcuckoo

    def __init__(self, config=None):
        cdef unordered_map[string,string] c_config
        print("---------------INIT PyRCuckoo---------")
        if config is not None:
            # print(config)
            for k in config:
                print("rcuckoo key:", k, "value: ", config[k])
                s_conf = str(config[k])
                c_config[k.encode('utf8')] = s_conf.encode('utf8')
        self.c_rcuckoo = new rw.RCuckoo(c_config)
        print("-----------END INIT PyRCuckoo---------")

    def get_state_machine_name(self):
        return self.c_rcuckoo.get_state_machine_name()

    def clear_statistics(self):
        self.c_rcuckoo.clear_statistics()

    def is_complete(self):
        return self.c_rcuckoo.is_complete()

    def get_completed_inserts(self):
        cdef vector[rw.Key] c_keys
        c_keys = self.c_rcuckoo.get_completed_inserts()
        ret_keys = []
        for k in c_keys:
            ret_keys.append(int.from_bytes(k.bytes, "little"))
        return ret_keys

    def set_max_fill(self, float max_fill):
        self.c_rcuckoo.set_max_fill(max_fill)
    
    def get_stats(self):
        cdef unordered_map[string,string] c_stats
        c_stats = self.c_rcuckoo.get_stats()
        ret_stats = {}
        for k, v in c_stats:
            ret_stats[k] = v
        return decode_cpp_stats(ret_stats)

    def fsm(self, message=None):
        cdef rw.VRMessage c_message
        if not message is None:
            c_message = encode_py_message_to_cpp_message(message.payload)
        output_messages = self.c_rcuckoo.fsm(c_message)
        ret_messages = []
        if len(output_messages) > 0:
            for m in output_messages:
                ret_messages.append(decode_cpp_message_to_python(m))
        return ret_messages

def lock_array_to_binary_string(lock_array):
    l_string = ""
    for l in lock_array:
        l_string += str(l)
    return l_string

def binary_string_to_lock_array(l_string):
    lock_array = []
    for l in l_string:
        lock_array.append(int(l))
    return lock_array


def decode_key_only_entry_from_string(k):
    shift = 0
    base_int = 0
    while len(k) > 0:
        base_str = k[:2]
        k = k[2:]
        base_int += int(base_str,16) << shift
        shift+=8
    return base_int

def decode_entry_from_string(e):
    k, v = e.split(":")
    return decode_key_only_entry_from_string(k)

def reverse_hex_string_in_bytes(hex_string):
    assert(len(hex_string) == 8)
    return hex_string[6:8] + hex_string[4:6] + hex_string[2:4] + hex_string[0:2]

def encode_entry_to_string(a):
    key = '{0:08X}'.format(a)
    key = reverse_hex_string_in_bytes(key)
    dummy_value = "0000"
    return key + ":" + dummy_value

def encode_entries_to_string(entries):
    str_list = ""
    for i in range(0, len(entries)):
        str_list += encode_entry_to_string(entries[i])
        if i != len(entries) - 1:
            str_list += ","
    return str_list.encode('utf8')


def decode_entry_from_binary_string(e):
    shift = 0
    base_int = 0
    e = e[::-1]
    while len(e) > 0:
        base_str = e[:1]
        e=e[1:]
        base_int += int(base_str,2) << shift
        shift+=1
    return base_int

def encode_entry_from_int(a):
    key = '{0:08X}'.format(int(str(a), 16))
    key = reverse_hex_string_in_bytes(key)
    dummy_value = "0000"
    e = key + ":" + dummy_value
    return e.encode('utf8')


def decode_entries_from_string(entries_string):
    entries = []
    for e in entries_string.split(","):
        entries.append(decode_entry_from_string(e))

    return entries

# def encode_entries_to_string(entries):


def encode_py_message_to_cpp_message(message):
    cdef rw.VRMessage c_message
    function_name = message["function"].__name__
    # print("function_name: ", function_name)
    # print("encoding messages INPUT: ", message)
    c_message.function = function_name.encode('utf8')
    for k in message["function_args"]:
        if k == "lock_index" or k == "bucket_id" or k == "bucket_offset" or k == "size":
            c_message.function_args[k.encode('utf8')] = str(message["function_args"][k]).encode('utf8')
        elif (k == "old" or k == "new" or k == "mask") and function_name == "masked_cas_lock_table":
            c_message.function_args[k.encode('utf8')] = (lock_array_to_binary_string(message["function_args"][k])).encode('utf8')
        elif (k == "old" or k == "new" or k == "mask" ) and function_name == "cas_table_entry":
            c_message.function_args[k.encode('utf8')] = str(message['function_args'][k]).encode('utf8')
        elif (k == "success"):
            if message['function_args'][k]:
                c_message.function_args[k.encode('utf8')] = "1".encode('utf8')
            else:
                c_message.function_args[k.encode('utf8')] = "0".encode('utf8')
        elif (k == "old" or k == "mask") and function_name == "fill_lock_table_masked_cas":
            c_message.function_args[k.encode('utf8')] = (lock_array_to_binary_string(message["function_args"][k])).encode('utf8')
        elif (k == "read" and function_name == "fill_table_with_read"):
            c_message.function_args[k.encode('utf8')] = encode_entries_to_string(message["function_args"][k])
        elif (k == "old" and function_name == "fill_table_with_cas"):
            c_message.function_args[k.encode('utf8')] = encode_entry_from_int(message["function_args"][k])
            # message.payload["function_args"][k] = encode_entry_from_binary_string(v.decode('utf8'))
        else:
            print("encode error : Unknown key: ", k, " in function_args for function: ", function_name)
            exit(0)
    # print("encoding messages OUTPUT: ", c_message)
    return c_message

def decode_cpp_message_to_python(rw.VRMessage c_message):

    # print("decoding messages INPUT: ", c_message)
    message = vrdma.Message({})
    function_name = c_message.function.decode('utf8')
    message.payload['function'] = vrdma.function_name_to_function_pointer_map[function_name]
    
    message.payload["function_args"] = {}
    for k, v in c_message.function_args:
        k= k.decode('utf8')
        if k == "lock_index" or k == "bucket_id" or k == "bucket_offset" or k == "size":
            message.payload["function_args"][k] = int(v.decode('utf8'))
        elif function_name == "fill_lock_table_masked_cas" and (k == "old" or k == "mask" ):
            message.payload["function_args"][k] = binary_string_to_lock_array(v.decode('utf8'))
        elif function_name == "fill_table_with_cas" and k == "old":
            message.payload["function_args"][k] = decode_entry_from_binary_string(v.decode('utf8'))
        elif function_name == "masked_cas_lock_table" and (k == "old" or k == "new" or k == "mask"):
            message.payload["function_args"][k] = binary_string_to_lock_array(v.decode('utf8'))
        elif function_name == "cas_table_entry" and (k == "old" or k == "new" or k == "mask"):
            message.payload["function_args"][k] = decode_key_only_entry_from_string(v.decode('utf8'))
        elif k == "read":
            message.payload["function_args"][k] = decode_entries_from_string(v.decode('utf8'))
        elif k == "success":
            message.payload["function_args"][k] = (v.decode('utf8') == '1')
        else:
            print("decode error : Unknown key: ", k, "function_name: ", function_name)
            exit(0)
    # print ("decoding messages OUTPUT: ", message)
    return message
            

    
cdef class PyMemory_State_Machine:
    cdef rw.Memory_State_Machine *c_memory_state_machine

    def __init__(self, config=None):
        cdef unordered_map[string,string] c_config
        print("---------------INIT PyMemory---------")
        if config is not None:
            # print(config)
            for k in config:
                print("memory: key:", k, "value: ", config[k])
                s_conf = str(config[k])
                c_config[k.encode('utf8')] = s_conf.encode('utf8')
        self.c_memory_state_machine = new rw.Memory_State_Machine(c_config)
        print("-----------END INIT PyMemory---------")

    def set_max_fill(self, int max_fill):
        self.c_memory_state_machine.set_max_fill(max_fill)

    def contains_duplicates(self):
        return self.c_memory_state_machine.contains_duplicates()

    def get_duplicates(self):
        print("todo translate duplicates -- currently the get duplicates function does not translate keys")
        cdef vector[rw.Duplicate_Entry] c_duplicates
        c_duplicates = self.c_memory_state_machine.get_duplicates()
        ret_duplicates = []
        for d in c_duplicates:
            ret_duplicates.append(d)
        return ret_duplicates

    def contains(self, key):
        cdef rw.Key c_key
        #copy the key over
        c_key.bytes = key.to_bytes(4, byteorder='little')
        return self.c_memory_state_machine.contains(c_key)


    def get_fill_percentage(self):
        return self.c_memory_state_machine.get_fill_percentage()

    def print_table(self):
        self.c_memory_state_machine.print_table()

    def fsm(self, message=None):
        cdef rw.VRMessage c_message
        if not message is None:
            c_message = encode_py_message_to_cpp_message(message.payload)

        output_messages = self.c_memory_state_machine.fsm(c_message)
        ret_messages = []
        if len(output_messages) > 0:
            for m in output_messages:
                ret_messages.append(decode_cpp_message_to_python(m))
        return ret_messages