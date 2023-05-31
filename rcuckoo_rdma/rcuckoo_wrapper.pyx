cimport rcuckoo_wrapper as rw
# cimport rcuckoo_wrapper as t
from libcpp.string cimport string


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
        return self.c_table.absolute_index_to_offset(absolute_index)
    
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