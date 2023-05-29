cimport rcuckoo_wrapper as rw
# cimport rcuckoo_wrapper as t

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

def rcuckoo_hash_locations(key, table_size):
    bytes_key = str(key).encode('utf-8')
    locations = rw.rcuckoo_hash_locations(bytes_key, table_size)
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
    return (hash_buckets.primary, hash_buckets.secondary)

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

    def unlock_all(self):
        return self.c_table.unlock_all()

    def print_table(self):
        return self.c_table.print_table()

    def lock_table_masked_cas(self, unsigned int lock_index, old, new_value, mask):
        return self.c_table.lock_table_masked_cas(lock_index, old, new_value, mask)


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
        return e.key

    def set_entry(self, unsigned int bucket_index, unsigned int offset, entry):
        #todo wrap the entry type
        cdef rw.Entry c_entry
        print("Entry not being set WARNING WARNING!!!!")
        # c_entry.key = int(entry)
        # c_entry.value = int(1)
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
    
    def absolute_index_to_offset(self, unsigned int absolute_index):
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


# cimport hash_wrapper as h
# cimport search_wrapper as search
# cimport tables_wrapper_def as t
# import pyximport; pyximport.install()
# cimport tables_wrapper as t
# import ctables as t
# cimport ctables as t
from cython.operator cimport dereference as deref
# cimport tables_wrapper as t

# cimport ctables as tab
# cimport tables_wrapper_forward_def as t

from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string

def get_table_id_from_index(unsigned int index):
    return rw.get_table_id_from_index(index)

def search_path_to_buckets(vector[rw.path_element] path):
    return rw.search_path_to_buckets(path)

def random_dfs_search(key, unsigned int table_size):
    return rw.random_dfs_search(key, table_size)

def bucket_cuckoo_insert(PyTable table, location_func, rw.Key key, vector[unsigned int] open_buckets):

    #todo check the name of the location func being passed in, and then select based on that. It sucks but it's the best way to do this.
    sub_location_func = rw.rcuckoo_hash_locations
    # new_table = rw.Table(deref(table.c_table))
    # return searcrw.bucket_cuckoo_insert(deref(table.c_table), sub_location_func, key, open_buckets)
    return rw.bucket_cuckoo_insert(deref(table.c_table), sub_location_func, key, open_buckets)
    
    # unsigned int next_search_index(path_element pe, t.hash_locations (*location_func) (string, unsigned int), t.Table table)
    # bool key_in_path(vector[path_element] path, t.Key key)
    # void print_path(vector[path_element] path)
    # string path_to_string(vector[path_element] path)
    # unsigned int path_index_range(vector[path_element] path)
    # vector[unsigned int] find_closest_target_n_bi_directional(t.Table table, rw.hash_locations (*location_func) (string, unsigned int), t.Key key, unsigned int n)

    # path_element pop_list(vector[a_star_pe] list, unordered_map[t.Key , a_star_pe] list_map)
    # void push_list(vector[a_star_pe] list, unordered_map[t.Key, a_star_pe] list_map, a_star_pe pe)
    # bool list_contains(unordered_map[t.Key, a_star_pe] list_map, t.Key key)
    # unsigned int next_table_index(unsigned int table_index)

    # unsigned int heuristic(unsigned int current_index, unsigned int target_index, unsigned int table_size)
    # unsigned int fscore(a_star_pe pe, unsigned int target_index, unsigned int table_size)

    # vector[path_element] a_star_search(t.Table table, t.hash_locations (*location_func) (string, unsigned int), t.Key key, vector[unsigned int] open_buckets)
    # vector[path_element] bucket_cuckoo_a_star_insert(t.Table table, t.hash_locations (*location_func) (string, unsigned int), t.Key key, vector[unsigned int] open_buckets)
    # vector[path_element] bucket_cuckoo_random_insert(t.Table table, t.hash_locations (*location_func) (string, unsigned int), t.Key key, vector[unsigned int] open_buckets)