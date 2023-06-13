from libcpp.string cimport string

cdef extern from "xxhash.h":
    ctypedef unsigned long long XXH64_hash_t

cdef extern from "hash.h":

    struct hash_locations:
        unsigned int primary
        unsigned int secondary

    void set_factor(float factor)
    float get_factor()
    unsigned int get_table_id_from_index(unsigned int index)
    unsigned int distance_to_bytes(unsigned int a, unsigned int b, unsigned int bucket_size, unsigned int entry_size);
    XXH64_hash_t h1(string key)
    XXH64_hash_t h2(string key)
    XXH64_hash_t h3(string key)

    unsigned int rcuckoo_primary_location(string key, unsigned int table_size)
    unsigned int h3_suffix_base_two(string key)
    unsigned int rcuckoo_secondary_location(string key, float factor, unsigned int table_size)
    unsigned int rcuckoo_secondary_location_independent(string key, unsigned int table_size)
    hash_locations rcuckoo_hash_locations(string key, unsigned int table_size)
    hash_locations rcuckoo_hash_locations_independent(string key, unsigned int table_size)

    struct race_bucket:
        unsigned int bucket
        unsigned int overflow

    struct race_hash_buckets:
        race_bucket primary
        race_bucket secondary

    race_bucket to_race_index_math(unsigned int index, unsigned int table_size)
    race_bucket race_primary_location(string key, unsigned int table_size)
    race_bucket race_secondary_location(string key, unsigned int table_size)
    race_hash_buckets race_hash_locations(string key, unsigned int table_size)

    void ten_k_hashes()

from libc cimport stdint
from libcpp cimport bool

cdef extern from "tables.h" namespace "cuckoo_tables":

    struct Key:
        stdint.uint8_t bytes[4]

    struct Value:
        stdint.uint8_t bytes[4]

    struct Entry:
        Key key
        Value value

    struct CasOperationReturn:
        bool success
        stdint.uint64_t original_value

    cdef cppclass Lock_Table:
        Lock_Table() except +
        Lock_Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock) except +
        void unlock_all()
        CasOperationReturn masked_cas(unsigned int index, stdint.uint64_t old, stdint.uint64_t new_value, stdint.uint64_t mask)
        void fill_masked_cas(unsigned int index, bool sucess, stdint.uint64_t new_value, stdint.uint64_t mask)

    cdef cppclass Table:
        Table() except +
        Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock) except +
        void unlock_all()
        void print_table()
        CasOperationReturn lock_table_masked_cas(unsigned int lock_index, stdint.uint64_t old, stdint.uint64_t new_value, stdint.uint64_t mask)
        CasOperationReturn fill_lock_table_masked_cas(unsigned int lock_index, bool success, stdint.uint64_t value, stdint.uint64_t mask)
        unsigned int get_buckets_per_row()
        unsigned int get_row_count()
        unsigned int get_bucket_size()
        unsigned int row_size_bytes()
        unsigned int n_buckets_size(unsigned int n_buckets)
        Entry get_entry(unsigned int bucket_index, unsigned int offset)
        void set_entry(unsigned int bucket_index, unsigned int offset, Entry entry)
        bool bucket_has_empty(unsigned int bucket_index)
        unsigned int get_first_empty_index(unsigned int bucket_index)
        bool bucket_contains(unsigned int bucket_index, Key key)
        float get_fill_percentage()
        bool full()
        Entry ** generate_bucket_cuckoo_hash_index(unsigned int memory_size, unsigned int bucket_size)
        unsigned int absolute_index_to_bucket_index(unsigned int absolute_index)
        unsigned int absolute_index_to_offset(unsigned int absolute_index)
        void assert_operation_in_table_bound(unsigned int bucket_index, unsigned int offset, unsigned int memory_size)
        bool contains_duplicates()
        unsigned int ** get_duplicates()

from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
# cimport tables_wrapper as t
# cimport hash_wrapper as h
from libcpp cimport bool

cdef extern from "search.h" namespace "cuckoo_search":

    struct path_element:
        Key key
        unsigned int table_index
        unsigned int bucket_index
        unsigned int offset

    struct a_star_pe:
        path_element pe
        path_element prior
        unsigned int distance
        unsigned int fscore

    unsigned int get_table_id_from_index(unsigned int index)
    vector[unsigned int] search_path_to_buckets(vector[path_element] path)
    vector[path_element] random_dfs_search(Key, unsigned int table_size)

    vector[path_element] bucket_cuckoo_insert(Table table, hash_locations (*location_func) (string, unsigned int), Key key, vector[unsigned int] open_buckets)
    unsigned int next_search_index(path_element pe, hash_locations (*location_func) (string, unsigned int), Table table)
    bool key_in_path(vector[path_element] path, Key key)
    void print_path(vector[path_element] path)
    string path_to_string(vector[path_element] path)
    unsigned int path_index_range(vector[path_element] path)
    vector[unsigned int] find_closest_target_n_bi_directional(Table table, hash_locations (*location_func) (string, unsigned int), Key key, unsigned int n)

    path_element pop_list(vector[a_star_pe] list, unordered_map[Key , a_star_pe] list_map)
    void push_list(vector[a_star_pe] list, unordered_map[Key, a_star_pe] list_map, a_star_pe pe)
    bool list_contains(unordered_map[Key, a_star_pe] list_map, Key key)
    unsigned int next_table_index(unsigned int table_index)

    unsigned int heuristic(unsigned int current_index, unsigned int target_index, unsigned int table_size)
    unsigned int fscore(a_star_pe pe, unsigned int target_index, unsigned int table_size)

    vector[path_element] a_star_search(Table table, hash_locations (*location_func) (string, unsigned int), Key key, vector[unsigned int] open_buckets)
    vector[path_element] bucket_cuckoo_a_star_insert(Table table, hash_locations (*location_func) (string, unsigned int), Key key, vector[unsigned int] open_buckets)
    vector[path_element] bucket_cuckoo_random_insert(Table table, hash_locations (*location_func) (string, unsigned int), Key key, vector[unsigned int] open_buckets)

cdef extern from "virtual_rdma.h" namespace "cuckoo_virtual_rdma":
    
    struct Request:
        unsigned int op
        Key key
        Value value

    struct VRMessage:
        unordered_map[string, string] payload

# cdef extern from "state_machines.h" namespace "cuckoo_state_machines":

#     cdef cppclass State_Machine:
#         State_Machine() except +
#         State_Machine(unordered_map[string, string] config) except +
#         # string get_state_machine_name()

#     cdef cppclass Client_State_Machine(State_Machine):
#         Client_State_Machine() except +
#         Client_State_Machine(unordered_map[string, string] config) except +
#         # string get_state_machine_name()


cdef extern from "cuckoo.h" namespace "cuckoo_rcuckoo":

    cdef cppclass RCuckoo:
        RCuckoo() except +
        RCuckoo(unordered_map[string, string] config) except +
        string get_state_machine_name()
        void clear_statistics()
        bool is_complete()
        vector[Key] get_completed_inserts()
        void set_max_fill(float max_fill)
        unordered_map[string, string] get_stats()


