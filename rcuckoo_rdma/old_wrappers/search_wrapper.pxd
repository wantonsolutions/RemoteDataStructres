from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
cimport tables_wrapper as t
cimport hash_wrapper as h
from libcpp cimport bool

cdef extern from "search.h" namespace "cuckoo_search":

    struct path_element:
        t.Key key
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
    vector[path_element] random_dfs_search(t.Key, unsigned int table_size)

    vector[path_element] bucket_cuckoo_insert(t.Table table, h.hash_locations (*location_func) (string, unsigned int), t.Key key, vector[unsigned int] open_buckets)
    unsigned int next_search_index(path_element pe, h.hash_locations (*location_func) (string, unsigned int), t.Table table)
    bool key_in_path(vector[path_element] path, t.Key key)
    void print_path(vector[path_element] path)
    string path_to_string(vector[path_element] path)
    unsigned int path_index_range(vector[path_element] path)
    vector[unsigned int] find_closest_target_n_bi_directional(t.Table table, h.hash_locations (*location_func) (string, unsigned int), t.Key key, unsigned int n)

    path_element pop_list(vector[a_star_pe] list, unordered_map[t.Key , a_star_pe] list_map)
    void push_list(vector[a_star_pe] list, unordered_map[t.Key, a_star_pe] list_map, a_star_pe pe)
    bool list_contains(unordered_map[t.Key, a_star_pe] list_map, t.Key key)
    unsigned int next_table_index(unsigned int table_index)

    unsigned int heuristic(unsigned int current_index, unsigned int target_index, unsigned int table_size)
    unsigned int fscore(a_star_pe pe, unsigned int target_index, unsigned int table_size)

    vector[path_element] a_star_search(t.Table table, h.hash_locations (*location_func) (string, unsigned int), t.Key key, vector[unsigned int] open_buckets)
    vector[path_element] bucket_cuckoo_a_star_insert(t.Table table, h.hash_locations (*location_func) (string, unsigned int), t.Key key, vector[unsigned int] open_buckets)
    vector[path_element] bucket_cuckoo_random_insert(t.Table table, h.hash_locations (*location_func) (string, unsigned int), t.Key key, vector[unsigned int] open_buckets)