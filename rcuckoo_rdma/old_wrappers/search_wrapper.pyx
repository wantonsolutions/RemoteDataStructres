cimport hash_wrapper as h
cimport search_wrapper as search
# cimport tables_wrapper_def as t
# import pyximport; pyximport.install()
# cimport tables_wrapper as t
# import ctables as t
# cimport ctables as t
from cython.operator cimport dereference as deref
cimport tables_wrapper as t

cimport ctables as tab
# cimport tables_wrapper_forward_def as t

from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string

def get_table_id_from_index(unsigned int index):
    return search.get_table_id_from_index(index)

def search_path_to_buckets(vector[search.path_element] path):
    return search.search_path_to_buckets(path)

def random_dfs_search(key, unsigned int table_size):
    return search.random_dfs_search(key, table_size)

def bucket_cuckoo_insert(table, location_func, t.Key key, vector[unsigned int] open_buckets):

    #todo check the name of the location func being passed in, and then select based on that. It sucks but it's the best way to do this.
    #BUG
    assert isinstance(table, tab.PyTable)
    sub_location_func = h.rcuckoo_hash_locations
    new_table = tab.Table(deref(table.c_table))
    # return search.bucket_cuckoo_insert(deref(table.c_table), sub_location_func, key, open_buckets)
    return search.bucket_cuckoo_insert(new_table, sub_location_func, key, open_buckets)
    
    # unsigned int next_search_index(path_element pe, t.hash_locations (*location_func) (string, unsigned int), t.Table table)
    # bool key_in_path(vector[path_element] path, t.Key key)
    # void print_path(vector[path_element] path)
    # string path_to_string(vector[path_element] path)
    # unsigned int path_index_range(vector[path_element] path)
    # vector[unsigned int] find_closest_target_n_bi_directional(t.Table table, h.hash_locations (*location_func) (string, unsigned int), t.Key key, unsigned int n)

    # path_element pop_list(vector[a_star_pe] list, unordered_map[t.Key , a_star_pe] list_map)
    # void push_list(vector[a_star_pe] list, unordered_map[t.Key, a_star_pe] list_map, a_star_pe pe)
    # bool list_contains(unordered_map[t.Key, a_star_pe] list_map, t.Key key)
    # unsigned int next_table_index(unsigned int table_index)

    # unsigned int heuristic(unsigned int current_index, unsigned int target_index, unsigned int table_size)
    # unsigned int fscore(a_star_pe pe, unsigned int target_index, unsigned int table_size)

    # vector[path_element] a_star_search(t.Table table, t.hash_locations (*location_func) (string, unsigned int), t.Key key, vector[unsigned int] open_buckets)
    # vector[path_element] bucket_cuckoo_a_star_insert(t.Table table, t.hash_locations (*location_func) (string, unsigned int), t.Key key, vector[unsigned int] open_buckets)
    # vector[path_element] bucket_cuckoo_random_insert(t.Table table, t.hash_locations (*location_func) (string, unsigned int), t.Key key, vector[unsigned int] open_buckets)