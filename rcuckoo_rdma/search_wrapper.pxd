from libcpp.vector cimport vector
from libcpp.string cimport string

cdef extern from "search.h" namespace "cuckoo_search":

    struct path_element:
        string key
        unsigned int table_index
        unsigned int bucket_index
        unsigned int offset

    struct a_star_pe:
        path_element pe
        path_element prior
        unsigned int distance
        unsigned int fscore

    unsigned int get_table_id_from_index(unsigned int index)
    vector[int] search_path_to_buckets(vector[path_element] path)
    vector[path_element] random_dfs_search(string key, unsigned int table_size)

    #TODO start here tomorrow

    vector[path_element] bucket_cuckoo_insert(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int>  open_buckets);
    unsigned int next_search_index(path_element pe, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Table table);
    bool key_in_path(std::vector<path_element> path, cuckoo_tables::Key key);
    void print_path(std::vector<path_element> path);
    string path_to_string(std::vector<path_element> path);
    unsigned int path_index_range(std::vector<path_element> path);
    std::vector<unsigned int> find_closest_target_n_bi_directional(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, unsigned int n);

    path_element pop_list(std::vector<a_star_pe> list, std::unordered_map<cuckoo_tables::Key, a_star_pe> list_map);
    void push_list(std::vector<a_star_pe> list, std::unordered_map<cuckoo_tables::Key, a_star_pe> list_map, a_star_pe pe);
    bool list_contains(std::unordered_map<cuckoo_tables::Key, a_star_pe> list_map, cuckoo_tables::Key key);
    unsigned int next_table_index(unsigned int table_index);

    unsigned int heuristic(unsigned int current_index, unsigned int target_index, unsigned int table_size);
    unsigned int fscore(a_star_pe pe, unsigned int target_index, unsigned int table_size);

    std::vector<path_element> a_star_search(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets);
    std::vector<path_element> bucket_cuckoo_a_star_insert(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets);
    std::vector<path_element> bucket_cuckoo_random_insert(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets);
} 
#endif