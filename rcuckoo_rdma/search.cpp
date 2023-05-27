#include "search.h"
#include <string>
#include <vector>
#include "tables.h"
#include "hash.h"
#include <unordered_map>
#include <iostream>

namespace cuckoo_search {
    unsigned int get_table_id_from_index(unsigned int index){
        return index / 2;
    }

    std::vector<unsigned int> search_path_to_buckets(std::vector<path_element> path){
        cout << "search_path_to_buckets not implemented" << endl;
        std::vector<unsigned int> buckets;
        return buckets;
    }

    std::vector<path_element> random_dfs_search(cuckoo_tables::Key key, unsigned int table_size){
        cout << "random_dfs_search not implemented" << endl;
        std::vector<path_element> path;
        return path;
    }
    std::vector<path_element> bucket_cuckoo_insert(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int>  open_buckets){
        cout << "bucket_cuckoo_insert not implemented" << endl;
        std::vector<path_element> path;
        return path;
    }
    unsigned int next_search_index(path_element pe, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Table table){
        cout << "next_search_index not implemented" << endl;
        return 0;
    }
    bool key_in_path(std::vector<path_element> path, cuckoo_tables::Key key){
        cout << "key_in_path not implemented" << endl;
        return false;
    }
    void print_path(std::vector<path_element> path){
        cout << "print_path not implemented" << endl;
    }
    string path_to_string(std::vector<path_element> path){
        cout << "path_to_string not implemented" << endl;
        return "";
    }
    unsigned int path_index_range(std::vector<path_element> path){
        cout << "path_index_range not implemented" << endl;
        return 0;
    }
    std::vector<unsigned int> find_closest_target_n_bi_directional(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, unsigned int n){
        cout << "find_closest_target_n_bi_directional not implemented" << endl;
        std::vector<unsigned int> targets;
        return targets;
    }

    path_element pop_list(std::vector<a_star_pe> list, std::unordered_map<cuckoo_tables::Key, a_star_pe> list_map){
        cout << "pop_list not implemented" << endl;
        path_element pe;
        return pe;
    }

    void push_list(std::vector<a_star_pe> list, std::unordered_map<cuckoo_tables::Key, a_star_pe> list_map, a_star_pe pe) {
        cout << "push_list not implemented" << endl;
    }
    bool list_contains(std::unordered_map<cuckoo_tables::Key, a_star_pe> list_map, cuckoo_tables::Key key){
        cout << "list_contains not implemented" << endl;
        return false;
    }
    unsigned int next_table_index(unsigned int table_index){
        cout << "next_table_index not implemented" << endl;
        return 0;
    }

    unsigned int heuristic(unsigned int current_index, unsigned int target_index, unsigned int table_size) {
        cout << "heuristic not implemented" << endl;
        return 0;
    }

    unsigned int fscore(a_star_pe pe, unsigned int target_index, unsigned int table_size){
        cout << "fscore not implemented" << endl;
        return 0;
    }

    std::vector<path_element> a_star_search(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets){
        cout << "a_star_search not implemented" << endl;
        std::vector<path_element> path;
        return path;
    }
    std::vector<path_element> bucket_cuckoo_a_star_insert(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets){
        cout << "bucket_cuckoo_a_star_insert not implemented" << endl;
        std::vector<path_element> path;
        return path;
    }

    std::vector<path_element> bucket_cuckoo_random_insert(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets){
        cout << "bucket_cuckoo_random_insert not implemented" << endl;
        std::vector<path_element> path;
        return path;
    }

}