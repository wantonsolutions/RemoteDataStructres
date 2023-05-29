#include "search.h"
#include <string>
#include <vector>
#include <set>
#include "tables.h"
#include "hash.h"
#include <unordered_map>
#include <iostream>
#include <assert.h>

namespace cuckoo_search {
    using namespace std;
    unsigned int get_table_id_from_index(unsigned int index){
        return index / 2;
    }

    //Take a path and return a vector of the bucket indices
    //The bucket indicies are unique
    vector<unsigned int> search_path_to_buckets(vector<path_element> path){
        cout << "search_path_to_buckets not implemented" << endl;
        vector<unsigned int> buckets;
        for (auto pe : path){
            buckets.push_back(pe.bucket_index);
        }
        set<unsigned int> s( buckets.begin(), buckets.end() );
        buckets.assign( s.begin(), s.end() );
        return buckets;
    }

    std::vector<path_element> random_dfs_search(cuckoo_tables::Key key, unsigned int table_size){
        cout << "random_dfs_search not implemented" << endl;
        cout <<"key: " << key.to_string() << endl;
        cout <<"table_size: " << table_size << endl;
        std::vector<path_element> path;
        return path;
    }
    std::vector<path_element> bucket_cuckoo_insert(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int>  open_buckets){
        cout << "bucket_cuckoo_insert not implemented" << endl;
        table.print_table();
        cout <<"key: " << key.to_string() << endl;
        cout <<"open_buckets: " << endl;
        for (auto b : open_buckets){
            cout << b << endl;
        }
        cout << "location_func" << location_func;
        std::vector<path_element> path;
        return path;
    }
    unsigned int next_search_index(path_element pe, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Table table){
        hash_locations locations = location_func(pe.key.to_string(), table.get_row_count());
        unsigned int table_index = next_table_index(pe.table_index);
        if (table_index == 0){
            return locations.primary;
        } else {
            return locations.secondary;
        }
    }
    bool key_in_path(std::vector<path_element> path, cuckoo_tables::Key key){
        for (auto pe: path){
            if (pe.key == key){
                return true;
            }
        }
        return false;
    }

    string path_to_string(std::vector<path_element> path){
        string s = "";
        for (long unsigned int i=0; i < path.size(); i++){
            string key = path[i].key.to_string();
            string prefix = to_string(i);
            s += prefix + " - " + key + ", ";
        }
        return s;
    }

    void print_path(std::vector<path_element> path){
        cout << path_to_string(path) << endl;
    }

    unsigned int path_index_range(std::vector<path_element> path){
        if (path.size() <= 0){
            return 0;
        }
        assert(path.size() > 0 );
        unsigned int min = path[0].bucket_index;
        unsigned int max = path[0].bucket_index;
        for (auto pe : path){
            if (pe.bucket_index < min){
                min = pe.bucket_index;
            }
            if (pe.bucket_index > max){
                max = pe.bucket_index;
            }
        }
        return max - min;
    }

    //A* search requires targets to search for. This function fines them.
    //It starts by finding the location we are inserting into, and then steps in both directions though the table looking for openings
    //It returns a vector of open slots
    std::vector<unsigned int> find_closest_target_n_bi_directional(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, unsigned int n){
        std::vector<unsigned int> targets;
        hash_locations locations = location_func(key.to_string(), table.get_row_count());
        unsigned int index_0 = locations.primary;
        unsigned int index_1 = locations.primary -1;
        unsigned int counter = 0;
        while (targets.size() < n){
            //deal with wrap around for unsigned ints
            if (index_0 >= table.get_row_count()){
                index_0 = 0;
            }
            if (index_1 >= table.get_row_count()){
                index_1 = table.get_row_count() - 1;
            }
            if (counter > table.get_row_count() / 2) {
                return targets;
            }
            if (index_0 < table.get_row_count()){
                if (table.bucket_has_empty(index_0)){
                    targets.push_back(index_0);
                }
            }
            if (index_1 < table.get_row_count() && index_1 != index_0){
                if (table.bucket_has_empty(index_1)){
                    targets.push_back(index_1);
                }
            }
            index_0++;
            index_1--;
            counter++;
        }
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