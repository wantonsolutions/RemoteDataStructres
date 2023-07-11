#include "search.h"
#include <string>
#include <vector>
#include <set>
#include "tables.h"
#include "hash.h"
#include "log.h"
#include <unordered_map>
#include <iostream>
#include <assert.h>
#include <algorithm>

namespace cuckoo_search {
    using namespace std;
    using namespace cuckoo_tables;

    unsigned int get_table_id_from_index(unsigned int index){
        return index / 2;
    }

    //Take a path and return a vector of the bucket indices
    //The bucket indicies are unique
    vector<unsigned int> search_path_to_buckets(vector<path_element> path){
        vector<unsigned int> buckets;
        for (auto pe : path){
            buckets.push_back(pe.bucket_index);
        }
        set<unsigned int> s( buckets.begin(), buckets.end() );
        buckets.assign( s.begin(), s.end() );

        vector<unsigned int> set_sorted_buckets(s.begin(), s.end());
        sort(set_sorted_buckets.begin(), set_sorted_buckets.end());

        //todo fix this functionality
        set_sorted_buckets.pop_back(); // This removes the -1 element which will be big 4294967295 after sorting unsigned ints (legacy from searching weird)
        // for (auto b : set_sorted_buckets){
        //     cout << "search path to buckets bucket " << b << endl;
        // }
        return set_sorted_buckets;
    }

    vector<path_element> random_dfs_search(Key key, unsigned int table_size){
        ALERT("random dfs search", "random_dfs_search not implemented");
        std::vector<path_element> path;
        return path;
    }
    vector<path_element> bucket_cuckoo_insert(Table table, hash_locations (*location_func) (string, unsigned int), Key key, vector<unsigned int>  open_buckets){
        ALERT("Bucket Cuckoo Insert", "Bucket Cuckoo Insert not implemented");
        vector<path_element> path;
        return path;
    }

    unsigned int table_index_to_hash_location(hash_locations locations, unsigned int table_index){
        if (table_index == 0){
            return locations.primary;
        } else {
            return locations.secondary;
        }
    }

    unsigned int next_search_index(path_element pe, hash_locations (*location_func) (string, unsigned int), Table table){
        hash_locations locations = location_func(pe.key.to_string(), table.get_row_count());
        unsigned int table_index = next_table_index(pe.table_index);
        return table_index_to_hash_location(locations, table_index);
    }
    bool key_in_path(vector<path_element> path, Key key){
        for (auto pe: path){
            if (pe.key == key){
                return true;
            }
        }
        return false;
    }

    string path_to_string(vector<path_element> path){
        string s = "";
        for (long unsigned int i=0; i < path.size(); i++){
            string prefix = to_string(i);
            s += prefix + " - " + path[i].to_string() + "\n";
        }
        return s;
    }

    void print_path(vector<path_element> path){
        cout << path_to_string(path) << endl;
    }

    unsigned int path_index_range(vector<path_element> path){
        if (path.size() <= 0){
            return 0;
        }
        //todo fix this functionality using this large value is bad
        #define UNSIGNED_INT_MAX 4294967295
        assert(path.size() > 0 );
        unsigned int min = UNSIGNED_INT_MAX;
        unsigned int max = 0;
        for (auto pe : path){
            if (pe.bucket_index < min){
                min = pe.bucket_index;
            }
            if (pe.bucket_index > max and pe.bucket_index != UNSIGNED_INT_MAX){
                max = pe.bucket_index;
            }
        }
        return max - min;
    }

    //A* search requires targets to search for. This function fines them.
    //It starts by finding the location we are inserting into, and then steps in both directions though the table looking for openings
    //It returns a vector of open slots
    vector<unsigned int> find_closest_target_n_bi_directional(Table table, hash_locations (*location_func) (string, unsigned int), Key key, unsigned int n){
        vector<unsigned int> targets;
        hash_locations locations = location_func(key.to_string(), table.get_row_count());
        unsigned int index_0 = locations.primary;
        unsigned int index_1 = (locations.primary -1);

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

    // list must be a heap
    a_star_pe pop_list(vector<a_star_pe> &list, unordered_map<Key, a_star_pe> &list_map){
        pop_heap(list.begin(), list.end());
        a_star_pe aspe = list.back();
        list.pop_back();
        size_t val = list_map.erase(aspe.pe.key);
        assert (val == 1);
        return aspe;
    }

    fast_a_star_pe fast_pop_list(vector<fast_a_star_pe> &list, unordered_map<Key, fast_a_star_pe> &list_map){
        pop_heap(list.begin(), list.end());
        fast_a_star_pe aspe = list.back();
        list.pop_back();
        size_t val = list_map.erase(*(aspe.pe.key));
        assert (val == 1);
        return aspe;
    }

    a_star_pe pop_key_from_list(vector<a_star_pe> &list, unordered_map<Key, a_star_pe> &list_map, Key key){
        a_star_pe aspe = list_map[key];
        size_t val = list_map.erase(key);
        assert (val == 1);
        vector<a_star_pe>::iterator it = find(list.begin(), list.end(), aspe);
        assert(it != list.end());
        list.erase(it);
        return aspe;
    }

    fast_a_star_pe fast_pop_key_from_list(vector<fast_a_star_pe> &list, unordered_map<Key, fast_a_star_pe> &list_map, Key key){
        fast_a_star_pe aspe = list_map[key];
        size_t val = list_map.erase(key);
        assert (val == 1);
        vector<fast_a_star_pe>::iterator it = find(list.begin(), list.end(), aspe);
        assert(it != list.end());
        list.erase(it);
        return aspe;
    }

    void push_list(vector<a_star_pe> &list, unordered_map<Key, a_star_pe> &list_map, a_star_pe aspe) {
        list.push_back(aspe);
        push_heap(list.begin(), list.end());
        list_map[aspe.pe.key] = aspe;
    }

    void fast_push_list(vector<fast_a_star_pe> &list, unordered_map<Key, fast_a_star_pe> &list_map, fast_a_star_pe aspe) {
        list.push_back(aspe);
        push_heap(list.begin(), list.end());
        Key key = *(aspe.pe.key);
        list_map[key] = aspe;
    }

    bool list_contains(unordered_map<Key, a_star_pe> &list_map, Key key){
        return list_map.find(key) != list_map.end();
    }

    bool fast_list_contains(unordered_map<Key, fast_a_star_pe> &list_map, Key key){
        return list_map.find(key) != list_map.end();
    }

    unsigned int next_table_index(unsigned int table_index){
        return (table_index + 1) % 2;
    }

    unsigned int heuristic(unsigned int current_index, unsigned int target_index, unsigned int table_size) {
        const unsigned int median = 4;
        unsigned int distance = 0;
        if (target_index == current_index){
            return distance;
        }
        unsigned int current_table = get_table_id_from_index(current_index);
        unsigned int target_table = get_table_id_from_index(target_index);
        if (current_table != target_table) {
            distance += 1;
        }
        return distance;
    }

    unsigned int fscore(a_star_pe pe, unsigned int target_index, unsigned int table_size){
        unsigned int g = pe.distance;
        unsigned int h = heuristic(pe.pe.bucket_index, target_index, table_size);
        return g + h;
    }

    unsigned int fast_fscore(fast_a_star_pe pe, unsigned int target_index, unsigned int table_size){
        unsigned int g = pe.distance;
        unsigned int h = heuristic(pe.pe.bucket_index, target_index, table_size);
        return g + h;
    }

    #define MAX_SEARCH_ITEMS 5000
    vector<path_element> a_star_search(Table table, hash_locations (*location_func) (string, unsigned int), Key key, std::vector<unsigned int> open_buckets){
        vector<path_element> path;
        const unsigned int target_count = 1;
        vector<unsigned int> targets = find_closest_target_n_bi_directional(table, location_func, key, target_count);
        bool found = false;
        a_star_pe closed_list_addressable[MAX_SEARCH_ITEMS];
        a_star_pe * prior_aspe = NULL;
        a_star_pe search_element;

        // Debugging print the list of targets
        for (auto target : targets){
            VERBOSE("DEBUG a_star search", "target in search pool: %d\n", target);
        }

        for (auto target : targets){
            VERBOSE("DEBUG a_star search", "searching for target: %d\n", target);
            path_element starting_pe = path_element(key, -1, -1, -1);
            search_element = a_star_pe(starting_pe, NULL, 0, 0);

            vector<a_star_pe> open_list;
            unordered_map<Key, a_star_pe> open_list_map;
            vector<a_star_pe> closed_list;
            unordered_map<Key, a_star_pe> closed_list_map;
            prior_aspe = NULL;
            unsigned int closed_list_addressable_index = 0;
            push_list(open_list, open_list_map, search_element);
            VERBOSE("DEBUG a_star search", "starting search element %s",search_element.pe.to_string().c_str());

            while (open_list.size() > 0){
                // cout << "top of search -- open list size: " << open_list.size() << endl;
                search_element = pop_list(open_list, open_list_map);
                VERBOSE("DEBUG popped a_star search", "starting search element %s",search_element.pe.to_string().c_str());
                //I need to store back pointers to the closed list so I can reconstruct the path
                closed_list_addressable[closed_list_addressable_index] = search_element;
                closed_list_addressable_index++;
                prior_aspe = &closed_list_addressable[closed_list_addressable_index - 1];
                // cout << "set the origin to the beginning of the closed list " << prior_aspe->pe.to_string() << endl;
                //todo closed list is not actually used, we only need the map remove it for optimizations
                push_list(closed_list, closed_list_map, search_element);
                VERBOSE("DEBUG pushed a_star search", "starting search element %s",search_element.pe.to_string().c_str());

                hash_locations locations = location_func(search_element.pe.key.to_string(), table.get_row_count());
                // cout << "locations for key: " << search_element.pe.key.to_string() << endl;
                // cout << "L1: " << locations.primary << " L2: " << locations.secondary << endl;
                unsigned int table_index = next_table_index(search_element.pe.table_index);
                // cout << "table index" << table_index << endl;
                unsigned int index = table_index_to_hash_location(locations, table_index);
                // cout << "index: " <<  index << endl;

                //if the index is not in the open buckets continue
                //Only check for open buckets if the size of the open buckets is greater than zero
                //somewhat unintuitive no open buckets means that they are all open
                if (open_buckets.size() > 0) {
                    // for (auto open_bucket : open_buckets){
                    //     VERBOSE("DEBUG a_star search", "open bucket: %d", open_bucket);
                    // }
                    if (std::find(open_buckets.begin(), open_buckets.end(), index) == open_buckets.end()) {
                        continue;
                    }
                }

                //We have found the slot if this is true
                if (table.bucket_has_empty(index)){
                    unsigned int offset = table.get_first_empty_index(index);
                    path_element open_pe = path_element(table.get_entry(index,offset).key, table_index, index, offset);
                    unsigned int distance = search_element.distance + 1;
                    unsigned int f_score = fscore(search_element, target, table.get_row_count());
                    a_star_pe open_a_star_pe = a_star_pe(open_pe, prior_aspe, search_element.distance+1, f_score);
                    // cout << "found target: " << open_a_star_pe.pe.to_string() << endl;
                    // cout << "setting prior to " << prior_aspe->pe.to_string() << endl;
                    // cout << "exiting search" << endl;
                    //todo this is a critial line but also a hack find a better way to set the tail of the search path
                    search_element = open_a_star_pe;
                    found=true;
                }

                if (found) {
                    break;
                }

                vector<a_star_pe> child_list;
                for (unsigned int i = 0; i < table.get_buckets_per_row(); i++){
                    path_element child_pe = path_element(table.get_entry(index, i).key, table_index, index, i);
                    unsigned int distance = search_element.distance + 1;
                    a_star_pe child = a_star_pe(child_pe, prior_aspe, distance, 0);
                    unsigned int f_score = fscore(child, target, table.get_row_count());
                    child.fscore = f_score;
                    child_list.push_back(child);
                }

                for (auto child : child_list){
                    if (list_contains(closed_list_map, child.pe.key)){
                        continue;
                    }
                    if (list_contains(open_list_map, child.pe.key)){
                        //remote the item from the open list and then reinsert it
                        //todo optimize by not poping from the list if we don't need to
                        a_star_pe existing_aspe = pop_key_from_list(open_list, open_list_map, child.pe.key);
                        if (child.distance < existing_aspe.distance){
                            existing_aspe.distance = child.distance;
                            existing_aspe.prior = child.prior;
                            existing_aspe.fscore = fscore(existing_aspe, target, table.get_row_count());
                        }
                        push_list(open_list, open_list_map, existing_aspe);
                    } else {
                        push_list(open_list, open_list_map, child);
                    }
                }
            }
            if (found) {
                break;
            }
        }

        if (found) {
            a_star_pe * back_tracker = &search_element;
            while (back_tracker != NULL){
                // cout << "pushing key to path: " << back_tracker->pe.to_string() << endl;
                path_element pe = back_tracker->pe;
                path.push_back(pe);
                back_tracker = back_tracker->prior;
            }
        }
        return path;
    }


    vector<path_element> a_star_search_fast(Table table, hash_locations (*location_func) (string, unsigned int), Key key, std::vector<unsigned int> open_buckets){
        vector<path_element> path;
        const unsigned int target_count = 1;
        vector<unsigned int> targets = find_closest_target_n_bi_directional(table, location_func, key, target_count);
        bool found = false;
        fast_a_star_pe closed_list_addressable[MAX_SEARCH_ITEMS];
        fast_a_star_pe * prior_aspe = NULL;
        fast_a_star_pe search_element;

        // Debugging print the list of targets
        for (auto target : targets){
            VERBOSE("DEBUG a_star search", "target in search pool: %d\n", target);
        }

        Key local_key = key;

        for (auto target : targets){
            VERBOSE("DEBUG a_star search", "searching for target: %d\n", target);
            fast_path_element starting_pe = fast_path_element(&key, -1, -1, -1);
            search_element = fast_a_star_pe(starting_pe, NULL, 0, 0);

            vector<fast_a_star_pe> open_list;
            unordered_map<Key, fast_a_star_pe> open_list_map;
            vector<fast_a_star_pe> closed_list;
            unordered_map<Key, fast_a_star_pe> closed_list_map;
            prior_aspe = NULL;
            unsigned int closed_list_addressable_index = 0;
            fast_push_list(open_list, open_list_map, search_element);
            VERBOSE("DEBUG a_star search", "starting search element %s",search_element.pe.to_string().c_str());


            while (open_list.size() > 0){
                // cout << "top of search -- open list size: " << open_list.size() << endl;
                // for (auto open_element : open_list){
                //     cout << "open list element: " << open_element.pe.to_string() << endl;
                // }

                search_element = fast_pop_list(open_list, open_list_map);
                //I need to store back pointers to the closed list so I can reconstruct the path
                closed_list_addressable[closed_list_addressable_index] = search_element;
                closed_list_addressable_index++;
                prior_aspe = &closed_list_addressable[closed_list_addressable_index - 1];
                // cout << "set the origin to the beginning of the closed list " << prior_aspe->pe.to_string() << endl;
                //todo closed list is not actually used, we only need the map remove it for optimizations
                fast_push_list(closed_list, closed_list_map, search_element);

                hash_locations locations = location_func(search_element.pe.key->to_string(), table.get_row_count());
                //print the locations of the key
                // cout << "locations for key: " << search_element.pe.key->to_string() << endl;
                // cout << "L1: " << locations.primary << " L2: " << locations.secondary << endl;
                unsigned int table_index = next_table_index(search_element.pe.table_index);
                // cout << "table index" << table_index << endl;
                unsigned int index = table_index_to_hash_location(locations, table_index);
                // cout << "index: " <<  index << endl;

                //if the index is not in the open buckets continue
                //Only check for open buckets if the size of the open buckets is greater than zero
                //somewhat unintuitive no open buckets means that they are all open
                if (open_buckets.size() > 0) {
                    // for (auto open_bucket : open_buckets){
                    //     VERBOSE("DEBUG a_star search", "open bucket: %d", open_bucket);
                    // }
                    if (std::find(open_buckets.begin(), open_buckets.end(), index) == open_buckets.end()) {
                        continue;
                    }
                }

                //We have found the slot if this is true
                if (table.bucket_has_empty(index)){
                    unsigned int offset = table.get_first_empty_index(index);
                    fast_path_element open_pe = fast_path_element(&(table.get_entry_pointer(index,offset)->key), table_index, index, offset);
                    unsigned int distance = search_element.distance + 1;
                    unsigned int f_score = fast_fscore(search_element, target, table.get_row_count());
                    fast_a_star_pe open_a_star_pe = fast_a_star_pe(open_pe, prior_aspe, search_element.distance+1, f_score);
                    // cout << "found target: " << open_a_star_pe.pe.to_string() << endl;
                    // cout << "setting prior to " << prior_aspe->pe.to_string() << endl;
                    // cout << "exiting search" << endl;
                    //todo this is a critial line but also a hack find a better way to set the tail of the search path
                    search_element = open_a_star_pe;
                    found=true;
                }

                if (found) {
                    break;
                }

                vector<fast_a_star_pe> child_list;
                for (unsigned int i = 0; i < table.get_buckets_per_row(); i++){
                    fast_path_element child_pe = fast_path_element(&(table.get_entry_pointer(index, i))->key, table_index, index, i);
                    unsigned int distance = search_element.distance + 1;
                    fast_a_star_pe child = fast_a_star_pe(child_pe, prior_aspe, distance, 0);
                    unsigned int f_score = fast_fscore(child, target, table.get_row_count());
                    child.fscore = f_score;
                    child_list.push_back(child);
                }

                for (auto child : child_list){
                    if (fast_list_contains(closed_list_map, *(child.pe.key))){
                        continue;
                    }
                    if (fast_list_contains(open_list_map, *(child.pe.key))){
                        //remote the item from the open list and then reinsert it
                        //todo optimize by not poping from the list if we don't need to
                        fast_a_star_pe existing_aspe = fast_pop_key_from_list(open_list, open_list_map, *(child.pe.key));
                        if (child.distance < existing_aspe.distance){
                            existing_aspe.distance = child.distance;
                            existing_aspe.prior = child.prior;
                            existing_aspe.fscore = fast_fscore(existing_aspe, target, table.get_row_count());
                        }
                        fast_push_list(open_list, open_list_map, existing_aspe);
                    } else {
                        fast_push_list(open_list, open_list_map, child);
                    }
                }
            }
            if (found) {
                break;
            }
        }

        if (found) {
            fast_a_star_pe * back_tracker = &search_element;
            while (back_tracker != NULL){
                path_element pe;
                pe.key = *(back_tracker->pe.key);
                pe.table_index = back_tracker->pe.table_index;
                pe.bucket_index = back_tracker->pe.bucket_index;
                pe.offset = back_tracker->pe.offset;
                path.push_back(pe);
                back_tracker = back_tracker->prior;
            }
        }
        return path;
    }

    std::vector<path_element> bucket_cuckoo_a_star_insert_fast(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets){
        std::vector<path_element> path = a_star_search_fast(table, location_func, key, open_buckets);
        return path;
    }


    std::vector<path_element> bucket_cuckoo_a_star_insert(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets){
        std::vector<path_element> path = a_star_search(table, location_func, key, open_buckets);
        return path;
    }

    std::vector<path_element> bucket_cuckoo_random_insert(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets){
        cout << "bucket_cuckoo_random_insert not implemented" << endl;
        std::vector<path_element> path;
        return path;
    }

}