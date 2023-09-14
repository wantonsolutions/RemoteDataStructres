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
        return set_sorted_buckets;
    }

    //Buckets should be initalized, it will be filled from the path
    void search_path_to_buckets_fast(vector<path_element> &path, vector<unsigned int> &buckets) {
        buckets.clear();
        for (auto pe : path){
            if (! (std::find(buckets.begin(), buckets.end(), pe.bucket_index) != buckets.end())){
                buckets.push_back(pe.bucket_index);
            }
        }
        sort(buckets.begin(), buckets.end());
        buckets.pop_back();
    }

    vector<path_element> random_dfs_search(Key key, unsigned int table_size){
        ALERT("random dfs search", "random_dfs_search not implemented");
        std::vector<path_element> path;
        return path;
    }
    vector<path_element> bucket_cuckoo_insert(Table table, hash_locations (*location_func) (string, unsigned int), Key key, vector<unsigned int>  open_buckets){
        ALERT("Bucket Cuckoo Insert", "Bucket Cuckoo Insert not implemented");;
        vector<path_element> path;
        return path;
    }

    inline unsigned int table_index_to_hash_location(hash_locations locations, unsigned int table_index){
        if (table_index == 0){
            return locations.primary;
        }
        return locations.secondary;
    }

    unsigned int next_search_index(path_element pe, hash_locations (*location_func) (Key, unsigned int), Table &table){
        hash_locations locations = location_func(pe.key, table.get_row_count());
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

    void search_closest_bi_n_locations_from_location(Table &table, hash_locations locations, unsigned int n, vector<unsigned int>& targets){
        unsigned int index_0 = locations.primary;
        unsigned int index_1 = (locations.primary -1);
        // vector <unsigned int> targets;
        targets.clear();

        unsigned int counter = 0;
        unsigned int row_count = table.get_row_count();

        while(targets.size() < n) {
            if (index_0 >= row_count){
                index_0 = 0;
            }
            if (table.bucket_has_empty(index_0)){
                targets.push_back(index_0);
            }
            index_0++;
        }




        // while (targets.size() < n){
        //     //deal with wrap around for unsigned ints
        //     if (index_0 >= row_count)[[unlikely]]{
        //         index_0 = 0;
        //     }
        //     if (index_1 >= row_count)[[unlikeely]]{
        //         index_1 = row_count - 1;
        //     }
        //     if (counter > row_count / 2)[[unlikely]]{
        //         return;
        //     }
        //     if (index_0 < row_count){
        //         // printf("index_0: %u\n", index_0);
        //         if (table.bucket_has_empty(index_0)){
        //             targets.push_back(index_0);
        //         }
        //     }
        //     if (index_1 < row_count && index_1 != index_0){
        //         if (table.bucket_has_empty(index_1)){
        //             targets.push_back(index_1);
        //         }
        //     }
        //     index_0++;
        //     index_1--;
        //     counter++;
        // }
    }

    //A* search requires targets to search for. This function fines them.
    //It starts by finding the location we are inserting into, and then steps in both directions though the table looking for openings
    //It returns a vector of open slots
    vector<unsigned int> find_closest_target_n_bi_directional(Table &table, hash_locations (*location_func) (string, unsigned int), Key key, unsigned int n){
        hash_locations locations = location_func(key.to_string(), table.get_row_count());
        vector<unsigned int> targets;
        search_closest_bi_n_locations_from_location(table, locations, n, targets);
        return targets;
    }

    vector<unsigned int> find_closest_target_n_bi_directional(Table &table, hash_locations (*location_func) (Key, unsigned int), Key key, unsigned int n){
        hash_locations locations = location_func(key, table.get_row_count());
        vector<unsigned int> targets;
        search_closest_bi_n_locations_from_location(table, locations, n,targets);
        return targets;
    }

    void find_closest_target_n_bi_directional(search_context &context, unsigned int n){
        hash_locations locations = context.location_func(context.key, context.table->get_row_count());
        search_closest_bi_n_locations_from_location(*context.table, locations, n,context.targets);
        return;
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

    #define DELETED_FSCORE 99999
    inline fast_a_star_pe fast_pop_list(vector<fast_a_star_pe> &list){
        // pop_heap(list.begin(), list.end());
        // fast_a_star_pe aspe = list.back();
        // list.pop_back();
        // return aspe;
        fast_a_star_pe min = list[0];
        int min_index = 0;
        for (int i=1; i < list.size(); i++){
            if (list[i].fscore == DELETED_FSCORE){
                continue;
            }
            if (list[i] < min){
                min = list[i];
                min_index = i;
            }
        }
        list[min_index].fscore = DELETED_FSCORE;
        // list.erase(list.begin() + min_index);
        return min;

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

    inline fast_a_star_pe fast_pop_key_from_list(vector<fast_a_star_pe> &list, Key * key){
        fast_a_star_pe ret_aspe;
        int i=0;
        for (auto aspe : list){
            if (aspe.pe.key == key){
                ret_aspe = aspe;
                list.erase(list.begin() + i);
                return ret_aspe;
            }
            i++;
        }
        assert(false);
    }

    void push_list(vector<a_star_pe> &list, unordered_map<Key, a_star_pe> &list_map, a_star_pe aspe) {
        list.push_back(aspe);
        push_heap(list.begin(), list.end());
        list_map[aspe.pe.key] = aspe;
    }

    inline void fast_push_list(vector<fast_a_star_pe> &list, fast_a_star_pe &aspe) {
        list.push_back(aspe);
        // push_heap(list.begin(), list.end());
    }

    bool list_contains(unordered_map<Key, a_star_pe> &list_map, Key key){
        return list_map.find(key) != list_map.end();
    }

    inline bool fast_list_contains(unordered_map<Key*, fast_a_star_pe> &list_map, Key *key){
        return list_map.find(key) != list_map.end();
    }

    inline bool fast_list_contains_vector(vector<fast_a_star_pe> &list, Key *key){
        for (int i=0; i<list.size(); i++){
            if (list[i].pe.key == key){
                return true;
            }
        }
        return false;
    }

    inline unsigned int next_table_index(unsigned int table_index){
        return (table_index + 1) % 2;
    }

    const std::vector<std::pair<float,int>> median_distances {
        {1.000000,1},
        {1.200000,1},
        {1.300000,1},
        {1.400000,1},
        {1.500000,1},
        {1.600000,1},
        {1.700000,1},
        {1.800000,1},
        {1.900000,3},
        {2.000000,3},
        {2.000000,3},
        {2.100000,3},
        {2.200000,3},
        {2.300000,5},
        {2.400000,5},
        {2.500000,7},
        {2.600000,9},
        {2.700000,11},
        {2.800000,13},
        {2.900000,17},
        {3.000000,21},
        {3.100000,25},
        {3.200000,33},
        {3.300000,41},
        {3.400000,51},
        {3.500000,65},
        {3.600000,87},
        {3.700000,107},
        {3.800000,147},
        {3.900000,187},
        {4.000000,247},
    };

    static volatile int median = 0;

    inline int get_median(){
        if (median != 0) [[likely]]{
            return median;
        } 
        float factor = get_factor();
        for (int i=0; i<median_distances.size(); i++){
            if (factor < median_distances[i].first){
                median = median_distances[i].second;
                return median_distances[i].second;
            }
        }

        exit(0);
    }

    unsigned int heuristic(unsigned int current_index, unsigned int target_index, unsigned int table_size) {
        const unsigned int median = get_median();
        unsigned int distance = 0;
        if (target_index == current_index){
            return distance;
        }
        unsigned int current_table = get_table_id_from_index(current_index);
        unsigned int target_table = get_table_id_from_index(target_index);
        if (current_table != target_table) {
            distance += 1;
        }

        distance += abs((int)current_index - (int)target_index) / median;

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

    unsigned int fast_min_fscore(fast_a_star_pe pe, vector<unsigned int>&target_index, unsigned int table_size){
        unsigned int g = pe.distance;
        unsigned int min_h = heuristic(pe.pe.bucket_index, target_index[0], table_size);
        for(int i=1; i<target_index.size(); i++){
            unsigned int h = heuristic(pe.pe.bucket_index, target_index[i], table_size);
            if (h < min_h){
                min_h = h;
            }
        }
        return g + min_h;
    }

    vector<path_element> a_star_search(Table table, hash_locations (*location_func) (string, unsigned int), Key key, std::vector<unsigned int> open_buckets){
        vector<path_element> path;
        const unsigned int target_count = 3;
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

    std::vector<path_element> a_star_path_to_path_element(fast_a_star_pe * start) {
        std::vector<path_element> path;
        fast_a_star_pe * back_tracker = start;
        while (back_tracker != NULL){
            path_element pe;
            pe.key = *(back_tracker->pe.key);
            pe.table_index = back_tracker->pe.table_index;
            pe.bucket_index = back_tracker->pe.bucket_index;
            pe.offset = back_tracker->pe.offset;
            path.push_back(pe);
            back_tracker = back_tracker->prior;
        }
        return path;
    }

    void a_star_path_to_path_element(search_context & context, fast_a_star_pe * start) {
        context.path.clear();
        fast_a_star_pe * back_tracker = start;
        while (back_tracker != NULL){
            path_element pe;
            pe.key = *(back_tracker->pe.key);
            pe.table_index = back_tracker->pe.table_index;
            pe.bucket_index = back_tracker->pe.bucket_index;
            pe.offset = back_tracker->pe.offset;
            context.path.push_back(pe);
            back_tracker = back_tracker->prior;
        }
        if (context.path.size() > (MAX_SEARCH_DEPTH + 1)) {
            ALERT("a-star", "path too long going to crash now lengths %d",context.path.size());
        }
        assert(context.path.size() <= (MAX_SEARCH_DEPTH+1));
    }

    inline fast_a_star_pe get_child(search_context &context, unsigned int table_index, unsigned int row, unsigned int offset, unsigned int target, unsigned int table_rows, fast_a_star_pe * prior_aspe, fast_a_star_pe search_element) {
        Key * child_key = &(context.table->get_entry_pointer(row, offset))->key;
        fast_path_element child_pe = fast_path_element(child_key, table_index, row, offset);
        unsigned int distance = search_element.distance + 1;
        fast_a_star_pe child = fast_a_star_pe(child_pe, prior_aspe, distance, 0);
        unsigned int f_score = fast_fscore(child, target, table_rows);
        child.fscore = f_score;
        return child;
    }

    inline fast_a_star_pe get_child_multi(search_context &context, unsigned int table_index, unsigned int row, unsigned int offset, vector<unsigned int> &target, unsigned int table_rows, fast_a_star_pe * prior_aspe, fast_a_star_pe search_element) {
        Key * child_key = &(context.table->get_entry_pointer(row, offset))->key;
        fast_path_element child_pe = fast_path_element(child_key, table_index, row, offset);
        unsigned int distance = search_element.distance + 1;
        fast_a_star_pe child = fast_a_star_pe(child_pe, prior_aspe, distance, 0);
        unsigned int f_score = fast_min_fscore(child, target, table_rows);
        child.fscore = f_score;
        return child;
    }
        // unsigned int offset = context.table->get_first_empty_index(index);
        // fast_path_element open_pe = fast_path_element(&(context.table->get_entry_pointer(index,offset)->key), table_index, index, offset);
        // unsigned int distance = search_element.distance + 1;
        // unsigned int f_score = fast_fscore(search_element, target, table_rows);
        // //todo this is a critial line but also a hack find a better way to set the tail of the search path
        // search_element = fast_a_star_pe(open_pe, prior_aspe, search_element.distance+1, f_score);

    inline bool row_has_been_visited_before(search_context & context, unsigned int row) {
        for (auto visited_bucket : context.visited_buckets){
            if (visited_bucket == row){
                // printf("row was visited before!");
                return true;
            }
            // if(context.visited_buckets.size() > 10000) {
            //     printf("[size %10d] visited bucket: %d, search bucket %d\n", context.visited_buckets.size(),visited_bucket, row);
            // }
        }
        return false;
    }

    #define MAX_A_STAR_DEPTH 50
    #define FAST_A_STAR_MAX_DEPTH 5
    void a_star_search_fast_context(search_context &context){
        // fast_a_star_pe closed_list_addressable[MAX_SEARCH_ITEMS];
        context.path.clear();
        const unsigned int target_count = 5;
        find_closest_target_n_bi_directional(context, target_count);

        bool found = false;
        fast_a_star_pe * prior_aspe = NULL;
        fast_a_star_pe search_element;
        bool restrict_search_to_subset_of_buckets=(context.open_buckets.size() > 0);
        const unsigned int table_rows = context.table->get_row_count();
        // Debugging print the list of targets

        for (auto target : context.targets){
            VERBOSE("DEBUG a_star search", "searching for target: %d\n", target);
            unsigned int closed_list_addressable_index = 0;
            fast_path_element starting_pe = fast_path_element(&(context.key), -1, -1, -1);
            search_element = fast_a_star_pe(starting_pe, NULL, 0, 0);
            prior_aspe = NULL;
            context.open_list.clear();
            context.closed_list.clear();
            context.visited_buckets.clear();
            context.path.clear();

            fast_push_list(context.open_list, search_element);
            VERBOSE("DEBUG a_star search", "starting search element %s",search_element.pe.to_string().c_str());

            int search_depth = 0;
            while (context.open_list.size() > 0){
                search_element = fast_pop_list(context.open_list);

                //I need to store back pointers to the closed list so I can reconstruct the path
                context.closed_list_addressable[closed_list_addressable_index] = search_element;
                closed_list_addressable_index++;
                prior_aspe = &context.closed_list_addressable[closed_list_addressable_index - 1];
                fast_push_list(context.closed_list, search_element);

                hash_locations locations = context.location_func(*(search_element.pe.key), table_rows);
                unsigned int table_index = next_table_index(search_element.pe.table_index);
                unsigned int index = table_index_to_hash_location(locations, table_index);

                //if the index is not in the open buckets continue
                //Only check for open buckets if the size of the open buckets is greater than zero
                //somewhat unintuitive no open buckets means that they are all open
                if (restrict_search_to_subset_of_buckets) {
                    if (std::find(context.open_buckets.begin(), context.open_buckets.end(), index) == context.open_buckets.end()) {
                        continue;
                    }
                }

                //We have found the slot if this is true
                if (context.table->bucket_has_empty(index)){
                    unsigned int offset = context.table->get_first_empty_index(index);
                    search_element = get_child(context, table_index, index, offset, target, table_rows, prior_aspe, search_element);
                    found=true;
                    break;
                }

                bool row_has_been_visited = row_has_been_visited_before(context, index);
                if (!row_has_been_visited) {
                    context.visited_buckets.push_back(index);
                    for (unsigned int i = 0; i < context.table->get_buckets_per_row(); i++){
                        fast_a_star_pe child = get_child(context, table_index, index, i, target, table_rows, prior_aspe, search_element);
                        fast_push_list(context.open_list, child);
                    }
                } else {
                    for (unsigned int i = 0; i < context.table->get_buckets_per_row(); i++){
                        fast_a_star_pe child = get_child(context, table_index, index, i, target, table_rows, prior_aspe, search_element);
                        //If the child is in the closed list just continue
                        if (fast_list_contains_vector(context.closed_list, child.pe.key)){
                            continue;
                        }
                        //If the child exists in the open list, update the distance
                        //TODO this is truely the slow path, consider using a hash tabler or something. It's just hard to find if values exist in the open list.
                        for (unsigned int i = 0; i < context.open_list.size(); i++){
                            if (context.open_list[i].pe.key == child.pe.key){
                                context.open_list[i].distance = child.distance;
                                context.open_list[i].prior = child.prior;
                                context.open_list[i].fscore = child.fscore;
                                break;
                            }
                        }
                    }
                }
                //TODO don't delete this is also protecting a segfault
                search_depth++;
                if(search_depth > context.max_search_depth) [[unlikely]]{
                    // ALERT("a-star", "search depth exceeded %d moving to the next target \n", context.max_search_depth);
                    break;
                }

            }
            if (found) {
                break;
            }
        }

        if (found) {
            a_star_path_to_path_element(context, &search_element);
        } else {
            //second search
            if (context.max_search_depth == MAX_A_STAR_DEPTH) {
                ALERT("a-star", "search depth exceeded %d moving to the next target \n", context.max_search_depth);
                return;
            } else {
                context.max_search_depth = MAX_A_STAR_DEPTH;
                a_star_search_fast_context(context);
            }
        }
    }


    void a_star_search_fast_context_multi_home(search_context &context){
        // fast_a_star_pe closed_list_addressable[MAX_SEARCH_ITEMS];
        context.path.clear();
        const unsigned int target_count = 1;
        find_closest_target_n_bi_directional(context, target_count);

        bool found = false;
        fast_a_star_pe * prior_aspe = NULL;
        fast_a_star_pe search_element;
        bool restrict_search_to_subset_of_buckets=(context.open_buckets.size() > 0);
        const unsigned int table_rows = context.table->get_row_count();
        // Debugging print the list of targets

        unsigned int closed_list_addressable_index = 0;
        fast_path_element starting_pe = fast_path_element(&(context.key), -1, -1, -1);
        search_element = fast_a_star_pe(starting_pe, NULL, 0, 0);
        prior_aspe = NULL;
        context.open_list.clear();
        context.closed_list.clear();
        context.visited_buckets.clear();

        fast_push_list(context.open_list, search_element);
        VERBOSE("DEBUG a_star search", "starting search element %s",search_element.pe.to_string().c_str());

        int search_depth = 0;
        while (context.open_list.size() > 0){
            search_element = fast_pop_list(context.open_list);

            //I need to store back pointers to the closed list so I can reconstruct the path
            context.closed_list_addressable[closed_list_addressable_index] = search_element;
            closed_list_addressable_index++;
            prior_aspe = &context.closed_list_addressable[closed_list_addressable_index - 1];
            fast_push_list(context.closed_list, search_element);

            hash_locations locations = context.location_func(*(search_element.pe.key), table_rows);
            unsigned int table_index = next_table_index(search_element.pe.table_index);
            unsigned int index = table_index_to_hash_location(locations, table_index);

            //if the index is not in the open buckets continue
            //Only check for open buckets if the size of the open buckets is greater than zero
            //somewhat unintuitive no open buckets means that they are all open
            if (restrict_search_to_subset_of_buckets) {
                if (std::find(context.open_buckets.begin(), context.open_buckets.end(), index) == context.open_buckets.end()) {
                    continue;
                }
            }

            //We have found the slot if this is true
            if (context.table->bucket_has_empty(index)){
                unsigned int offset = context.table->get_first_empty_index(index);
                search_element = get_child_multi(context, table_index, index, offset, context.targets, table_rows, prior_aspe, search_element);
                found=true;
                break;
            }

            bool row_has_been_visited = row_has_been_visited_before(context, index);
            if (!row_has_been_visited) {
                context.visited_buckets.push_back(index);
                for (unsigned int i = 0; i < context.table->get_buckets_per_row(); i++){
                    fast_a_star_pe child = get_child_multi(context, table_index, index, i, context.targets, table_rows, prior_aspe, search_element);
                    fast_push_list(context.open_list, child);
                }
            } else {
                for (unsigned int i = 0; i < context.table->get_buckets_per_row(); i++){
                    fast_a_star_pe child = get_child_multi(context, table_index, index, i, context.targets, table_rows, prior_aspe, search_element);
                    //If the child is in the closed list just continue
                    if (fast_list_contains_vector(context.closed_list, child.pe.key)){
                        continue;
                    }
                    //If the child exists in the open list, update the distance
                    //TODO this is truely the slow path, consider using a hash tabler or something. It's just hard to find if values exist in the open list.
                    for (unsigned int i = 0; i < context.open_list.size(); i++){
                        if (context.open_list[i].pe.key == child.pe.key){
                            context.open_list[i].distance = child.distance;
                            context.open_list[i].prior = child.prior;
                            context.open_list[i].fscore = child.fscore;
                            break;
                        }
                    }
                }
            }
            //TODO don't delete this is also protecting a segfault
            search_depth++;
            if(search_depth > MAX_SEARCH_DEPTH) [[unlikely]]{
                // ALERT("search depth exceeded %d moving to the next target \n", MAX_SEARCH_DEPTH);
                break;
            }

        }
            // if (found) {
            //     break;
            // }

        if (found) {
            a_star_path_to_path_element(context, &search_element);
        }
    }

    vector<path_element> a_star_search_fast(Table& table, hash_locations (*location_func) (Key, unsigned int), Key key, std::vector<unsigned int> open_buckets){
        fast_a_star_pe closed_list_addressable[MAX_SEARCH_ITEMS];
        vector<path_element> path;
        const unsigned int target_count = 15;
        vector<unsigned int> targets = find_closest_target_n_bi_directional(table, location_func, key, target_count);
        bool found = false;
        fast_a_star_pe * prior_aspe = NULL;
        fast_a_star_pe search_element;

        const unsigned int table_rows = table.get_row_count();
        // Debugging print the list of targets

        for (auto target : targets){
            VERBOSE("DEBUG a_star search", "searching for target: %d\n", target);
            fast_path_element starting_pe = fast_path_element(&key, -1, -1, -1);
            search_element = fast_a_star_pe(starting_pe, NULL, 0, 0);

            vector<fast_a_star_pe> open_list;
            // unordered_map<Key *, fast_a_star_pe> open_list_map;
            vector<fast_a_star_pe> closed_list;
            // unordered_map<Key *, fast_a_star_pe> closed_list_map;
            prior_aspe = NULL;
            unsigned int closed_list_addressable_index = 0;
            fast_push_list(open_list, search_element);
            VERBOSE("DEBUG a_star search", "starting search element %s",search_element.pe.to_string().c_str());

            int search_depth = 0;
            while (open_list.size() > 0){
                search_element = fast_pop_list(open_list);

                //I need to store back pointers to the closed list so I can reconstruct the path
                closed_list_addressable[closed_list_addressable_index] = search_element;
                closed_list_addressable_index++;
                prior_aspe = &closed_list_addressable[closed_list_addressable_index - 1];
                fast_push_list(closed_list, search_element);

                hash_locations locations = location_func(*(search_element.pe.key), table_rows);
                // ALERT("DEBUG", "hash locations 1) %d 2) %d  table_size %d\n", locations.primary, locations.secondary, table_rows);
                unsigned int table_index = next_table_index(search_element.pe.table_index);
                unsigned int index = table_index_to_hash_location(locations, table_index);

                //if the index is not in the open buckets continue
                //Only check for open buckets if the size of the open buckets is greater than zero
                //somewhat unintuitive no open buckets means that they are all open

                if (open_buckets.size() > 0) {
                    if (std::find(open_buckets.begin(), open_buckets.end(), index) == open_buckets.end()) {
                        continue;
                    }
                }

                //We have found the slot if this is true
                if (table.bucket_has_empty(index)){
                    unsigned int offset = table.get_first_empty_index(index);
                    fast_path_element open_pe = fast_path_element(&(table.get_entry_pointer(index,offset)->key), table_index, index, offset);
                    unsigned int distance = search_element.distance + 1;
                    unsigned int f_score = fast_fscore(search_element, target, table_rows);
                    // cout << "found target: " << index << endl;
                    // cout << "setting prior to " << prior_aspe->pe.to_string() << endl;
                    // cout << "exiting search" << endl;
                    //todo this is a critial line but also a hack find a better way to set the tail of the search path
                    search_element = fast_a_star_pe(open_pe, prior_aspe, search_element.distance+1, f_score);
                    found=true;
                    break;
                }

                for (unsigned int i = 0; i < table.get_buckets_per_row(); i++){
                    Key * child_key = &(table.get_entry_pointer(index, i))->key;
                    if (fast_list_contains_vector(closed_list,child_key)){
                        continue;
                    }

                    fast_path_element child_pe = fast_path_element(child_key, table_index, index, i);
                    unsigned int distance = search_element.distance + 1;
                    fast_a_star_pe child = fast_a_star_pe(child_pe, prior_aspe, distance, 0);
                    unsigned int f_score = fast_fscore(child, target, table_rows);
                    child.fscore = f_score;

                    // if (fast_list_contains(open_list_map, child.pe.key)){
                    bool open_list_found = false;
                    for (unsigned int i = 0; i < open_list.size(); i++){
                        if (open_list[i].pe.key == child.pe.key){
                            open_list[i].distance = child.distance;
                            open_list[i].prior = child.prior;
                            open_list[i].fscore = child.fscore;
                            open_list_found = true;
                            break;
                        }
                    }
                    if (!open_list_found){
                        fast_push_list(open_list, child);
                    }
                }

                //TODO don't delete this is also protecting a segfault
                search_depth++;
                if(search_depth > FAST_A_STAR_MAX_DEPTH) [[unlikely]]{
                    // ALERT("search depth exceeded %d moving to the next target \n", MAX_SEARCH_DEPTH);
                    break;
                }
            }
            if (found) {
                break;
            }
        }

        if (found) {
            path = a_star_path_to_path_element(&search_element);
        }
        return path;
    }

    #define DEPTH_LIMIT 30
    void random_dfs_search(search_context &context){
        if (context.path.size() > DEPTH_LIMIT){
            context.found = false;
            return;
        }

        path_element pe = context.path.back();
        if (std::find(context.visited_buckets.begin(), context.visited_buckets.end(), pe.key.to_uint64_t()) != context.visited_buckets.end()){
            context.found=false;
            return;
        } else {
            context.visited_buckets.push_back(pe.key.to_uint64_t());
        }

        int index = next_search_index(pe, context.location_func, *context.table);
        int table_index = next_table_index(pe.table_index);

        if (context.open_buckets.size() > 0){
            if (std::find(context.open_buckets.begin(), context.open_buckets.end(), index) == context.open_buckets.end()){
                context.found=false;
                return;
            }
        }

        if (context.table->bucket_has_empty(index)){
            int bucket_index = context.table->get_first_empty_index(index);
            path_element open_pe = path_element(context.table->get_entry_pointer(index,bucket_index)->key, table_index, index, bucket_index);
            context.path.push_back(open_pe);
            context.found = true;
            return;
        }

        //now we need to choose a random entry in the bucket
        unsigned int static_rand_seed = time(NULL);
        int starting_index = rand_r(&static_rand_seed) % context.table->get_buckets_per_row();
        for (int i = 0; i < context.table->get_buckets_per_row(); i++){
            int bucket_index = (starting_index + i) % context.table->get_buckets_per_row();
            Key * child_key = &(context.table->get_entry_pointer(index, bucket_index)->key);
            if (std::find(context.visited_buckets.begin(), context.visited_buckets.end(), child_key->to_uint64_t()) != context.visited_buckets.end()){
                continue;
            }

            path_element child_pe = path_element(*child_key, table_index, index, bucket_index);
            context.path.push_back(child_pe);
            random_dfs_search(context);
            if (context.found){
                return;
            }
            context.path.pop_back();
        }
        context.found=false;
    }



    #define MAX_BFS_DEPTH 10000
    void bfs_search(search_context & context) {
        context.path.clear();
        context.visited_buckets.clear();

        
        path_element starting_pe = path_element(context.key, -1, -1, -1);
        bfs_pe starting_bfs_pe = bfs_pe(starting_pe, NULL);
        // bfs_queue.push_back(starting_bfs_pe);
        context.found=false;

        int front_index = 0;
        int back_index = 1;

        context.closed_list_bfs_addressable[front_index] = starting_bfs_pe;
        // while(bfs_queue.size() > 0){
        while(front_index < back_index){
            //pop off the first
            bfs_pe *current_ptr;
            current_ptr = &context.closed_list_bfs_addressable[front_index];
            front_index++;

            int index = next_search_index(current_ptr->pe, context.location_func, *context.table);
            int table_index = next_table_index(current_ptr->pe.table_index);

            //Move on if we cant search this bucket
            if (context.open_buckets.size() > 0){
                if (std::find(context.open_buckets.begin(), context.open_buckets.end(), index) == context.open_buckets.end()){
                    continue;
                }
            }

            //move on if this bucket is allready visited
            if (std::find(context.visited_buckets.begin(), context.visited_buckets.end(), index) != context.visited_buckets.end()){
                continue;
            } 
            context.visited_buckets.push_back(index);


            if (context.table->bucket_has_empty(index)){
                int bucket_index = context.table->get_first_empty_index(index);
                path_element open_pe = path_element(context.table->get_entry_pointer(index,bucket_index)->key, table_index, index, bucket_index);
                bfs_pe final_pe = bfs_pe(open_pe, current_ptr);

                context.closed_list_bfs_addressable[back_index] = final_pe;
                context.found = true;
                break;
            }

            for (int i = 0; i < context.table->get_buckets_per_row(); i++){
                // printf("appending keys\n");
                int bucket_index = i;
                Key * child_key = &(context.table->get_entry_pointer(index, bucket_index)->key);
                path_element child_pe = path_element(*child_key, table_index, index, bucket_index);
                bfs_pe chile_bfs_pe = bfs_pe(child_pe, current_ptr);
                context.closed_list_bfs_addressable[back_index] = chile_bfs_pe;
                back_index++;
            }



        }

        //reconstruct the path
        if (context.found){
            context.path.clear();
            bfs_pe *current = &context.closed_list_bfs_addressable[back_index];
            while(current != NULL){
                context.path.push_back(current->pe);
                current = current->prior;
            }
        }
        // print_path(context.path);
        return;


    }

    void random_search_fast_context(search_context &context) {
        context.path.clear();
        path_element starting_pe = path_element(context.key, -1, -1, -1);
        context.path.push_back(starting_pe);
        context.visited_buckets.clear();
        random_dfs_search(context);
        reverse(context.path.begin(), context.path.end());
        if (!context.found){
            context.path.clear();
        }

    }

    void bfs_search_fast_context(search_context &context) {
        bfs_search(context);
        if (!context.found){
            context.path.clear();
        }
    }


// def random_dfs_search(table, location_func, path, open_buckets, visited):
//     if len(path) > DEPTH_LIMIT:
//         return False, path

//     pe = path[-1]
//     if pe.key in visited:
//         return False, path
//     else:
//         visited[pe.key] = True
//     index = next_search_index(pe, location_func, table)
//     table_index = next_table_index(pe.table_index)

//     if open_buckets != None:
//         if not index in open_buckets:
//             return False, path

//     if table.bucket_has_empty(index):
//         # print("Found Slot:" + str(search_element.pe))
//         bucket_index = table.get_first_empty_index(index)
//         pe = path_element(table.get_entry(index,bucket_index),table_index,index,bucket_index)
//         path.append(pe)
//         return True, path
    
//     #here we have a full bucket we need to evict a candidate
//     #randomly select an eviction candidate
//     indicies = list(range(0, table.get_buckets_per_row()))
//     random.shuffle(indicies)
//     for evict_index in indicies:
//         pe = path_element(table.get_entry(index,evict_index), table_index, index, evict_index)
//         if not key_in_path(path, pe.key):
//             path.append(pe)
//         else:
//             continue
        
//         success, path = random_dfs_search(table, location_func, path, open_buckets, visited)

//         if success:
//             return True, path
//         else:
//             path.pop()
        
//     return False, path


    std::vector<path_element> bucket_cuckoo_a_star_insert_fast(cuckoo_tables::Table &table, hash_locations (*location_func) (cuckoo_tables::Key, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets){
        std::vector<path_element> path = a_star_search_fast(table, location_func, key, open_buckets);
        return path;
    }

    void bucket_cuckoo_a_star_insert_fast_context(search_context &context) {
        context.max_search_depth = FAST_A_STAR_MAX_DEPTH;
        // a_star_search_fast_context(context);
        a_star_search_fast_context_multi_home(context);
    }

    void bucket_cuckoo_random_insert_fast_context(search_context &context) {
        random_search_fast_context(context);
    }

    void bucket_cuckoo_bfs_insert_fast_context(search_context & context) {
        bfs_search_fast_context(context);
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

    std::vector<path_element> bucket_cuckoo_random_insert(cuckoo_tables::Table & table, hash_locations (*location_func) (cuckoo_tables::Key, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets){
        cout << "bucket_cuckoo_random_insert not implemented" << endl;
        std::vector<path_element> path;
        return path;
    }

}