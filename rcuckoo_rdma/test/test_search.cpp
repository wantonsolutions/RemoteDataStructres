#include "../search.h"
#include "../tables.h"
#include "../hash.h"
#include <iostream>
#include <vector>
#include <cassert>

using namespace std;
using namespace cuckoo_search;
using namespace cuckoo_tables;

int test_0() {
    //Test 0 make sure we return the correct result on a basic path
    vector<path_element> path;
    for (int i=0; i < 5; i++){
        path_element pe;
        pe.bucket_index=i;
        pe.table_index=i%2;
        path.push_back(pe);
    }
    vector<unsigned int> expected_result = {0, 1, 2, 3, 4};
    vector<unsigned int> buckets = search_path_to_buckets(path);
    if (buckets == expected_result){
        return 0;
    } else {
        return 1;
    }
}

int test_1() {
    //Test 1 make sure we return the correct result on a basic path
    vector<path_element> path;
    for (int i=0; i < 5; i++){
        path_element pe;
        pe.bucket_index=i;
        pe.table_index=i%2;
        path.push_back(pe);
        path.push_back(pe);
    }
    vector<unsigned int> expected_result = {0, 1, 2, 3, 4};
    vector<unsigned int> buckets = search_path_to_buckets(path);
    if (buckets == expected_result){
        return 0;
    } else {
        return 1;
    }
}

int test_2() {
    //should make a table with a single row. We should return a single value
    Table table = Table(64, 8, 1);
    Key key;
    key.bytes[0] = 0;
    vector<unsigned int> open_buckets = find_closest_target_n_bi_directional(table, rcuckoo_hash_locations, key, 1);
    // table.print_table();
    // cout << "open_buckets: " << endl;
    vector<unsigned int> expected_result = {0};
    if (open_buckets == expected_result){
        return 0;
    } else {
        return 1;
    }
}

int test_3() {
    //should make a table with a single row. We should return a single value
    Table table = Table(128, 8, 1);
    Key key;
    key.bytes[0] = 1;
    vector<unsigned int> open_buckets = find_closest_target_n_bi_directional(table, rcuckoo_hash_locations, key, 2);
    // table.print_table();
    // cout << "open_buckets: " << endl;
    vector<unsigned int> expected_result = {0};
    if (open_buckets == expected_result){
        return 0;
    } else {
        return 1;
    }
}

void run_basic_table_tests() {
    vector<int (*)()> tests = {test_0, test_1, test_2, test_3};
    for (int i=0; i < tests.size(); i++){
        int result = tests[i]();
        if (result == 0){
            cout << "Test " << i << " passed" << endl;
        } else {
            cout << "Test " << i << " failed" << endl;
        }
    }
}

void insert_cuckoo_path(Table &table, vector<path_element> path) {
    assert(path.size() >= 2);
    for (int i=path.size()-2; i >=0; i--){
        Entry e;
        e.key = path[i+1].key;
        table.set_entry(path[i].bucket_index, path[i].offset, e);
    }
}

void fill_key(Key &key, int value) {
    uint8_t *vp = (uint8_t*)&value;
    key.bytes[0] = vp[0];
    key.bytes[1] = vp[1];
    key.bytes[2] = vp[2];
    key.bytes[3] = vp[3];
} 

void run_single_insert() {
    // unsigned int indexes = 1024 * 1024;
    // unsigned int indexes = 32;
    unsigned int indexes = 1024 * 128;
    unsigned int buckets = 8;
    unsigned int memory = indexes * sizeof(Entry);
    Table table = Table(memory, buckets, 1);
    Key key;
    int total_inserts = indexes+1;
    vector<unsigned int> open_buckets; //empty open buckets
    int i;
    int inserted=0;
    for (i=0; i< total_inserts;i++){
        fill_key(key, i);
        vector<path_element> path = bucket_cuckoo_a_star_insert(table, rcuckoo_hash_locations, key, open_buckets);
        // print_path(path);
        if (path.size() == 0){
            // cout << "failed to insert key: " << key.to_string() << endl;
            continue;
        }
        if (float(inserted)/float(total_inserts) > 0.9){
            break;
        }
        inserted++;
        insert_cuckoo_path(table, path);
    }

    // cout << "final table" << endl;
    table.print_table();
    cout << " fill " << i << "/" << total_inserts << " " << i/total_inserts << "\%" << "final fill: " << table.get_fill_percentage() << endl;
}



// void run_single_insert() {
//     Table table = Table(64, 8, 1);
//     Key key;
//     key.bytes[0] = 0;
//     vector<unsigned int> open_buckets; //empty open buckets
//     vector<path_element> path = bucket_cuckoo_a_star_insert(table, rcuckoo_hash_locations, key, open_buckets);
//     print_path(path);

// }

int main() {
    // run_basic_table_tests();
    run_single_insert();
    // cout << "Hello Search Test!" << endl;
}