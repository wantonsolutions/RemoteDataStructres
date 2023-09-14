#include "../search.h"
#include "../tables.h"
#include "../hash.h"
#include <iostream>
#include <vector>
#include <cassert>
#include <chrono>

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
    // table.print_table();
    // for (int i=0; i < path.size(); i++){
    //     printf("inserting %d %d\n", path[i].bucket_index, path[i].offset);
    // }
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
        vector<path_element> path = bucket_cuckoo_a_star_insert_fast(table, rcuckoo_hash_locations, key, open_buckets);
        if (path.size() == 0){
            continue;
        }
        if (float(inserted)/float(total_inserts) > 0.9){
            break;
        }
        inserted++;
        insert_cuckoo_path(table, path);
    }

    cout << " fill " << i << "/" << total_inserts << " " << i/total_inserts << "\%" << "final fill: " << table.get_fill_percentage() << endl;
}

void run_single_insert_context() {
    unsigned int indexes = 1024 * 1024 * 5;
    unsigned int buckets = 8;
    unsigned int memory = indexes * sizeof(Entry);
    Table table = Table(memory, buckets, 1);
    Key key;
    int total_inserts = indexes+1;
    vector<unsigned int> open_buckets; //empty open buckets
    search_context context;
    context.table = &table;
    context.location_func = rcuckoo_hash_locations;
    context.open_buckets = open_buckets;
    int i;
    int inserted=0;
    for (i=0; i< total_inserts;i++){
        fill_key(key, i);
        bucket_cuckoo_a_star_insert_fast_context(context);
        context.key = key;
        // vector<path_element> path = bucket_cuckoo_a_star_insert_fast(table, rcuckoo_hash_locations, key, open_buckets);
        if (context.path.size() == 0){
            printf("failed to insert\n");
            continue;
        }
        if (float(inserted)/float(total_inserts) > 0.9){
            break;
        }
        inserted++;
        insert_cuckoo_path(table, context.path);
    }

    cout << " fill " << i << "/" << total_inserts << " " << i/total_inserts << "\%" << "final fill: " << table.get_fill_percentage() << endl;
}

void run_random_insert_context() {
    unsigned int indexes = 1024  * 1024;
    unsigned int buckets = 8;
    unsigned int memory = indexes * sizeof(Entry);
    Table table = Table(memory, buckets, 1);
    Key key;
    int total_inserts = indexes+1;
    vector<unsigned int> open_buckets; //empty open buckets
    search_context context;
    context.table = &table;
    context.location_func = rcuckoo_hash_locations;
    context.open_buckets = open_buckets;
    int i;
    int inserted=0;
    for (i=0; i< total_inserts;i++){
        fill_key(key, i);
        bucket_cuckoo_random_insert_fast_context(context);
        context.key = key;
        // vector<path_element> path = bucket_cuckoo_a_star_insert_fast(table, rcuckoo_hash_locations, key, open_buckets);
        // print_path(context.path);
        if (context.path.size() == 0){
            printf("failed to insert\n");
            if (context.found == false) {
                printf("not found\n");
            }
            continue;
        }
        if (float(inserted)/float(total_inserts) > 0.9){
            break;
        }
        inserted++;
        insert_cuckoo_path(table, context.path);
    }
    context.table->print_table();

    cout << " fill " << i << "/" << total_inserts << " " << i/total_inserts << "\%" << "final fill: " << context.table->get_fill_percentage() << endl;
}


void time_and_check_search_algorithms() {

    // unsigned int indexes = 1024 * 1024;
    unsigned int indexes = 1024 * 128;
    unsigned int buckets = 8;
    unsigned int memory = indexes * sizeof(Entry);
    int total_inserts = indexes+1;
    Key key;
    vector<unsigned int> open_buckets; //empty open buckets
    int i;
    int inserted=0;

    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;
    printf("starting search\n");


    //The first stable insert
    Table table_0 = Table(memory, buckets, 1);
    auto t1 = high_resolution_clock::now();
    for (i=1; i< total_inserts;i++){
        fill_key(key, i);
        vector<path_element> path = bucket_cuckoo_a_star_insert(table_0, rcuckoo_hash_locations, key, open_buckets);
        if (path.size() == 0){
            continue;
        }
        if (float(inserted)/float(total_inserts) > 0.9){
            break;
        }
        inserted++;
        insert_cuckoo_path(table_0, path);
    }
    auto t2 = high_resolution_clock::now();
    auto duration_0 = duration_cast<milliseconds>( t2 - t1 ).count();


    printf("---------------------------------\n");
    //The second table
    inserted=0;
    Table table_1 = Table(memory, buckets, 1);
    auto t3 = high_resolution_clock::now();
    for (i=1; i< total_inserts;i++){
        fill_key(key, i);
        vector<path_element> path = bucket_cuckoo_a_star_insert_fast(table_1, rcuckoo_hash_locations, key, open_buckets);
        if (path.size() == 0){
            continue;
        }
        if (float(inserted)/float(total_inserts) > 0.9){
            break;
        }
        inserted++;
        insert_cuckoo_path(table_1, path);
    }
    auto t4 = high_resolution_clock::now();
    auto duration_1 = duration_cast<milliseconds>( t4 - t3 ).count();

    printf("\n");
    cout << "final table" << endl;
    if (!(table_0.get_fill_percentage() == table_1.get_fill_percentage())){
        cout << "Tables are not equal" << endl;
        table_0.print_table();
        table_1.print_table();
        exit(1);
    } else {
        cout << "Test Passed" << endl;
    }
    // if (!(table_0 == table_1)){
    //     cout << "Tables are not equal" << endl;
    //     table_0.print_table();
    //     table_1.print_table();
    //     exit(1);
    // } else {
    //     cout << "Test Passed" << endl;
    // }

    cout << "slow fill: " << duration_0 << endl;
    cout << "fast fill: " << duration_1 << endl;
    cout << "speedup " << float(duration_0)/float(duration_1) << "x" << endl;

    cout << "fast fill searches per second " << float(inserted)/(float(duration_1 / 1000.0)) << endl;
}

void time_and_check_search_algorithms_2() {

    // unsigned int indexes = 1024 * 1024;
    unsigned int indexes = 1024 * 128;
    unsigned int buckets = 8;
    unsigned int memory = indexes * sizeof(Entry);
    int total_inserts = indexes+1;
    Key key;
    vector<unsigned int> open_buckets; //empty open buckets
    int i;
    int inserted=0;

    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;
    printf("starting search\n");

    //The second table
    inserted=0;
    Table table_1 = Table(memory, buckets, 1);
    auto t3 = high_resolution_clock::now();
    for (i=1; i< total_inserts;i++){
        fill_key(key, i);
        vector<path_element> path = bucket_cuckoo_a_star_insert_fast(table_1, rcuckoo_hash_locations, key, open_buckets);
        if (path.size() == 0){
            continue;
        }
        if (float(inserted)/float(total_inserts) > 0.9){
            break;
        }
        inserted++;
        insert_cuckoo_path(table_1, path);
    }
    auto t4 = high_resolution_clock::now();
    auto duration_1 = duration_cast<milliseconds>( t4 - t3 ).count();

    //The second table
    inserted=0;
    Table table_2 = Table(memory, buckets, 1);
    search_context context;
    context.table = &table_2;
    context.location_func = rcuckoo_hash_locations;
    context.open_buckets = open_buckets;

    auto t5 = high_resolution_clock::now();
    for (i=1; i< total_inserts;i++){
        fill_key(key, i);
        context.key = key;
        bucket_cuckoo_a_star_insert_fast_context(context);

        // vector<path_element> path = bucket_cuckoo_a_star_insert_fast(table_2, rcuckoo_hash_locations, key, open_buckets);
        if (context.path.size() == 0){
            continue;
        }
        if (float(inserted)/float(total_inserts) > 0.9){
            break;
        }
        inserted++;
        insert_cuckoo_path(table_2, context.path);
    }
    auto t6 = high_resolution_clock::now();
    auto duration_2 = duration_cast<milliseconds>( t6 - t5 ).count();

    printf("\n");
    cout << "final table" << endl;
    if (!(table_1.get_fill_percentage() == table_2.get_fill_percentage())){
        cout << "Tables are not equal" << endl;
        // table_1.print_table();
        // table_2.print_table();
        // exit(1);
        cout << table_1.get_fill_percentage() << " " << table_2.get_fill_percentage() << endl;
        cout << "equality test failed" << endl;
    } else {
        cout << "Test Passed" << endl;
    }

    cout << "slow fill: " << duration_1 << endl;
    cout << "fast fill: " << duration_2 << endl;
    cout << "speedup " << float(duration_1)/float(duration_2) << "x" << endl;

    for(int i=0;i<table_1.get_row_count();i++) {
        for (int j=0;j<table_1.get_buckets_per_row();j++){
            if (table_1.get_entry(i,j) != table_2.get_entry(i,j)){
                cout << "Tables are not equal" << endl;
                // table_1.print_table();
                // table_2.print_table();
                exit(1);
            }
        }
    }
    cout << "totally identical seriously";
    cout << "fast fill searches per second " << float(inserted)/(float(duration_1 / 1000.0)) << endl;

}
void time_and_check_search_algorithms_3() {

    unsigned int indexes = 1024 * 1024;
    // unsigned int indexes = 1024 * 128;
    unsigned int buckets = 8;
    unsigned int memory = indexes * sizeof(Entry);
    int total_inserts = indexes+1;
    Key key;
    vector<unsigned int> open_buckets; //empty open buckets
    int i;
    int inserted=0;

    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;
    printf("starting search\n");


    //The second table
    inserted=0;
    Table table_1 = Table(memory, buckets, 1);
    search_context context;
    context.table = &table_1;
    context.location_func = rcuckoo_hash_locations;
    context.open_buckets = open_buckets;

    auto t0 = high_resolution_clock::now();
    for (i=0; i< total_inserts;i++){
        fill_key(key, i);
        bucket_cuckoo_bfs_insert_fast_context(context);
        context.key = key;
        // vector<path_element> path = bucket_cuckoo_a_star_insert_fast(table, rcuckoo_hash_locations, key, open_buckets);
        // print_path(context.path);
        if (context.path.size() == 0){
            printf("failed to insert\n");
            if (context.found == false) {
                printf("not found\n");
            }
            continue;
        }
        if (float(inserted)/float(total_inserts) > 0.9){
            break;
        }
        inserted++;
        insert_cuckoo_path(table_1, context.path);
    }
    auto t1 = high_resolution_clock::now();
    auto duration_1 = duration_cast<milliseconds>( t1 - t0 ).count();


    inserted=0;
    Table table_2 = Table(memory, buckets, 1);
    context.table = &table_2;
    context.location_func = rcuckoo_hash_locations;
    context.open_buckets = open_buckets;

    auto t5 = high_resolution_clock::now();
    for (i=1; i< total_inserts;i++){
        fill_key(key, i);
        context.key = key;
        bucket_cuckoo_a_star_insert_fast_context(context);

        // vector<path_element> path = bucket_cuckoo_a_star_insert_fast(table_2, rcuckoo_hash_locations, key, open_buckets);
        if (context.path.size() == 0){
            continue;
        }
        if (float(inserted)/float(total_inserts) > 0.9){
            break;
        }
        inserted++;
        insert_cuckoo_path(table_2, context.path);
    }
    auto t6 = high_resolution_clock::now();
    auto duration_2 = duration_cast<milliseconds>( t6 - t5 ).count();

    printf("\n");
    cout << "final table" << endl;
    if (!(table_1.get_fill_percentage() == table_2.get_fill_percentage())){
        cout << "Tables are not equal" << endl;
        // table_1.print_table();
        // table_2.print_table();
        // exit(1);
        cout << table_1.get_fill_percentage() << " " << table_2.get_fill_percentage() << endl;
        cout << "equality test failed" << endl;
    } else {
        cout << "Test Passed" << endl;
    }

    cout << "slow fill: " << duration_1 << endl;
    cout << "fast fill: " << duration_2 << endl;
    cout << "speedup " << float(duration_1)/float(duration_2) << "x" << endl;

    for(int i=0;i<table_1.get_row_count();i++) {
        for (int j=0;j<table_1.get_buckets_per_row();j++){
            if (table_1.get_entry(i,j) != table_2.get_entry(i,j)){
                cout << "Tables are not equal" << endl;
                // table_1.print_table();
                // table_2.print_table();
                exit(1);
            }
        }
    }
    cout << "totally identical seriously";
    cout << "fast fill searches per second " << float(inserted)/(float(duration_1 / 1000.0)) << endl;

}



// void run_single_insert() {
//     Table table = Table(64, 8, 1);
//     Key key;
//     key.bytes[0] = 0;
//     vector<unsigned int> open_buckets; //empty open buckets
//     vector<path_element> path = bucket_cuckoo_a_star_insert(table, rcuckoo_hash_locations, key, open_buckets);
//     print_path(path);

// }

void run_bfs_insert_context() {

    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;
    printf("starting search\n");


    //The second table

    auto t0 = high_resolution_clock::now();

    unsigned int indexes = 1024 * 1024;
    unsigned int buckets = 8;
    unsigned int memory = indexes * sizeof(Entry);
    Table table = Table(memory, buckets, 1);
    Key key;
    int total_inserts = indexes+1;
    vector<unsigned int> open_buckets; //empty open buckets
    search_context context;
    context.table = &table;
    context.location_func = rcuckoo_hash_locations;
    context.open_buckets = open_buckets;
    int i;
    int inserted=0;
    for (i=0; i< total_inserts;i++){
        fill_key(key, i);
        bucket_cuckoo_bfs_insert_fast_context(context);
        context.key = key;
        // vector<path_element> path = bucket_cuckoo_a_star_insert_fast(table, rcuckoo_hash_locations, key, open_buckets);
        // print_path(context.path);
        if (context.path.size() == 0){
            printf("failed to insert\n");
            if (context.found == false) {
                printf("not found\n");
            }
            continue;
        }
        if (float(inserted)/float(total_inserts) > 0.9){
            break;
        }
        inserted++;
        insert_cuckoo_path(table, context.path);
    }
    // context.table->print_table();
    auto t1 = high_resolution_clock::now();
    auto duration_1 = duration_cast<milliseconds>( t1 - t0 ).count();
    cout << "fill time " << duration_1 << endl;

    cout << " fill " << i << "/" << total_inserts << " " << i/total_inserts << "\%" << "final fill: " << context.table->get_fill_percentage() << endl;
}

int main() {
    // run_basic_table_tests();
    // run_single_insert();
    // time_and_check_search_algorithms();
    // run_single_insert_context();
    // run_random_insert_context();
    run_bfs_insert_context();
    // time_and_check_search_algorithms_2();
    // time_and_check_search_algorithms_3();
    // cout << "Hello Search Test!" << endl;
}