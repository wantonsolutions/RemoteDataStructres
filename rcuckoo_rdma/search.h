#pragma once
#ifndef SEARCH_H
#define SEARCH_H

#include <string>
#include <vector>
#include "tables.h"
#include "hash.h"
#include <unordered_map>

#define MAX_SEARCH_ITEMS 5000
#define MAX_SEARCH_DEPTH 50
namespace cuckoo_search {



    typedef struct path_element {
        cuckoo_tables::Key key;
        unsigned int table_index;
        unsigned int bucket_index;
        unsigned int offset;
        path_element() {};
        path_element(cuckoo_tables::Key _key, unsigned int _table_index, unsigned int _bucket_index, unsigned int _offset) {
            key = _key;
            table_index = _table_index;
            bucket_index = _bucket_index;
            offset = _offset;
        };
        std::string to_string(){
            return "key: " + key.to_string() + " table_index: " + std::to_string(table_index) + " bucket_index: " + std::to_string(bucket_index) + " offset: " + std::to_string(offset);
        }
    } path_element;

    typedef struct fast_path_element {
        cuckoo_tables::Key *key;
        uint8_t table_index;
        unsigned int bucket_index;
        uint8_t offset;
        fast_path_element() {};
        fast_path_element(cuckoo_tables::Key * _key, uint8_t _table_index, unsigned int _bucket_index, unsigned int _offset) {
            key = _key;
            table_index = _table_index;
            offset = _offset;
            bucket_index = _bucket_index;
        };
        std::string to_string(){
            return "key: " + key->to_string() + " table_index: " + std::to_string(table_index) + " bucket_index: " + std::to_string(bucket_index) + " offset: " + std::to_string(offset);
        }
    } fast_path_element;

    typedef struct fast_a_star_pe {
        fast_path_element pe;
        fast_a_star_pe *prior;
        unsigned int distance;
        unsigned int fscore;
        std::string to_string() {
            return pe.to_string() + " distance: " + std::to_string(distance) + " fscore: " + std::to_string(fscore);
        }
        bool operator<( const fast_a_star_pe & aspe ) const {
            return fscore < aspe.fscore;
        }
        bool operator==( const fast_a_star_pe & aspe ) const {
            return *(pe.key) == *(aspe.pe.key);
        }
        fast_a_star_pe() {};
        fast_a_star_pe(fast_path_element _pe, fast_a_star_pe * _prior, unsigned int _distance, unsigned int _fscore) {
            pe = _pe;
            prior = _prior;
            distance = _distance;
            fscore = _fscore;}
    } fast_a_star_pe;

    typedef struct a_star_pe {
        path_element pe;
        a_star_pe *prior;
        unsigned int distance;
        unsigned int fscore;
        std::string to_string();
        bool operator<( const a_star_pe & aspe ) const {
            return fscore < aspe.fscore;
        }
        bool operator==( const a_star_pe & aspe ) const {
            return pe.key == aspe.pe.key;
        }
        a_star_pe() {};
        a_star_pe(path_element _pe, a_star_pe * _prior, unsigned int _distance, unsigned int _fscore) {
            pe = _pe;
            prior = _prior;
            distance = _distance;
            fscore = _fscore;}
    } a_star_pe;

    typedef struct bfs_pe {
        path_element pe;
        bfs_pe * prior;
        std::string to_string() {
            return "this: " + pe.to_string() + "prior: " + prior->pe.to_string();
        }
        bfs_pe() {};
        bfs_pe(path_element _pe, bfs_pe * _prior) {
            pe = _pe;
            prior = _prior;
        }
    } bfs_pe;

    typedef struct search_context {
        //input values
        cuckoo_tables::Table *table;
        hash_locations (*location_func) (cuckoo_tables::Key, unsigned int);
        cuckoo_tables::Key key;
        vector<unsigned int> open_buckets;
        //return values
        vector<path_element> path;
        //internal_values
        fast_a_star_pe closed_list_addressable[MAX_SEARCH_ITEMS];
        bfs_pe closed_list_bfs_addressable[MAX_SEARCH_ITEMS];
        vector<fast_a_star_pe> open_list;
        vector<fast_a_star_pe> closed_list;
        vector<bfs_pe> bfs_queue;
        vector<unsigned int> targets;
        vector<unsigned int> visited_buckets;
        int max_search_depth;
        bool found;
    } search_context;

    unsigned int get_table_id_from_index(unsigned int index);
    std::vector<unsigned int> search_path_to_buckets(std::vector<path_element> path);
    void search_path_to_buckets_fast(vector<path_element> &path, vector<unsigned int> &buckets);
    std::vector<path_element> random_dfs_search(cuckoo_tables::Key key, unsigned int table_size);
    std::vector<path_element> bucket_cuckoo_insert(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int>  open_buckets);
    // unsigned int next_search_index(path_element pe, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Table table);

    unsigned int next_search_index(path_element pe, hash_locations (*location_func) (Key, unsigned int), Table &table);
    bool key_in_path(std::vector<path_element> path, cuckoo_tables::Key key);
    void print_path(std::vector<path_element> path);
    string path_to_string(std::vector<path_element> path);
    unsigned int path_index_range(std::vector<path_element> path);
    std::vector<unsigned int> find_closest_target_n_bi_directional(cuckoo_tables::Table &table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, unsigned int n);

    a_star_pe pop_list(std::vector<a_star_pe> &list, std::unordered_map<cuckoo_tables::Key, a_star_pe> &list_map);
    void push_list(std::vector<a_star_pe> &list, std::unordered_map<cuckoo_tables::Key, a_star_pe> &list_map, a_star_pe pe);
    bool list_contains(std::unordered_map<cuckoo_tables::Key, a_star_pe> &list_map, cuckoo_tables::Key key);
    unsigned int next_table_index(unsigned int table_index);

    unsigned int heuristic(unsigned int current_index, unsigned int target_index, unsigned int table_size);
    unsigned int fscore(a_star_pe pe, unsigned int target_index, unsigned int table_size);

    std::vector<path_element> a_star_search(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets);
    std::vector<path_element> a_star_search_fast(cuckoo_tables::Table &table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets);
    void bucket_cuckoo_a_star_insert_fast_context(search_context &context);
    std::vector<path_element> bucket_cuckoo_a_star_insert(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets);
    std::vector<path_element> bucket_cuckoo_random_insert(cuckoo_tables::Table table, hash_locations (*location_func) (std::string, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets);
    std::vector<path_element> bucket_cuckoo_a_star_insert_fast(cuckoo_tables::Table &table, hash_locations (*location_func) (Key, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets);
    std::vector<path_element> bucket_cuckoo_random_insert(cuckoo_tables::Table & table, hash_locations (*location_func) (cuckoo_tables::Key, unsigned int), cuckoo_tables::Key key, std::vector<unsigned int> open_buckets);


    void bucket_cuckoo_random_insert_fast_context(search_context &context);
    void bucket_cuckoo_bfs_insert_fast_context(search_context & context);

} 
#endif