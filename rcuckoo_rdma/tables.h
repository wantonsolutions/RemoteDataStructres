#pragma once
#ifndef TABLES_H
#define TABLES_H

#include <stdint.h>
#include <string>

namespace cuckoo_tables {

    typedef struct Entry {
        //todo add some entry functions
        unsigned int key;
        unsigned int value;
    } Entry;

    typedef struct CasOperationReturn {
        bool success;
        uint64_t original_value;
    } CasOperationReturn;


    class Lock_Table {
        public:
            Lock_Table();
            Lock_Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock);
            // ~Lock_Table();
            void unlock_all();
            CasOperationReturn masked_cas(unsigned int index, uint64_t old, uint64_t new_value, uint64_t mask);
            void fill_masked_cas(unsigned int index, bool success, uint64_t new_value, uint64_t mask);
            std::string to_string();

        private:
            unsigned int _total_locks;
            uint8_t *_locks;
    };
    
    class Table {
        public:
            Table();
            Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock);
            // ~Table();
            void unlock_all();
            void print_table();
            CasOperationReturn lock_table_masked_cas(unsigned int lock_index, uint64_t old, uint64_t new_value, uint64_t mask);
            CasOperationReturn fill_lock_table_masked_cas(unsigned int lock_index, bool success, uint64_t value, uint64_t mask);
            unsigned int get_buckets_per_row();
            unsigned int get_row_count();
            unsigned int get_bucket_size();
            unsigned int row_size_bytes();
            unsigned int row_size_indexes();
            unsigned int n_buckets_size(unsigned int n_buckets);
            Entry get_entry(unsigned int bucket_index, unsigned int offset);
            void set_entry(unsigned int bucket_index, unsigned int offset, Entry entry);
            bool bucket_has_empty(unsigned int bucket_index);
            unsigned int get_first_empty_index(unsigned int bucket_index);
            bool bucket_contains(unsigned int bucket_index, unsigned int key);
            bool contains(unsigned int key);
            float get_fill_percentage();
            bool full();
            Entry ** generate_bucket_cuckoo_hash_index(unsigned int memory_size, unsigned int bucket_size);
            unsigned int find_empty_index(unsigned int bucket_index);
            unsigned int absolute_index_to_bucket_index(unsigned int absolute_index);
            unsigned int absolute_index_to_offset(unsigned int absolute_index);
            void assert_operation_in_table_bound(unsigned int bucket_index, unsigned int offset, unsigned int memory_size);
            bool contains_duplicates();
            unsigned int ** get_duplicates();

        private:
            unsigned int _memory_size;
            unsigned int _bucket_size;
            unsigned int _buckets_per_lock;
            Entry **_table;
            Lock_Table _lock_table;
            unsigned int _table_size;
            unsigned int _fill;
    };
}

#endif