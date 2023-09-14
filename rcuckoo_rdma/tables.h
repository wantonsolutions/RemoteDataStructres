#pragma once
#ifndef TABLES_H
#define TABLES_H

#include <stdint.h>
#include <string>
#include <vector>
#include <assert.h>

using namespace std;


namespace cuckoo_tables {

    #ifndef KEY_SIZE
    #define KEY_SIZE 4
    #endif

    #ifndef VALUE_SIZE
    #define VALUE_SIZE 4
    #endif

    // std::vector<char> HexToBytes(const std::string& hex) {
    //     std::vector<char> bytes;
    //     for (unsigned int i = 0; i < hex.length(); i += 2) {
    //         std::string byteString = hex.substr(i, 2);
    //         char byte = (char) strtol(byteString.c_str(), NULL, 16);
    //         bytes.push_back(byte);
    //     }
    //     return bytes;
    // }


    typedef struct Key { 
        uint8_t bytes[KEY_SIZE];
        string to_string();
        uint64_t to_uint64_t();
        bool is_empty();
        bool operator==(const Key& rhs) const {
            if(KEY_SIZE == 4){
                uint32_t lhs_int = *(uint32_t*)bytes;
                uint32_t rhs_int = *(uint32_t*)rhs.bytes;
                return lhs_int == rhs_int;
            } else {

                for (int i = KEY_SIZE; i >= 0; i++){
                    if (bytes[i] != rhs.bytes[i]){
                        return false;
                    }
                }
                return true;
            }
        }
        bool operator=(const Key& rhs) {
            for (int i = 0; i < KEY_SIZE; i++){
                bytes[i] = rhs.bytes[i];
            }
            return true;
        }
        template <typename T>
        void set(T val) {
            for (int i = 0; i < KEY_SIZE && i < sizeof(val); i++){
                bytes[i] = (val >> (8 * i)) & 0xFF;
            }
        }

        Key(string key) {
            for (long unsigned int i = 0; i < KEY_SIZE*2 && i < key.size(); i+=2){
                std::string byteString = key.substr(i, 2);
                uint8_t byte = (uint8_t) strtol(byteString.c_str(), NULL, 16);
                bytes[i/2] = byte;
            }
        }
        Key(){
            for (auto i = 0; i < KEY_SIZE; i++){
                bytes[i] = 0;
            }
        }

    } Key;



    typedef struct Value { 
        uint8_t bytes[VALUE_SIZE];
        string to_string();
        bool is_empty();
        bool operator==(const Value& rhs) const {
            for (int i = 0; i < VALUE_SIZE; i++){
                if (bytes[i] != rhs.bytes[i]){
                    return false;
                }
            }
            return true;
        }
        bool operator=(const Value& rhs) {
            for (int i = 0; i < VALUE_SIZE; i++){
                bytes[i] = rhs.bytes[i];
            }
            return true;
        }
        template <typename T>
        void set(T val) {
            for (int i = 0; i < VALUE_SIZE && i < sizeof(val); i++){
                bytes[i] = (val >> (8 * i)) & 0xFF;
            }
        }
        Value(string value){
            for (long unsigned int i = 0; i < VALUE_SIZE && i < value.size(); i++){
                bytes[i] = value[i] - '0';
            }
        }
        Value(){
            for (int i = 0; i < VALUE_SIZE; i++){
                bytes[i] = 0;
            }
        }
    } Value;

    typedef struct Entry {
        //todo add some entry functions
        Key key;
        Value value;
        string to_string();
        bool is_empty();
        Entry() {
            this->key = Key();
            this->value = Value();
        }
        Entry(string str_key, string str_value) {
            this->key = Key(str_key);
            this->value = Value(str_value);
        }
        Entry(Key key, Value value) {
            this->key = key;
            this->value = value;
        }

        bool operator==(const Entry& rhs) const {
            return this->key == rhs.key && this->value == rhs.value;
        }
        bool operator!=(const Entry& rhs) const {
            return !(*this == rhs);
        }

        uint64_t get_as_uint64_t() {
            assert(sizeof(Entry) == 8);
            uint64_t entry64 = 0;
            int i=0;
            for (; i < KEY_SIZE; i++){
                entry64 |= (uint64_t) this->key.bytes[i] << (8 * i);
            }
            for(; i < VALUE_SIZE;i++){
                entry64 |= (uint64_t) this->value.bytes[i - KEY_SIZE] << (8 * i);
            }
            return entry64;
        }
        void set_as_uint64_t(uint64_t entry64) {
            assert(sizeof(Entry) == 8);
            int i=0;
            for (; i < KEY_SIZE; i++){
                this->key.bytes[i] = (entry64 >> (8 * i)) & 0xFF;
            }
            for(; i < VALUE_SIZE;i++){
                this->value.bytes[i - KEY_SIZE] = (entry64 >> (8 * i)) & 0xFF;
            }
        }
    } Entry;

    typedef struct CasOperationReturn {
        bool success;
        uint64_t original_value;
        CasOperationReturn(){
            this->success = false;
            this->original_value = 0;
        }
        CasOperationReturn(bool success, uint64_t original_value) {
            this->success = success;
            this->original_value = original_value;
        }
    } CasOperationReturn;

    typedef struct Duplicate_Entry {
        Entry first_entry;
        int first_entry_row;
        int first_entry_column;
        Entry second_entry;
        int second_entry_row;
        int second_entry_column;
    } Duplicate_Entry;


    class Lock_Table {
        public:
            Lock_Table();
            Lock_Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock);
            // ~Lock_Table();
            void unlock_all();
            CasOperationReturn masked_cas(unsigned int index, uint64_t old, uint64_t new_value, uint64_t mask);
            void fill_masked_cas(unsigned int index, bool success, uint64_t new_value, uint64_t mask);
            void * get_lock_table_address();
            unsigned int get_lock_table_size_bytes();
            void set_lock_table_address(void * address);
            void * get_lock_pointer(unsigned int lock_index);
            string to_string();

        private:
            unsigned int _total_locks;
            unsigned int _total_lock_entries;
            uint8_t *_locks;
    };
    
    class Table {
        public:
            Table();
            Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock);


            bool operator==(const Table &rhs) const;
            // ~Table();
            void unlock_all();
            string to_string();
            string row_to_string(unsigned int row);
            void print_table();
            void print_lock_table();
            Entry ** get_underlying_table();
            void set_underlying_table(Entry ** table);
            CasOperationReturn lock_table_masked_cas(unsigned int lock_index, uint64_t old, uint64_t new_value, uint64_t mask);
            void fill_lock_table_masked_cas(unsigned int lock_index, bool success, uint64_t value, uint64_t mask);
            unsigned int get_table_size_bytes() const;
            unsigned int get_buckets_per_row() const;
            unsigned int get_row_count() const;
            unsigned int get_bucket_size();
            unsigned int row_size_bytes();
            unsigned int get_entry_size_bytes();
            unsigned int n_buckets_size(unsigned int n_buckets);
            Entry get_entry(unsigned int bucket_index, unsigned int offset) const;
            void set_entry(unsigned int bucket_index, unsigned int offset, Entry entry);
            Entry * get_entry_pointer(unsigned int bucket_index, unsigned int offset);
            bool bucket_has_empty(unsigned int bucket_index);
            unsigned int get_first_empty_index(unsigned int bucket_index);

            bool contains(Key key);
            bool bucket_contains(unsigned int bucket_index, Key key);

            float get_fill_percentage_fast();
            float get_fill_percentage();
            bool full();
            Entry ** generate_bucket_cuckoo_hash_index(unsigned int memory_size, unsigned int bucket_size);
            unsigned int absolute_index_to_bucket_index(unsigned int absolute_index);
            unsigned int absolute_index_to_bucket_offset(unsigned int absolute_index);
            void assert_operation_in_table_bound(unsigned int bucket_index, unsigned int offset, unsigned int read_size);
            bool contains_duplicates();
            vector<Duplicate_Entry> get_duplicates();


            void * get_underlying_lock_table_address();
            unsigned int get_underlying_lock_table_size_bytes();
            void set_underlying_lock_table_address(void * address);
            void * get_lock_pointer(unsigned int lock_index);


        private:
            unsigned int _memory_size;
            unsigned int _bucket_size;
            unsigned int _table_size;
            unsigned int _buckets_per_lock;
            Entry **_table;
            Lock_Table _lock_table;
            unsigned int _fill;
    };
}

namespace std {

    using namespace cuckoo_tables;
    template <>
    struct hash<Key>
    {
        std::size_t operator()(const Key& k) const
        {
        using std::size_t;
        using std::hash;
        using std::string;

        // Compute individual hash values for first,
        // second and third and combine them using XOR
        // and bit shifting:

        std::size_t s = hash<string>()("salt");
        for (int i = 0; i < KEY_SIZE; i++){
            s ^= (hash<int>()(k.bytes[i]) << i);
        }
        return s;
        }
        };
}

#endif