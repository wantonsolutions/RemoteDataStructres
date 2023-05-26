#include "tables.h"
#include <cstddef>


namespace cuckoo_tables {

    /*lock table functions*/
    Lock_Table::Lock_Table(){
        //todo implement
    }
    Lock_Table::Lock_Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock){
        //todo implement
    }
    void Lock_Table::unlock_all(){
        //todo implement
    }
    CasOperationReturn Lock_Table::masked_cas(unsigned int index, uint64_t old, uint64_t new_value, uint64_t mask){
        return CasOperationReturn();
        //todo implement
    }
    void Lock_Table::fill_masked_cas(unsigned int index, bool success, uint64_t new_value, uint64_t mask){
        //todo implement

    }


    Table::Table(){
        //todo implement
        _entry_size = 0;
        _memory_size = 0;
        _bucket_size = 0;
        _buckets_per_lock = 0;
        _fill = 0;
    }

    Table::Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock){
        //todo implement
        _entry_size =8;
        _memory_size = memory_size;
        _bucket_size = bucket_size;
        _buckets_per_lock = buckets_per_lock;
        _fill = 0;
    }


    void Table::unlock_all(){
        //todo implement
        return;
    }
    void Table::print_table(){
        //todo implement
        return;
    }
    CasOperationReturn Table::lock_table_masked_cas(unsigned int lock_index, uint64_t old, uint64_t new_value, uint64_t mask){
        //todo implement
        return CasOperationReturn();
    }
    CasOperationReturn Table::fill_lock_table_masked_cas(unsigned int lock_index, bool success, uint64_t value, uint64_t mask){
        //todo implement
        return CasOperationReturn();
    }
    unsigned int Table::get_bucket_size(){
        //todo implement
        return 0;
    }
    unsigned int Table::row_size_bytes(){
        //todo implement
        return 0;
    }
    unsigned int Table::row_size_indexes(){
        //todo implement
        return 0;
    }
    unsigned int Table::n_buckets_size(unsigned int n_buckets) {
        return _entry_size * n_buckets;
    }
    Entry Table::get_entry(unsigned int bucket_index, unsigned int offset){
        //todo implement
        return Entry();
    }
    void Table::set_entry(unsigned int bucket_index, unsigned int offset, Entry entry){
        //todo implement
    }
    bool Table::bucket_has_empty(unsigned int bucket_index){
        //todo implement
        return false;
    }
    unsigned int Table::get_first_empty_index(unsigned int bucket_index){
        //todo implement
        return 0;

    }
    bool Table::bucket_contains(unsigned int bucket_index, unsigned int key){
        //todo implement
        return false;

    }
    bool Table::contains(unsigned int key){
        //todo implement
        return false;

    }
    float Table::get_fill_percentage(){
        //todo implement
        return 0.0;

    }
    bool Table::full(){
        //todo implement
        return false;

    }
    Entry ** Table::generate_bucket_cuckoo_hash_index(unsigned int memory_size, unsigned int bucket_size){
        //todo implement
        return NULL;

    }
    unsigned int Table::find_empty_index(unsigned int bucket_index){
        //todo implement
        return 0;

    }
    unsigned int Table::absolute_index_to_bucket_index(unsigned int absolute_index){
        //todo implement
        return 0;
    }
    unsigned int Table::absolute_index_to_offset(unsigned int absolute_index){
        //todo implement
        return 0;
    }
    void Table::assert_operation_in_table_bound(unsigned int bucket_index, unsigned int offset, unsigned int memory_size){
        //todo implement
        return;
    }
    bool Table::contains_duplicates(){
        //todo implement
        return false;
    }
    unsigned int ** Table::get_duplicates(){
        //todo implement
        return NULL;
    }


}