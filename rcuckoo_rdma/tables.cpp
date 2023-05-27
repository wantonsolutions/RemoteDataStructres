#include "tables.h"
#include <cstddef>
#include <cassert>
#include <string>
#include <iostream>
// #include "spdlog/spdlog.h" //sudo apt install libspdlog-dev

namespace cuckoo_tables {

    using namespace std;

    /*lock table functions*/
    Lock_Table::Lock_Table(){
        _total_locks = 0;
        _locks = NULL;

    }
    Lock_Table::Lock_Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock){
        cout << "Lock_Table::Lock_Table" << memory_size << " " << bucket_size << " " << buckets_per_lock;
        assert(memory_size % sizeof(Entry) == 0 );
        unsigned int row_size = memory_size / sizeof(Entry);
        assert(row_size % bucket_size == 0);
        unsigned int table_rows = row_size / bucket_size;
        assert(table_rows % buckets_per_lock == 0);
        _total_locks = table_rows / buckets_per_lock;
        _locks = new uint8_t[_total_locks];
        this->unlock_all();

    }

    // Lock_Table::~Lock_Table(){
    //     delete[] _locks;
    // }
    void Lock_Table::unlock_all(){
        for (unsigned int i = 0; i < _total_locks; i++){
            _locks[i] = 0;
        }
    }
    CasOperationReturn Lock_Table::masked_cas(unsigned int index, uint64_t old, uint64_t new_value, uint64_t mask){

        uint64_t *va = (uint64_t *) &_locks[index];
        CasOperationReturn atomic_response = CasOperationReturn();
        atomic_response.original_value = *va;
        if (!((old ^ *va) & mask)) {
            *va = (*va & ~(mask)) | (new_value & mask);
        }
        atomic_response.success = (atomic_response.original_value == *va);
        return atomic_response;
    }

    void Lock_Table::fill_masked_cas(unsigned int index, bool success, uint64_t new_value, uint64_t mask){
        //todo implement
        uint64_t *va = (uint64_t *) &_locks[index];
        // if (success) {
        *va = (*va & ~(mask)) | (new_value & mask);
        // }
        if (!success) {
            // printf("fill_masked_cas failed\n");
        }
    }

    string Lock_Table::to_string(){
        string output_string = "";
        for (unsigned int i = 0; i < _total_locks; i++){
            output_string += std::to_string(i) + ": " + std::to_string(_locks[i]) + "\n";
        }
        return output_string;
    }


    Table::Table(){
        //todo implement
        _bucket_size = 0;
        _buckets_per_lock = 0;
        _fill = 0;
    }

    Table::Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock){
        //todo implement
        _memory_size = memory_size;
        _bucket_size = bucket_size;
        _buckets_per_lock = buckets_per_lock;
        _fill = 0;
        _lock_table = Lock_Table(memory_size, bucket_size, buckets_per_lock);

    }
    // Table::~Table(){
    //     //todo implement
    // }


    void Table::unlock_all(){
        //todo implement
        return;
    }
    void Table::print_table(){
        cout << "Table::print_table" << endl;
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
    unsigned int Table::get_row_count(){
        //todo implement
        return 0;
    }
    unsigned int Table::get_buckets_per_row(){
        //todo implement
        return 0;
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
        return sizeof(Entry) * n_buckets;
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