#include "tables.h"
#include <cstddef>
#include <cassert>
#include <string>
#include <iostream>
// #include "spdlog/spdlog.h" //sudo apt install libspdlog-dev

namespace cuckoo_tables {

    using namespace std;

    string Key::to_string(){
        string s = "";
        for (int i = 0; i < KEY_SIZE; i++){
            s += std::to_string(bytes[i]);
        }
        return s;
    }

    bool Key::is_empty(){
        for (int i = 0; i < KEY_SIZE; i++){
            if (bytes[i] != 0){
                return false;
            }
        }
        return true;
    }

    string Value::to_string(){
        string s = "";
        for (int i = 0; i < VALUE_SIZE; i++){
            s += std::to_string(bytes[i]);
        }
        return s;
    }

    bool Value::is_empty(){
        for (int i = 0; i < VALUE_SIZE; i++){
            if (bytes[i] != 0){
                return false;
            }
        }
        return true;
    }

    string Entry::to_string(){
        return key.to_string() + ":" + value.to_string();
    }

    bool Entry::is_empty(){
        return key.is_empty() && value.is_empty();
    }

    /*lock table functions*/
    Lock_Table::Lock_Table(){
        _total_locks = 0;
        _locks = NULL;

    }
    Lock_Table::Lock_Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock){
        assert(memory_size % sizeof(Entry) == 0 );
        unsigned int row_size = memory_size / sizeof(Entry);
        assert(row_size % bucket_size == 0);
        unsigned int table_rows = row_size / bucket_size;
        assert(table_rows % buckets_per_lock == 0);
        _total_locks = table_rows / buckets_per_lock;
        _locks = new uint8_t[_total_locks];
        this->unlock_all();

    }

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
        *va = (*va & ~(mask)) | (new_value & mask);
        if (!success) {
            cout << "fill_masked_cas failed" << endl;
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
        _memory_size = 0;
        _bucket_size = 0;
        _buckets_per_lock = 0;
        _fill = 0;
        _table_size = 0;
        _table = NULL;
        _lock_table = Lock_Table();
    }

    Table::Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock){
        assert(memory_size > 0);
        assert(bucket_size > 0);
        assert(memory_size >= bucket_size);
        assert(memory_size >= bucket_size * sizeof(Entry));
        assert(memory_size % sizeof(Entry) == 0);

        _memory_size = memory_size;
        _bucket_size = bucket_size;
        _buckets_per_lock = buckets_per_lock;
        _fill = 0;
        unsigned int total_entries = memory_size / sizeof(Entry);
        assert(total_entries % bucket_size == 0);
        _table_size = int(memory_size / bucket_size) / sizeof(Entry);
        _table = this->generate_bucket_cuckoo_hash_index(memory_size, bucket_size);
        _lock_table = Lock_Table(memory_size, bucket_size, buckets_per_lock);

    }

    void Table::unlock_all(){
        _lock_table.unlock_all();
        return;
    }

    void Table::print_table(){
        cout << "Table::print_table" << endl;
        for (unsigned int i = 0; i < _table_size; i++){
            cout << i << ") ";
            for (unsigned int j = 0; j < _bucket_size; j++){
                cout << "[" << _table[i][j].to_string() << "]";
            }
            cout << endl;
        }
        cout << "Lock Table" << endl;
        cout << _lock_table.to_string() << endl;
        return;
    }

    CasOperationReturn Table::lock_table_masked_cas(unsigned int lock_index, uint64_t old, uint64_t new_value, uint64_t mask){
        return _lock_table.masked_cas(lock_index, old, new_value, mask);
    }

    void Table::fill_lock_table_masked_cas(unsigned int lock_index, bool success, uint64_t value, uint64_t mask){
        
        _lock_table.fill_masked_cas(lock_index, success, value, mask);
    }
    unsigned int Table::get_row_count(){
        return _table_size;
    }

    unsigned int Table::get_buckets_per_row(){
        return _bucket_size;
    }

    unsigned int Table::get_bucket_size(){
        return _bucket_size * sizeof(Entry);
    }

    unsigned int Table::row_size_bytes(){
        return this->n_buckets_size(_bucket_size);
    }

    unsigned int Table::n_buckets_size(unsigned int n_buckets) {
        return sizeof(Entry) * n_buckets;
    }

    Entry Table::get_entry(unsigned int bucket_index, unsigned int offset){
        return _table[bucket_index][offset];
    }

    void Table::set_entry(unsigned int bucket_index, unsigned int offset, Entry entry){
        Entry old = _table[bucket_index][offset];
        _table[bucket_index][offset] = entry;
        if (old.is_empty()){
            _fill++;
        }

        if (entry.is_empty()){
            _fill--;
        }
    }

    bool Table::bucket_has_empty(unsigned int bucket_index){
        bool has_empty = false;
        for (unsigned int i = 0; i < _bucket_size; i++){
            if (_table[bucket_index][i].is_empty()){
                has_empty = true;
                break;
            }
        }
        return has_empty;
    }

    unsigned int Table::get_first_empty_index(unsigned int bucket_index){
        unsigned int empty_index = -1;
        for (unsigned int i = 0; i < _bucket_size; i++){
            if (_table[bucket_index][i].is_empty()){
                empty_index = i;
                break;
            }
        }
        return empty_index;

    }

    bool Table::bucket_contains(unsigned int bucket_index, Key key){
        bool contains = false;
        for (unsigned int i = 0; i < _bucket_size; i++){
            if (_table[bucket_index][i].key == key){
                contains = true;
                break;
            }
        }
        return contains;

    }

    float Table::get_fill_percentage(){
        unsigned int max_fill = _table_size * _bucket_size;
        unsigned int current_fill = 0;
        for (unsigned int i = 0; i < _table_size; i++){
            for (unsigned int j = 0; j < _bucket_size; j++){
                if (!_table[i][j].is_empty()){
                    current_fill++;
                }
            }
        } 
        return float(current_fill) / float(max_fill);
    }

    bool Table::full(){
        return _fill == _table_size * _bucket_size;
    }

    Entry ** Table::generate_bucket_cuckoo_hash_index(unsigned int memory_size, unsigned int bucket_size){
        cout << "Table::generate_bucket_cuckoo_hash_index" << endl;
        //Sanity checking asserts
        assert(memory_size > 0);
        assert(bucket_size > 0);
        assert(memory_size >= bucket_size);
        assert(memory_size >= bucket_size * sizeof(Entry));
        assert(memory_size % sizeof(Entry) == 0);
        unsigned int total_entries = memory_size / sizeof(Entry);
        assert(total_entries % bucket_size == 0);
        unsigned int total_rows = int(memory_size / bucket_size) / sizeof(Entry);
        cout << "memory_size: " << memory_size << endl;
        cout << "bucket_size: " << bucket_size << endl;
        cout << "total_entries: " << total_entries << endl;

        //Allocate memory for the table
        unsigned int nrows = total_rows;
        unsigned int ncols = bucket_size;
        Entry* pool = NULL;

        try {
            Entry ** ptr = new Entry*[nrows];
            pool = new Entry[nrows * ncols]{Entry()};

            for (unsigned i = 0; i < nrows; ++i, pool += ncols ){
                ptr[i] = pool;
            }
            return ptr;
        } catch (std::bad_alloc& ba) {
            std::cerr << "bad_alloc caught: " << ba.what() << '\n';
            return NULL;
        }
        return NULL;

    }
    unsigned int Table::absolute_index_to_bucket_index(unsigned int absolute_index){
        return absolute_index / _bucket_size;
    }
    unsigned int Table::absolute_index_to_offset(unsigned int absolute_index){
        return absolute_index % _bucket_size;
    }
    void Table::assert_operation_in_table_bound(unsigned int bucket_index, unsigned int offset, unsigned int read_size){
        unsigned int total_indexes = read_size / sizeof(Entry);
        unsigned int max_read = bucket_index * offset + total_indexes;
        unsigned int table_bound = _table_size * _bucket_size;
        assert (max_read <= table_bound);
        return;
    }
    bool Table::contains_duplicates(){
        cout << "Table::contains_duplicates NOT IMPLEMENTED" << endl;
        //todo implement
        return true;
    }
    unsigned int ** Table::get_duplicates(){
        cout << "Table::get_duplicates NOT IMPLEMENTED" << endl;
        //todo implement
        return NULL;
    }
}