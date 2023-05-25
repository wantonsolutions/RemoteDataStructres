#include "tables.h"

namespace cuckoo_tables {

    Table::Table(){
        _entry_size = 0;
        _memory_size = 0;
        _bucket_size = 0;
        _buckets_per_lock = 0;
        _fill = 0;
    }

    Table::Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock){
        _entry_size =8;
        _memory_size = memory_size;
        _bucket_size = bucket_size;
        _buckets_per_lock = buckets_per_lock;
        _fill = 0;
    }

    unsigned int Table::n_buckets_size(unsigned int n_buckets) {
        return _entry_size * n_buckets;
    }

}