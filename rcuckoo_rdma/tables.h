#pragma once
#ifndef TABLES_H
#define TABLES_H

namespace cuckoo_tables {
    class Table {
        public:
            // **Entry table;
            // LockTable lock_table;
            Table();
            Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock);
            unsigned int n_buckets_size(unsigned int n_buckets);

        private:
            unsigned int _entry_size;
            unsigned int _memory_size;
            unsigned int _bucket_size;
            unsigned int _buckets_per_lock;
            unsigned int _fill;
    };
}

#endif