# cdef extern from "tables.cpp" namespace "cuckoo_tables":
#     pass

from libc cimport stdint
from libcpp cimport bool

cdef extern from "tables.h" namespace "cuckoo_tables":

    struct Key:
        stdint.uint8_t bytes[4]

    struct Value:
        stdint.uint8_t bytes[4]

    struct Entry:
        Key key
        Value value

    struct CasOperationReturn:
        bool success
        stdint.uint64_t original_value

    cdef cppclass Lock_Table:
        Lock_Table() except +
        Lock_Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock) except +
        void unlock_all()
        CasOperationReturn masked_cas(unsigned int index, stdint.uint64_t old, stdint.uint64_t new_value, stdint.uint64_t mask)
        void fill_masked_cas(unsigned int index, bool sucess, stdint.uint64_t new_value, stdint.uint64_t mask)

    cdef cppclass Table:
        Table() except +
        Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock) except +
        void unlock_all()
        void print_table()
        CasOperationReturn lock_table_masked_cas(unsigned int lock_index, stdint.uint64_t old, stdint.uint64_t new_value, stdint.uint64_t mask)
        CasOperationReturn fill_lock_table_masked_cas(unsigned int lock_index, bool success, stdint.uint64_t value, stdint.uint64_t mask)
        unsigned int get_buckets_per_row()
        unsigned int get_row_count()
        unsigned int get_bucket_size()
        unsigned int row_size_bytes()
        unsigned int n_buckets_size(unsigned int n_buckets)
        Entry get_entry(unsigned int bucket_index, unsigned int offset)
        void set_entry(unsigned int bucket_index, unsigned int offset, Entry entry)
        bool bucket_has_empty(unsigned int bucket_index)
        unsigned int get_first_empty_index(unsigned int bucket_index)
        bool bucket_contains(unsigned int bucket_index, Key key)
        float get_fill_percentage()
        bool full()
        Entry ** generate_bucket_cuckoo_hash_index(unsigned int memory_size, unsigned int bucket_size)
        unsigned int absolute_index_to_bucket_index(unsigned int absolute_index)
        unsigned int absolute_index_to_bucket_offset(unsigned int absolute_index)
        void assert_operation_in_table_bound(unsigned int bucket_index, unsigned int offset, unsigned int memory_size)
        bool contains_duplicates()
        unsigned int ** get_duplicates()

cdef class PyTable:
    cdef Table * c_table
