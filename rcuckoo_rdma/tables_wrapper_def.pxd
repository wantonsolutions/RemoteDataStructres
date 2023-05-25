cdef extern from "tables.cpp":
    pass

cdef extern from "tables.h" namespace "cuckoo_tables":
    cdef cppclass Table:
        Table() except +
        Table(unsigned int memory_size, unsigned int bucket_size, unsigned int buckets_per_lock) except +
        # unsigned int n_buckets_size(unsigned int n_buckets)