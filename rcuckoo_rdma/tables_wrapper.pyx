# distutils: language = c++
# from tables_wrapper cimport Table as t
# from tables_wrapper import Table as t
# cimport tables_wrapper_def as t
cimport tables_wrapper_def as t

cdef class Table:
    cdef t.Table c_table


    def __init__(self, unsigned int memory_size=-1, unsigned int bucket_size=-1, unsigned int buckets_per_lock=-1):
        if memory_size is not -1 and bucket_size is not -1 and buckets_per_lock is not -1:
            self.c_table = t.Table(memory_size, bucket_size, buckets_per_lock)
        else:
            self.c_table = t.Table()

    def n_buckets_size(self, unsigned int n_buckets):
        return self.c_table.n_buckets_size(n_buckets)