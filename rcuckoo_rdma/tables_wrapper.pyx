# distutils: language = c++
# from tables_wrapper cimport Table as t
# from tables_wrapper import Table as t
# cimport tables_wrapper_def as t
cimport tables_wrapper_def as t
from libcpp cimport bool


cdef class Lock_Table:
    cdef t.Lock_Table c_lock_table

    def __init__(self, unsigned int memory_size=-1, unsigned int bucket_size=-1, unsigned int buckets_per_lock=-1):
        if memory_size is not -1 and bucket_size is not -1 and buckets_per_lock is not -1:
            self.c_lock_table = t.Lock_Table(memory_size, bucket_size, buckets_per_lock)
        else:
            self.c_lock_table = t.Lock_Table()

    def unlock_all(self):
        return self.c_lock_table.unlock_all()

    def masked_cas(self, unsigned int index, old, new_value, mask):
        #todo: these are getting called with arrays convert them to uin64_t
        response = self.c_lock_table.masked_cas(index, old, new_value, mask)
        return (response.success, response.new_value)

    def fill_masked_cas(self, unsigned int index, bool success, new_value, mask):
        #todo: these are getting called with arrays convert them to uin64_t
        self.c_lock_table.fill_masked_cas(index, success, new_value, mask)
        return

cdef class Table:
    cdef t.Table c_table

    def __init__(self, unsigned int memory_size=-1, unsigned int bucket_size=-1, unsigned int buckets_per_lock=-1):
        if memory_size is not -1 and bucket_size is not -1 and buckets_per_lock is not -1:
            self.c_table = t.Table(memory_size, bucket_size, buckets_per_lock)
        else:
            self.c_table = t.Table()

    def unlock_all(self):
        return self.c_table.unlock_all()

    def print_table(self):
        return self.c_table.print_table()

    def lock_table_masked_cas(self, unsigned int lock_index, old, new_value, mask):
        return self.c_table.lock_table_masked_cas(lock_index, old, new_value, mask)


    def fill_lock_table_masked_cas(self, unsigned int lock_index, bool success, value, mask):
        return self.c_table.fill_lock_table_masked_cas(lock_index, success, value, mask)

    def get_buckets_per_row(self):
        return self.c_table.get_buckets_per_row()

    def get_row_count(self):
        return self.c_table.get_row_count()

    def get_bucket_size(self):
        return self.c_table.get_bucket_size()

    def row_size_bytes(self):
        return self.c_table.row_size_bytes()

    def row_size_indexes(self):
        return self.c_table.row_size_indexes()

    def n_buckets_size(self, unsigned int n_buckets):
        return self.c_table.n_buckets_size(n_buckets)

    def get_entry(self, unsigned int bucket_index, unsigned int offset):
        e = self.c_table.get_entry(bucket_index, offset)
        return e.key

    def set_entry(self, unsigned int bucket_index, unsigned int offset, entry):
        #todo wrap the entry type
        print("WARNING WARNING WARNING set_entry")
        # c_entry = Entry(entry, 0)
        c_entry = entry
        self.c_table.set_entry(bucket_index, offset, c_entry)

    
    def bucket_has_empty(self, unsigned int bucket_index):
        return self.c_table.bucket_has_empty(bucket_index)

    def get_first_empty_index(self, unsigned int bucket_index):
        return self.c_table.get_first_empty_index(bucket_index)

    def bucket_contains(self, unsigned int bucket_index, unsigned int key):
        return self.c_table.bucket_contains(bucket_index, key)

    def contains(self, unsigned int key):
        return self.c_table.contains(key)

    def get_fill_percentage(self):
        return self.c_table.get_fill_percentage()

    def full(self):
        return self.c_table.full()

    def generate_bucket_cuckoo_hash_index(self, unsigned int memory_size, unsigned int bucket_size):
        #todo: this is a crazy pointer thing watch out
        print("WARTING WARNTING WARING generate_bucket_cuckoo_hash_index not implemented")
        return None
        # return self.c_table.generate_bucket_cuckoo_hash_index(memory_size, bucket_size)

    def find_empty_index(self, unsigned int bucket_index):
        return self.c_table.find_empty_index(bucket_index)
    
    def absolute_index_to_bucket_index(self, unsigned int absolute_index):
        return self.c_table.absolute_index_to_bucket_index(absolute_index)
    
    def absolute_index_to_offset(self, unsigned int absolute_index):
        return self.c_table.absolute_index_to_offset(absolute_index)
    
    def assert_operation_in_table_bound(self, unsigned int bucket_index, unsigned int offset, unsigned int memory_size):
        return self.c_table.assert_operation_in_table_bound(bucket_index, offset, memory_size)

    def contains_duplicates(self):
        return self.c_table.contains_duplicates()

    def get_duplicates(self):
        print("WARTING WARNTING WARING get_duplicates not implemented")
        # duplicates = self.c_table.get_duplicates()
        # #todo do something with the duplicates
        # print(duplicates)
        return None