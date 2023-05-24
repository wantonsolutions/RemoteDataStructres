import logging
from . import hash
logger = logging.getLogger('root')

TABLE_ENTRY_SIZE=8

class Lock:
    def __init__(self):
        self.lock_state = 0

    def lock(self):
        self.lock_state = 1

    def unlock(self):
        self.lock_state = 0

    def is_locked(self):
        return self.lock_state == 1

    def equals(self, other):
        return self.lock_state == other

    def set_bit(self, bit):
        self.lock_state = bit

    def __str__(self):
        if self.is_locked():
            return "L"
        else:
            return "U"

    def __repr__(self):
        return self.__str__()

class LockTable:
    def __init__(self, memory_size, entry_size, bucket_size, buckets_per_lock):
        self.memory_size = memory_size
        self.entry_size = entry_size
        self.bucket_size = bucket_size
        self.buckets_per_lock = buckets_per_lock
        rows = int(((memory_size/entry_size))/bucket_size)
        assert rows % buckets_per_lock == 0, "Number of buckets per lock must be a factor of the number of rows"
        self.total_locks = int(rows/buckets_per_lock)
        # print("Lock Table Locks:" + str(self.total_locks))
        self.locks = [Lock() for i in range(self.total_locks)]

    def unlock_all(self):
        for i in range(len(self.locks)):
            self.locks[i].unlock()


        # print(self)

    def set_lock(self, lock_index):
        if self.locks[lock_index].is_locked():
            return False
        self.locks[lock_index].lock()
        return True
    
    def unlock_lock(self, lock_index):
        if not self.locks[lock_index].is_locked():
            return False
        self.locks[lock_index].unlock()
        return True

    
    def multi_lock(self, lock_indexes):
        for i in lock_indexes:
            if self.locks[i].is_locked():
                return False
        for i in lock_indexes:
            self.locks[i].lock()
        return True

    def multi_unlock(self, lock_indexes):
        for i in lock_indexes:
            if not self.locks[i].is_locked():
                return False
        for i in lock_indexes:
            self.locks[i].unlock()
        return True


    def get_cas_range(self, starting_index):
        locks=[Lock()] * CAS_SIZE
        index = starting_index
        for i in range(CAS_SIZE):
            if index >= len(self.locks):
                break
            locks[i] = self.locks[index]
            index += 1
        return locks

    def fill_cas_range(self, starting_index, locks):
        index = starting_index
        for i in range(CAS_SIZE):
            if index >= len(self.locks):
                break
            locks[i] = self.locks[index]
            index += 1
        # return locks

    def __str__(self):
        s = ""
        for i in range(len(self.locks)):
            bottom_bucket = i*self.buckets_per_lock
            top_bucket = bottom_bucket + self.buckets_per_lock - 1
            s += str(bottom_bucket) + "-" + str(top_bucket) + ":" + str(self.locks[i]) + "\n"
        return s[:len(s)-1]
class Table:
    def __init__(self, memory_size, bucket_size, buckets_per_lock):
        # self.number_tables=2
        self.entry_size=8
        self.memory_size=memory_size
        self.bucket_size=bucket_size
        self.buckets_per_lock=buckets_per_lock
        self.table=self.generate_bucket_cuckoo_hash_index(memory_size, bucket_size)
        self.table_size = len(self.table)

        self.lock_table = LockTable(memory_size, self.entry_size, bucket_size, self.buckets_per_lock)
        self.fill = 0

    def unlock_all(self):
        self.lock_table.unlock_all()

    def print_table(self):
        for i in range(0, self.table_size):
            print('{0: <5}'.format(str(i)+")"), end='')
            for j in range(0, self.bucket_size):
                print("[" + '{0: <5}'.format(str(self.table[i][j])) + "] ", end='')
            print("")

    def get_bucket_size(self):
        return len(self.table[0]) * TABLE_ENTRY_SIZE
        
    def row_size_bytes(self):
        return self.n_buckets_size(self.bucket_size)

    def row_size_in_indexes(self):
        return len(self.table[0])

    def n_buckets_size(self, n_buckets):
        return n_buckets * TABLE_ENTRY_SIZE

    def get_entry(self, bucket, offset):
        return self.table[bucket][offset]

    def set_entry(self, bucket, offset, entry):
        #maintain fill counter
        old = self.table[bucket][offset]
        if old == None:
            self.fill += 1
        if entry == None:
            self.fill -=1
            # print("Fill: " + str(self.fill))
        self.table[bucket][offset] = entry


    def bucket_has_empty(self, bucket_index):
        return None in self.table[bucket_index]

    def get_first_empty_index(self, bucket_index):
        for i in range(len(self.table[bucket_index])):
            if self.table[bucket_index][i] == None:
                return i
        return len(self.table[bucket_index])


        

    def bucket_contains(self, bucket_index, value):
        return value in self.table[bucket_index]

    def contains(self, value):
        locations = hash.rcuckoo_hash_locations(value, self.table_size)
        for bucket_index in locations:
            if self.bucket_contains(bucket_index, value):
                return True
        return False

    def get_fill_percentage(self):
        # print("fill: ",self.fill)
        # print("table size: ", self.table_size)
        # print("bucket size: ", self.bucket_size)
        return float(self.fill)/float(self.table_size * self.bucket_size)

    def full(self):
        return self.fill == self.table_size * self.bucket_size


    def generate_bucket_cuckoo_hash_index(self, memory_size, bucket_size):

        total_rows = int(memory_size/bucket_size)/self.entry_size
        logger.debug("Memory Size: " + str(memory_size))
        logger.debug("Bucket Size: " + str(bucket_size))
        logger.debug("Total Rows: " + str(total_rows))

        #we must have more than one row. I'm not worried about
        #the pessimal case here, the point is to test hash
        #tables
        row_err = "We must have more than 1 row"
        assert total_rows > 1, row_err

        #for cuckoo hashing we have two tables, therefore memory size must be a power of two

        # table_div_error = "Memory must divide evenly across tables. Memory:" + str(memory_size) + " Tables:" + str(self.number_tables)
        # assert memory_size % self.number_tables == 0, table_div_error

        #We also need the number of buckets to fit into memory correctly
        bucket_div_error = "Memory must divide evenly across buckets. Memory:" + str(memory_size) + " Buckets:" + str(bucket_size)
        assert (memory_size) % bucket_size == 0, bucket_div_error

        #finally each entry in the bucket is 8 bytes
        entry_div_error = "Memory must divide evenly across entries. Memory:" + str(memory_size) + " Entries:" + str(self.entry_size)
        assert ((memory_size)/bucket_size % self.entry_size == 0), entry_div_error

        table = []
        rows = int((memory_size/self.entry_size)/bucket_size)

        logger.debug("Generating table with " + str(rows) + " rows and " + str(bucket_size) + " buckets per row")
        for i in range(rows):
            table.append([None]*bucket_size)
        return table


    def find_empty_index(self, bucket_index):
        bucket = self.table[bucket_index]
        empty_index = -1
        for i in range(len(bucket)):
            if bucket[i] == None:
                empty_index = i
                break
        return empty_index


    def absolute_index_to_bucket_index(self, index):
        bucket_offset = index % self.bucket_size
        bucket = int(index/self.bucket_size)
        return (bucket,bucket_offset)


    def assert_operation_in_table_bound(self, bucket_id, bucket_offset, size):
        self.assert_read_table_properties(size)
        total_indexs = size/8
        max_read=bucket_id * bucket_offset + total_indexs
        table_bound=self.table_size * self.bucket_size
        assert max_read <= table_bound, "read is out of bounds " + str(max_read) + " " + str(table_bound)

    def assert_read_table_properties(self, read_size):
        assert self.table != None, "table is not initalized"
        assert self.table[0] != None, "table must have dimensions"
        assert read_size % TABLE_ENTRY_SIZE == 0, "size must be a multiple of 8"

    #check if the table has any duplicates in it
    def contains_duplicates(self):
        check_dict = dict()
        for i in range(len(self.table)):
            for j in range(len(self.table[i])):
                if self.table[i][j] != None:
                    if self.table[i][j] in check_dict:
                        return True
                    else:
                        check_dict[self.table[i][j]] = 1

    def get_duplicates(self):
        check_dict = dict()
        duplicates = []
        for i in range(len(self.table)):
            for j in range(len(self.table[i])):
                if self.table[i][j] != None:
                    if self.table[i][j] in check_dict:
                        tup = (self.table[i][j], check_dict[self.table[i][j]], (i,j))
                        duplicates.append(tup)
                    else:
                        check_dict[self.table[i][j]] = (i,j)
        return duplicates