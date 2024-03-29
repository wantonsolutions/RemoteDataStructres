import logging
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

CAS_SIZE = 64
inverted_mask_index_global = [0] * CAS_SIZE

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


    def masked_cas(self, lock_index, old, new, mask):
        #create an inverted index of the mask, to reduce the indexes we needto check.
        #i've pre

        #inflate the lock index, here we work per bit, but the lock index indexes the byte
        lock_index *=8

        global inverted_mask_index_global
        c=0
        for i in range(len(mask)):
            if mask[i] == 1:
                inverted_mask_index_global[c]=i
                c+=1

        inverted_mask_index = inverted_mask_index_global[0:c]
        #XOR check that the old value in the cas matches the existing lock table.
        for v in inverted_mask_index:
            if not self.locks[lock_index+v].equals(old[v]):
                #logger.critical("FAILED CAS (Client 0)" + str(self))
                return (False, old)
        
        #now we just apply. At this point we know that the old value is the same as the current so we can lock and unlock accordingly.
        for v in inverted_mask_index:
            self.locks[lock_index+v].set_bit(new[v])
        #logger.critical("SUCCESS CAS (Client 0)" + str(self))
        return (True, new)
    
    def fill_masked_cas(self, lock_index, success, new, mask):

        #inflate the lock index, here we work per bit, but the lock index indexes the byte
        lock_index *=8
        index = lock_index
        for v, m in zip(new, mask):
            if index >= len(self.locks):
                break
            #If this value is part of the mask, set it to the cas value
            if m == True:
                self.locks[index] = v
            index += 1

        if not success:
            logger.warning("returned value is not the same as the value in the table, inserted it anyways")


    def __str__(self):
        s = ""
        for i in range(len(self.locks)):
            s += "(" + str(i) + ":" + str(self.locks[i]) + ")"
        return s
        # for i in range(len(self.locks)):
        #     bottom_bucket = i*self.buckets_per_lock
        #     top_bucket = bottom_bucket + self.buckets_per_lock - 1
        #     s += str(bottom_bucket) + "-" + str(top_bucket) + ":" + str(self.locks[i]) + "\n"
        # return s[:len(s)-1]

        
class Table:
    def __init__(self, memory_size, bucket_size, buckets_per_lock):
        # self.number_tables=2
        self.entry_size=8
        self.memory_size=memory_size
        self.bucket_size=bucket_size
        self.buckets_per_lock=buckets_per_lock
        self.table=self.generate_bucket_cuckoo_hash_index(memory_size, bucket_size)

        self.lock_table = LockTable(memory_size, self.entry_size, bucket_size, self.buckets_per_lock)
        self.fill = 0

    def unlock_all(self):
        self.lock_table.unlock_all()

    def print_table(self):
        for i in range(0, self.get_row_count()):
            print('{0: <5}'.format(str(i)+")"), end='')
            for j in range(0, self.bucket_size):
                print("[" + '{0: <5}'.format(str(self.table[i][j])) + "] ", end='')
            print("")

    def lock_table_masked_cas(self, lock_index, old, new, mask):
        assert len(old) == CAS_SIZE, "old must be 64 bytes"
        assert len(old) == len(new), "old and new must be the same length"
        assert len(new) == len(mask), "new and mask must be the same length"

        assert self.lock_table != None, "lock table is not initalized"
        assert lock_index < self.lock_table.total_locks, "lock index is out of bounds"
        return self.lock_table.masked_cas(lock_index, old, new, mask)

    def fill_lock_table_masked_cas(self, lock_index, success, value, mask):
        assert len(value) == CAS_SIZE, "value must be 64 bytes"
        assert len(value) == len(mask), "value and mask must be the same length"

        assert self.lock_table != None, "lock table is not initalized"
        assert lock_index < len(self.lock_table), "lock index is out of bounds"

        return self.lock_table.fill_masked_cas(lock_index, success, value, mask)


    def get_bucket_size(self):
        return len(self.table[0]) * TABLE_ENTRY_SIZE
        
    def row_size_bytes(self):
        return self.n_buckets_size(self.bucket_size)

    def get_buckets_per_row(self):
        return self.bucket_size

    def get_row_count(self):
        return len(self.table)

    def n_buckets_size(self, n_buckets):
        return n_buckets * TABLE_ENTRY_SIZE

    def get_entry(self, bucket_index, offset):
        return self.table[bucket_index][offset]

    def set_entry(self, bucket_index, offset, entry):
        #maintain fill counter
        old = self.table[bucket_index][offset]
        if old == None:
            self.fill += 1
        if entry == None:
            self.fill -=1
            # print("Fill: " + str(self.fill))
        self.table[bucket_index][offset] = entry


    def bucket_has_empty(self, bucket_index):
        assert bucket_index < self.get_row_count(), "Bucket index out of range for table index: " + str(bucket_index) + " :" +str(self.table.get_row_count())
        return None in self.table[bucket_index]

    def get_first_empty_index(self, bucket_index):
        for i in range(len(self.table[bucket_index])):
            if self.table[bucket_index][i] == None:
                return i
        return -1
        

    def bucket_contains(self, bucket_index, value):
        return value in self.table[bucket_index]

    def contains(self, value):
        for i in range(len(self.table)):
            if self.bucket_contains(i, value):
                return True
        return False

    def get_fill_percentage(self):
        return float(self.fill)/float(self.get_row_count() * self.bucket_size)

    def full(self):
        return self.fill == self.get_row_count() * self.bucket_size


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


    def absolute_index_to_bucket_index(self, index):
        bucket = int(index/self.bucket_size)
        return bucket

    def absolute_index_to_bucket_offset(self, index):
        bucket_offset = index % self.bucket_size
        return bucket_offset


    def assert_operation_in_table_bound(self, bucket_id, bucket_offset, size):
        # self.assert_read_table_properties(size)
        total_indexs = size/8
        max_read=bucket_id * bucket_offset + total_indexs
        table_bound=self.get_row_count() * self.bucket_size
        assert max_read <= table_bound, "read is out of bounds " + str(max_read) + " " + str(table_bound)

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