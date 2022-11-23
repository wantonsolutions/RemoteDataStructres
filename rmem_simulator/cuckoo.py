import hash

TABLE_ENTRY_SIZE = 8

def generate_bucket_cuckoo_hash_index(memory_size, bucket_size):
    number_tables=2
    entry_size=8
    #for cuckoo hashing we have two tables, therefore memory size must be a power of two
    assert(memory_size % number_tables == 0)
    #We also need the number of buckets to fit into memory correctly
    assert((memory_size/number_tables) % bucket_size == 0)
    #finally each entry in the bucket is 8 bytes
    assert(((memory_size/number_tables)/bucket_size % entry_size == 0))
    table = []
    rows = int((memory_size/entry_size)/bucket_size)
    for i in range(rows):
        table.append([None]*bucket_size)
    return table

def absolute_index_to_bucket_index(index, bucket_size):
    bucket_offset = index % bucket_size
    bucket = int(index/bucket_size)
    return (bucket,bucket_offset)

def cas_table_entry(table, bucket_id, bucket_offset, old, new):
    v = table[bucket_id][bucket_offset]
    if v == old:
        table[bucket_id][bucket_offset] = new
        return True
    else:
        return False

def assert_read_table_properties(table, read_size):
    assert table != None, "table is not initalized"
    assert table[0] != None, "table must have dimensions"
    assert read_size % TABLE_ENTRY_SIZE == 0, "size must be a multiple of 8"

def assert_operation_in_table_bound(table, bucket_id, bucket_offset, size):
    assert_read_table_properties(table, size)

    bucket_size = len(table[0])
    total_indexs = size/8
    total_buckets = len(table)
    assert bucket_id * bucket_offset + total_indexs <= total_buckets * bucket_size, "operation is outside of the table"

def read_table_entry(table, bucket_id, bucket_offset, size):

    assert_operation_in_table_bound(table, bucket_id, bucket_offset, size)

    bucket_size = len(table[0])
    total_indexs = size/TABLE_ENTRY_SIZE

    #collect the read
    read = []
    base = bucket_id * bucket_size + bucket_offset
    for i in range(total_indexs):
        bucket, offset = absolute_index_to_bucket_index(base + i, bucket_size)
        read.append(table[bucket][offset])
    return read

def fill_table_with_read(table, bucket_id, bucket_offset, size, read):
    assert_operation_in_table_bound(table, bucket_id, bucket_offset, size)

    bucket_size = len(table[0])
    total_indexs = len(read)/TABLE_ENTRY_SIZE

    #write read to the table
    base = bucket_id * bucket_size + bucket_offset
    for i in range(total_indexs):
        bucket, offset = absolute_index_to_bucket_index(base + i, bucket_size)
        table[bucket][offset] = read[i]


def basic_insert(value):

    