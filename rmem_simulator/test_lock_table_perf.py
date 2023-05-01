from cuckoo import *
from test_lock import blank_known


memory_size = 1024
entry_size = 8
bucket_size = 8
buckets_per_lock = 1
lock_table = LockTable(memory_size, entry_size, bucket_size, buckets_per_lock)

#test_size

old, new, mask = blank_known()

new[0] = 1
mask[0] = 1

old_u = new
new_u = old
index = 0

TEST_SIZE=100000

for i in range(0, TEST_SIZE):
    masked_cas_lock_table(lock_table, index, old, new, mask)
    masked_cas_lock_table(lock_table, index, old_u, new_u, mask)