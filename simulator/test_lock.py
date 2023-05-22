from cuckoo import *
#redefinition of CAS size for testing purposes
# CAS_SIZE=4 

#TODO start here after lunch we are generating the actual messages

def blank_known():
    old, new, mask = [0]*CAS_SIZE, [0]*CAS_SIZE, [0]*CAS_SIZE
    return old, new, mask

def one_lock_per_bucket_small():
    locks_per_bucket = 1
    buckets = [0, 1, 2, 3]
    known_locks = [0, 1, 2, 3]
    locks = get_lock_index(buckets, locks_per_bucket)
    assert locks == known_locks
    print("one lock per bucket (buckets, correct, output)",buckets,known_locks,locks)

def two_lock_per_bucket_small():
    locks_per_bucket = 2
    buckets = [0, 1, 2, 3]
    known_locks = [0, 1]
    locks = get_lock_index(buckets, locks_per_bucket)
    assert locks == known_locks
    print("two lock per bucket (buckets, correct, output)",buckets,known_locks,locks)

def three_lock_per_bucket_small():
    locks_per_bucket = 3
    buckets = [0, 1, 2, 3]
    known_locks = [0, 1]
    locks = get_lock_index(buckets, locks_per_bucket)
    assert locks == known_locks
    print("three lock per bucket (buckets, correct, output)",buckets,known_locks,locks)

def four_lock_per_bucket_small():
    locks_per_bucket = 4
    buckets = [0, 1, 2, 3]
    known_locks = [0]
    locks = get_lock_index(buckets, locks_per_bucket)
    assert locks == known_locks
    print("four lock per bucket (buckets, correct, output)",buckets,known_locks,locks)


def seperate_locks_large():
    locks_per_bucket = 4
    buckets = [0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15]
    known_locks = [0, 2, 3]
    locks = get_lock_index(buckets, locks_per_bucket)
    assert locks == known_locks
    print("four lock per bucket (buckets, correct, output)",buckets,known_locks,locks)

def seperate_locks_large_2():
    locks_per_bucket = 4
    buckets = [0, 1, 2, 3, 8, 9, 10, 11, 13]
    known_locks = [0, 2, 3]
    locks = get_lock_index(buckets, locks_per_bucket)
    assert locks == known_locks
    print("four lock per bucket gapped (buckets, correct, output)",buckets,known_locks,locks)

def test_lock_index():
    one_lock_per_bucket_small()
    two_lock_per_bucket_small()
    three_lock_per_bucket_small()
    four_lock_per_bucket_small()
    seperate_locks_large()
    seperate_locks_large_2()

def lock_to_bits_test():
    test_lock_array_to_bits()
    test_lock_array_to_bits_gaps()

def test_lock_array_to_bits():
    lock_array = [0, 1, 2, 3]
    old, new, mask = blank_known()
    new[0], new[1], new[2], new[3] = 1, 1, 1, 1
    mask[0], mask[1], mask[2], mask[3] = 1, 1, 1, 1

    known = (0, old, new, mask)

    output = lock_array_to_bits(lock_array)
    print(known, output)
    assert known == output, "lock array to bits failed"
    print("lock array to bits (correct, output)",known,output)

def test_lock_array_to_bits_gaps():
    lock_array = [0, 2]
    old, new, mask = blank_known()
    new[0], new[2] = 1, 1
    mask[0], mask[2] = 1, 1
    known = (0, old, new, mask)
    output = lock_array_to_bits(lock_array)
    print(known, output)
    assert known == output, "lock array to bits failed"
    print("lock array to bits (correct, output)",known,output)

def test_break_list_to_chunks():
    locks = [0, 6, 25, 36, 100, 101] 
    locks_per_message = 2
    known = [[0, 6], [25, 36], [100, 101]]
    output = break_list_to_chunks(locks, locks_per_message)
    assert known == output, "break list to chunks failed"
    print("break list to chunks (correct, output)",known,output)

def test_get_locks_messages():
    buckets = [0, 1, 2, 3, 8, 9, 10, 11, 13]
    lock_index = [0, 2, 3]
    locks_per_bucket = 4
    locks_per_message = 2
    old_1, new_1, mask_1 = blank_known()
    new_1[0], new_1[2] = 1, 1
    mask_1[0], mask_1[2] = 1, 1

    old_2, new_2, mask_2 = blank_known()
    new_2[0]=1
    mask_2[0]=1


    known = [(0, old_1, new_1, mask_1,), (3, old_2, new_2, mask_2)]
    output = get_lock_messages(buckets, locks_per_bucket, locks_per_message)
    assert known == output, "get locks messages failed" + "\n" + str(known) + "\n" + str(output)
    print("get locks messages (correct, output)",known,output)

#uncomment to test
# test_lock_index()
# lock_to_bits_test()
# test_break_list_to_chunks()
# test_get_locks_messages()
