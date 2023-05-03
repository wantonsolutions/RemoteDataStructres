import hashlib
import logging
import xxhash
logger = logging.getLogger('root')

DEFAULT_FACTOR=2.3
factor=DEFAULT_FACTOR

def set_factor(f):
    global factor
    factor=f

def get_factor():
    global factor
    return factor

def h1(key):
    key = str(key)
    return hashlib.md5(str(key).encode('utf-8')).hexdigest()

def h2(key):
    val = str(key)
    return hashlib.sha1(val.encode('utf-8')).hexdigest()

def h3(key):
    val = str(key) + "salty"
    return hashlib.sha1(val.encode('utf-8')).hexdigest()

def xxh1(key):
    return xxhash.xxh64(str(key)).hexdigest()

def xxh2(key):
    return xxhash.xxh64(str(key) + "salty").hexdigest()

#cuckoo specific hashing
def rcuckoo_primary_location(key, table_size):
    h = int(h1(key),16)
    h = (h % int(table_size / 2)) * 2
    return h

def get_table_id_from_index(index):
    return index % 2

def to_base(n, base):
    if n == 0:
        return '0'
    nums = []
    while n:
        n, r = divmod(n, base)
        nums.append(str(r))
    return ''.join(reversed(nums))

def h3_suffix_base(key, base):
    val = int(h3(key),16)
    val = to_base(val,base)
    zeros = len(val) - len(val.rstrip('0'))
    return zeros

def rcuckoo_secondary_location(key, factor, table_size):
    base = 2
    primary = rcuckoo_primary_location(key, table_size)
    exp = (h3_suffix_base(key,base) + factor) #use base 2 for key probability
    mod_size = int((factor ** exp)) #generated max suffix size
    secondary = (int(h2(key),16)) % mod_size #perform the modulo
    if secondary % 2 == 0: #make sure that the secondary location is odd
        secondary+=1

    return (primary + secondary) % table_size

def rcuckoo_secondary_location_independent(key, factor, table_size):
    h = int(h2(key),16)
    h = (h % int(table_size / 2)) * 2 + 1
    return h

def rcuckoo_hash_locations(key, table_size):
    global factor
    p = rcuckoo_primary_location(key, table_size)
    s = rcuckoo_secondary_location(key, factor, table_size)
    return (p,s)

def rcuckoo_hash_locations_independent(key, table_size):
    global factor
    p = rcuckoo_primary_location(key, table_size)
    s = rcuckoo_secondary_location_independent(key, factor, table_size)
    return (p,s)



## race

def to_race_index_math(index, table_size):
    if table_size % 3 != 0:
        table_size -= (table_size % 3) 

    two_thirds_table_size = int(int(table_size * 2)/3)
    index = (index % two_thirds_table_size)
    index_div_two = int(index / 2)

    # print("index: ", index)

    #even
    bucket=0
    if index % 2 == 0:
        bucket = index + index_div_two
        overflow = bucket+1
    else:
        bucket += index + (index_div_two + 1)
        overflow = bucket-1
    return (bucket, overflow)


def to_race_index(h, index, table_size):
    index = int(h(index),16)
    return to_race_index_math(index, table_size)


def race_primary_location(key, table_size):
    return to_race_index(xxh1, key, table_size)

def race_secondary_location(key, table_size):
    return to_race_index(xxh2, key, table_size)

def race_hash_locations(key, table_size):
    p = race_primary_location(key, table_size)
    s = race_secondary_location(key, table_size)
    return (p,s)

