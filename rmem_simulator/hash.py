import hashlib
import logging
logger = logging.getLogger('root')

def h1(key):
    return hashlib.md5(str(key).encode('utf-8')).hexdigest()

def h2(key):
    val = str(key) + "salt"
    return hashlib.sha1(val.encode('utf-8')).hexdigest()

def h3(key):
    val = str(key) + "salty"
    return hashlib.sha1(val.encode('utf-8')).hexdigest()

def primary_location(key, table_size):
    hash = int(h1(key),32)
    #print("hash: " + str(hash))
    #print("table_size: " + str(table_size))
    mhash = (hash % int(table_size/2)) * 2
    #print(mhash)
    return mhash

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
    val = int(h3(key),32)
    val = to_base(val,base)
    zeros = len(val) - len(val.rstrip('0'))
    return zeros

def secondary_location(key, factor, table_size):
    base = 2
    primary = primary_location(key, table_size)
    exp = (h3_suffix_base(key,base) + factor) #use base 2 for key probability
    mod_size = int((factor ** exp)) #generated max suffix size
    secondary = (int(h2(key),32)) % mod_size #perform the modulo
    secondary = secondary * 2 + 1 #make sure that the secondary location is odd

    return (primary + secondary) % table_size

def hash_locations(key, table_size):
    factor=2.0
    p = primary_location(key, table_size)
    s = secondary_location(key, factor, table_size)
    if p > s:
        logger.debug("primary is greater than secondary: " + str(p) + " " + str(s))
    return (primary_location(key, table_size), secondary_location(key, factor, table_size))
