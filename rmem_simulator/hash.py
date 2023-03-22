import hashlib
import logging
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

def primary_location(key, table_size):
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

def secondary_location(key, factor, table_size):
    base = 2
    primary = primary_location(key, table_size)
    exp = (h3_suffix_base(key,base) + factor) #use base 2 for key probability
    mod_size = int((factor ** exp)) #generated max suffix size
    secondary = (int(h2(key),16)) % mod_size #perform the modulo
    if secondary % 2 == 0: #make sure that the secondary location is odd
        secondary+=1
    # secondary = secondary * 2 + 1 #make sure that the secondary location is odd

    return (primary + secondary) % table_size

def hash_locations(key, table_size):
    global factor
    p = primary_location(key, table_size)
    s = secondary_location(key, factor, table_size)
    # print(p,s)
    if p > s:
        logger.debug("primary is greater than secondary: " + str(p) + " " + str(s))
    return (p,s)
    # return (primary_location(key, table_size), secondary_location(key, factor, table_size))
