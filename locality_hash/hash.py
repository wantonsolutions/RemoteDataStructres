import hashlib

def h1(key):
    return hashlib.md5(str(key).encode('utf-8')).hexdigest()

def h2(key):
    val = str(key) + "salt"
    return hashlib.sha1(val.encode('utf-8')).hexdigest()

def h3(key):
    val = str(key) + "salty"
    return hashlib.sha1(val.encode('utf-8')).hexdigest()

def primary_location(key, table_size):
    return int(h1(key),16) % table_size

def secondary_location(key, table_size):
    return int(h2(key),16) % table_size

def h3_suffix(key):
    val = int(h3(key),32)
    val = str(bin(val))[2:]
    zeros = len(val) - len(val.rstrip('0'))
    return zeros

def secondary_bounded_location(key, table_size, suffix_size):
    primary = primary_location(key, table_size)
    secondary = (int(h2(key),16)) % suffix_size
    return (primary + secondary) % table_size

def secondary_bounded_location_exp(key, table_size, suffix_size):
    primary = primary_location(key, table_size)
    zeros = (h3_suffix(key) + 1) * suffix_size
    #how do increase bin size?
    secondary = (int(h2(key),16)) % (2**zeros)
    #secondary = (int(h2(key),16)) % int((2**zeros) / 2) #does not really work
    # secondary = int(secondary/2) #todo play with this line to see how it affects the performance
    return (primary + secondary) % table_size


def get_locations(key, table_size, suffix):
    return (primary_location(key, table_size), secondary_location(key, table_size))

def get_locations_bounded(key, table_size, suffix_size):
    #return (primary_location(key, table_size), secondary_bounded_location(key, table_size, suffix_size))
    return (primary_location(key, table_size), secondary_bounded_location_exp(key, table_size, suffix_size))