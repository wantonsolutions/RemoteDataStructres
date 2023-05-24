# import cython
# from chash cimport set_factor as c_set_factor
# from chash cimport * as c_hash
cimport hash_wrapper as h


def set_factor(x):
    return h.set_factor(x)

def get_factor():
    return h.get_factor()

def distance_to_bytes(a, b, bucket_size, entry_size):
    return h.distance_to_bytes(a, b, bucket_size, entry_size)

def h1(key):
    return h.h1(key)

def h2(key):
    return h.h2(key)

def h3(key):
    return h.h3(key)

def rcuckoo_primary_location(key, table_size):
    return h.rcuckoo_primary_location(key, table_size)

def h3_suffix_base_two(key):
    return h.h3_suffix_base_two(key)

def rcuckoo_secondary_location(key, factor, table_size):
    return h.rcuckoo_secondary_location(key, factor, table_size)

def rcuckoo_secondary_location_independent(key, table_size):
    return h.rcuckoo_secondary_location_independent(key, table_size)


def rcuckoo_hash_locations(key, table_size):
    bytes_key = str(key).encode('utf-8')
    locations = h.rcuckoo_hash_locations(bytes_key, table_size)
    return (locations.primary, locations.secondary)

def rcuckoo_hash_locations_independent(key, table_size):
    locations = h.rcuckoo_hash_locations_independent(key, table_size)
    return (locations.primary, locations.secondary)

def ten_k_hashes():
    return h.ten_k_hashes()