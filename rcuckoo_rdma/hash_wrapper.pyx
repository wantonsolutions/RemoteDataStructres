# import cython
# from chash cimport set_factor as c_set_factor
# from chash cimport * as c_hash
cimport hash_wrapper as h


def set_factor(x):
    return h.set_factor(x)

def get_factor():
    return h.get_factor()

def get_table_id_from_index(index):
    return h.get_table_id_from_index(index)

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
    bytes_key = str(key).encode('utf-8')
    locations = h.rcuckoo_hash_locations_independent(bytes_key, table_size)
    return (locations.primary, locations.secondary)

def to_race_index_math(index,table_size):
    locations = h.to_race_index_math(index, table_size)
    return (locations.bucket, locations.overflow)

def race_primary_location(key, table_size):
    bytes_key = str(key).encode('utf-8')
    locations = h.race_primary_location(bytes_key, table_size)
    return (locations.bucket, locations.overflow)

def race_secondary_location(key, table_size):
    bytes_key = str(key).encode('utf-8')
    locations = h.race_secondary_location(bytes_key, table_size)
    return (locations.bucket, locations.overflow)

def race_hash_locations(key, table_size):
    bytes_key = str(key).encode('utf-8')
    hash_buckets = h.race_hash_locations(bytes_key, table_size)
    return (hash_buckets.primary, hash_buckets.secondary)

def ten_k_hashes():
    return h.ten_k_hashes()