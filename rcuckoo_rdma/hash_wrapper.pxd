from libcpp.string cimport string

cdef extern from "xxhash.h":
    ctypedef unsigned long long XXH64_hash_t

cdef extern from "hash.h":
    void set_factor(float factor)
    float get_factor()

    struct hash_locations:
        unsigned int primary
        unsigned int secondary

    XXH64_hash_t h1(string key)
    XXH64_hash_t h2(string key)
    XXH64_hash_t h3(string key)
    unsigned int rcuckoo_primary_location(string key, int table_size)
    unsigned int h3_suffix_base_two(string key)
    unsigned int rcuckoo_secondary_location(string key, float factor, int table_size)
    unsigned int rcuckoo_secondary_location_independent(string key, int table_size)
    hash_locations rcuckoo_hash_locations(string key, int table_size)
    hash_locations rcuckoo_hash_locations_independent(string key, int table_size)
    void ten_k_hashes()