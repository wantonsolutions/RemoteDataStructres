#ifndef HASH_H
#define HASH_H

#include "xxhash.h"
#include <string>

using namespace std;

typedef struct hash_locations{
    unsigned int primary;
    unsigned int secondary;
} hash_locations;



void set_factor(float factor);
float get_factor();
unsigned int get_table_id_from_index(unsigned int index);
unsigned int distance_to_bytes(unsigned int a, unsigned int b, unsigned int bucket_size, unsigned int entry_size);
XXH64_hash_t h1(string key);
XXH64_hash_t h2(string key);
XXH64_hash_t h3(string key);

unsigned int rcuckoo_primary_location(string key, unsigned int table_size);
unsigned int h3_suffix_base_two(string key);
unsigned int rcuckoo_secondary_location(string key, float factor, unsigned int table_size);
unsigned int rcuckoo_secondary_location_independent(string key, unsigned int table_size);
hash_locations rcuckoo_hash_locations(string key, unsigned int table_size);
hash_locations rcuckoo_hash_locations_independent(string key, unsigned int table_size);

typedef struct race_bucket{
    unsigned int bucket;
    unsigned int overflow;
} race_bucket;

typedef struct race_hash_buckets{
    race_bucket primary;
    race_bucket secondary;
} race_hash_buckets;

race_bucket to_race_index_math(unsigned int index, unsigned int table_size);
race_bucket race_primary_location(string key, unsigned int table_size);
race_bucket race_secondary_location(string key, unsigned int table_size);
race_hash_buckets race_hash_locations(string key, unsigned int table_size);
void ten_k_hashes();


#endif