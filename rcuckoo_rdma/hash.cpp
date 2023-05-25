#include "xxhash.h"
#include <cmath>
#include <string>
#include <assert.h>
// #include <cstdlib>
#include "hash.h"

using namespace std;

float DEFAULT_FACTOR = 2.3;

void set_factor(float factor){
    // float local_factor = factor;
    DEFAULT_FACTOR = factor;
}

float get_factor(){
    return DEFAULT_FACTOR;
}

inline XXH64_hash_t xxhash_value(const string& str)
{
    return XXH64(str.c_str(), str.size(), 0);
}

XXH64_hash_t h1(string key){
    return xxhash_value(key);
}

XXH64_hash_t h2(string key){
    assert(key.size() >= 4);
    key[0] = ~key[0];
    key[1] = ~key[1];
    return xxhash_value(key);
}

XXH64_hash_t h3(string key){
    assert(key.size() >= 4);
    key[2] = ~key[2];
    key[3] = ~key[3];
    return xxhash_value(key);
}

unsigned int rcuckoo_primary_location(string key, int table_size){
    XXH64_hash_t hash = h1(key);
    #ifdef DEBUG
    cout << "hash: " << hash << " table size " << table_size <<  endl;
    #endif
    return hash % table_size;
}

unsigned int h3_suffix_base_two(string key){
    XXH64_hash_t hash = h3(key);
    int zeros = __builtin_clz(hash);
    #ifdef DEBUG
    cout << "key: " << key << " hash: " << hash << " zeros: " << zeros << endl;
    #endif
    return zeros;
}

unsigned int rcuckoo_secondary_location(string key, float factor, int table_size){
    int primary = rcuckoo_primary_location(key, table_size);
    int zeros = h3_suffix_base_two(key);
    float exponent = (float)zeros + factor;
    #ifdef DEBUG
    cout << "key: " << key << " zeros: " << zeros << " exponent: " << exponent << endl;
    #endif
    int mod_size = (int)pow(factor, exponent);
    int secondary = h2(key) % mod_size;
    if (secondary % 2 == 0) {
        secondary = secondary + 1;
    }
    return (primary + secondary) % table_size;
}

unsigned int rcuckoo_secondary_location_independent(string key, int table_size){
    XXH64_hash_t hash = h2(key);
    unsigned int location = ((hash % (table_size / 2)) * 2) + 1;
    return location;
}

unsigned int get_table_id_from_index(unsigned int index){
    return index % 2;
}


unsigned int distance_to_bytes(unsigned int a, unsigned int b, unsigned int bucket_size, unsigned int entry_size){
    unsigned int bucket_width = bucket_size * entry_size;
    return abs(int(a-b))*bucket_width;
}


hash_locations rcuckoo_hash_locations(string key, int table_size){
    hash_locations hl;
    hl.primary = rcuckoo_primary_location(key, table_size);
    hl.secondary = rcuckoo_secondary_location(key, DEFAULT_FACTOR, table_size);
    return hl;
}

hash_locations rcuckoo_hash_locations_independent(string key, int table_size){
    hash_locations hl;
    hl.primary = rcuckoo_primary_location(key, table_size);
    hl.secondary = rcuckoo_secondary_location_independent(key, table_size);
    return hl;
}

void ten_k_hashes(){
    string key = "asdfasdfasdfasdf";
    for(int i=0; i<10000; i++){
        key[i%key.size()] = key[i%key.size()] + 1;
        hash_locations hl = rcuckoo_hash_locations(key, 1000000);
        // XXH64_hash_t hash = h1(key);
    }
}

// Race functions
race_bucket to_race_index_math(unsigned int index, unsigned int table_size){
    if ((table_size % 3) != 0){
        table_size -= (table_size % 3);
    }
    unsigned int two_thirds_table_size = (table_size * 2) / 3;
    index = index % two_thirds_table_size;
    unsigned int index_div_two = index / 2;

    race_bucket rb;
    if (index % 2 == 0){
        rb.bucket = index + index_div_two;
        rb.overflow = rb.bucket + 1;
    } else {
        rb.bucket = index_div_two + two_thirds_table_size;
        rb.overflow = rb.bucket - 1;
    }
    return rb;
}

race_bucket race_primary_location(string key, unsigned int table_size){
    unsigned int index = h1(key);
    return to_race_index_math(index, table_size);
}

race_bucket race_secondary_location(string key, unsigned int table_size){
    unsigned int index = h2(key);
    return to_race_index_math(index, table_size);
}

race_hash_buckets race_hash_locations(string key, unsigned int table_size){
    race_hash_buckets rhb;
    rhb.primary = race_primary_location(key, table_size);
    rhb.secondary = race_secondary_location(key, table_size);
    return rhb;
}

