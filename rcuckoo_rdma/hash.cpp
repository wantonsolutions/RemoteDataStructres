#include "xxhash.h"
#include <cmath>
#include <string>
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
    key[0] = !key[0];
    return xxhash_value(key);
}

XXH64_hash_t h3(string key){
    key[1] = !key[1];
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