#include "xxhash.h"
#include <cmath>
#include <string>
#include <assert.h>
#include <iostream>
// #include <cstdlib>
#include "hash.h"
#include "tables.h"
#include <strings.h>
#include <openssl/md5.h>

using namespace std;

float DEFAULT_FACTOR = 2.3;
// float DEFAULT_FACTOR = 3.0;

void set_factor(float factor){
    // float local_factor = factor;
    DEFAULT_FACTOR = factor;
}

float get_factor(){
    return DEFAULT_FACTOR;
}

inline XXH64_hash_t xxhash_value(const Key& key)
{
    return XXH64(key.bytes, KEY_SIZE, 0);

    // MD5 version
    // unsigned char hash_bytes[16];
    // bzero(hash_bytes, 16);
    // MD5((const unsigned char*)key.bytes, KEY_SIZE, (unsigned char*)hash_bytes);
    // XXH64_hash_t hash = 0;
    // for (int i = 0; i < 8; i++) {
    //     hash = hash << 8;
    //     hash += hash_bytes[i];
    // }
    // return hash;
}

inline XXH64_hash_t xxhash_value(const string& str)
{
    return XXH64(str.c_str(), str.size(), 0);
}

XXH64_hash_t h1(string key){
    return xxhash_value(key);
}

XXH64_hash_t h1(Key key) {
    return xxhash_value(key);
}

XXH64_hash_t h2(string key){
    assert(key.size() >= 4);
    key[0] = ~key[0];
    key[1] = ~key[1];
    return xxhash_value(key);
}

XXH64_hash_t h2(Key key){
    assert(KEY_SIZE >= 4);
    key.bytes[0] = ~key.bytes[0];
    key.bytes[1] = ~key.bytes[1];
    return xxhash_value(key);
}

XXH64_hash_t h3(string key){
    assert(key.size() >= 4);
    key[2] = ~key[2];
    key[3] = ~key[3];
    return xxhash_value(key);
}

XXH64_hash_t h3(Key key){
    assert(KEY_SIZE >= 4);
    key.bytes[2] = ~key.bytes[2];
    key.bytes[3] = ~key.bytes[3];
    return xxhash_value(key);
}

unsigned int rcuckoo_primary_location(string key, unsigned int table_size){
    XXH64_hash_t hash = h1(key);
    #ifdef DEBUG
    cout << "hash: " << hash << " table size " << table_size <<  endl;
    #endif
    if (hash % 2 == 0) {
        hash +=1;
    }
    return hash % table_size;
}

unsigned int rcuckoo_primary_location(Key key, unsigned int table_size){
    XXH64_hash_t hash = h1(key);
    #ifdef DEBUG
    cout << "hash: " << hash << " table size " << table_size <<  endl;
    #endif
    if (hash % 2 == 0) {
        hash +=1;
    }
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

unsigned int h3_suffix_base_two(Key key){
    XXH64_hash_t hash = h3(key);
    int zeros = __builtin_clz(hash);
    #ifdef DEBUG
    cout << "key: " << key << " hash: " << hash << " zeros: " << zeros << endl;
    #endif
    return zeros;
}
            // }
            // for (unsigned int i = 0; i < _lock_list.size(); i++) {
            //     receive_successful_unlocking_message(i);
            // }

inline double fastPow(double a, double b) {
  union {
    double d;
    int x[2];
  } u = { a };
  u.x[1] = (int)(b * (u.x[1] - 1072632447) + 1072632447);
  u.x[0] = 0;
  return u.d;
}



unsigned int rcuckoo_secondary_location(string key, float factor, unsigned int table_size){
    int primary = rcuckoo_primary_location(key, table_size);
    int zeros = h3_suffix_base_two(key);
    float exponent = (float)zeros + factor;
    #ifdef DEBUG
    cout << "key: " << key << " zeros: " << zeros << " exponent: " << exponent << endl;
    #endif
    int mod_size = (int)fastPow(factor, exponent);
    int secondary = h2(key) % mod_size;
    if (secondary % 2 == 0) {
        secondary = secondary + 1;
    }
    return (primary + secondary) % table_size;
}

unsigned int rcuckoo_secondary_location(Key key, float factor, unsigned int table_size){
    int primary = rcuckoo_primary_location(key, table_size);
    int zeros = h3_suffix_base_two(key);
    float exponent = (float)zeros + factor;
    #ifdef DEBUG
    cout << "key: " << key << " zeros: " << zeros << " exponent: " << exponent << endl;
    #endif
    int mod_size = (int)fastPow(factor, exponent);
    int secondary = h2(key) % mod_size;
    if (secondary % 2 == 0) {
        secondary = secondary + 1;
    }
    return (primary + secondary) % table_size;
}

unsigned int rcuckoo_secondary_location_independent(string key, unsigned int table_size){
    // XXH64_hash_t hash = h2(key);
    // unsigned int location = ((hash % (table_size / 2)) * 2) + 1;
    // return location;
    XXH64_hash_t hash = h2(key);
    if (hash % 2 == 1) {
        hash +=1;
    }
    return hash % table_size;

}

unsigned int rcuckoo_secondary_location_independent(Key key, unsigned int table_size){
    XXH64_hash_t hash = h2(key);
    if (hash % 2 == 1) {
        hash +=1;
    }
    return hash % table_size;
}

// unsigned int get_table_id_from_index(unsigned int index){
//     return index % 2;
// }


unsigned int distance_to_bytes(unsigned int a, unsigned int b, unsigned int bucket_size, unsigned int entry_size){
    unsigned int bucket_width = bucket_size * entry_size;
    return abs(int(a-b))*bucket_width;
}


hash_locations rcuckoo_hash_locations(string key, unsigned int table_size){
    hash_locations hl;
    hl.primary = rcuckoo_primary_location(key, table_size);
    hl.secondary = rcuckoo_secondary_location(key, DEFAULT_FACTOR, table_size);
    // cout << "key " << key << "primary: " << hl.primary << " secondary: " << hl.secondary << endl;
    return hl;
}

hash_locations rcuckoo_hash_locations(Key key, unsigned int table_size){
    hash_locations hl;
    hl.primary = rcuckoo_primary_location(key, table_size);
    hl.secondary = rcuckoo_secondary_location(key, DEFAULT_FACTOR, table_size);
    // cout << "key " << key.to_string() << "primary: " << hl.primary << " secondary: " << hl.secondary << "table size" << table_size << endl;
    return hl;
}

hash_locations rcuckoo_hash_locations_independent(string key, unsigned int table_size){
    hash_locations hl;
    hl.primary = rcuckoo_primary_location(key, table_size);
    hl.secondary = rcuckoo_secondary_location_independent(key, table_size);
    return hl;
}

hash_locations rcuckoo_hash_locations_independent(Key key, unsigned int table_size){
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
        printf("hash_loactions primary: %d secondary: %d\n", hl.primary, hl.secondary);
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

