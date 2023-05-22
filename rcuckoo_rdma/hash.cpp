#include <boost/python.hpp>
#include <cstdio>
#include <openssl/evp.h>
#include <string>

#include <iostream>
#include <iomanip>
#include <sstream>
#include <openssl/md5.h>
#include <tuple>
#include "xxhash.h"

using namespace std;

// #define DEBUG

float DEFAULT_FACTOR=2.3;

void set_factor(float factor){
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

typedef struct hash_locations{
    unsigned int primary;
    unsigned int secondary;
} hash_locations;

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

//Boost export python module
BOOST_PYTHON_MODULE(hash)
{
    using namespace boost::python;
    def("set_factor", set_factor);
    def("get_factor", get_factor);
    def("h1", h1);
    def("h2", h2);
    def("h3", h3);
    def("rcuckoo_primary_location", rcuckoo_primary_location);
    def("rcuckoo_secondary_location", rcuckoo_secondary_location);
    def("rcuckoo_hash_locations", rcuckoo_hash_locations);
    def("rcuckoo_hash_locations_independent", rcuckoo_hash_locations_independent);
    def("ten_k_hashes", ten_k_hashes);

    class_<hash_locations>("hash_locations")
        .def_readwrite("primary", &hash_locations::primary)
        .def_readwrite("secondary", &hash_locations::secondary)
    ;
}