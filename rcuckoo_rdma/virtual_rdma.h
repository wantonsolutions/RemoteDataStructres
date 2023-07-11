#ifndef VIRTUAL_RDMA_H
#define VIRTUAL_RDMA_H

#include "tables.h"
#include <string>
#include <unordered_map>
#include <vector>
#include <any>
#include "assert.h"
#include "hash.h"
#include "search.h"

using namespace std;
using namespace cuckoo_search;

namespace cuckoo_virtual_rdma {
    enum operation {
        GET,
        PUT,
        DELETE,
        NO_OP
    };

    enum message_type {
        READ_REQUEST,
        READ_RESPONSE,
        WRITE_REQUEST,
        WRITE_RESPONSE,
        CAS_REQUEST,
        CAS_RESPONSE,
        MASKED_CAS_REQUEST,
        MASKED_CAS_RESPONSE,
        NO_OP_MESSAGE
    };

    typedef struct VRMessage {
        string function;
        message_type type; // same as function but not the string
        // VRMessage(){
        //     function = "";
        // }
        unordered_map<string,string> function_args;
        message_type get_message_type();
        uint32_t get_message_size_bytes();
        string to_string();
        VRMessage& operator=(const VRMessage& other);

    } VRMessage;

    typedef struct Request {
        enum operation op;
        Key key;
        Value value;

        Request();
        Request(operation op, Key key, Value value);
        ~Request() {}
        string to_string();

    } Request;


    typedef struct VRMaskedCasData {
        unsigned int min_lock_index;
        uint64_t mask;
        uint64_t new_value;
        uint64_t old;
        string to_string();
    } VRMaskedCasData;

    #define ETHERNET_SIZE 18
    #define IP_SIZE 20
    #define UDP_SIZE 8
    #define ROCE_SIZE 16
    #define BASE_ROCE_SIZE (ETHERNET_SIZE + IP_SIZE + UDP_SIZE + ROCE_SIZE)

    #define ROCE_READ_HEADER 16
    #define ROCE_READ_RESPONSE_HEADER 4
    #define ROCE_WRITE_HEADER 24
    #define ROCE_WRITE_RESPONSE_HEADER 4
    #define CAS_HEADER 28
    #define CAS_RESPONSE_HEADER 8
    #define MASKED_CAS_HEADER 44
    #define MASKED_CAS_RESPONSE_HEADER 8

    #define READ_REQUEST_SIZE (BASE_ROCE_SIZE + ROCE_READ_HEADER)
    #define READ_RESPONSE_SIZE (BASE_ROCE_SIZE + ROCE_READ_RESPONSE_HEADER)
    #define WRITE_REQUEST_SIZE (BASE_ROCE_SIZE + ROCE_WRITE_HEADER)
    #define WRITE_RESPONSE_SIZE (BASE_ROCE_SIZE + ROCE_WRITE_RESPONSE_HEADER)
    #define CAS_REQUEST_SIZE (BASE_ROCE_SIZE + CAS_HEADER)
    #define CAS_RESPONSE_SIZE (BASE_ROCE_SIZE + CAS_RESPONSE_HEADER)
    #define MASKED_CAS_REQUEST_SIZE (BASE_ROCE_SIZE + MASKED_CAS_HEADER)
    #define MASKED_CAS_RESPONSE_SIZE (BASE_ROCE_SIZE + MASKED_CAS_RESPONSE_HEADER)


    string message_type_to_function_string(message_type type);
    uint32_t header_size(message_type type);
    unordered_map<string,string> unpack_read_read_response(VRMessage &msg);
    void fill_local_table_with_read_response(Table &table, unordered_map<string,string> &args);

    void fill_table_with_read(Table &table, uint32_t bucket_id, uint32_t bucket_offset, uint32_t size, vector<Entry> &entries);
    vector<Entry> read_table_entry(Table &table, uint32_t bucket_id, uint32_t bucket_offset, uint32_t size);

    CasOperationReturn cas_table_entry(Table &table, uint32_t bucket_id, uint32_t bucket_offset, uint64_t old, uint64_t new_value);
    CasOperationReturn masked_cas_lock_table(Table &table, uint32_t lock_index, uint64_t old, uint64_t new_value, uint64_t mask);

    //Encoding and decoding functions
    vector<string> split(const string &str, const string &delim);
    vector<Entry> decode_entries_from_string(string str_entries);
    string encode_entries_to_string(vector<Entry> &entries);
    int keys_contained_in_read_response(const Key &key, const vector<Entry> &entries);

    unsigned int single_read_size_bytes(hash_locations buckets, unsigned int row_size_bytes);
    vector<VRMessage> read_threshold_message(hash_locations (*location_function)(string, unsigned int), Key current_read_key, unsigned int read_threshold_bytes,unsigned int table_size,unsigned int row_size_bytes);
    vector<unsigned int> lock_indexes_to_buckets(vector<unsigned int> lock_indexes, unsigned int buckets_per_lock);



    vector<VRMaskedCasData> get_lock_list(vector<unsigned int> buckets, unsigned int buckets_per_lock, unsigned int locks_per_message);

    vector<unsigned int> get_unique_lock_indexes(vector<unsigned int> buckets, unsigned int buckets_per_lock);
    unsigned int byte_aligned_index(unsigned int index);
    unsigned int sixty_four_aligned_index(unsigned int index);
    unsigned int get_min_sixty_four_aligned_index(vector<unsigned int> indexes);
    vector<vector<unsigned int>> break_lock_indexes_into_chunks(vector<unsigned int> lock_indexes, unsigned int locks_per_message);
    vector<VRMaskedCasData> lock_chunks_to_masked_cas_data(vector<vector<unsigned int>> lock_chunks);
    vector<VRMaskedCasData> unlock_chunks_to_masked_cas_data(vector<vector<unsigned int>> lock_chunks);
    vector<VRMaskedCasData> get_lock_or_unlock_list(vector<unsigned int> buckets, unsigned int buckets_per_lock, unsigned int locks_per_message, bool locking);
    vector<VRMaskedCasData> get_lock_list(vector<unsigned int> buckets, unsigned int buckets_per_lock, unsigned int locks_per_message);
    vector<VRMaskedCasData> get_unlock_list(vector<unsigned int> buckets, unsigned int buckets_per_lock, unsigned int locks_per_message);
    VRMessage read_request_message(unsigned int start_bucket, unsigned int offset, unsigned int size);
    vector<VRMessage> multi_bucket_read_message(hash_locations buckets, unsigned int row_size_bytes);
    VRMessage single_bucket_read_message(unsigned int bucket, unsigned int row_size_bytes);
    vector<VRMessage> single_bucket_read_messages(hash_locations buckets, unsigned int row_size_bytes);
    vector<VRMessage> read_threshold_message(hash_locations (*location_function)(Key, unsigned int), Key key, unsigned int read_threshold_bytes,unsigned int table_size,unsigned int row_size_bytes);

    vector<unsigned int> lock_message_to_lock_indexes(VRMessage lock_message);
    VRMessage create_masked_cas_message_from_lock_list(VRMaskedCasData masked_cas_data);
    vector<VRMessage> create_masked_cas_messages_from_lock_list(vector<VRMaskedCasData> masked_cas_list);
    VRMessage get_covering_read_from_lock_message(VRMessage lock_message, unsigned int buckets_per_lock, unsigned int row_size_bytes);

    VRMessage cas_table_entry_message(unsigned int bucket_index, unsigned int bucket_offset, Key old, Key new_value);
    VRMessage next_cas_message(vector<path_element> search_path, unsigned int index);
    vector<VRMessage> gen_cas_messages(vector<path_element> search_path);

}

#endif