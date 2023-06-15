#ifndef VIRTUAL_RDMA_H
#define VIRTUAL_RDMA_H

#include "tables.h"
#include <string>
#include <unordered_map>
#include <vector>
#include <any>
#include "assert.h"

using namespace std;

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
        MASKED_CAS_RESPONSE
    };

    typedef struct VRMessage {
        string function;
        unordered_map<string,string> function_args;
        message_type get_message_type();
        uint32_t get_message_size_bytes();
        string to_string();
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

}

#endif