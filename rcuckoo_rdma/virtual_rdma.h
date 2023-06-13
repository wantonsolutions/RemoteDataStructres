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
        DELETE
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


    uint32_t header_size(message_type type);

}

#endif