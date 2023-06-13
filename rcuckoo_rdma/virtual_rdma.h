#ifndef VIRTUAL_RDMA_H
#define VIRTUAL_RDMA_H

#include "tables.h"
#include <string>
#include <unordered_map>
#include <vector>
#include <any>

using namespace std;

namespace cuckoo_virtual_rdma {
    enum operation {
        GET,
        PUT,
        DELETE
    };

    typedef struct VRMessage {
        unordered_map<string,string> payload;
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
}

#endif