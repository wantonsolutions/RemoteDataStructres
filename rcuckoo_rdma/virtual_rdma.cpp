#include "virtual_rdma.h"
#include <string.h>

using namespace std;
namespace cuckoo_virtual_rdma {

    Request::Request(operation op, Key key, Value value){
        this->op = op;
        this->key = key;
        this->value = value;
    } 

    Request::Request(){
        this->op = GET;
        memset(key.bytes, 0, KEY_SIZE);
        memset(value.bytes, 0, VALUE_SIZE);
    }

    string Request::to_string(){
        string s = "";
        switch (op) {
            case GET:
                s += "GET";
                break;
            case PUT:
                s += "PUT";
                break;
            case DELETE:
                s += "DELETE";
                break;
        }
        s += " " + key.to_string() + " " + value.to_string();
        return s;
    }

    message_type VRMessage::get_message_type(){
        if (function == "cas_table_entry") {
            return CAS_REQUEST;
        } else if (function == "fill_table_with_cas"){
            return CAS_RESPONSE;
        } else if (function == "masked_cas_lock_table") {
            return MASKED_CAS_REQUEST;
        } else if (function == "fill_lock_table_masked_cas") {
            return MASKED_CAS_RESPONSE;
        } else if (function == "read_table_entry") {
            return READ_REQUEST;
        } else if (function == "fill_table_with_read") {
            return READ_RESPONSE;
        } else {
            printf("Error unknown function %s\n", function.c_str());
            exit(1);
        }
    }

    uint32_t VRMessage::get_message_size_bytes() {
        uint32_t size = 0;
        size += header_size(get_message_type());
        if (function_args.find("size") != function_args.end()) {
            size += stoi(function_args["size"]);
        }
        return size;
    }


    uint32_t header_size(message_type type){
        switch(type){
            case READ_REQUEST:
                return READ_REQUEST_SIZE;
            case READ_RESPONSE:
                return READ_RESPONSE_SIZE;
            case WRITE_REQUEST:
                return WRITE_REQUEST_SIZE;
            case WRITE_RESPONSE:
                return WRITE_RESPONSE_SIZE;
            case CAS_REQUEST:
                return CAS_REQUEST_SIZE;
            case CAS_RESPONSE:
                return CAS_RESPONSE_SIZE;
            case MASKED_CAS_REQUEST:
                return MASKED_CAS_REQUEST_SIZE;
            case MASKED_CAS_RESPONSE:
                return MASKED_CAS_RESPONSE_SIZE;
            default:
                printf("error unknown enum\n");
                exit(0);
        }
    }
}