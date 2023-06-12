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
}