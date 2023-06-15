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

    string message_type_to_function_string(message_type type){
        switch(type){
            case READ_REQUEST:
                return "read_table_entry";
            case READ_RESPONSE:
                return "fill_table_with_read";
            // case WRITE_REQUEST:
            //     return "write_table_entry";
            // case WRITE_RESPONSE:
            //     return "fill_table_with_write";
            case CAS_REQUEST:
                return "cas_table_entry";
            case CAS_RESPONSE:
                return "fill_table_with_cas";
            case MASKED_CAS_REQUEST:
                return "masked_cas_lock_table";
            case MASKED_CAS_RESPONSE:
                return "fill_lock_table_masked_cas";
            default:
                printf("Error unknown message type %d\n", type);
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

    string VRMessage::to_string(){
        string s = "";
        s += function + " ";
        for (auto const& x : function_args) {
            s += x.first + ":" + x.second + " ";
        }
        return s;
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

    unordered_map<string,string> unpack_read_read_response(VRMessage &msg) {
        assert (msg.get_message_type() == READ_RESPONSE);
        printf("unpacking read response %s\n", msg.to_string().c_str());
        return msg.function_args;
    }

    void fill_table_with_read(Table &table, uint32_t bucket_id, uint32_t bucket_offset, uint32_t size, vector<Entry> &entries) {
        table.assert_operation_in_table_bound(bucket_id, bucket_offset, size);
        uint32_t total_indexes = size / sizeof(Entry);
        assert (entries.size() == total_indexes);

        uint32_t base = bucket_id * table.get_buckets_per_row() + bucket_offset;

        for (int i=0; i<total_indexes; i++) {
            uint32_t bucket = table.absolute_index_to_bucket_index(base + i);
            uint32_t offset = table.absolute_index_to_bucket_offset(base + i);
            table.set_entry(bucket, offset, entries[i]);
        }
        return;

    }

    vector<Entry> read_table_entry(Table &table, uint32_t bucket_id, uint32_t bucket_offset, uint32_t size) {
        table.assert_operation_in_table_bound(bucket_id, bucket_offset, size);
        uint32_t total_indexes = size / sizeof(Entry);
        vector<Entry> entries;
        uint32_t base = bucket_id * table.get_buckets_per_row() + bucket_offset;

        for (int i=0; i<total_indexes; i++) {
            uint32_t bucket = table.absolute_index_to_bucket_index(base + i);
            uint32_t offset = table.absolute_index_to_bucket_offset(base + i);
            entries.push_back(table.get_entry(bucket, offset));
        }
        return entries;
    }


    CasOperationReturn cas_table_entry(Table &table, uint32_t bucket_id, uint32_t bucket_offset, uint64_t old, uint64_t new_value) {
        Entry e = table.get_entry(bucket_id, bucket_offset);
        uint64_t ret_val = e.get_as_uint64_t();
        bool success = false;
        Entry new_entry;

        if (ret_val == old) {
            new_entry.set_as_uint64_t(new_value);
            success = true;
        } 
        return CasOperationReturn(success, ret_val);
    }

    CasOperationReturn masked_cas_lock_table(Table &table, uint32_t lock_index, uint64_t old, uint64_t new_value, uint64_t mask) {
        return table.lock_table_masked_cas(lock_index, old, new_value, mask);
    }

    vector<string> split(const string &str, const string &delim)
    {
        vector<string> tokens;
        size_t prev = 0, pos = 0;
        do
        {
            pos = str.find(delim, prev);
            if (pos == string::npos)
                pos = str.length();
            string token = str.substr(prev, pos - prev);
            if (!token.empty())
                tokens.push_back(token);
            prev = pos + delim.length();
        } while (pos < str.length() && prev < str.length());
        return tokens;
    }

    vector<Entry> decode_entries_from_string(string str_entries) {
        vector<Entry> entries;
        vector<string> entries_str = split(str_entries, ",");
        for (int i=0; i<entries_str.size(); i++) {
            vector<string> kv = split(entries_str[i], ":");
            Entry e = Entry(kv[0], kv[1]);
            entries.push_back(e);
        }
        return entries;
    }

    string encode_entries_to_string(vector<Entry> &entries) {
        string entries_str;
        for (int i=0; i<entries.size(); i++) {
            entries_str += entries[i].to_string();
            if (i != entries.size() - 1) {
                entries_str += ",";
            }
        }
        return entries_str;
    }

    void fill_local_table_with_read_response(Table &table, unordered_map<string,string> &args) {
        try {
            uint32_t bucket_id = stoi(args["bucket_id"]);
            uint32_t bucket_offset = stoi(args["bucket_offset"]);
            uint32_t size = stoi(args["size"]);
            string read = args["read"];
            //This parsing assumes the encoding
            //key1:value1,key2:value2,key3:value3
            vector<Entry> entries = decode_entries_from_string(args["read"]);
            fill_table_with_read(table, bucket_id, bucket_offset, size, entries);
        } catch (exception &e) {
            printf("error in fill_local_table_with_read_response %s\n", e.what());
            exit(1);
        }
        return;
    }

    int keys_contained_in_read_response(const Key &key, const vector<Entry> &entries) {
        int count = 0;
        for (int i=0; i<entries.size(); i++) {
            if (entries[i].key == key) {
                count++;
            }
        }
        return count;
    }
}