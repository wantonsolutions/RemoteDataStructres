#include <string>
#include "state_machines.h"
#include "tables.h"
#include <stdio.h>
#include <string.h>
#include <unordered_map>
#include <vector>
#include <any>


using namespace std;
namespace cuckoo_state_machines {

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

    State_Machine::State_Machine() {
        _config = unordered_map<string,string>();
        clear_statistics();
    }

    State_Machine::State_Machine(unordered_map<string,string>config) {
        _config = config;
        clear_statistics();
    }

    void State_Machine::clear_statistics() {
        //clear control state
        _complete = false;
        _inserting = false;
        _reading = false;

        //clear counters (statistics)
        _total_bytes = 0;
        _read_bytes = 0;
        _write_bytes = 0;
        _cas_bytes = 0;

        _total_reads = 0;
        _total_writes = 0;
        _total_cas = 0;
        _total_cas_failures = 0;

        _current_insert_key = Key();


        _insert_path_lengths = vector<int>();
        _index_range_per_insert = vector<int>();
        _current_insert_messages = 0;
        _messages_per_insert = vector<int>();
        _completed_inserts = vector<Key>();
        _completed_insert_count = 0;
        _failed_inserts = vector<Key>();
        _failed_insert_count = 0;
        _insert_operation_bytes = 0;
        _insert_operation_messages = 0;

        _current_insert_rtt = 0;
        _insert_rtt = vector<int>();
        _insert_rtt_count = 0;

        _current_read_messages = 0;
        _messages_per_read = vector<int>();
        _completed_reads = vector<Key>();
        _completed_read_count = 0;
        _failed_reads = vector<Key>();
        _failed_read_count = 0;
        _read_operation_bytes = 0;
        _read_operation_messages = 0;
        _current_read_rtt =0;
        _read_rtt = vector<int>();
        _read_rtt_count = 0;
    }

    void State_Machine::complete_read_stats(bool success, Key read_key){
        if (success) {
            _completed_reads.push_back(read_key);
            _completed_read_count++;
        } else {
            _failed_reads.push_back(read_key);
            _failed_read_count++;
        }
        _messages_per_read.push_back(_current_read_messages);
        _read_rtt.push_back(_current_read_rtt);
        _read_rtt_count += _read_rtt_count;

        _current_read_messages = 0;
        _current_read_rtt = 0;

    }
    void State_Machine::complete_insert_stats(bool success){
        if (success) {
            _completed_inserts.push_back(_current_insert_key);
            _completed_insert_count++;
        } else {
            _failed_inserts.push_back(_current_insert_key);
            _failed_insert_count++;
        }
        _messages_per_insert.push_back(_current_insert_messages);
        _insert_rtt.push_back(_current_insert_rtt);
        _insert_rtt_count += _insert_rtt_count;

        _current_insert_messages = 0;
        _current_insert_rtt = 0;

    }
    unordered_map<string, any> State_Machine::get_stats(){

    }

}
