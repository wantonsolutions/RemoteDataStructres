#include <string>
#include <stdio.h>
#include <string.h>
#include <unordered_map>
#include <vector>
#include <any>

#include "state_machines.h"
#include "tables.h"
#include "virtual_rdma.h"


using namespace std;

template <typename T>
string array_to_string(vector<T> array) {
    string result = "";
    for (auto i = 0; i < array.size(); i++) {
        result += to_string(array[i]);
        if (i < array.size() - 1) {
            result += ",";
        }
    }
    return result;
}

string key_array_to_string(vector<Key> array) {
    string result = "";
    for (auto i = 0; i < array.size(); i++) {
        result += array[i].to_string();
        if (i < array.size() - 1) {
            result += ",";
        }
    }
    return result;
}

namespace cuckoo_state_machines {


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

    string State_Machine::get_state_machine_name() {
        return "State Machine Super Class";
    }

    bool State_Machine::is_complete(){
        return _complete;
    }

    vector<Key> State_Machine::get_completed_inserts() {
        return _completed_inserts;
    }

    void State_Machine::set_max_fill(float max_fill){
        printf("SET MAX FILL Overload this function");
        throw logic_error("SET MAX FILL Overload this function");
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
    unordered_map<string, string> State_Machine::get_stats(){
        unordered_map<string, string> stats;
        stats["total_bytes"] = to_string(_total_bytes);
        stats["read_bytes"] = to_string(_read_bytes);
        stats["write_bytes"] = to_string(_write_bytes);
        stats["cas_bytes"] = to_string(_cas_bytes);

        stats["total_reads"] = to_string(_total_reads);
        stats["total_writes"] = to_string(_total_writes);
        stats["total_cas"] = to_string(_total_cas);
        stats["total_cas_failures"] = to_string(_total_cas_failures);

        stats["insert_path_lengths"] = array_to_string(_insert_path_lengths);
        stats["index_range_per_insert"] = array_to_string(_index_range_per_insert);
        stats["messages_per_insert"] = array_to_string(_messages_per_insert);
        stats["completed_inserts"] = key_array_to_string(_completed_inserts);
        stats["completed_insert_count"] = to_string(_completed_insert_count);
        stats["failed_inserts"] = key_array_to_string(_failed_inserts);
        stats["failed_insert_count"] = to_string(_failed_insert_count);
        stats["insert_operation_bytes"] = to_string(_insert_operation_bytes);
        stats["insert_operation_messages"] = to_string(_insert_operation_messages);

        stats["insert_rtt"] = array_to_string(_insert_rtt);
        stats["insert_rtt_count"] = to_string(_insert_rtt_count);

        stats["messages_per_read"] = array_to_string(_messages_per_read);
        stats["completed_reads"] = key_array_to_string(_completed_reads);
        stats["completed_read_count"] = to_string(_completed_read_count);
        stats["failed_reads"] = key_array_to_string(_failed_reads);
        stats["failed_read_count"] = to_string(_failed_read_count);
        stats["read_operation_bytes"] = to_string(_read_operation_bytes);
        stats["read_operation_messages"] = to_string(_read_operation_messages);
        stats["read_rtt"] = array_to_string(_read_rtt);
        stats["read_rtt_count"] = to_string(_read_rtt_count);

        return stats;
    }

    void State_Machine::update_message_stats(vector<VRMessage> messages){

        for (auto message : messages) {
            uint32_t message_size_bytes = message.get_message_size_bytes();
            if (_inserting) {
                _current_insert_messages++;
                _insert_operation_bytes += message_size_bytes;
                _insert_operation_messages++;
            } else if (_reading) {
                _current_read_messages++;
                _read_operation_bytes += message_size_bytes;
                _read_operation_messages++;
            } else {
                printf("ERROR: update_message_stats called when not reading or inserting\n");
                throw logic_error("ERROR: update_message_stats called when not reading or inserting");
            }

            _total_bytes += message_size_bytes;
            switch (message.get_message_type()){
            case READ_REQUEST:
                _total_reads++;
                _read_bytes += message_size_bytes;
                break;
            case READ_RESPONSE:
                _read_bytes += message_size_bytes;
                break;
            case WRITE_REQUEST:
                printf("TODO track writes\n");
                break;
            case WRITE_RESPONSE:
                printf("TODO track writes\n");
                break;
            case CAS_REQUEST:
            case MASKED_CAS_REQUEST:
                _total_cas++;
                _cas_bytes += message_size_bytes;
                break;
            case CAS_RESPONSE:
            case MASKED_CAS_RESPONSE:
                _cas_bytes += message_size_bytes;
                if (message.function_args["success"] == "false") {
                    _total_cas_failures++;
                }
                break;
            default:
                printf("ERROR: unknown message type\n");
                throw logic_error("ERROR: unknown message type");
            }
        }

    }

    vector<VRMessage> State_Machine::fsm(vector<VRMessage> messages) {
        update_message_stats(messages);
        vector<VRMessage> output_messages = fsm_logic(messages);
        update_message_stats(output_messages);
        return output_messages;
    }

    vector<VRMessage> State_Machine::fsm_logic(vector<VRMessage> messages) {
        printf("FSM Logic must be implemented by a subclass\n");
        throw logic_error("FSM Logic must be implemented by a subclass");
    }

    Client_State_Machine::Client_State_Machine() {
        printf("client state machine constructor with no argument\n");

    }


    Client_State_Machine::Client_State_Machine(unordered_map<string,string> config) : State_Machine(config) {
        printf("client state machine constructor with config argument\n");
    }

    string Client_State_Machine::get_state_machine_name() {
        return "Client State Machine Super Class";
    }

}
