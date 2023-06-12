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
    unordered_map<string, any> State_Machine::get_stats(){
        unordered_map<string, any> stats;
        stats["total_bytes"] = _total_bytes;
        stats["read_bytes"] = _read_bytes;
        stats["write_bytes"] = _write_bytes;
        stats["cas_bytes"] = _cas_bytes;

        stats["total_reads"] = _total_reads;
        stats["total_writes"] = _total_writes;
        stats["total_cas"] = _total_cas;
        stats["total_cas_failures"] = _total_cas_failures;

        stats["insert_path_lengths"] = _insert_path_lengths;
        stats["index_range_per_insert"] = _index_range_per_insert;
        stats["messages_per_insert"] = _messages_per_insert;
        stats["completed_inserts"] = _completed_inserts;
        stats["completed_insert_count"] = _completed_insert_count;
        stats["failed_inserts"] = _failed_inserts;
        stats["failed_insert_count"] = _failed_insert_count;
        stats["insert_operation_bytes"] = _insert_operation_bytes;
        stats["insert_operation_messages"] = _insert_operation_messages;

        stats["insert_rtt"] = _insert_rtt;
        stats["insert_rtt_count"] = _insert_rtt_count;

        stats["messages_per_read"] = _messages_per_read;
        stats["completed_reads"] = _completed_reads;
        stats["completed_read_count"] = _completed_read_count;
        stats["failed_reads"] = _failed_reads;
        stats["failed_read_count"] = _failed_read_count;
        stats["read_operation_bytes"] = _read_operation_bytes;
        stats["read_operation_messages"] = _read_operation_messages;
        stats["read_rtt"] = _read_rtt;
        stats["read_rtt_count"] = _read_rtt_count;

        return stats;
    }

    void State_Machine::update_message_stats(vector<VRMessage> messages){
        printf("TODO implement update virtual message stats");
    }

    vector<VRMessage> State_Machine::fsm() {
        printf("TODO implement fsm");
        return vector<VRMessage>();
    }

    vector<VRMessage> State_Machine::fsm_logic(vector<VRMessage> messages) {
        printf("FSM Logic must be implemented by a subclass");
        throw logic_error("FSM Logic must be implemented by a subclass");
    }

}
