#pragma once
#ifndef STATE_MACHINES_H
#define STATE_MACHINES_H

#include "tables.h"
#include "virtual_rdma.h"
#include <string>
#include <unordered_map>
#include <vector>
#include <any>

using namespace std;
using namespace cuckoo_virtual_rdma;

namespace cuckoo_state_machines {


    class State_Machine {
        public:
            State_Machine();
            State_Machine(unordered_map<string, string> config);
            ~State_Machine() {}
            void clear_statistics();

            string get_state_machine_name();
            bool is_complete();
            vector<Key> get_completed_inserts();
            void set_max_fill(float max_fill);

            void complete_read_stats(bool success, Key read_key);
            void complete_insert_stats(bool success);
            unordered_map<string, string> get_stats();

            vector<VRMessage> fsm(vector<VRMessage> messages);
            vector<VRMessage> fsm_logic(vector<VRMessage> messages);


        protected:
            unordered_map<string, string> _config;
            bool _complete;
            bool _inserting;
            bool _reading;

            //State Machine Statistics
            uint64_t _total_bytes;
            uint64_t _read_bytes;
            uint64_t _write_bytes;
            uint64_t _cas_bytes;

            uint32_t _total_reads;
            uint32_t _total_writes;
            uint32_t _total_cas;
            uint32_t _total_cas_failures;

            Key _current_insert_key;

            vector<int> _insert_path_lengths;
            vector<int> _index_range_per_insert;
            uint32_t _current_insert_messages;
            vector<int> _messages_per_insert;
            vector<Key> _completed_inserts;
            uint32_t _completed_insert_count;
            vector<Key> _failed_inserts;
            uint32_t _failed_insert_count;
            uint64_t _insert_operation_bytes;
            uint64_t _insert_operation_messages;
            //track rtt
            uint32_t _current_insert_rtt;
            vector<int> _insert_rtt;
            uint64_t _insert_rtt_count;

            //read stats
            uint32_t _current_read_messages;
            vector<int> _messages_per_read;
            vector<Key> _completed_reads;
            uint64_t _completed_read_count;
            vector<Key> _failed_reads;
            uint64_t _failed_read_count;
            uint64_t _read_operation_bytes;
            uint64_t _read_operation_messages;
            uint32_t _current_read_rtt;
            vector<int> _read_rtt;
            uint64_t _read_rtt_count;

        private:
            void update_message_stats(vector<VRMessage>);
    };


    class Client_State_Machine : public State_Machine {
        public:
            Client_State_Machine();
            Client_State_Machine(unordered_map<string, string> config);
            ~Client_State_Machine() {}
            string get_state_machine_name();

    };
}

#endif