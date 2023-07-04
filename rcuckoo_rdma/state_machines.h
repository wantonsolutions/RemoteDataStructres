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

            vector<VRMessage> fsm(VRMessage messages);
            virtual vector<VRMessage> fsm_logic(VRMessage messages) { printf("fsm_logic not implemented\n"); return vector<VRMessage>(); };


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

    enum ycsb_workload {
        A,
        B,
        C,
        W,
    };

    const char* get_ycsb_workload_name(ycsb_workload workload);

    class Client_Workload_Driver {
        public:
            Client_Workload_Driver();
            Client_Workload_Driver(unordered_map<string, string> config);
            ~Client_Workload_Driver() {}
            unordered_map<string,string> get_stats();
            void set_workload(ycsb_workload workload);
            void set_workload(string workload);
            Request next();
            virtual vector<VRMessage> fsm_logic(VRMessage messages) { printf("fsm_logic not implemented CLIENT STATE MACHINE\n"); return vector<VRMessage>(); };

        private:
            int _total_requests;
            int _client_id;
            int _num_clients;
            bool _deterministic;
            int _random_factor;
            int _completed_requests;
            int _completed_puts;
            int _completed_gets;
            ycsb_workload _workload;
            Request _last_request;

            void record_last_request();
            Key unique_insert(int insert_index, int client_id, int total_clients, int factor);
            Key unique_get(int get_index, int client_id, int total_clients, int factor);
            Request next_put();
            Request next_get();
            operation gen_next_operation();

    };


    enum client_state {
        IDLE,
        READING,
        AQUIRE_LOCKS,
        RELEASE_LOCKS_TRY_AGAIN,
        INSERTING,
    };

    const char* get_client_state_name(client_state state);

    typedef struct read_status {
        bool complete;
        bool success;
    } read_status;

    class Client_State_Machine : public State_Machine {
        public:
            Client_State_Machine();
            Client_State_Machine(unordered_map<string, string> config);
            void set_max_fill(int max_fill);
            ~Client_State_Machine() {}
            void clear_statistics();
            void set_workload(ycsb_workload workload);
            vector<VRMessage> begin_read(vector<VRMessage> messages);
            bool read_complete();
            bool read_successful(Key key);
            read_status wait_for_read_messages_fsm(Table &table, VRMessage message, const Key &key);
            string get_state_machine_name();
            vector<VRMessage> general_idle_fsm();
            unordered_map<string, string> get_stats();
            virtual vector<VRMessage> put();
            virtual vector<VRMessage> get();

        protected:
            uint32_t _total_inserts;
            uint32_t _id;
            client_state _state;
            uint32_t _max_fill;

            //Read State Machine
            Key _current_read_key;
            // Key _current_insert_key;
            uint32_t _outstanding_read_requests;
            uint32_t _read_values_found;
            vector<Key> _read_values;
            uint32_t _duplicates_found;
            Client_Workload_Driver _workload_driver;
    };


    class TableFullException : public std::exception {
    public:
    char * what () {
            char * message = new char[100];
            sprintf(message, "Table is full to the max fill rate -- :) this is the only good exception");
            return message;
        }
    };

    class Memory_State_Machine : public State_Machine {
        public:
            Memory_State_Machine();
            Memory_State_Machine(unordered_map<string, string> config);
            void set_max_fill(int max_fill);
            bool contains_duplicates();
            vector<Duplicate_Entry> get_duplicates();
            bool contains(Key key);
            float get_fill_percentage();
            void print_table();

            void fill_table_with_incremental_values();
            unsigned int get_table_size();
            Entry ** get_table_pointer();
            vector<VRMessage> fsm_logic(VRMessage messages);

        private:
            Table _table;
            uint32_t _max_fill;
    };
}

#endif