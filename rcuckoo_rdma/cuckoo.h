#pragma once
#ifndef CUCKOO_H
#define CUCKOO_H

#include "state_machines.h"
#include "tables.h"
#include "search.h"
#include <unordered_map>

using namespace cuckoo_state_machines;
using namespace cuckoo_search;

namespace cuckoo_rcuckoo {


    class RCuckoo : public Client_State_Machine {
        public:
            RCuckoo();
            RCuckoo(unordered_map<string, string> config);
            ~RCuckoo() {}

            const char * log_id();

            void set_search_function(unordered_map<string, string> config);
            void set_location_function(unordered_map<string, string> config);


            vector<VRMessage> fsm_logic(VRMessage messages);
            vector<VRMessage> get();
            vector<VRMessage> aquire_locks();
            vector<VRMessage> put();
            vector<VRMessage> search();
            vector<VRMessage> begin_insert();
            vector<VRMessage> retry_insert();
            vector<VRMessage> release_locks_batched();
            void insert_cas_fsm(VRMessage message);
            vector<VRMessage> release_locks_fsm(VRMessage message);
            vector <VRMessage> insert_and_release_fsm(VRMessage message);
            vector<VRMessage> get_current_locking_message_with_covering_read();
            void clear_statistics();
            string get_state_machine_name();
            vector<VRMessage> idle_fsm(VRMessage message);
            vector<VRMessage> read_fsm(VRMessage message);
            vector<VRMessage> aquire_locks_with_reads_fsm(VRMessage message);

            void receive_successful_locking_message(VRMessage message);
            void receive_successful_unlocking_message(VRMessage message);


            void complete_insert_stats(bool success);
            void complete_insert();

            //Table and lock table functions
            Entry ** get_table_pointer();
            unsigned int get_table_size_bytes();
            void print_table();
            Entry * get_entry_pointer(unsigned int bucket_id, unsigned int offset);
            void * get_lock_table_pointer();
            unsigned int get_lock_table_size_bytes();
            void * get_lock_pointer(unsigned int lock_index);


            /* RDMA specific functions */
            vector<VRMessage> rdma_fsm(VRMessage message);


        private:

            unsigned int _read_threshold_bytes;
            unsigned int _buckets_per_lock;
            unsigned int _locks_per_message;


            Table _table;
            // Key _current_insert_key;
            vector<path_element> _search_path;
            int _search_path_index;
            vector<unsigned int> _locks_held;
            vector<VRMessage> _current_locking_messages;
            vector<VRMessage> _current_locking_read_messages;
            int _locking_message_index;


            // hash_locations  (*_location_function)(string, unsigned int);
            hash_locations  (*_location_function)(Key, unsigned int);

            vector<path_element> (RCuckoo::*_table_search_function)(vector<unsigned int> searchable_buckets);
            vector<path_element> a_star_insert_self(vector<unsigned int> searchable_buckets);
            vector<path_element> random_insert_self(vector<unsigned int> searchable_buckets);


            bool read_complete();
            bool all_locks_aquired();
            bool all_locks_released();
            VRMessage get_prior_locking_message();
            VRMessage get_current_locking_message();

    };
}

#endif
