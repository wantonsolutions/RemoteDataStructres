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
            void set_search_function(unordered_map<string, string> config);
            void set_location_function(unordered_map<string, string> config);

            vector<VRMessage> fsm_logic(VRMessage messages);
            vector<VRMessage> get();
            vector<VRMessage> put();

            void clear_statistics();
            string get_state_machine_name();

        private:

            unsigned int _read_threshold_bytes;
            unsigned int _buckets_per_lock;

            Table _table;
            Key _current_insert_key;
            vector<Key> _search_path;
            int _search_path_index;
            vector<int> _locks_held;
            vector<VRMessage> _current_locking_messages;
            vector<VRMessage> _current_locking_read_messages;
            int _current_locking_message_index;


            hash_locations  (*_location_function)(string, unsigned int);

            vector<path_element> (RCuckoo::*_table_search_function)(vector<unsigned int> searchable_buckets);
            vector<path_element> a_star_insert_self(vector<unsigned int> searchable_buckets);
            vector<path_element> random_insert_self(vector<unsigned int> searchable_buckets);
    };
}

#endif
