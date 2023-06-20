// #include "state_machines.h"
#include <string>
#include <unordered_map>
#include <iostream>
#include <sstream>
#include <iterator>
#include <vector>
#include <set>
#include "cuckoo.h"
#include "tables.h"
#include "search.h"
#include "hash.h"

#define DEBUG

using namespace std;
using namespace cuckoo_search;
using namespace cuckoo_state_machines;
namespace cuckoo_rcuckoo {
    RCuckoo::RCuckoo() : Client_State_Machine() {
        _table = Table();
        printf("RCUCKOO no args, get outta here\n");
        exit(0);
    }

    RCuckoo::RCuckoo(unordered_map<string, string> config) : Client_State_Machine(config) {
        //Try to init the table
        try {
            unsigned int memory_size = stoi(config["memory_size"]);
            unsigned int bucket_size = stoi(config["bucket_size"]);
            unsigned int buckets_per_lock = stoi(config["buckets_per_lock"]);
            printf("Creating Table : Table_size: %d, bucket_size %d, buckets_per_lock %d\n", memory_size, bucket_size, buckets_per_lock);
            _table = Table(memory_size, bucket_size, buckets_per_lock);
        } catch (exception& e) {
            printf("ERROR: Memory_State_Machine config missing required field\n");
            throw logic_error("ERROR: Memory_State_Machine config missing required field");
        }

        clear_statistics();
        //try to parse state machine config
        try{
            _read_threshold_bytes = stoi(config["read_threshold_bytes"]);
            _buckets_per_lock = stoi(config["buckets_per_lock"]);
            _locks_per_message = stoi(config["locks_per_message"]);

        } catch (exception& e) {
            printf("ERROR: RCuckoo config missing required field\n");
            throw logic_error("ERROR: RCuckoo config missing required field");
        }
        assert(_read_threshold_bytes > 0);
        assert(_read_threshold_bytes > _table.row_size_bytes());

        set_search_function(config);
        set_location_function(config);
    }

    string RCuckoo::get_state_machine_name() {
        return "RCuckoo";
    }

    void RCuckoo::clear_statistics(){
        Client_State_Machine::clear_statistics();
        _current_insert_key = Key();
        _search_path = vector<path_element>();
        _search_path_index = 0;
        _locks_held = vector<unsigned int>();
        _current_locking_messages = vector<VRMessage>();
        _current_locking_read_messages = vector<VRMessage>();
        _locking_message_index = 0;
        return;
    }

    void RCuckoo::set_search_function(unordered_map<string, string> config) {
        string search_function_name = config["search_function"];
        printf("setting search function: %s\n",search_function_name);
        if (search_function_name == "a_star") {
            _table_search_function = &RCuckoo::a_star_insert_self;
        } else if (search_function_name == "random") {
            _table_search_function = &RCuckoo::random_insert_self;
        } else {
            printf("ERROR: Invalid search function name\n");
            throw logic_error("ERROR: Invalid search function name");
        }
    }

    void RCuckoo::set_location_function(unordered_map<string, string> config) {
        string location_function_name = config["location_function"];
        if (location_function_name == "dependent") {
            _location_function = &rcuckoo_hash_locations;
        } else if (location_function_name == "independent") {
            _location_function = &rcuckoo_hash_locations_independent;
        } else {
            printf("ERROR: Invalid location function name\n");
            throw logic_error("ERROR: Invalid location function name");
        }
    }

    vector<path_element> RCuckoo::a_star_insert_self(vector<unsigned int> searchable_buckets) {
        return bucket_cuckoo_a_star_insert(_table, _location_function, _current_insert_key, searchable_buckets);
    }

    vector<path_element> RCuckoo::random_insert_self(vector<unsigned int> searchable_buckets) {
        return bucket_cuckoo_random_insert(_table, _location_function, _current_insert_key, searchable_buckets);
    }

    vector<VRMessage> RCuckoo::get(){
        _current_read_rtt++;
        vector<VRMessage> messages = read_threshold_message(_location_function, _current_read_key, _read_threshold_bytes, _table.get_row_count(), _table.row_size_bytes());
        return Client_State_Machine::begin_read(messages);
    }

    vector<VRMessage> RCuckoo::get_current_locking_message_with_covering_read() {
        VRMessage lock_message = _current_locking_messages[_locking_message_index];
        VRMessage read_message = get_covering_read_from_lock_message(lock_message, _buckets_per_lock, _table.row_size_bytes());
        _outstanding_read_requests++;
        vector<VRMessage> lock_and_read_messages = {lock_message, read_message};
        return lock_and_read_messages;

    }

    vector<VRMessage> RCuckoo::aquire_locks() {
        _locking_message_index = 0;
        vector<unsigned int> buckets = search_path_to_buckets(_search_path);
        
        #ifdef DEBUG
        stringstream ss;
        copy(buckets.begin(), buckets.end(), ostream_iterator<unsigned int>(ss, " "));
        printf("gathering locks for buckets %s\n", ss.str().c_str());
        #endif

        vector<VRMaskedCasData> lock_list = get_lock_list(buckets, _buckets_per_lock, _locks_per_message);
        vector<VRMessage> masked_cas_messages = create_masked_cas_messages_from_lock_list(lock_list);
        _current_locking_messages = masked_cas_messages;
        return get_current_locking_message_with_covering_read();
    }

    vector<VRMessage> RCuckoo::search() {
        printf("RCuckoo Enter Search\n");
        vector<unsigned int> searchable_buckets;
        _search_path = (this->*_table_search_function)(searchable_buckets);
        //Search failed
        if (_search_path.size() <= 0) {
            printf("Search Failed for key %s unable to continue client %d is done\n", _current_insert_key.to_string().c_str(), _id);
            _complete=true;
            _state = IDLE;
            return vector<VRMessage>();
        }

        #ifdef DEBUG
        printf("Current insert Value %s\n", _current_insert_key.to_string().c_str());
        printf("Successful local search for [key %s] -> [path %s]\n", _current_insert_key.to_string().c_str(), path_to_string(_search_path).c_str());
        #endif

        _state = AQUIRE_LOCKS;
        return aquire_locks();
    }

    vector<VRMessage> RCuckoo::put() {
        _current_insert_rtt++;
        return search();
    }

    vector<VRMessage> RCuckoo::idle_fsm(VRMessage message) {
        printf("RCuckoo::idle_fsm\n");
        if (message.get_message_type() != NO_OP_MESSAGE) {
            printf("Message: %s\n", message.to_string().c_str());
            throw logic_error("ERROR: Client in idle state received message of type");
        }
        return Client_State_Machine::general_idle_fsm();
    }

    vector<VRMessage> RCuckoo::read_fsm(VRMessage message) {
        read_status rs = wait_for_read_messages_fsm(_table, message, _current_read_key);
        if (rs.complete) {
            _state = IDLE;
            State_Machine::complete_read_stats(rs.success, _current_read_key);
            _reading=false;
        }
        return vector<VRMessage>();
    }


    vector<VRMessage> RCuckoo::fsm_logic(VRMessage message){

        //The client is done
        if (_complete && _state == IDLE) {
            return vector<VRMessage>();
        }

        if (_state == IDLE) {
            return idle_fsm(message);
        }

        if (_state == READING) {
            return read_fsm(message);
        }

        if (_state == AQUIRE_LOCKS) {
            return aquire_locks_with_reads_fsm(message);
        }

        // if (_state == RELEASE_LOCKS_TRY_AGAIN) {
        //     return release_locks_fsm(message);
        // }

        // if (_state == INSERTING) {
        //     return insert_and_release_fsm(message);
        // }
    }

    void RCuckoo::receive_successful_locking_message(VRMessage message) {
        vector<unsigned int> lock_indexes = lock_message_to_lock_indexes(message);
        for (unsigned int i = 0; i < lock_indexes.size(); i++) {
            _locks_held.push_back(lock_indexes[i]);
        }
        //checking that the locks do not have duplicates
        assert(_locks_held.size() == set<unsigned int>(_locks_held.begin(), _locks_held.end()).size());
        _locking_message_index++;
        
    }

    VRMessage RCuckoo::get_prior_locking_message() {
        return _current_locking_messages[_locking_message_index - 1];
    }

    bool RCuckoo::all_locks_aquired() {
        return (_locking_message_index >= _current_locking_messages.size() && _locks_held.size() > 0);
    }

    bool RCuckoo::read_complete() {
        return (_outstanding_read_requests == 0);
    }

    vector<VRMessage> RCuckoo::begin_insert() {
        _state = INSERTING;
        printf("TODO we got here motherfucker!! -- if you get here today congrats");
        exit(0);


    }

    vector<VRMessage> RCuckoo::aquire_locks_with_reads_fsm(VRMessage message) {
        if (message.get_message_type() == MASKED_CAS_RESPONSE) {
            try {
                assert(message.function_args["success"] == "True" || message.function_args["success"] == "false");
                bool success = message.function_args["success"] == "True";
                if (success) {
                    printf("Client %d successfully aquired lock %s\n", _id, message.function_args["lock_id"].c_str());
                    printf("message received %s\n", message.to_string().c_str());
                    VRMessage issued_locking_message = get_prior_locking_message();
                    receive_successful_locking_message(issued_locking_message);
                }
                if (!all_locks_aquired()) {
                    _current_insert_rtt++;
                    return get_current_locking_message_with_covering_read();
                }
            } catch (const std::out_of_range& oor) {
                printf("ERROR: Client %d received malformed masked cas response %s\n", _id, message.to_string().c_str());
                throw logic_error("ERROR: Client received malformed masked cas response");
            }
        }

        if (message.get_message_type() == READ_RESPONSE) {
            fill_local_table_with_read_response(_table, message.function_args);
            _outstanding_read_requests--;
            if (all_locks_aquired() && read_complete()) {
                return begin_insert();
            }
        }

        return vector<VRMessage>();
    }


}