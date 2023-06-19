// #include "state_machines.h"
#include <string>
#include <unordered_map>
#include <iostream>
#include <sstream>
#include <vector>
#include "cuckoo.h"
#include "tables.h"
#include "search.h"
#include "hash.h"

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
    }

    string RCuckoo::get_state_machine_name() {
        return "RCuckoo";
    }

    void RCuckoo::clear_statistics(){
        Client_State_Machine::clear_statistics();
        _current_insert_key = Key();
        _search_path = vector<path_element>();
        _search_path_index = 0;
        _locks_held = vector<int>();
        _current_locking_messages = vector<VRMessage>();
        _current_locking_read_messages = vector<VRMessage>();
        _locking_message_index = 0;
        return;
    }

    void RCuckoo::set_search_function(unordered_map<string, string> config) {
        string search_function_name = config["search_function"];
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

    vector<VRMessage> RCuckoo::aquire_locks() {
        _locking_message_index = 0;
        vector<unsigned int> buckets = search_path_to_buckets(_search_path);
        
        #ifdef DEBUG
        stringstream ss;
        copy(buckets.begin(), buckets.end(), ostream_iterator<unsigned int>(ss, " "));
        printf("gathering locks for buckets %s\n", ss.str().c_str());
        #endif

        vector<VRMaskedCasData> lock_list = get_lock_list(buckets, _buckets_per_lock, _locks_per_message);
        return ;
    }

    vector<VRMessage> RCuckoo::search() {
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
        printf("Current insert Value %s\n", _current_insert_key.to_string());
        printf("Successful local search for [key %s] -> [path %s]\n", _current_insert_key.to_string().c_str(), path_to_string(_search_path).c_str());
        #endif

        _state = AQUIRE_LOCKS;
        return aquire_locks();
    }

    vector<VRMessage> RCuckoo::put() {
        _current_insert_rtt++;
        return search();
    }


}