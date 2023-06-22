// #include "state_machines.h"
#include <string>
#include <unordered_map>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <iterator>
#include <vector>
#include <set>
#include "cuckoo.h"
#include "tables.h"
#include "search.h"
#include "hash.h"
#include "log.h"

#define DEBUG

using namespace std;
using namespace cuckoo_search;
using namespace cuckoo_state_machines;
namespace cuckoo_rcuckoo {

    string vector_to_string(vector<unsigned int> buckets) {

        stringstream ss;
        copy(buckets.begin(), buckets.end(), ostream_iterator<unsigned int>(ss, " "));
        return ss.str();
        // return "";
    }

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
            _id = stoi(config["id"]);

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
        
        INFO(log_id(), "[aquire_locks] gathering locks for buckets %s\n", vector_to_string(buckets).c_str());

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

    void RCuckoo::complete_insert_stats(bool success){
        _insert_path_lengths.push_back(_search_path.size());
        _index_range_per_insert.push_back(path_index_range(_search_path));
        Client_State_Machine::complete_insert_stats(success);
        return;
    }

    void RCuckoo::complete_insert(){
        INFO(log_id(), "[complete_insert] key %s\n", _current_insert_key.to_string().c_str());
        _state = IDLE;
        _inserting = false;
        complete_insert_stats(true);
        return;
    }

    void RCuckoo::insert_cas_fsm(VRMessage message) {
        assert(message.get_message_type() == CAS_RESPONSE);
        //Note: I used to fill the table with my cas response, the issue is that I only get the old value back.
        // fill_local_table_with_cas_response(_table, message.function_args);
        assert(message.function_args["success"] == "1" || message.function_args["success"] == "0");
        bool success = (bool)stoi(message.function_args["success"]);
        if (!success) [[unlikely]] {
            ALERT(log_id(), "CAS Failed for key %s\n", _current_insert_key.to_string().c_str());
            ALERT(log_id(), "failed cas message: %s\n", message.to_string().c_str());
            throw logic_error("ERROR: CAS Failed");
        }
        _search_path_index--;
        if (_search_path_index == 0) {
            VERBOSE(log_id(), "Insert Path Complete for key %s all responses collected\n", _current_insert_key.to_string().c_str());
        } else if (_search_path_index < 0 ) {
            throw logic_error("Error we have received too many CAS responses");
        }
        return;
    }



    vector<VRMessage> RCuckoo::release_locks_fsm(VRMessage message) {
        vector<VRMessage> response;
        if (message.get_message_type() != MASKED_CAS_RESPONSE) {
            VERBOSE(log_id(), "Entered release locks fsm with no masked cas message");
            return response;
        }
        assert(message.function_args["success"] == "1" || message.function_args["success"] == "0");
        bool success = (bool)stoi(message.function_args["success"]);

        if (!success) [[unlikely]] {
            ALERT(log_id(), "Masked CAS Failed for key %s\n", _current_insert_key.to_string().c_str());
            ALERT(log_id(), "failed masked cas message: %s\n", message.to_string().c_str());
            throw logic_error("ERROR: Masked CAS Failed");
        }
        receive_successful_unlocking_message(message);
        if (all_locks_released()){
            assert(_state == INSERTING || _state == RELEASE_LOCKS_TRY_AGAIN);
            switch(_state){
                case INSERTING:
                    complete_insert();
                    return response;
                
                case RELEASE_LOCKS_TRY_AGAIN:
                    return search();
            }
        }
        return response;



    }

    vector<VRMessage> RCuckoo::insert_and_release_fsm(VRMessage message) {
        switch(message.get_message_type()){
            case CAS_RESPONSE:
                insert_cas_fsm(message);
                //insert_cas_fsm has no response
                return vector<VRMessage>();
            case MASKED_CAS_RESPONSE:
                return release_locks_fsm(message);
            case NO_OP_MESSAGE:
                break;
            default:
                throw logic_error("ERROR: Client in insert_and_release_fsm received message of type");
                break;
        }
        return vector<VRMessage>();
    }

    vector<VRMessage> RCuckoo::fsm_logic(VRMessage message){

        ALERT(log_id(), "Received Message: %s\n", message.to_string().c_str());
        vector<VRMessage> response = vector<VRMessage>();


        //The client is done
        if (_complete && _state == IDLE) {
            response = vector<VRMessage>();
            return response;
        }

        switch(_state) {
            case IDLE:
                response = idle_fsm(message);
                break;
            case READING:
                response = read_fsm(message);
                break;
            case AQUIRE_LOCKS:
                response = aquire_locks_with_reads_fsm(message);
                break;
            case RELEASE_LOCKS_TRY_AGAIN:
                response = release_locks_fsm(message);
                break;
            case INSERTING:
                response = insert_and_release_fsm(message);
                break;
            default:
                throw logic_error("ERROR: Invalid state");
        }

        for (unsigned int i = 0; i < response.size(); i++) {
            ALERT(log_id(), "Sending Message: %s\n", response[i].to_string().c_str());
        }
        return response;
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

    void RCuckoo::receive_successful_unlocking_message(VRMessage message) {
        vector<unsigned int> unlock_indexes = lock_message_to_lock_indexes(message);
        for (unsigned int i = 0; i < unlock_indexes.size(); i++) {
            _locks_held.erase(remove(_locks_held.begin(), _locks_held.end(), unlock_indexes[i]), _locks_held.end());
        }
        //checking that the locks do not have duplicates
        assert(_locks_held.size() == set<unsigned int>(_locks_held.begin(), _locks_held.end()).size());
        _locking_message_index++;
    }

    VRMessage RCuckoo::get_prior_locking_message() {
        return _current_locking_messages[_locking_message_index - 1];
    }

    VRMessage RCuckoo::get_current_locking_message() {
        return _current_locking_messages[_locking_message_index];
    }

    bool RCuckoo::all_locks_aquired() {
        return (_locking_message_index >= _current_locking_messages.size() && _locks_held.size() > 0);
    }

    bool RCuckoo::all_locks_released() {
        return (_locks_held.size() == 0);
    }

    bool RCuckoo::read_complete() {
        return (_outstanding_read_requests == 0);
    }

    vector<VRMessage> RCuckoo::retry_insert() {
        _state = RELEASE_LOCKS_TRY_AGAIN;
        return release_locks_batched();
    }

    vector<VRMessage> RCuckoo::release_locks_batched(){
        INFO("release_locks_batched", "Enter releasing locks batched");
        _locking_message_index=0;
        vector<unsigned int> buckets = lock_indexes_to_buckets(_locks_held, _buckets_per_lock);
        //log info buckets
        for (unsigned int i = 0; i < buckets.size(); i++) {
            INFO(log_id(), "bucket %d", buckets[i]);
        }
        assert(is_sorted(buckets.begin(), buckets.end()));
        vector<VRMaskedCasData> unlock_list = get_unlock_list(buckets, _buckets_per_lock, _locks_per_message);
        INFO("release_locks_batched", "unlock list size %d", unlock_list.size());
        _current_locking_messages = create_masked_cas_messages_from_lock_list(unlock_list);
        INFO("release_locks_batched", "Exit releasing locks batched");
        return _current_locking_messages;
    }

    vector<VRMessage> RCuckoo::begin_insert() {
        _state = INSERTING;
        INFO(log_id(), "begin_insert");
        vector<unsigned int> search_buckets = lock_indexes_to_buckets(_locks_held, _buckets_per_lock);
        //print search buckets
        for (unsigned int i = 0; i < search_buckets.size(); i++) {
            INFO(log_id(), "search bucket %d", search_buckets[i]);
        }
        _search_path = (this->*_table_search_function)(search_buckets);
        if (_search_path.size() <=0 ) {
            ALERT(log_id(), "Second Search Failed for key %s retry time\n", _current_insert_key.to_string().c_str(), _id);
            _current_insert_rtt++;
            return retry_insert();
        }
        _search_path_index = _search_path.size() -1;
        vector<VRMessage> insert_messages = gen_cas_messages(_search_path);
        vector<VRMessage> unlock_messages = release_locks_batched();

        for ( unsigned int i = 0; i < unlock_messages.size(); i++) {
            insert_messages.push_back(unlock_messages[i]);
        }
        _current_insert_rtt++;
        INFO(log_id(), "end begin_insert");
        return insert_messages;
    }

    const char * RCuckoo::log_id() {
        return("client " + to_string(_id)).c_str();
    }

    vector<VRMessage> RCuckoo::aquire_locks_with_reads_fsm(VRMessage message) {
        INFO(log_id(), "enter aquire locks");
        INFO(log_id(), "aquire locks with reads fsm Message %s", message.to_string().c_str());
        if (message.get_message_type() == MASKED_CAS_RESPONSE) {
            INFO(log_id(), "got cas response message");
            try {
                bool success = (bool)stoi(message.function_args["success"]);
                if (success) {
                    INFO(log_id(), "successfully aquired lock %s\n", _id, message.function_args["lock_index"].c_str());
                    VRMessage issued_locking_message = get_current_locking_message();
                    INFO(log_id(), "grabbed issued locking message %s", issued_locking_message.to_string().c_str());
                    receive_successful_locking_message(issued_locking_message);
                    INFO(log_id(), "completed successful lock parse and receive");
                } else {
                    ALERT(log_id(), "failed to aquire lock %s\n", _id, message.function_args["lock_index"].c_str());
                }

                if (!all_locks_aquired()) {
                    _current_insert_rtt++;
                    return get_current_locking_message_with_covering_read();
                }
                INFO(log_id(), "Completed parsing masked cas message");
            } catch (exception e) {
                printf("ERROR: Client %d received malformed masked cas response %s\n", _id, message.to_string().c_str());
                throw logic_error("ERROR: Client received malformed masked cas response");
            }
        }

        if (message.get_message_type() == READ_RESPONSE) {
            fill_local_table_with_read_response(_table, message.function_args);
            ALERT(log_id(), "----Local table after read------");
            _table.print_table();
            ALERT(log_id(), "----/Local table after read-----");
            _outstanding_read_requests--;
            if (all_locks_aquired() && read_complete()) {
                return begin_insert();
            }
        }

        return vector<VRMessage>();
    }


}