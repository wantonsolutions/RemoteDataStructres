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
#include "rdma_helper.h"
#include <cassert>

#define DEBUG

using namespace std;
using namespace cuckoo_search;
using namespace cuckoo_state_machines;
using namespace rdma_helper;

namespace cuckoo_rcuckoo {

    Entry ** RCuckoo::get_table_pointer() {
        return _table.get_underlying_table();
    }


    void * RCuckoo::get_lock_table_pointer(){
        return _table.get_underlying_lock_table_address();
    }

    void * RCuckoo::get_lock_pointer(unsigned int lock_index){
        return _table.get_lock_pointer(lock_index);
    }

    unsigned int RCuckoo::get_lock_table_size_bytes(){
        return _table.get_underlying_lock_table_size_bytes();
    }

    void RCuckoo::print_table() {
        _table.print_table();
    }

    
    void RCuckoo::set_global_start_flag(bool * flag) {
        _global_start_flag = flag;
    }

    void RCuckoo::set_global_end_flag(bool * flag) {
        _global_end_flag = flag;
    }


    string vector_to_string(vector<unsigned int> buckets) {

        stringstream ss;
        copy(buckets.begin(), buckets.end(), ostream_iterator<unsigned int>(ss, " "));
        return ss.str();
        // return "";
    }

    unsigned int RCuckoo::get_table_size_bytes() {
        return _table.get_table_size_bytes();
    }

    Entry * RCuckoo::get_entry_pointer(unsigned int bucket_id, unsigned int offset){
        return _table.get_entry_pointer(bucket_id, offset);
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
            assert(_locks_per_message == 64);
            _id = stoi(config["id"]);

        } catch (exception& e) {
            printf("ERROR: RCuckoo config missing required field\n");
            throw logic_error("ERROR: RCuckoo config missing required field");
        }
        assert(_read_threshold_bytes > 0);
        assert(_read_threshold_bytes > _table.row_size_bytes());

        set_search_function(config);
        set_location_function(config);

        sprintf(_log_identifier, "Client: %3d", _id);
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
        // printf("setting search function: %s\n",search_function_name);
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
        return bucket_cuckoo_a_star_insert_fast(_table, _location_function, _current_insert_key, searchable_buckets);
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

        get_lock_list_fast(buckets, _fast_lock_chunks, _lock_list, _buckets_per_lock, _locks_per_message);
        vector<VRMessage> masked_cas_messages = create_masked_cas_messages_from_lock_list(_lock_list);
        _current_locking_messages = masked_cas_messages;
        return get_current_locking_message_with_covering_read();
    }

    vector<VRMessage> RCuckoo::search() {
        vector<unsigned int> searchable_buckets;
        _search_path = (this->*_table_search_function)(searchable_buckets);
        //Search failed
        if (_search_path.size() <= 0) {
            ALERT(log_id(), "Search Failed for key %s unable to continue client %d is done\n", _current_insert_key.to_string().c_str(), _id);
            _complete=true;
            _state = IDLE;
            return vector<VRMessage>();
        }
        VERBOSE("search", "Successful local search for [key %s] -> [path %s]\n", _current_insert_key.to_string().c_str(), path_to_string(_search_path).c_str());
        _state = AQUIRE_LOCKS;
        return aquire_locks();
    }

    vector<VRMessage> RCuckoo::put() {
        _current_insert_rtt++;
        return search();
    }

    vector<VRMessage> RCuckoo::idle_fsm(VRMessage message) {
        if (message.get_message_type() != NO_OP_MESSAGE) {
            ALERT(log_id(),"Message: %s\n", message.to_string().c_str());
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

        // complete_insert_stats(true);
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
            return response;
        }
        assert(message.function_args["success"] == "1" || message.function_args["success"] == "0");
        bool success = (bool)stoi(message.function_args["success"]);

        if (!success) [[unlikely]] {
            ALERT(log_id(), "Masked CAS Failed for key %s\n", _current_insert_key.to_string().c_str());
            ALERT(log_id(), "failed masked cas message: %s\n", message.to_string().c_str());
            exit(1);
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
                    // return put_direct();
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

        if (message.get_message_type() != NO_OP_MESSAGE) {
            VERBOSE(log_id(), "Received Message: %s\n", message.to_string().c_str());
        }
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
            VERBOSE(log_id(), "Sending Message: %s\n", response[i].to_string().c_str());
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

    void RCuckoo::receive_successful_locking_message(VRMaskedCasData message) {
        unsigned int lock_indexes[BITS_IN_MASKED_CAS];
        int total_locks = lock_message_to_lock_indexes(message, lock_indexes);
        for (unsigned int i = 0; i < total_locks; i++) {
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

    void RCuckoo::receive_successful_unlocking_message(VRMaskedCasData message) {
        unsigned int lock_indexes[BITS_IN_MASKED_CAS];
        int total_locks = lock_message_to_lock_indexes(message, lock_indexes);
        for (unsigned int i = 0; i < total_locks; i++) {
            _locks_held.erase(remove(_locks_held.begin(), _locks_held.end(), lock_indexes[i]), _locks_held.end());
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

    void RCuckoo::fill_current_unlock_list() {
        vector<unsigned int> buckets = lock_indexes_to_buckets(_locks_held, _buckets_per_lock);
        //log info buckets
        sort(buckets.begin(), buckets.end());
        assert(is_sorted(buckets.begin(), buckets.end()));
        get_unlock_list_fast(buckets, _fast_lock_chunks, _lock_list, _buckets_per_lock, _locks_per_message);
        //_lock_list is now full of locks
    }

    vector<VRMessage> RCuckoo::release_locks_batched(){
        _locking_message_index=0;
        fill_current_unlock_list();
        _current_locking_messages = create_masked_cas_messages_from_lock_list(_lock_list);
        return _current_locking_messages;
    }


    vector<VRMessage> RCuckoo::begin_insert() {
        _state = INSERTING;
        vector<unsigned int> search_buckets = lock_indexes_to_buckets(_locks_held, _buckets_per_lock);
        _search_path = (this->*_table_search_function)(search_buckets);
        if (_search_path.size() <=0 ) {
            INFO(log_id(), "Second Search Failed for key %s retry time\n", _current_insert_key.to_string().c_str(), _id);
            INFO(log_id(), "Hi Stew, I\'m hoping you have a great day", _current_insert_key.to_string().c_str(), _id);
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
        return insert_messages;
    }

    const char * RCuckoo::log_id() {
        return _log_identifier;
    }

    vector<VRMessage> RCuckoo::aquire_locks_with_reads_fsm(VRMessage message) {
        if (message.get_message_type() == MASKED_CAS_RESPONSE) {
            VERBOSE(log_id(), "got masked cas response: %s\n", message.to_string().c_str());
            try {
                bool success = (bool)stoi(message.function_args["success"]);
                if (success) {
                    VRMessage issued_locking_message = get_current_locking_message();
                    receive_successful_locking_message(issued_locking_message);
                } else {
                    INFO(log_id(), "failed to aquire lock %s\n", message.function_args["lock_index"].c_str());
                }

                if (!all_locks_aquired()) {
                    _current_insert_rtt++;
                    return get_current_locking_message_with_covering_read();
                }
            } catch (exception e) {
                printf("ERROR: Client %d received malformed masked cas response %s\n", _id, message.to_string().c_str());
                throw logic_error("ERROR: Client received malformed masked cas response");
            }
        }

        if (message.get_message_type() == READ_RESPONSE) {
            fill_local_table_with_read_response(_table, message.function_args);
            VERBOSE(log_id(), "local table after read:\n %s", _table.to_string().c_str());
            _outstanding_read_requests--;
            if (all_locks_aquired() && read_complete()) {
                return begin_insert();
            }
        }

        return vector<VRMessage>();
    }

    /******* DIRECT RDMA CALLS ********/

    void RCuckoo::init_rdma_structures(rcuckoo_rdma_info info){ 

        assert(info.qp != NULL);
        assert(info.table_mr != NULL);
        assert(info.lock_table_mr != NULL);
        assert(info.completion_queue != NULL);
        assert(info.remote_table_config != NULL);

        _qp = info.qp;
        _table_mr = info.table_mr;
        _lock_table_mr = info.lock_table_mr;
        _completion_queue = info.completion_queue;
        _table_config = info.remote_table_config;
        _wr_id = 10000000;

        _wc = (struct ibv_wc *) calloc (MAX_CONCURRENT_CUCKOO_MESSAGES, sizeof(struct ibv_wc));

    }


    uint64_t RCuckoo::local_to_remote_table_address(uint64_t local_address){
        uint64_t base_address = (uint64_t) get_table_pointer()[0];
        uint64_t address_offset = local_address - base_address;
        uint64_t remote_address = (uint64_t) _table_config->table_address + address_offset;
        // remote_address += 64 + (sizeof(Entry) * 2);
        return remote_address;
    }

    void RCuckoo::send_virtual_read_message(VRMessage message, uint64_t wr_id){
        //translate address locally for bucket id
        unsigned int bucket_offset = stoi(message.function_args["bucket_offset"]);
        unsigned int bucket_id = stoi(message.function_args["bucket_id"]);
        unsigned int size = stoi(message.function_args["size"]);

        uint64_t local_address = (uint64_t) get_entry_pointer(bucket_id, bucket_offset);
        uint64_t remote_server_address = local_to_remote_table_address(local_address);

        // printf("table size       %d\n", _table_config->table_size_bytes);
        // printf("table address =  %p\n", _rcuckoo->get_table_pointer());
        // printf("offset pointer = %p\n", _rcuckoo->get_entry_pointer(bucket_id, bucket_offset));
        // printf("offset =         %p\n", (void *) (local_address - (uint64_t) _rcuckoo->get_table_pointer()));

        bool success = rdmaRead(
            _qp,
            local_address,
            remote_server_address,
            size,
            _table_mr->lkey,
            _table_config->remote_key,
            true,
            wr_id
        );
        if (!success) {
            printf("rdma read failed\n");
            exit(1);
        }
        VERBOSE("State Machine Wrapper", "sent virtual read message (do do do)\n");
    }

    void RCuckoo::send_virtual_read_message(VRReadData message, uint64_t wr_id) {
        //translate address locally for bucket id
        unsigned int bucket_offset = message.offset;
        unsigned int bucket_id = message.row;
        unsigned int size = message.size;

        uint64_t local_address = (uint64_t) get_entry_pointer(bucket_id, bucket_offset);
        uint64_t remote_server_address = local_to_remote_table_address(local_address);

        // printf("table size       %d\n", _table_config->table_size_bytes);
        // printf("table address =  %p\n", _rcuckoo->get_table_pointer());
        // printf("offset pointer = %p\n", _rcuckoo->get_entry_pointer(bucket_id, bucket_offset));
        // printf("offset =         %p\n", (void *) (local_address - (uint64_t) _rcuckoo->get_table_pointer()));

        // bool success = rdmaRead(
        bool success = rdmaReadExp(
            _qp,
            local_address,
            remote_server_address,
            size,
            _table_mr->lkey,
            _table_config->remote_key,
            true,
            wr_id
        );
        if (!success) {
            printf("rdma read failed\n");
            exit(1);
        }
        VERBOSE("State Machine Wrapper", "sent virtual read message (do do do 2)\n");
        // VERBOSE("State Machine Wrapper", "sent virtual read message\n");

    }

    void RCuckoo::send_virtual_cas_message(VRMessage message, uint64_t wr_id){
        uint32_t bucket_id = stoi(message.function_args["bucket_id"]);
        uint32_t bucket_offset = stoi(message.function_args["bucket_offset"]);
        uint64_t old = stoull(message.function_args["old"], nullptr, 16);
        old = __builtin_bswap64(old);
        old = old >> 32;
        uint64_t new_val = stoull(message.function_args["new"], nullptr, 16);
        new_val = __builtin_bswap64(new_val);
        new_val = new_val >> 32;

        uint64_t local_address = (uint64_t) get_entry_pointer(bucket_id, bucket_offset);
        uint64_t remote_server_address = local_to_remote_table_address(local_address);



        bool success = rdmaCompareAndSwapExp(
            _qp, 
            local_address, 
            remote_server_address,
            old, 
            new_val, 
            _table_mr->lkey,
            _table_config->remote_key, 
            true, 
            wr_id);

        if (!success) {
            printf("rdma cas failed\n");
            exit(1);
        }

    }

    void RCuckoo::send_virtual_cas_message(VRCasData message, uint64_t wr_id){

        // ALERT("sending cas data", "data %s\n", message.to_string().c_str());
        uint64_t local_address = (uint64_t) get_entry_pointer(message.row, message.offset);
        uint64_t remote_server_address = local_to_remote_table_address(local_address);

        bool success = rdmaCompareAndSwap(
            _qp, 
            local_address, 
            remote_server_address,
            message.old, 
            message.new_value, 
            _table_mr->lkey,
            _table_config->remote_key, 
            true, 
            wr_id);

        if (!success) {
            printf("rdma cas failed\n");
            exit(1);
        }

    }

    void RCuckoo::send_virtual_masked_cas_message(VRMessage message, uint64_t wr_id) {
        int lock_index = stoi(message.function_args["lock_index"]);
        uint64_t local_lock_address = (uint64_t) get_lock_pointer(lock_index);
        // uint64_t remote_lock_address = local_to_remote_lock_table_address(local_lock_address);
        uint64_t remote_lock_address = (uint64_t) _table_config->lock_table_address + lock_index;
        // remote_lock_address = 0;

        uint64_t compare = stoull(message.function_args["old"], 0, 2);
        compare = __builtin_bswap64(compare);
        uint64_t swap = stoull(message.function_args["new"], 0, 2);
        swap = __builtin_bswap64(swap);
        uint64_t mask = stoull(message.function_args["mask"], 0, 2);
        mask = __builtin_bswap64(mask);


        // remote_lock_address = __builtin_bswap64(remote_lock_address);


        // VERBOSE(log_id(), "local_lock_address %lu\n", local_lock_address);
        // VERBOSE(log_id(), "remote_lock_address %lu\n", remote_lock_address);
        // VERBOSE(log_id(), "compare %lu\n", compare);
        // VERBOSE(log_id(), "swap %lu\n", swap);
        // VERBOSE(log_id(), "mask %lu\n", mask);
        // VERBOSE(log_id(), "_lock_table_mr->lkey %u\n", _lock_table_mr->lkey);
        // VERBOSE(log_id(), "_table_config->lock_table_key %u\n", _table_config->lock_table_key);

        bool success = rdmaCompareAndSwapMask(
            _qp,
            local_lock_address,
            remote_lock_address,
            compare,
            swap,
            _lock_table_mr->lkey,
            _table_config->lock_table_key,
            mask,
            true,
            wr_id);

        if (!success) {
            printf("rdma masked cas failed failed\n");
            exit(1);
        }
    }

    void RCuckoo::send_virtual_masked_cas_message(VRMaskedCasData message, uint64_t wr_id) {
        uint64_t local_lock_address = (uint64_t) get_lock_pointer(message.min_lock_index);
        // uint64_t remote_lock_address = local_to_remote_lock_table_address(local_lock_address);
        uint64_t remote_lock_address = (uint64_t) _table_config->lock_table_address + message.min_lock_index;
        // remote_lock_address = 0;

        // uint64_t compare = stoull(message.function_args["old"], 0, 2);
        // compare = __builtin_bswap64(compare);
        // uint64_t swap = stoull(message.function_args["new"], 0, 2);
        // swap = __builtin_bswap64(swap);
        // uint64_t mask = stoull(message.function_args["mask"], 0, 2);
        // mask = __builtin_bswap64(mask);

        uint64_t compare = __builtin_bswap64(message.old);
        uint64_t swap = __builtin_bswap64(message.new_value);
        uint64_t mask = __builtin_bswap64(message.mask);


        // remote_lock_address = __builtin_bswap64(remote_lock_address);


        VERBOSE(log_id(), "local_lock_address %lu\n", local_lock_address);
        VERBOSE(log_id(), "remote_lock_address %lu\n", remote_lock_address);
        VERBOSE(log_id(), "compare %lu\n", compare);
        VERBOSE(log_id(), "swap %lu\n", swap);
        VERBOSE(log_id(), "mask %lu\n", mask);
        VERBOSE(log_id(), "_lock_table_mr->lkey %u\n", _lock_table_mr->lkey);
        VERBOSE(log_id(), "_table_config->lock_table_key %u\n", _table_config->lock_table_key);

        bool success = rdmaCompareAndSwapMask(
            _qp,
            local_lock_address,
            remote_lock_address,
            compare,
            swap,
            _lock_table_mr->lkey,
            _table_config->lock_table_key,
            mask,
            true,
            wr_id);

        if (!success) {
            printf("rdma masked cas failed failed\n");
            exit(1);
        }
    }

    void RCuckoo::send_lock_and_cover_message(VRMaskedCasData lock_message, VRReadData read_message, uint64_t wr_id) {
        #define READ_AND_COVER_MESSAGE_COUNT 2
        struct ibv_sge sg [READ_AND_COVER_MESSAGE_COUNT];
        struct ibv_exp_send_wr wr [READ_AND_COVER_MESSAGE_COUNT];

        //Lock
        uint64_t local_lock_address = (uint64_t) get_lock_pointer(lock_message.min_lock_index);
        uint64_t remote_lock_address = (uint64_t) _table_config->lock_table_address + lock_message.min_lock_index;
        uint64_t compare = __builtin_bswap64(lock_message.old);
        uint64_t swap = __builtin_bswap64(lock_message.new_value);
        uint64_t mask = __builtin_bswap64(lock_message.mask);

        setRdmaCompareAndSwapMask(
            &sg[0],
            &wr[0],
            _qp,
            local_lock_address,
            remote_lock_address,
            compare,
            swap,
            _lock_table_mr->lkey,
            _table_config->lock_table_key,
            mask,
            true,
            wr_id);

        uint64_t local_address = (uint64_t) get_entry_pointer(read_message.row, read_message.offset);
        uint64_t remote_server_address = local_to_remote_table_address(local_address);

        //Covering Read
        setRdmaReadExp(
            &sg[1],
            &wr[1],
            local_address,
            remote_server_address,
            read_message.size,
            _table_mr->lkey,
            _table_config->remote_key,
            true,
            wr_id + 1
        );
        
        send_bulk(READ_AND_COVER_MESSAGE_COUNT, _qp, wr);
    }

    void RCuckoo::send_insert_and_unlock_messages(vector<VRCasData> &insert_messages, vector<VRMaskedCasData> & unlock_messages, uint64_t wr_id) {
        #define MAX_INSERT_AND_UNLOCK_MESSAGE_COUNT 64
        struct ibv_sge sg [MAX_INSERT_AND_UNLOCK_MESSAGE_COUNT];
        struct ibv_exp_send_wr wr [MAX_INSERT_AND_UNLOCK_MESSAGE_COUNT];
        assert(insert_messages.size() + unlock_messages.size() <= MAX_INSERT_AND_UNLOCK_MESSAGE_COUNT);

        int total_messages = insert_messages.size() + unlock_messages.size();

        for ( unsigned int i=0; i < insert_messages.size(); i++) {
            uint64_t local_address = (uint64_t) get_entry_pointer(insert_messages[i].row, insert_messages[i].offset);
            uint64_t remote_server_address = local_to_remote_table_address(local_address);
            setRdmaCompareAndSwapExp(
                &sg[i],
                &wr[i],
                _qp, 
                local_address, 
                remote_server_address,
                insert_messages[i].old, 
                insert_messages[i].new_value, 
                _table_mr->lkey,
                _table_config->remote_key, 
                true, 
                wr_id);
            wr_id++;
        }

        for ( unsigned int i=0; i < unlock_messages.size(); i++) {
            uint64_t local_lock_address = (uint64_t) get_lock_pointer(unlock_messages[i].min_lock_index);
            uint64_t remote_lock_address = (uint64_t) _table_config->lock_table_address + unlock_messages[i].min_lock_index;
            uint64_t compare = __builtin_bswap64(unlock_messages[i].old);
            uint64_t swap = __builtin_bswap64(unlock_messages[i].new_value);
            uint64_t mask = __builtin_bswap64(unlock_messages[i].mask);

            setRdmaCompareAndSwapMask(
                &sg[i + insert_messages.size()],
                &wr[i + insert_messages.size()],
                _qp,
                local_lock_address,
                remote_lock_address,
                compare,
                swap,
                _lock_table_mr->lkey,
                _table_config->lock_table_key,
                mask,
                true,
                wr_id);
            wr_id++;
        }
        send_bulk(total_messages, _qp, wr);
        
    }


    void RCuckoo::insert_direct() {

        _state = INSERTING;

        VERBOSE("pre search", "searching in buckets..\n");
        for (int i=0; i< _locks_held.size();i++) {
            VERBOSE("pre search", "lock %d\n", _locks_held[i]);
        }
        vector<unsigned int> search_buckets = lock_indexes_to_buckets(_locks_held, _buckets_per_lock);
        _search_path = (this->*_table_search_function)(search_buckets);
        _search_path_index = _search_path.size() -1;
        if (_search_path.size() <=0 ) {
            WARNING(log_id(), "Second Search Failed for key %s retry time\n", _current_insert_key.to_string().c_str(), _id);
            assert(search_buckets.size() == _locks_held.size());
            for (int i=0; i< search_buckets.size(); i++) {
                WARNING(log_id(), "search_buckets[%d] = %d\n", i, search_buckets[i]);
                WARNING(log_id(), "locks_held[%d] = %d\n", i, _locks_held[i]);
                WARNING(log_id(), "%s\n", _table.row_to_string(search_buckets[i]).c_str());


            }
            INFO(log_id(), "Hi Stew, I\'m hoping you have a great day", _current_insert_key.to_string().c_str(), _id);
            _state = RELEASE_LOCKS_TRY_AGAIN;
        }


        vector<VRCasData> insert_messages;
        // vector<VRMessage> unlock_messages = release_locks_batched();
        _locking_message_index = 0;
        fill_current_unlock_list();
        // _lock_list is now full

        INFO("insert direct", "about to unlock a total of %d lock messages\n", unlock_messages.size());


        unsigned int total_messages = _lock_list.size();
        if (_state == INSERTING) {
            insert_messages = gen_cas_data(_search_path);
            total_messages += insert_messages.size();
        }

        send_insert_and_unlock_messages(insert_messages, _lock_list, _wr_id);
        _wr_id += total_messages;

        //Bulk poll to receive all messages
        int n=0;
        while(n < total_messages) {
            n += bulk_poll(_completion_queue, total_messages - n, _wc + n);
        }

        if (_state == INSERTING) {
            for (unsigned int i = 0; i < insert_messages.size(); i++) {
                if (_wc[i].status != IBV_WC_SUCCESS) {
                    printf("insert failed\n");
                    exit(1);
                }
            }
        }

        for (unsigned int i = total_messages - _lock_list.size() ; i < total_messages; i++) {
            if (_wc[i].status != IBV_WC_SUCCESS) {
                printf("unlock failed\n");
                exit(1);
            }
            //TODO check if pthe unlock failed
            receive_successful_unlocking_message(_lock_list[i-insert_messages.size()]);
        }

        if (_state == INSERTING) {
            complete_insert();
        } else if (_state == RELEASE_LOCKS_TRY_AGAIN) {
            _current_insert_rtt++;
        } else {
            printf("invalid state\n");
            exit(1);
        }
        return;

    }

    void RCuckoo::put_direct() {
        _current_insert_rtt++;

        /* copied from the search function */ 
        vector<unsigned int> searchable_buckets;

        _search_path = (this->*_table_search_function)(searchable_buckets);
        //Search failed
        if (_search_path.size() <= 0) {
            ALERT(log_id(), "Search Failed for key %s unable to continue client %d is done\n", _current_insert_key.to_string().c_str(), _id);
            _complete=true;
            _state = IDLE;
            return;
        }
        VERBOSE("search", "Successful local search for [key %s] -> [path %s]\n", _current_insert_key.to_string().c_str(), path_to_string(_search_path).c_str());
        _state = AQUIRE_LOCKS;


        /* copied from aquire locks function */ 
        _locking_message_index = 0;
        search_path_to_buckets_fast(_search_path, _buckets);
        
        INFO(log_id(), "[aquire_locks] gathering locks for buckets %s\n", vector_to_string(_buckets).c_str());

        get_lock_list_fast(_buckets, _fast_lock_chunks, _lock_list ,_buckets_per_lock, _locks_per_message);
        get_covering_reads_from_lock_list(_lock_list, _covering_reads ,_buckets_per_lock, _table.row_size_bytes());

        for (unsigned int i = 0; i < _lock_list.size(); i++) {
            INFO(log_id(), "[aquire_locks] lock %d -> [lock %s] [read %s]\n", i, lock_list[i].to_string().c_str(), covering_reads[i].to_string().c_str());
        }

        assert(_lock_list.size() == _covering_reads.size());

        bool locking_complete = false;
        bool failed_last_request = false;

        //Inital send TODO probably put this in a while loop
        unsigned int message_index = 0;
        while (!locking_complete) {

            assert(message_index < _lock_list.size());

            VRMaskedCasData lock = _lock_list[message_index];
            VRReadData read = _covering_reads[message_index];

            _wr_id++;
            int outstanding_cas_wr_id = _wr_id;
            _wr_id++;
            int outstanding_read_wr_id = _wr_id;
            send_lock_and_cover_message(lock, read, outstanding_cas_wr_id);

            int outstanding_messages = 2; //It's two because we send the read and CAS
            assert(_completion_queue != NULL);
            int n = bulk_poll(_completion_queue, outstanding_messages, _wc);

            if (n == 1) {
                VERBOSE(log_id(), "first poll missed the read, polling again\n");
                n += bulk_poll(_completion_queue, outstanding_messages - n, _wc + n);
            }

            _outstanding_read_requests--;
            assert(n == outstanding_messages); //This is just a safty for now.
            if (_wc[0].status != IBV_WC_SUCCESS) {
                ALERT("lock aquire", " masked cas failed somehow\n");
                exit(1);
            }

            if (_wc[1].status != IBV_WC_SUCCESS) {
                ALERT("lock aquire", " spanning read failed somehow\n");
                exit(1);
            }

            uint64_t old_value = lock.old;
            uint64_t mask = lock.mask;
            int lock_index = lock.min_lock_index;
            uint64_t received_locks = *((uint64_t*) get_lock_pointer(lock_index));

            //?? bswap the DMA'd value?
            received_locks = __builtin_bswap64(received_locks);
            *((uint64_t*) get_lock_pointer(lock_index)) = received_locks;


            if ((received_locks & mask) == old_value) {
                // ALERT(log_id(), "we got the lock!\n");
                receive_successful_locking_message(lock);
                message_index++;
                if (failed_last_request) {
                    failed_last_request = false;
                }
            } else {
                failed_last_request = true;
            }

            if (_lock_list.size() == message_index) {
                locking_complete = true;
                SUCCESS(log_id(), " [put-direct] we got all the locks!\n");
                break;
            }
            _current_insert_rtt++;
        }

        return insert_direct();
    }


    vector<VRMessage> RCuckoo::rdma_fsm(VRMessage message) {
        if (message.get_message_type() != NO_OP_MESSAGE) {
            VERBOSE(log_id(), "Received Message: %s\n", message.to_string().c_str());
        }
        vector<VRMessage> response = vector<VRMessage>();

        //Hold here until the global start flag is set
        while(!*_global_start_flag){
            ALERT(log_id(), "not globally started");
        };
        ALERT(log_id(),"Starting RDMA FSM Start Flag Set\n");
        while(!*_global_end_flag) {

            //The client is done
            if (_complete && _state == IDLE) {
                return response;
            }

            Request next_request;
            switch(_state) {
                case IDLE:

                    next_request = _workload_driver.next();
                    VERBOSE("DEBUG: general idle fsm","Generated New Request: %s\n", next_request.to_string().c_str());

                    if (next_request.op == NO_OP) {
                            break;
                    } else if (next_request.op == PUT) {
                        _current_insert_key = next_request.key;
                        put_direct();
                    } else if (next_request.op == GET) {
                        _current_read_key = next_request.key;
                        response=get();
                    } else {
                        printf("ERROR: unknown operation\n");
                        throw logic_error("ERROR: unknown operation");
                    }
                    break;


                case READING:
                    read_fsm(message);
                    break;
                case RELEASE_LOCKS_TRY_AGAIN:
                    put_direct();
                    break;
                default:
                    throw logic_error("ERROR: Invalid state");
            }

            for (unsigned int i = 0; i < response.size(); i++) {
                VERBOSE(log_id(), "Sending Message: %s\n", response[i].to_string().c_str());
            }

        }
        ALERT(log_id(), "BREAKING EXIT FLAG!!\n");

        if(*_global_end_flag == true) {
            _complete = true;
        }

        return response;
    }
}



