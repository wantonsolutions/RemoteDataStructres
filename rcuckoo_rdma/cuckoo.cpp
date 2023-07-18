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

        sprintf(_log_identifier, "Client: %d", _id);
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

        vector<VRMaskedCasData> lock_list = get_lock_list(buckets, _buckets_per_lock, _locks_per_message);
        vector<VRMessage> masked_cas_messages = create_masked_cas_messages_from_lock_list(lock_list);
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
                    // return search();
                    return put_direct();
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
        _locking_message_index=0;
        vector<unsigned int> buckets = lock_indexes_to_buckets(_locks_held, _buckets_per_lock);
        //log info buckets
        assert(is_sorted(buckets.begin(), buckets.end()));
        vector<VRMaskedCasData> unlock_list = get_unlock_list(buckets, _buckets_per_lock, _locks_per_message);
        _current_locking_messages = create_masked_cas_messages_from_lock_list(unlock_list);
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
        VERBOSE("State Machine Wrapper", "sent virtual read message\n");
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



        bool success = rdmaCompareAndSwap(
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

    vector<VRMessage> RCuckoo::put_direct() {
        _current_insert_rtt++;

        /* copied from the search function */ 
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


        /* copied from aquire locks function */ 
        _locking_message_index = 0;
        vector<unsigned int> buckets = search_path_to_buckets(_search_path);
        
        INFO(log_id(), "[aquire_locks] gathering locks for buckets %s\n", vector_to_string(buckets).c_str());

        vector<VRMaskedCasData> lock_list = get_lock_list(buckets, _buckets_per_lock, _locks_per_message);
        vector<VRMessage> masked_cas_messages = create_masked_cas_messages_from_lock_list(lock_list);
        _current_locking_messages = masked_cas_messages;
        
        


        bool locking_complete = false;
        bool failed_last_request = false;

        //Inital send TODO probably put this in a while loop
        while (!locking_complete) {
            vector<VRMessage> lock_and_read =  get_current_locking_message_with_covering_read();
            VRMessage lock_message = lock_and_read[0];
            VRMessage read_message = lock_and_read[1];

            _wr_id++;
            int outstanding_cas_wr_id = _wr_id;
            //TODO obviously combine these two functions into a single one
            send_virtual_masked_cas_message(lock_message, outstanding_cas_wr_id);
            _wr_id++;
            int outstanding_read_wr_id = _wr_id;
            send_virtual_read_message(read_message, outstanding_read_wr_id);

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

            //TODO check for duplicate messages
            //TODO check that we actually got the lock we wanted.
            // ALERT(log_id(), "checking if we actually got the lock!\n");
            uint64_t old_value = stoull(lock_message.function_args["old"], 0, 2);
            uint64_t mask = stoull(lock_message.function_args["mask"], 0, 2);


            int lock_index = stoi(lock_message.function_args["lock_index"]);
            uint64_t received_locks = *((uint64_t*) get_lock_pointer(lock_index));


            //?? bswap the DMA'd value?
            received_locks = __builtin_bswap64(received_locks);
            *((uint64_t*) get_lock_pointer(lock_index)) = received_locks;


            if ((received_locks & mask) == old_value) {
                VERBOSE(log_id(), "we got the lock!\n");
                receive_successful_locking_message(lock_message);
                if (failed_last_request) {
                    failed_last_request = false;
                    // ALERT("put direct", "grabbed some locks after failing!\n");
                    // ALERT(log_id(), "received_locks %016lx\n", received_locks);
                    // ALERT(log_id(), "old_value      %016lx\n", old_value);
                    // ALERT(log_id(), "mask           %016lx\n", mask);
                }
            } else {
                // ALERT("put direct", "we did not get the lock reissuing request!\n");
                // ALERT(log_id(), "received_locks %016lx\n", received_locks);
                // ALERT(log_id(), "old_value      %016lx\n", old_value);
                // ALERT(log_id(), "mask           %016lx\n", mask);
                failed_last_request = true;
            }

            if (all_locks_aquired()) {
                locking_complete = true;
                break;
            }
            _current_insert_rtt++;

            // lock_and_read = get_current_locking_message_with_covering_read();
            // lock_message = lock_and_read[0];
            // read_message = lock_and_read[1];
        }

        return begin_insert();
    }


    vector<VRMessage> RCuckoo::rdma_fsm(VRMessage message) {

        if (message.get_message_type() != NO_OP_MESSAGE) {
            VERBOSE(log_id(), "Received Message: %s\n", message.to_string().c_str());
        }
        vector<VRMessage> response = vector<VRMessage>();


        //The client is done
        if (_complete && _state == IDLE) {
            response = vector<VRMessage>();
            return response;
        }

        Request next_request;
        switch(_state) {
            case IDLE:

                next_request = _workload_driver.next();
                VERBOSE("DEBUG: general idle fsm","Generated New Request: %s\n", next_request.to_string().c_str());

                if (next_request.op == NO_OP) {
                     response = vector<VRMessage>();
                } else if (next_request.op == PUT) {
                    _current_insert_key = next_request.key;
                    response=put_direct();
                } else if (next_request.op == GET) {
                    _current_read_key = next_request.key;
                    response=get();
                } else {
                    printf("ERROR: unknown operation\n");
                    throw logic_error("ERROR: unknown operation");
                }
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
}



