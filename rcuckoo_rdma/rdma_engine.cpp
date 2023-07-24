
#include "virtual_rdma.h"
#include "state_machines.h"
#include "rdma_engine.h"
#include "cuckoo.h"
#include <vector>
#include <infiniband/verbs.h>
#include <atomic>

#include "rdma_common.h"
#include "rdma_client_lib.h"
#include "config.h"
#include "log.h"
#include "memcached.h"
#include "rdma_helper.h"

#include <chrono>


using namespace std;
using namespace cuckoo_state_machines;
using namespace rdma_helper;

namespace cuckoo_rdma_engine {
    
    RDMA_Engine::RDMA_Engine(){
        ALERT("RDMA Engine", "Don't blindly allocate an RDMA state machine");
        exit(1);
    }

            
    RDMA_Engine::RDMA_Engine(unordered_map<string, string> config) {


        try {
            _num_clients = stoi(config["num_clients"]);

        } catch (exception &e) {
            ALERT("RDMA Engine",": unable to parse rdma engine config %s", e.what());
            exit(1);
        } 

        VERBOSE("RDMA Engine",": Initializing RDMA Engine for %d clients\n");
        _state_machines = (State_Machine_Wrapper *) calloc(_num_clients, sizeof(State_Machine_Wrapper));

        try {
            RDMAConnectionManagerArguments args;


            args.num_qps = _num_clients;
            if (args.num_qps < 1) {
                printf("Error: num_qps must be at least 1\n");
                exit(1);
            }
            #define MAX_RDMA_ENGINE_QPS 24
            if (args.num_qps > MAX_RDMA_ENGINE_QPS) {
                printf("Error: num_qps must be at most %d, we are only enabling a few QP per process\n", MAX_RDMA_ENGINE_QPS);
                printf("TODO; we probably need a better way to scale clients if we are going more than this.\n");
                exit(1);
            }

            //resolve the address from the config
            struct sockaddr_in server_sockaddr = server_address_to_socket_addr(config["server_address"]);
            args.server_sockaddr = &server_sockaddr;

            printf("assigning base_port %s\n", config["base_port"].c_str());
            args.base_port = stoi(config["base_port"]);

            // _connection_manager = RDMAConnectionManager(args);
            _connection_manager = new RDMAConnectionManager(args);
            printf("RDMAConnectionManager created\n");
        } catch (exception& e) {
            printf("RDMAConnectionManager failed to create\n");
            printf("Error: %s\n", e.what());
            exit(1);
            return;
        }

        int i;
        try {

            for (i=0;i<_num_clients;i++) {
                config["id"] = to_string(i);
                _state_machines[i]._id = i;
                _state_machines[i]._qp = _connection_manager->client_qp[i];
                _state_machines[i]._rcuckoo = new RCuckoo(config);
                _state_machines[i]._state_machine = _state_machines[i]._rcuckoo;
                _state_machines[i]._memory_state_machine = new Memory_State_Machine(config);
                _state_machines[i]._memory_state_machine->set_table_pointer(_state_machines[i]._rcuckoo->get_table_pointer());
                _state_machines[i]._memory_state_machine->set_underlying_lock_table_address(_state_machines[i]._rcuckoo->get_lock_table_pointer());


                //Exchange the connection information on the QP
                //the first index of the Entry** is the beginning of the table
                char * table_pointer = (char *) _state_machines[i]._rcuckoo->get_table_pointer()[0];
                uint32_t table_size = _state_machines[i]._rcuckoo->get_table_size_bytes();

                char * lock_table_pointer = (char *) _state_machines[i]._rcuckoo->get_lock_table_pointer();
                uint32_t lock_table_size = _state_machines[i]._rcuckoo->get_lock_table_size_bytes();

                _state_machines[i]._table_config = memcached_get_table_config();
                assert(_state_machines[i]._table_config != NULL);
                assert(_state_machines[i]._table_config->table_size_bytes == table_size);
                printf("got a table config from the memcached server and it seems to line up\n");
                // rcuckoo->print_table();

                //register the memory
                printf("registering table memory\n");
                // const int extra_table_memory = 256; //I get out of range errros unless i make the table a bit bigger
                _state_machines[i]._table_mr = rdma_buffer_register(_connection_manager->pd, table_pointer, table_size, MEMORY_PERMISSION);

                printf("register lock table memory\n");
                _state_machines[i]._lock_table_mr = rdma_buffer_register(_connection_manager->pd, lock_table_pointer, lock_table_size, MEMORY_PERMISSION);

                //Request a completion queue
                printf("requesting completion queue\n");
                void * context = NULL;
                _state_machines[i]._completion_queue = _connection_manager->client_cq_threads[i];
                if(_state_machines[i]._completion_queue == NULL){
                    printf("completion queue is null\n");
                }


                assert(_state_machines[i]._completion_queue != NULL);

                struct rcuckoo_rdma_info info;
                info.qp= _state_machines[i]._qp;
                info.table_mr = _state_machines[i]._table_mr;
                info.lock_table_mr = _state_machines[i]._lock_table_mr;
                info.completion_queue = _state_machines[i]._completion_queue;
                info.remote_table_config = _state_machines[i]._table_config;
                _state_machines[i]._rcuckoo->init_rdma_structures(info);
            }

        } catch (exception& e) {
            ALERT("RDMA Engin", "RDMAConnectionManager failed to create state machine wrapper %i\n", i);
            ALERT("RDMA Engin", "Error: %s\n", e.what());
            exit(1);
            return;
        }
        printf("returning from engine constructor\n");
        return;
    }


    typedef void * (*THREADFUNCPTR)(void *);
    bool RDMA_Engine::start() {
        VERBOSE("RDMA Engine", "starting rdma engine\n");
        VERBOSE("RDMA Engine", "for the moment just start the first of the state machines\n");
        pthread_t* thread_ids = (pthread_t*) calloc(_num_clients, sizeof(pthread_t));

        bool global_start_flag = false;
        bool global_end_flag = false;


        // bool *global_start_flag = (bool *) calloc(_num_clients, sizeof(bool));
        // bool *global_end_flag = (bool *) calloc(_num_clients, sizeof(bool));

        for(int i=0;i<_num_clients;i++){
            _state_machines[i]._rcuckoo->set_global_start_flag(&global_start_flag);
            _state_machines[i]._rcuckoo->set_global_end_flag(&global_end_flag);
        }
        for (int i=0;i<_num_clients;i++) {
            printf("Creating Client Thread %d\n", i);
            pthread_create(&thread_ids[i], NULL, (THREADFUNCPTR)&State_Machine_Wrapper::start, &_state_machines[i]);
        }

        using std::chrono::high_resolution_clock;
        using std::chrono::duration_cast;
        using std::chrono::duration;
        using std::chrono::milliseconds;

        //Start the treads
        auto t1 = high_resolution_clock::now();
        global_start_flag = true;
        sleep(10);
        global_end_flag = true;
        auto t2 = high_resolution_clock::now();
        auto ms_int = duration_cast<milliseconds>(t2 - t1);

        //Get all of the threads to join
        for (int i=0;i<_num_clients;i++){
            printf("Joining Client Thread %d\n", i);
            pthread_join(thread_ids[i],NULL);
        }


        //Collect statistics from each of the threads
        vector<unordered_map<string,string>> statistics;
        for (int i=0;i<_num_clients;i++) {
            printf("Grabbing Statistics Off of Client Thread %d\n", i);
            statistics.push_back(_state_machines[i]._rcuckoo->get_stats());
        }

        //print statistics
        for (int i=0;i<_num_clients;i++) {
            printf("Printing Statistics Off of Client Thread %d\n", i);
            for (auto it = statistics[i].begin(); it != statistics[i].end(); ++it) {
                printf("%s: %s\n", it->first.c_str(), it->second.c_str());
            }
        }
        uint64_t puts = stoull(statistics[0]["completed_puts"]);
        printf("Throughput: %f\n", puts / (ms_int.count() / 1000.0));


 
        free(thread_ids);
        VERBOSE("RDMA Engine", "done running state machine!");
        return true;
    }

    const char * State_Machine_Wrapper::log_id() {
        return _log_identifier;
    }

    uint64_t State_Machine_Wrapper::local_to_remote_table_address(uint64_t local_address){
        uint64_t base_address = (uint64_t) _rcuckoo->get_table_pointer()[0];
        uint64_t address_offset = local_address - base_address;
        uint64_t remote_address = (uint64_t) _table_config->table_address + address_offset;
        // remote_address += 64 + (sizeof(Entry) * 2);
        return remote_address;
    }

    void State_Machine_Wrapper::send_virtual_read_message(VRMessage message, uint64_t wr_id){
        //translate address locally for bucket id
        unsigned int bucket_offset = stoi(message.function_args["bucket_offset"]);
        unsigned int bucket_id = stoi(message.function_args["bucket_id"]);
        unsigned int size = stoi(message.function_args["size"]);

        uint64_t local_address = (uint64_t) _rcuckoo->get_entry_pointer(bucket_id, bucket_offset);
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
    void State_Machine_Wrapper::send_virtual_cas_message(VRMessage message, uint64_t wr_id){
        uint32_t bucket_id = stoi(message.function_args["bucket_id"]);
        uint32_t bucket_offset = stoi(message.function_args["bucket_offset"]);
        uint64_t old = stoull(message.function_args["old"], nullptr, 16);
        old = __builtin_bswap64(old);
        old = old >> 32;
        uint64_t new_val = stoull(message.function_args["new"], nullptr, 16);
        new_val = __builtin_bswap64(new_val);
        new_val = new_val >> 32;

        uint64_t local_address = (uint64_t) _rcuckoo->get_entry_pointer(bucket_id, bucket_offset);
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


    void State_Machine_Wrapper::send_virtual_masked_cas_message(VRMessage message, uint64_t wr_id) {
        int lock_index = stoi(message.function_args["lock_index"]);
        uint64_t local_lock_address = (uint64_t) _rcuckoo->get_lock_pointer(lock_index);
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


    
    #define MAX_CONCURRENT_MESSAGES 32
    void * State_Machine_Wrapper::start(void * args){
        VRMessage message_tracker[MAX_CONCURRENT_MESSAGES];



        sprintf(_log_identifier, "SMW: %d", _id);
        // debug_masked_cas();

        vector<VRMessage> ingress_messages;
        VRMessage init;
        ingress_messages.push_back(init);


        struct ibv_wc *wc = (struct ibv_wc *) calloc (MAX_CONCURRENT_MESSAGES, sizeof(struct ibv_wc));

        uint64_t wr_id_counter = 0;
        unordered_map<uint64_t, VRMessage> wr_id_to_message;
        vector<VRMessage> messages;

        int outstanding_messages = 0;
        while(true){
            //Pop off an ingress message


            VRMessage current_ingress_message;
            if (ingress_messages.size() > 0){
                current_ingress_message = *ingress_messages.begin();
                ingress_messages.erase(ingress_messages.begin());
            } else {
                // printf("nothing to supply to the state machine, sending in blank message\n");
            }

            // messages = _state_machine->fsm(current_ingress_message);
            messages = _rcuckoo->rdma_fsm(current_ingress_message);
            if (messages.size() > 0){
                ALERT(log_id(), "State Machine returned %d messages\n", messages.size());
                ALERT(log_id(), "State Machine returned %s\n", messages[0].to_string().c_str());
            }

            if (messages.size() == 0 && _state_machine->is_complete()) {
                ALERT(log_id(), "State Machine is complete\n");
                return NULL;
            }
            for (int i = 0; i < messages.size(); i++){
                VERBOSE(log_id(), "Sending message: %s\n", messages[i].to_string().c_str());
                messages[i].type = messages[i].get_message_type();
                bool sent = false;
                switch(messages[i].get_message_type()) {
                    case READ_REQUEST:
                        send_virtual_read_message(messages[i], wr_id_counter);
                        sent = true;
                        break;
                    case CAS_REQUEST:
                        send_virtual_cas_message(messages[i], wr_id_counter);
                        sent = true;
                        break;
                    case MASKED_CAS_REQUEST:
                        send_virtual_masked_cas_message(messages[i], wr_id_counter);
                        sent = true;
                        break;
                    default:
                        ALERT(log_id(), "message type not supported\n");
                        exit(1);
                }
                if (sent) {
                    VERBOSE(log_id(), "writing to wr_id_to_message for id (%d) %s\n", wr_id_counter, messages[i].to_string().c_str());
                    uint64_t modulo_id = wr_id_counter % MAX_CONCURRENT_MESSAGES;
                    message_tracker[modulo_id] = messages[i];
                    message_tracker[modulo_id].type = messages[i].type;
                    message_tracker[modulo_id].function = messages[i].function;

                    for (auto const& x : messages[i].function_args)
                    {
                        message_tracker[modulo_id].function_args[x.first] = x.second;
                    }

                    outstanding_messages++;
                    wr_id_counter++;
                }
            }


            if (outstanding_messages > 0 ) {
                //Now we deal with the message recipt
                int n = bulk_poll(_completion_queue, outstanding_messages, wc);
                if (n < 0) {
                    ALERT(log_id(), "polling failed\n");
                    exit(1);
                }
                for (int j=0;j<n;j++) {
                    if (wc[j].status != IBV_WC_SUCCESS) {
                        ALERT(log_id(), "RDMA read failed with status %s on request %d\n", ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                        exit(1);
                    } else {
                        VERBOSE(log_id(), "Message Received wih work request %d\n", wc[j].wr_id);
                        VRMessage outgoing = message_tracker[wc[j].wr_id % MAX_CONCURRENT_MESSAGES];
                        outstanding_messages--;

                        if (outgoing.get_message_type() == READ_REQUEST){
                            vector<VRMessage> incomming = _memory_state_machine->fsm(outgoing);

                            for (int i = 0; i < incomming.size(); i++){
                                VERBOSE(log_id(), "memory state machine incomming message: %s\n", incomming[i].to_string().c_str());
                                ingress_messages.push_back(incomming[i]);
                            }
                        } else if (outgoing.get_message_type() == MASKED_CAS_REQUEST) {

                            /* at this point in time we should not fail here because only unlock messages are being issued */
                            // printf("received a masked cas response\n");
                            vector<VRMessage> incomming = _memory_state_machine->fsm(outgoing);
                            for (int i = 0; i < incomming.size(); i++){
                                VERBOSE(log_id(), "memory state machine incomming message: %s\n", incomming[i].to_string().c_str());
                                //Now I'm going to check if this failed. If it did I'm not going to set the success flag
                                //At this point the incomming message is filled with the current state of the memory.
                                //In order for the masked cas to be a success the old value of the outgoing message must be the same as what is currently in memory.
                                uint64_t old_value = stoull(outgoing.function_args["old"], 0, 2);
                                uint64_t mask = stoull(outgoing.function_args["mask"], 0, 2);
                                uint64_t current_value = stoull(incomming[i].function_args["old"], 0, 2);
                                if ((old_value != mask) || old_value != (current_value & mask)) {
                                    ALERT(log_id(), "WARNING it seems we failed during unlock (succeding anyways) ");
                                    ALERT(log_id(), "old value %lx != current value %lx (mask %lx)", old_value, current_value, mask);
                                    // incomming[i].function_args["success"] = "0";
                                    incomming[i].function_args["success"] = "1";
                                    throw std::runtime_error("failed to unlock");
                                } else {
                                    // ALERT(log_id(), "SUCCESS unlock old value %lx == current value %lx (mask %lx)", old_value, current_value, mask);
                                    incomming[i].function_args["success"] = "1";
                                }
                                ingress_messages.push_back(incomming[i]);
                            }
                            
                        } else if (outgoing.get_message_type() == CAS_REQUEST) {
                            // printf("received a cas response\n");
                            vector<VRMessage> incomming = _memory_state_machine->fsm(outgoing);
                            for (int i = 0; i < incomming.size(); i++){
                                VERBOSE(log_id(), "memory state machine incomming message: %s\n", incomming[i].to_string().c_str());
                                incomming[i].function_args["success"] = "1";
                                ingress_messages.push_back(incomming[i]);
                            }

                        } else {
                            ALERT(log_id(), "unknown message type %s", outgoing.to_string().c_str());
                            exit(1);
                        }
                    }
                }
                assert(_memory_state_machine->get_table_pointer() == _rcuckoo->get_table_pointer());
            }
        }

    }
    // void RDMA_Engine::debug_masked_cas(){
    //     struct ibv_wc *wc = (struct ibv_wc *) calloc (64, sizeof(struct ibv_wc));
    //     // uint64_t remote_lock_address = 0;
    //     uint64_t remote_lock_address = 16;
    //     // remote_lock_address = 1 << 8; // wtf this seems to work....

    //     // remote_lock_address = __builtin_bswap64(remote_lock_address);
    //     // uint64_t remote_lock_address = ((uint64_t)1) << 0;
    //     uint64_t local_lock_address = (uint64_t) _rcuckoo->get_lock_pointer(0);
    //     uint64_t mask = 1;
    //     uint64_t swap = 1;
    //     uint64_t compare=0;
    //     uint64_t wr_id =0;


    //     printf("local_lock_address %lu\n", local_lock_address);
    //     printf("remote_lock_address %lu\n", remote_lock_address);
    //     printf("compare %lu\n", compare);
    //     printf("swap %lu\n", swap);
    //     printf("mask %lu\n", mask);
    //     printf("_lock_table_mr->lkey %u\n", _lock_table_mr->lkey);
    //     printf("_table_config->lock_table_key %u\n", _table_config->lock_table_key);

    //     printf("remote address %lx\n", remote_lock_address);

    //     bool success = rdmaCompareAndSwapMask(
    //         _connection_manager->client_qp[0],
    //         local_lock_address,
    //         remote_lock_address,
    //         compare,
    //         swap,
    //         _lock_table_mr->lkey,
    //         _table_config->lock_table_key,
    //         mask,
    //         true,
    //         wr_id);


    //     if (!success) {
    //         printf("rdma cas failed\n");
    //         exit(1);
    //     }
    //     int n = bulk_poll(_completion_queue, 1, wc);
    //     if (n < 0) {
    //         printf("polling failed\n");
    //         exit(1);
    //     }
    //     // for (int k=0;k<MAX_CONCURRENT_MESSAGES;k++) {
    //     //     printf("message_tracker sub 1[%d] = %s\n", k, message_tracker[k].to_string().c_str());
    //     // }
    //     printf("-------------------------------------Sending-------------------------------------\n");
    //     for (int j=0;j<n;j++) {
    //         if (wc[j].status != IBV_WC_SUCCESS) {

    //             printf("RDMA masked cas failed with status %s (%d)on request %d\n", ibv_wc_status_str(wc[j].status),wc[j].status, wc[j].wr_id);
    //             // reset_qp(_connection_manager->client_qp[0]);
    //             exit(0);
    //         } else {
    //             printf("Message Received with work request %d\n", wc[j].wr_id);
    //         }

    //     }
    //     printf("completed masked cas debug\n");
    //     exit(0);
    // }

}

