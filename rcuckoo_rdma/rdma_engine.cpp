
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
#include <linux/kernel.h>
#include <sched.h>

#include <chrono>


using namespace std;
using namespace cuckoo_state_machines;
using namespace rdma_helper;
using namespace cuckoo_rcuckoo;


#define MAX_THREADS 40
volatile bool global_start_flag = false;
volatile bool global_prime_flag = false;
volatile bool global_end_flag = false;
RCuckoo *rcuckoo_state_machines[MAX_THREADS];

const int yeti_core_order[40]={1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63,65,67,69,71,73,75,77,79};
const int yeti_control_core = 0;
int yak_core_order[24]={0,2,4,6,8,10,12,14,16,18,20,22,1,3,5,7,9,11,13,15,17,19,21,23};
int yak_control_core = 0;


int stick_thread_to_core(pthread_t thread, int core_id) {
  int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
  if (core_id < 0 || core_id >= num_cores) {
        ALERT("CORE_PIN_DEATH","%s: core_id %d invalid total cores %d\n", __func__, core_id,_SC_NPROCESSORS_ONLN);
        exit(0);
  }
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  return pthread_setaffinity_np(thread, sizeof(pthread_t), &cpuset);
}

typedef struct rcuckoo_init_arg{
    unordered_map <string, string> config;
    RDMAConnectionManager * cm;
    int id;
} rcuckoo_init_arg;

void * rcuckoo_thread_init(void * arg) {
    rcuckoo_init_arg * rcuckoo_arg = (rcuckoo_init_arg *) arg;
    unordered_map <string, string> _config;
    std::copy(rcuckoo_arg->config.begin(), rcuckoo_arg->config.end(), std::inserter(_config, _config.end()));
    

    ALERT("RDMA Engine", "Rcuckoo instace %i\n", rcuckoo_arg->id);
    _config["id"]=to_string(rcuckoo_arg->id);
    rcuckoo_state_machines[rcuckoo_arg->id] = new RCuckoo(_config);
    // _state_machines[i]._id = i;
    // _state_machines[i]._rcuckoo = new RCuckoo(config);

    struct rcuckoo_rdma_info info;
    info.qp = rcuckoo_arg->cm->client_qp[rcuckoo_arg->id];
    info.completion_queue = rcuckoo_arg->cm->client_cq_threads[rcuckoo_arg->id];
    info.pd = rcuckoo_arg->cm->pd;
    // _state_machines[i]._rcuckoo->init_rdma_structures(info);
    rcuckoo_state_machines[rcuckoo_arg->id]->init_rdma_structures(info);


    pthread_exit(NULL);
}

namespace cuckoo_rdma_engine {

    void * cuckoo_fsm_runner(void * args){
        VERBOSE("RDMA Engine","launching threads in a cuckoo fsm runner\n");
        RCuckoo * cuck = (RCuckoo *) args;
        cuck->rdma_fsm();
        pthread_exit(NULL);
    }
    
    RDMA_Engine::RDMA_Engine(){
        ALERT("RDMA Engine", "Don't blindly allocate an RDMA state machine");
        exit(1);
    }

            
    RDMA_Engine::RDMA_Engine(unordered_map<string, string> config) {

        _config = config;


        try {
            _num_clients = stoi(config["num_clients"]);
            _prime = (config["prime"] == "true");

        } catch (exception &e) {
            ALERT("RDMA Engine",": unable to parse rdma engine config %s", e.what());
            exit(1);
        } 

        VERBOSE("RDMA Engine",": Initializing RDMA Engine for %d clients\n");
        for(int i=0;i<_num_clients;i++) {
            VERBOSE("RDMA Engine",": Initializing RDMA Engine for client %d\n", i);
            _state_machines.push_back(State_Machine_Wrapper());
        }


        try {
            RDMAConnectionManagerArguments args;


            args.num_qps = _num_clients;
            if (args.num_qps < 1) {
                ALERT("RDMA Engine", "Error: num_qps must be at least 1\n");
                exit(1);
            }
            #define MAX_RDMA_ENGINE_QPS MAX_THREADS
            if (args.num_qps > MAX_RDMA_ENGINE_QPS) {
                ALERT("RDMA Engine", "Error: num_qps must be at most %d, we are only enabling a few QP per process\n", MAX_RDMA_ENGINE_QPS);
                ALERT("RDMA Engine", "TODO; we probably need a better way to scale clients if we are going more than this.\n");
                exit(1);
            }

            //resolve the address from the config
            struct sockaddr_in server_sockaddr = server_address_to_socket_addr(config["server_address"]);
            args.server_sockaddr = &server_sockaddr;

            INFO("RDMA Engine","assigning base_port %s\n", config["base_port"].c_str());
            args.base_port = stoi(config["base_port"]);

            // _connection_manager = RDMAConnectionManager(args);
            _connection_manager = new RDMAConnectionManager(args);
            VERBOSE("RDMA Engine", "RDMAConnectionManager created\n");
        } catch (exception& e) {
            ALERT("RDMA Engine", "RDMAConnectionManager failed to create\n");
            ALERT("RDMA Engine", "Error: %s\n", e.what());
            exit(1);
            return;
        }

        int i;


        pthread_t thread_ids[MAX_THREADS];
        rcuckoo_init_arg rcuckoo_args[MAX_THREADS];
        try {
            for (i=0;i<_num_clients;i++) {

                rcuckoo_args[i].config = config;
                rcuckoo_args[i].cm = _connection_manager;
                rcuckoo_args[i].id = i;
                pthread_create(&thread_ids[i], NULL, rcuckoo_thread_init,&rcuckoo_args[i]);

                // ALERT("RDMA Engine", "Rcuckoo instace %i\n", i);
                // config["id"] = to_string(i);
                // rcuckoo_state_machines[i] = new RCuckoo(config);
                // // _state_machines[i]._id = i;
                // // _state_machines[i]._rcuckoo = new RCuckoo(config);

                // struct rcuckoo_rdma_info info;
                // info.qp = _connection_manager->client_qp[i];
                // info.completion_queue = _connection_manager->client_cq_threads[i];
                // info.pd = _connection_manager->pd;
                // // _state_machines[i]._rcuckoo->init_rdma_structures(info);
                // rcuckoo_state_machines[i]->init_rdma_structures(info);
            }

        } catch (exception& e) {
            ALERT("RDMA Engine", "RDMAConnectionManager failed to create state machine wrapper %i\n", i);
            ALERT("RDMA Engine", "Error: %s\n", e.what());
            exit(1);
            return;
        }

        for (i=0;i<_num_clients;i++) {
            ALERT("RDMA Engine", "Joinging Thread %d\n",i);
            pthread_join(thread_ids[i], NULL);
        }
        ALERT("RDMA Engine", "Init Cuckoo Threads Joined\n");
        return;
    }


    experiment_control * RDMA_Engine::get_experiment_control(){
        return memcached_get_experiment_control();
    }

    memory_stats * RDMA_Engine::get_memory_stats(){
        int wait_coutner = 0;
        while (true) {
            memory_stats * stats = memcached_get_memory_stats();
            if (stats->finished_run) {
                return stats;
            }
            wait_coutner++;
            usleep(1000);
            printf("Waiting for the memory server to deliver the stats %d\n", wait_coutner);

            if (wait_coutner > 1000) {
                printf("Error: the memory server is not responding\n");
                exit(1);
            }
        }
    }

    bool RDMA_Engine::start() {
        VERBOSE("RDMA Engine", "starting rdma engine\n");
        VERBOSE("RDMA Engine", "for the moment just start the first of the state machines\n");
        assert(_num_clients <= MAX_THREADS);
        if (_num_clients > MAX_THREADS) {
            ALERT("RDMA Engine", "Error: num_clients must be at most %d, we are only enabling a few QP per process\n", MAX_THREADS);
            ALERT("RDMA Engine", "TODO; we probably need a better way to scale clients if we are going more than this.\n");
            exit(1);
        }

        //if we are not priming then set the prime flag right away
        if(!_prime){
            global_prime_flag=true;
        }

        pthread_t thread_ids[MAX_THREADS];
        for(int i=0;i<_num_clients;i++){
            rcuckoo_state_machines[i]->set_global_start_flag(&global_start_flag);
            rcuckoo_state_machines[i]->set_global_end_flag(&global_end_flag);
            rcuckoo_state_machines[i]->set_global_prime_flag(&global_prime_flag);
        }
        for (int i=0;i<_num_clients;i++) {
            INFO("RDMA Engine","Creating Client Thread %d\n", i);
            pthread_create(&thread_ids[i], NULL, &cuckoo_fsm_runner, (rcuckoo_state_machines[i]));
            stick_thread_to_core(thread_ids[i], yeti_core_order[i]);
        }
        stick_this_thread_to_core(yeti_control_core);

        using std::chrono::high_resolution_clock;
        using std::chrono::duration_cast;
        using std::chrono::duration;
        using std::chrono::milliseconds;

        while(true){
            experiment_control *ec = get_experiment_control();
            if(ec->experiment_start){
                ALERT("RDMA Engine", "Experiment Starting Globally\n");
                global_start_flag = true;
                break;
            }
        }

        //Start the treads
        auto t1 = high_resolution_clock::now();
        bool priming_action_taken = false;

        while(true){
            experiment_control *ec = get_experiment_control();
            if(ec->experiment_stop){
                ALERT("RDMA Engine", "Experiment Stop Globally\n");
                global_end_flag = true;
                break;
            }
            //reset statistics if we are doing a priming run
            if(_prime &&
             ec->priming_complete && 
             !priming_action_taken) {
                ALERT("RDMA Engine", "Experiment Priming Complete -- do priming things\n");
                priming_action_taken = true;
                global_prime_flag = true;
                // for(int i=0;i<_num_clients;i++){
                //     rcuckoo_state_machines[i]->clear_statistics();
                // }
                // global_prime_flag = false;
                t1=high_resolution_clock::now();
            }
            if(global_end_flag == true) {
                break;
            }
            // free(ec);
        }
        auto t2 = high_resolution_clock::now();
        auto ms_int = duration_cast<milliseconds>(t2 - t1);

        //Get all of the threads to join
        for (int i=0;i<_num_clients;i++){
            INFO("RDMA Engine", "Joining Client Thread %d\n", i);
            pthread_join(thread_ids[i],NULL);
        }
        SUCCESS("RDMA Engine", "Experiment Complete\n");


        //Collect statistics from each of the threads
        vector<unordered_map<string,string>> client_statistics;
        for (int i=0;i<_num_clients;i++) {
            INFO("RDMA Engine", "Grabbing Statistics Off of Client Thread %d\n", i);
            client_statistics.push_back(rcuckoo_state_machines[i]->get_stats());
        }
        SUCCESS("RDMA Engine", "Grabbed Statistics Off of All Client %d Threads\n", _num_clients);

        uint64_t puts = 0;
        uint64_t gets = 0;
        for (int i=0;i<_num_clients;i++) {
            puts += stoull(client_statistics[i]["completed_puts"]);
            gets += stoull(client_statistics[i]["completed_gets"]);
        }

        unordered_map<string,string> system_statistics;
        system_statistics["runtime_ms"] = to_string(ms_int.count());
        system_statistics["runtime_s"]= to_string(ms_int.count() / 1000.0);
        system_statistics["put_throughput"] = to_string(puts / (ms_int.count() / 1000.0));
        system_statistics["get_throughput"] = to_string(gets / (ms_int.count() / 1000.0));
        system_statistics["throughput"]= to_string((puts + gets) / (ms_int.count() / 1000.0));


        float throughput = puts / (ms_int.count() / 1000.0);
        SUCCESS("RDMA Engine", "Throughput: %f\n", throughput);
        ALERT("", "%d,%f\n", _num_clients, throughput);

        memory_stats *ms = get_memory_stats();

        unordered_map<string,string> memory_statistics;
        memory_statistics["fill"]= to_string(ms->fill);




        write_statistics(_config, system_statistics, client_statistics, memory_statistics);
 
        // free(thread_ids);
        VERBOSE("RDMA Engine", "done running state machine!");
        return true;
    }


    State_Machine_Wrapper::State_Machine_Wrapper() {};

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

            messages = _state_machine->fsm(current_ingress_message);
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

