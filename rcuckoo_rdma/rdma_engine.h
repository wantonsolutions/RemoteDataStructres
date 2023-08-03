#pragma once
#ifndef RDMA_ENGINE_H
#define RDMA_ENGINE_H

#include "virtual_rdma.h"
#include "state_machines.h"
#include "cuckoo.h"
#include "rdma_client_lib.h"
#include "config.h"

using namespace cuckoo_state_machines;
using namespace cuckoo_rcuckoo;
#define ID_SIZE 64

namespace cuckoo_rdma_engine {

    //This class is basically a struct
    //It's point is just to hold a bunch of pointers to associated objects
    class State_Machine_Wrapper {
        public:
            State_Machine_Wrapper();
            State_Machine_Wrapper(int id, State_Machine * state_machine, ibv_qp * qp, RCuckoo * rcuckoo, ibv_mr * table_mr, ibv_mr * lock_table_mr, Memory_State_Machine * memory_state_machine, table_config * table_config, struct ibv_cq * completion_queue);

            int _id;
            State_Machine * _state_machine;
            ibv_qp * _qp;
            RCuckoo * _rcuckoo;
            ibv_mr * _table_mr;
            ibv_mr * _lock_table_mr;
            Memory_State_Machine * _memory_state_machine;
            table_config * _table_config;
            struct ibv_cq *_completion_queue;

            void * start(void* args);
            const char * log_id();
            uint64_t local_to_remote_table_address(uint64_t local_address);
            void send_virtual_read_message(VRMessage message, uint64_t wr_id);
            void send_virtual_cas_message(VRMessage message, uint64_t wr_id);
            void send_virtual_masked_cas_message(VRMessage message, uint64_t wr_id);
        private:
            char _log_identifier[ID_SIZE];
    };
    
    class RDMA_Engine {
        public:
            RDMA_Engine();
            ~RDMA_Engine() {
                printf("RDMA_Engine destructor\n");
            }
            RDMA_Engine(unordered_map<string, string> config);
            void debug_masked_cas();
            bool start();

        private:
            bool _prime;
            int _num_clients;
            void start_distributed_experiment();
            void stop_distributed_experiment();
            experiment_control *get_experiment_control();
            memory_stats * get_memory_stats();
            unordered_map<string, string> _config;
            vector<State_Machine_Wrapper> _state_machines;
            RDMAConnectionManager  *_connection_manager;
    };
}

#endif