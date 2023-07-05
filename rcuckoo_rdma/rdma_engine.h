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

namespace cuckoo_rdma_engine {
    
    class RDMA_Engine {
        public:
            RDMA_Engine();
            ~RDMA_Engine() {}
            RDMA_Engine(unordered_map<string, string> config, RCuckoo * rcuckoo);
            // using namespace cuckoo_rcuckoo;
            // set_rcuckoo_state_machine(cuckoo_rcuckoo::RCuckoo * rcuckoo);
            uint64_t local_to_remote_table_address(uint64_t local_address);
            bool start();

        private:
            State_Machine * _state_machine;
            RCuckoo * _rcuckoo;
            ibv_mr * _table_mr;
            Memory_State_Machine * _memory_state_machine;
            table_config * _table_config;
            struct ibv_cq *_completion_queue;
            RDMAConnectionManager  *_connection_manager;
    };
}

#endif