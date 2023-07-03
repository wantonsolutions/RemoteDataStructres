#pragma once
#ifndef RDMA_ENGINE_H
#define RDMA_ENGINE_H

#include "virtual_rdma.h"
#include "state_machines.h"

using namespace cuckoo_state_machines;
namespace cuckoo_rdma_engine {
    
    class RDMA_Engine {
        public:
            RDMA_Engine();
            ~RDMA_Engine() {}
            RDMA_Engine(unordered_map<string, string> config, State_Machine * state_machine);
            bool start_state_machine();

        private:
            State_Machine * _state_machine;
    };
}

#endif