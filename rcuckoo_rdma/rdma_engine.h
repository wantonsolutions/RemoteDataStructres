#pragma once
#ifndef RDMA_ENGINE_H
#define RDMA_ENGINE_H

#include "virtual_rdma.h"
#include "state_machines.h"
#include "cuckoo.h"

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
            bool start();

        private:
            State_Machine * _state_machine;
    };
}

#endif