#pragma once
#ifndef CUCKOO_H
#define CUCKOO_H

#include "state_machines.h"
#include <unordered_map>

using namespace cuckoo_state_machines;

namespace cuckoo_rcuckoo {
    class RCuckoo : public Client_State_Machine {
        public:
            RCuckoo();
            RCuckoo(unordered_map<string, string> config);
            ~RCuckoo() {}
            string get_state_machine_name();

    };
}

#endif
