
#include "virtual_rdma.h"
#include "state_machines.h"
#include "rdma_engine.h"
#include <vector>


using namespace std;
using namespace cuckoo_state_machines;

namespace cuckoo_rdma_engine {
    
    RDMA_Engine::RDMA_Engine(){
        _state_machine = new State_Machine();
    }
            
    RDMA_Engine::RDMA_Engine(unordered_map<string, string> config, State_Machine * state_machine) {
        _state_machine = state_machine;
    }
    
    bool RDMA_Engine::start_state_machine(){
        VRMessage init;
        vector<VRMessage> messages = _state_machine->fsm(init);
        while(true){
            //send messages
            for (int i = 0; i < messages.size(); i++){
                printf("Sending message: %s\n", messages[i].to_string().c_str());
            }

            //receive messages
            printf("polling for messagge");
            messages = _state_machine->fsm(init);
            printf("exiting because I don't actually have the logic.");
            exit(0);

        }

    }

}