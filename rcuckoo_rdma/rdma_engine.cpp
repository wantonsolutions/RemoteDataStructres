
#include "virtual_rdma.h"
#include "state_machines.h"
#include "rdma_engine.h"
#include "cuckoo.h"
#include <vector>
#include <infiniband/verbs.h>

#include "rdma_common.h"
#include "rdma_client_lib.h"


using namespace std;
using namespace cuckoo_state_machines;

namespace cuckoo_rdma_engine {
    
    RDMA_Engine::RDMA_Engine(){
        _state_machine = new State_Machine();
    }
            
    RDMA_Engine::RDMA_Engine(unordered_map<string, string> config, RCuckoo * rcuckoo) {
        _state_machine = rcuckoo;

        try {
            RDMAConnectionManagerArguments args;
            struct sockaddr_in server_sockaddr;
            int base_port = DEFAULT_RDMA_PORT;
            bzero(&server_sockaddr, sizeof server_sockaddr);
            server_sockaddr.sin_family = AF_INET;
            server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);


            args.num_qps = stoi(config["num_qps"]);
            string server_address = config["server_address"];

            //resolve the address from the config
            char * address = new char[server_address.length() + 1];
            strcpy(address, server_address.c_str());
            int ret = get_addr(address, (struct sockaddr*) &server_sockaddr);
            if (ret != 0) {
                printf("Error: get_addr failed\n");
                exit(1);
            } else {
                printf("get_addr succeeded\n");
                printf("connecting to %s\n", inet_ntoa(server_sockaddr.sin_addr));
            }
            args.server_sockaddr = &server_sockaddr;
            args.base_port = stoi(config["base_port"]);

            RDMAConnectionManager cm = RDMAConnectionManager(args);
            printf("RDMAConnectionManager created\n");
        } catch (exception& e) {
            printf("RDMAConnectionManager failed to create\n");
            printf("Error: %s\n", e.what());
            exit(1);
        }
    }

    // RDMA_Engine::set_rcuckoo_state_machine(RCuckoo * rcuckoo){
    //     _state_machine = dynamic_cast<RCuckoo>rcuckoo;
    // }
    
    bool RDMA_Engine::start(){
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