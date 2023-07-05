
#include "virtual_rdma.h"
#include "state_machines.h"
#include "rdma_engine.h"
#include "cuckoo.h"
#include <vector>
#include <infiniband/verbs.h>

#include "rdma_common.h"
#include "rdma_client_lib.h"
#include "config.h"
#include "memcached.h"


using namespace std;
using namespace cuckoo_state_machines;

namespace cuckoo_rdma_engine {

    int bulk_poll(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc) {
        int n = 0;
        int ret = 0;
        do {
            n = ibv_poll_cq(cq, num_entries, wc);       // get upto num_concur entries
            if (n < 0) {
                printf("Failed to poll cq for wc due to %d\n", ret);
                rdma_error("Failed to poll cq for wc due to %d \n", ret);
                exit(1);
            }
            if (n == 0) {
                //todo deal with a global variable being used to break the polling here
                //we should deal with finished running xput elsewhere (hard to tell where though)
                //todo we can stop polling here if we add a global stop variable and we will need to
                // if (finished_running_xput) {
                //     break;
                // }
            }     
        } while (n < 1);
        return n;
    }


    static inline void fillSgeWr(ibv_sge &sg, ibv_send_wr &wr, uint64_t source,
                             uint64_t size, uint32_t lkey) {
        memset(&sg, 0, sizeof(sg));
        sg.addr = (uintptr_t)source;
        sg.length = size;
        sg.lkey = lkey;

        memset(&wr, 0, sizeof(wr));
        wr.wr_id = 0;
        wr.sg_list = &sg;
        wr.num_sge = 1;
    }

    // for RC & UC
    bool rdmaRead(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t size,
                uint32_t lkey, uint32_t remoteRKey, bool signal, uint64_t wrID) {
        struct ibv_sge sg;
        struct ibv_send_wr wr;
        struct ibv_send_wr *wrBad;

        fillSgeWr(sg, wr, source, size, lkey);

        wr.opcode = IBV_WR_RDMA_READ;

        if (signal) {
            wr.send_flags = IBV_SEND_SIGNALED;
        }

        wr.wr.rdma.remote_addr = dest;
        wr.wr.rdma.rkey = remoteRKey;
        wr.wr_id = wrID;

        if (ibv_post_send(qp, &wr, &wrBad)) {
            printf("send with rdma read failed");
        }
        return true;
    }

    
    RDMA_Engine::RDMA_Engine(){
        _state_machine = new State_Machine();
    }

            
    RDMA_Engine::RDMA_Engine(unordered_map<string, string> config, RCuckoo * rcuckoo) {
        _state_machine = rcuckoo;
        _rcuckoo = rcuckoo;
        _memory_state_machine = new Memory_State_Machine(config);
        _memory_state_machine->set_table_pointer(_rcuckoo->get_table_pointer());

        try {
            RDMAConnectionManagerArguments args;


            args.num_qps = stoi(config["num_qps"]);
            if (args.num_qps < 1) {
                printf("Error: num_qps must be at least 1\n");
                exit(1);
            }
            if (args.num_qps > 1) {
                printf("Error: num_qps must be at most %d, we are only enabling a single QP per process\n", 1);
                printf("TODO; figure out a proper way to set up threading for QP's this likely only works for one FSM\n");
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

            //Exchange the connection information on the QP
            int qp_num = 0;
            char * table_pointer = (char *) rcuckoo->get_table_pointer();
            uint32_t table_size = rcuckoo->get_table_size_bytes();

            _table_config = memcached_get_table_config();
            assert(_table_config != NULL);
            assert(_table_config->table_size_bytes == table_size);
            printf("got a table config from the memcached server and it seems to line up\n");
            rcuckoo->print_table();

            //register the memory
            printf("registering table memory");
            _table_mr = rdma_buffer_register(_connection_manager->pd, table_pointer, table_size, MEMORY_PERMISSION);

            //Request a completion queue
            printf("requesting completion queue\n");
            void * context = NULL;
            _completion_queue = _connection_manager->client_cq_threads[qp_num];
            if(_completion_queue == NULL){
                printf("completion queue is null\n");
            }

            assert(_completion_queue != NULL);

        } catch (exception& e) {
            printf("RDMAConnectionManager failed to create\n");
            printf("Error: %s\n", e.what());
            exit(1);
            return;
        }
        printf("returning from engine constructor\n");
        return;
    }

    uint64_t RDMA_Engine::local_to_remote_table_address(uint64_t local_address){
        uint64_t base_address = (uint64_t) _rcuckoo->get_table_pointer()[0];
        uint64_t address_offset = local_address - base_address;
        uint64_t remote_address = (uint64_t) _table_config->table_address + address_offset;
        // remote_address += 64 + (sizeof(Entry) * 2);
        return remote_address;
    }

    
    bool RDMA_Engine::start(){
        vector<VRMessage> ingress_messages;
        VRMessage init;
        ingress_messages.push_back(init);


        // vector<VRMessage> messages = _state_machine->fsm(init);

        uint32_t remote_key = _table_config->remote_key;
        int num_concur = 1;
        struct ibv_wc *wc = (struct ibv_wc *) calloc (num_concur, sizeof(struct ibv_wc));

        uint64_t wr_id_counter = 0;
        unordered_map<uint64_t, VRMessage> wr_id_to_message;

        while(true){
            //send messages

            //Pop off an ingress message
            printf("popping off first ingress message\n");

            VRMessage current_ingress_message;
            
            if (ingress_messages.size() > 0){
                current_ingress_message = ingress_messages[0];
                ingress_messages.erase(ingress_messages.begin());
            }
            vector<VRMessage> messages = _state_machine->fsm(current_ingress_message);


            int sent_messages = 0;
            for (int i = 0; i < messages.size(); i++){
                printf("Sending message: %s\n", messages[i].to_string().c_str());
                VRMessage message = messages[i];
                if (message.get_message_type() == READ_REQUEST) {

                    printf("storing message to map\n");
                    wr_id_to_message[wr_id_counter] = message;
                    printf("starting rdma read\n");

                    ibv_qp *qp = _connection_manager->client_qp[0];
                    //translate address locally for bucket id
                    unsigned int bucket_offset = stoi(message.function_args["bucket_offset"]);
                    unsigned int bucket_id = stoi(message.function_args["bucket_id"]);
                    unsigned int size = stoi(message.function_args["size"]);


                    uint64_t local_address = (uint64_t) _rcuckoo->get_entry_pointer(bucket_id, bucket_offset);
                    uint64_t remote_server_address = local_to_remote_table_address(local_address);
                    // printf("local address: %lu\n", local_address);
                    // printf("remote server address: %p\n", (void*) remote_server_address);
                    // printf("size: %d\n", size);
                    // printf("local key: %d\n", _table_mr->lkey);
                    // printf("remote key: %d\n", remote_key);

                    // printf("quick sleep\n");
                    // sleep(2);
                    // printf("done sleep sending read\n");

                    bool success = rdmaRead(
                        _connection_manager->client_qp[0],
                        local_address,
                        remote_server_address,
                        size,
                        _table_mr->lkey,
                        remote_key,
                        true,
                        wr_id_counter
                    );
                    sent_messages++;
                    wr_id_counter++;
                    if (!success) {
                        printf("rdma read failed\n");
                        exit(1);
                    }
                }
            }

            if (sent_messages > 0 ) {
                //Now we deal with the message recipt
                int n = bulk_poll(_completion_queue, num_concur, wc);
                if (n < 0) {
                    printf("polling failed\n");
                    exit(1);
                }
                for (int j=0;j<n;j++) {
                    if (wc[j].status != IBV_WC_SUCCESS) {
                        printf("RDMA read failed with status %s\n", ibv_wc_status_str(wc[j].status));
                        exit(1);
                    } else {
                        printf("RDMA read succeeded\n");

                        //The result of the operation is in the local buffer
                        //Perform a faux delivery to our own state machine
                        VRMessage outgoing = wr_id_to_message[wc[j].wr_id];
                        vector<VRMessage> incomming = _memory_state_machine->fsm(outgoing);
                        wr_id_to_message.erase(wc[j].wr_id);
                        printf("memory state machine table\n");
                        _memory_state_machine->print_table();

                        for (int i = 0; i < incomming.size(); i++){
                            printf("memory state machine incomming message: %s\n", incomming[i].to_string().c_str());
                            ingress_messages.push_back(incomming[i]);
                        }
                    }
                }
                _rcuckoo->print_table();
            }

            // //receive messages
            // printf("polling for messagge\n");
            // // messages = _state_machine->fsm(init);
            // printf("exiting because I don't actually have the logic.");
            // exit(0);

        }

    }

}