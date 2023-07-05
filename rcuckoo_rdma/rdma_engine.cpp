
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


            // int ret = cm.client_xchange_metadata_with_server(qp_num, NULL, 256);      // 1 MB
            // int ret = _connection_manager->client_xchange_metadata_with_server(qp_num, table_pointer, table_size);      // 1 MB
            // if (ret != 0) {
            //     printf("Error: client_xchange_metadata_with_server failed\n");
            //     exit(1);
            // } else {
            //     printf("client_xchange_metadata_with_server succeeded\n");
            // }
            // _table_mr = _connection_manager->client_qp_src_mr[qp_num];
            // assert(_table_mr != NULL);

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
            // int ret = ibv_get_cq_event(_connection_manager->io_completion_channel_threads[0], &_completion_queue, &context);
            // result_t result;
            // if (ret) {
            //     printf("Failed to get next CQ event due to %d \n", -errno);
            //     exit(0);
            // }
            // ret = ibv_req_notify_cq(_completion_queue, 0);
            // if (ret) {
            //     printf("Failed to request further notifications %d \n", -errno);
            //     exit(0);
            // }

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
        VRMessage init;

        vector<VRMessage> messages = _state_machine->fsm(init);

        uint32_t remote_key = _table_config->remote_key;
        int num_concur = 1;
        struct ibv_wc *wc = (struct ibv_wc *) calloc (num_concur, sizeof(struct ibv_wc));

        while(true){
            //send messages
            for (int i = 0; i < messages.size(); i++){
                printf("Sending message: %s\n", messages[i].to_string().c_str());
                VRMessage message = messages[i];
                if (message.get_message_type() == READ_REQUEST) {
                    printf("starting rdma read\n");
                    ibv_qp *qp = _connection_manager->client_qp[0];

                    //translate address locally for bucket id
                    unsigned int bucket_offset = stoi(message.function_args["bucket_offset"]);
                    unsigned int bucket_id = stoi(message.function_args["bucket_id"]);
                    unsigned int size = stoi(message.function_args["size"]);

                    bucket_id = 1;
                    bucket_offset = 0;
                    size = 64;

                    uint64_t local_address = (uint64_t) _rcuckoo->get_entry_pointer(bucket_id, bucket_offset);
                    uint64_t remote_server_address = local_to_remote_table_address(local_address);
                    printf("local address: %lu\n", local_address);
                    printf("remote server address: %p\n", (void*) remote_server_address);
                    printf("size: %d\n", size);
                    printf("local key: %d\n", _table_mr->lkey);
                    printf("remote key: %d\n", remote_key);

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
                        1337
                    );
                    if (!success) {
                        printf("rdma read failed\n");
                        exit(1);
                    }
                    // printf("quick sleep 2\n");
                    // sleep(2);
                    // printf("done sleep polling for response\n");
                    int n = bulk_poll(_completion_queue, num_concur, wc);
                    printf("done polling!!\n");
                    _rcuckoo->print_table();
                }
            }


            //receive messages
            printf("polling for messagge\n");
            // messages = _state_machine->fsm(init);
            printf("exiting because I don't actually have the logic.");
            exit(0);

        }

    }

}