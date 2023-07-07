
#include "virtual_rdma.h"
#include "state_machines.h"
#include "rdma_engine.h"
#include "cuckoo.h"
#include <vector>
#include <infiniband/verbs.h>

#include "rdma_common.h"
#include "rdma_client_lib.h"
#include "config.h"
#include "log.h"
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

    static inline void fillSgeWr(ibv_sge &sg, ibv_exp_send_wr &wr, uint64_t source,
                                uint64_t size, uint32_t lkey) {
        memset(&sg, 0, sizeof(sg));
        sg.addr = (uint64_t)source;
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

    // for RC & UC
    bool rdmaCompareAndSwap(ibv_qp *qp, uint64_t source, uint64_t dest,
                            uint64_t compare, uint64_t swap, uint32_t lkey,
                            uint32_t remoteRKey, bool signal, uint64_t wrID) {
        struct ibv_sge sg;
        struct ibv_send_wr wr;
        struct ibv_send_wr *wrBad;

        fillSgeWr(sg, wr, source, 8, lkey);

        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;

        if (signal) {
            wr.send_flags = IBV_SEND_SIGNALED;
        }

        wr.wr.atomic.remote_addr = dest;
        wr.wr.atomic.rkey = remoteRKey;
        wr.wr.atomic.compare_add = compare;
        wr.wr.atomic.swap = swap;
        wr.wr_id = wrID;

        if (ibv_post_send(qp, &wr, &wrBad)) {
            printf("send with rdma compare and swap failed\n");
            sleep(5);
            return false;
        }
        return true;
    }

    bool rdmaCompareAndSwapMask(ibv_qp *qp, uint64_t source, uint64_t dest,
                                uint64_t compare, uint64_t swap, uint32_t lkey,
                                uint32_t remoteRKey, uint64_t mask, bool singal) {
        struct ibv_sge sg;
        struct ibv_exp_send_wr wr;
        struct ibv_exp_send_wr *wrBad;
        fillSgeWr(sg, wr, source, 8, lkey);

        wr.next = NULL;
        wr.exp_opcode = IBV_EXP_WR_EXT_MASKED_ATOMIC_CMP_AND_SWP;
        wr.exp_send_flags = IBV_EXP_SEND_EXT_ATOMIC_INLINE;

        if (singal) {
            wr.exp_send_flags |= IBV_EXP_SEND_SIGNALED;
        }

        wr.ext_op.masked_atomics.log_arg_sz = 3;
        wr.ext_op.masked_atomics.remote_addr = dest;
        wr.ext_op.masked_atomics.rkey = remoteRKey;

        auto &op = wr.ext_op.masked_atomics.wr_data.inline_data.op.cmp_swap;
        op.compare_val = compare;
        op.swap_val = swap;

        op.compare_mask = mask;
        op.swap_mask = mask;

        int ret = ibv_exp_post_send(qp, &wr, &wrBad);
        if (ret) {
            printf("MSKCAS FAILED : Return code %d\n", ret);
            return false;
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
        _memory_state_machine->set_underlying_lock_table_address(_rcuckoo->get_lock_table_pointer());

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

            char * lock_table_pointer = (char *) rcuckoo->get_lock_table_pointer();
            uint32_t lock_table_size = rcuckoo->get_lock_table_size_bytes();

            _table_config = memcached_get_table_config();
            assert(_table_config != NULL);
            assert(_table_config->table_size_bytes == table_size);
            printf("got a table config from the memcached server and it seems to line up\n");
            rcuckoo->print_table();

            //register the memory
            printf("registering table memory\n");
            _table_mr = rdma_buffer_register(_connection_manager->pd, table_pointer, table_size, MEMORY_PERMISSION);

            printf("register lock table memory\n");
            _lock_table_mr = rdma_buffer_register(_connection_manager->pd, lock_table_pointer, lock_table_size, MEMORY_PERMISSION);

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

    void RDMA_Engine::send_virtual_read_message(VRMessage message, uint64_t wr_id){
        ibv_qp *qp = _connection_manager->client_qp[0];
        //translate address locally for bucket id
        unsigned int bucket_offset = stoi(message.function_args["bucket_offset"]);
        unsigned int bucket_id = stoi(message.function_args["bucket_id"]);
        unsigned int size = stoi(message.function_args["size"]);

        uint64_t local_address = (uint64_t) _rcuckoo->get_entry_pointer(bucket_id, bucket_offset);
        uint64_t remote_server_address = local_to_remote_table_address(local_address);

        bool success = rdmaRead(
            qp,
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
        VERBOSE("RDMA ENGINE", "sent virtual read message\n");
    }
    void RDMA_Engine::send_virtual_cas_message(VRMessage message, uint64_t wr_id){
        ibv_qp * qp = _connection_manager->client_qp[0];
        uint32_t bucket_id = stoi(message.function_args["bucket_id"]);
        uint32_t bucket_offset = stoi(message.function_args["bucket_offset"]);
        uint32_t old = stoul(message.function_args["old"], nullptr, 16);
        old = __builtin_bswap64(old);
        old = old << 32;
        uint32_t new_val = stoul(message.function_args["new"], nullptr, 16);
        new_val = __builtin_bswap64(new_val);
        new_val = new_val <<32;

        uint64_t local_address = (uint64_t) _rcuckoo->get_entry_pointer(bucket_id, bucket_offset);
        uint64_t remote_server_address = local_to_remote_table_address(local_address);



        bool success = rdmaCompareAndSwap(
            qp, 
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

    void RDMA_Engine::send_virtual_masked_cas_message(VRMessage message, uint64_t wr_id) {
        ibv_qp * qp = _connection_manager->client_qp[0];
        int lock_index = stoi(message.function_args["lock_index"]);
        uint64_t local_lock_address = (uint64_t) _rcuckoo->get_lock_pointer(lock_index);
        // uint64_t remote_lock_address = local_to_remote_lock_table_address(local_lock_address);
        uint64_t remote_lock_address = (uint64_t) _table_config->lock_table_address + lock_index;

        uint64_t compare = stoull(message.function_args["old"], 0, 2);
        printf("compare %lu\n", compare);
        compare = __builtin_bswap64(compare);
        uint64_t swap = stoull(message.function_args["new"], 0, 2);
        printf("swap %lu\n", swap);
        swap = __builtin_bswap64(swap);
        uint64_t mask = stoull(message.function_args["mask"], 0, 2);
        printf("mask %lu\n", mask);
        mask = __builtin_bswap64(mask);

        printf("sending masked cas message\n");


        printf("local_lock_address %lu\n", local_lock_address);
        printf("remote_lock_address %lu\n", remote_lock_address);
        printf("compare %lu\n", compare);
        printf("swap %lu\n", swap);
        printf("mask %lu\n", mask);
        printf("_lock_table_mr->lkey %u\n", _lock_table_mr->lkey);
        printf("_table_config->lock_table_key %u\n", _table_config->lock_table_key);

        bool success = rdmaCompareAndSwapMask(
            qp,
            local_lock_address,
            remote_lock_address,
            compare,
            swap,
            _lock_table_mr->lkey,
            _table_config->lock_table_key,
            mask,
            true);

        if (!success) {
            printf("rdma masked cas failed failed\n");
            exit(1);
        }
        printf("completed masked cas request\n");


    }
    
    #define MAX_CONCURRENT_MESSAGES 4
    VRMessage message_tracker[MAX_CONCURRENT_MESSAGES];
    bool RDMA_Engine::start(){
        vector<VRMessage> ingress_messages;
        VRMessage init;
        ingress_messages.push_back(init);


        // vector<VRMessage> messages = _state_machine->fsm(init);

        int num_concur = 1;
        struct ibv_wc *wc = (struct ibv_wc *) calloc (num_concur, sizeof(struct ibv_wc));

        uint64_t wr_id_counter = 0;
        unordered_map<uint64_t, VRMessage> wr_id_to_message;
        vector<VRMessage> messages;

        while(true){
            //Pop off an ingress message
            VRMessage current_ingress_message;
            if (ingress_messages.size() > 0){
                printf("popping off first ingress message\n");
                current_ingress_message = ingress_messages[0];
                ingress_messages.erase(ingress_messages.begin());
            } else {
                // printf("nothing to supply to the state machine, sending in blank message\n");
            }

            messages = _state_machine->fsm(current_ingress_message);
            int sent_messages = 0;
            for (int i = 0; i < messages.size(); i++){
                printf("Sending message: %s\n", messages[i].to_string().c_str());
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
                        printf("message type not supported\n");
                        exit(1);
                }
                if (sent) {
                    printf("writing to wr_id_to_message with %s\n", messages[i].to_string().c_str());
                    message_tracker[wr_id_counter % MAX_CONCURRENT_MESSAGES] = messages[i];
                    // wr_id_to_message[wr_id_counter] = messages[i];
                    printf("setting wrid message %d\n", wr_id_counter);
                    sent_messages++;
                    wr_id_counter++;
                }
            }

            for (int k=0;k<MAX_CONCURRENT_MESSAGES;k++) {
                printf("message_tracker[%d] = %s\n", k, message_tracker[k].to_string().c_str());
            }

            if (sent_messages > 0 ) {
                //Now we deal with the message recipt
                printf("pooling\n");
                int n = bulk_poll(_completion_queue, sent_messages, wc);
                if (n < 0) {
                    printf("polling failed\n");
                    exit(1);
                }
                for (int k=0;k<MAX_CONCURRENT_MESSAGES;k++) {
                    printf("message_tracker sub 1[%d] = %s\n", k, message_tracker[k].to_string().c_str());
                }
                for (int j=0;j<n;j++) {
                    if (wc[j].status != IBV_WC_SUCCESS) {
                        printf("RDMA read failed with status %s\n", ibv_wc_status_str(wc[j].status));
                        exit(1);
                    } else {
                        printf("Message Received with work request %d\n", wc[j].wr_id);

                        for (int k=0;k<MAX_CONCURRENT_MESSAGES;k++) {
                            printf("message_tracker[%d] = %s\n", k, message_tracker[k].to_string().c_str());
                        }
                        //The result of the operation is in the local buffer
                        //Perform a faux delivery to our own state machine

                        // if (wr_id_to_message.find(wc[j].wr_id) == wr_id_to_message.end()) {
                        //     printf("wr_id not found in wr_id_to_message\n");
                        //     exit(1);
                        // }
                        VRMessage outgoing = message_tracker[wc[j].wr_id % MAX_CONCURRENT_MESSAGES];
                        printf("about to deliver wr_id %d :: %s\n", wc[j].wr_id, message_tracker[wc[j].wr_id % MAX_CONCURRENT_MESSAGES].to_string().c_str());
                        // printf("oputging message type %s\n", outgoing.function.c_str());
                        // for (int k=0;k<outgoing.function.size();k++){
                        //     printf("%c", outgoing.function[k]);
                        // }
                        // if (outgoing.function[0] == 'm' &&  outgoing.function[1] == 'a' && outgoing.function[2] == 's' && outgoing.function[3] == 'k'){
                        //     printf("assigning mask function\n");
                        //     outgoing.function="masked_cas_lock_table";
                        // }
                        printf("outgoing message type %d\n", outgoing.get_message_type());

                        if (outgoing.get_message_type() == READ_REQUEST){
                            printf("delivering message to state machine: %s\n", outgoing.to_string().c_str());
                            vector<VRMessage> incomming = _memory_state_machine->fsm(outgoing);
                            printf("eraing message from wr_id_to_message %d\n", wc[j].wr_id);
                            wr_id_to_message.erase(wc[j].wr_id);
                            printf("memory state machine table\n");
                            _memory_state_machine->print_table();
                            for (int i = 0; i < incomming.size(); i++){
                                printf("memory state machine incomming message: %s\n", incomming[i].to_string().c_str());
                                ingress_messages.push_back(incomming[i]);
                            }
                        } else if (outgoing.get_message_type() == MASKED_CAS_REQUEST) {
                            printf("received a masked cas response\n");
                            printf("gottac check if it is the same as reality\n");
                            exit(0);
                        } else if (outgoing.get_message_type() == CAS_REQUEST) {
                            printf("received a cas response\n");
                            exit(0);

                        } else {
                            printf("unknown message type %s", outgoing.to_string().c_str());
                            exit(1);
                        }
                    }
                }
                printf("memory state machine table after operations\n");
                _memory_state_machine->print_table();
                printf("rcuckoo table after operations\n");
                _rcuckoo->print_table();
                assert(_memory_state_machine->get_table_pointer() == _rcuckoo->get_table_pointer());
            }

            // //receive messages
            // printf("polling for messagge\n");
            // // messages = _state_machine->fsm(init);
            // printf("exiting because I don't actually have the logic.");
            // exit(0);

        }

    }

}