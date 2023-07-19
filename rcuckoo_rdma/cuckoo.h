#pragma once
#ifndef CUCKOO_H
#define CUCKOO_H

#include "state_machines.h"
#include "tables.h"
#include "search.h"
#include "config.h"
#include <unordered_map>
#include <infiniband/verbs.h>

using namespace cuckoo_state_machines;
using namespace cuckoo_search;

#define MAX_CONCURRENT_CUCKOO_MESSAGES 32
#define ID_SIZE 64

namespace cuckoo_rcuckoo {


    typedef struct rcuckoo_rdma_info {
        ibv_qp *qp;
        ibv_mr *table_mr;
        ibv_mr *lock_table_mr;
        struct ibv_cq * completion_queue;
        table_config *remote_table_config;
    } rcuckoo_rdma_info;

    class RCuckoo : public Client_State_Machine {
        public:
            RCuckoo();
            RCuckoo(unordered_map<string, string> config);
            ~RCuckoo() {}

            const char * log_id();

            void set_search_function(unordered_map<string, string> config);
            void set_location_function(unordered_map<string, string> config);


            vector<VRMessage> fsm_logic(VRMessage messages);
            vector<VRMessage> get();
            vector<VRMessage> aquire_locks();
            vector<VRMessage> put();
            vector<VRMessage> search();
            vector<VRMessage> begin_insert();
            vector<VRMessage> retry_insert();
            vector<VRMessage> release_locks_batched();
            void insert_cas_fsm(VRMessage message);
            vector<VRMessage> release_locks_fsm(VRMessage message);
            vector <VRMessage> insert_and_release_fsm(VRMessage message);
            vector<VRMessage> get_current_locking_message_with_covering_read();
            void clear_statistics();
            string get_state_machine_name();
            vector<VRMessage> idle_fsm(VRMessage message);
            vector<VRMessage> read_fsm(VRMessage message);
            vector<VRMessage> aquire_locks_with_reads_fsm(VRMessage message);

            void receive_successful_locking_message(VRMessage message);
            void receive_successful_unlocking_message(VRMessage message);


            void receive_successful_locking_message(VRMaskedCasData message);
            void receive_successful_unlocking_message(VRMaskedCasData message);


            void complete_insert_stats(bool success);
            void complete_insert();

            //Table and lock table functions
            Entry ** get_table_pointer();
            unsigned int get_table_size_bytes();
            void print_table();
            Entry * get_entry_pointer(unsigned int bucket_id, unsigned int offset);
            void * get_lock_table_pointer();
            unsigned int get_lock_table_size_bytes();
            void * get_lock_pointer(unsigned int lock_index);


            /* RDMA specific functions */
            uint64_t local_to_remote_table_address(uint64_t local_address);
            void send_virtual_read_message(VRMessage message, uint64_t wr_id);
            void send_virtual_read_message(VRReadData message, uint64_t wr_id);
            void send_virtual_cas_message(VRMessage message, uint64_t wr_id);
            void send_virtual_masked_cas_message(VRMessage message, uint64_t wr_id);
            void send_virtual_masked_cas_message(VRMaskedCasData message, uint64_t wr_id);
            vector<VRMessage> rdma_fsm(VRMessage message);
            void init_rdma_structures(rcuckoo_rdma_info info);
            vector<VRMessage> put_direct();
            vector<VRMessage> insert_direct();


            vector<VRMaskedCasData> get_current_unlock_list();


        private:

            char _log_identifier[ID_SIZE];
            unsigned int _read_threshold_bytes;
            unsigned int _buckets_per_lock;
            unsigned int _locks_per_message;


            Table _table;
            // Key _current_insert_key;
            vector<path_element> _search_path;
            int _search_path_index;
            vector<unsigned int> _locks_held;
            vector<VRMessage> _current_locking_messages;
            vector<VRMessage> _current_locking_read_messages;


            int _locking_message_index;

            /*rdma specific variables*/
            ibv_qp * _qp;
            ibv_mr *_table_mr;
            ibv_mr *_lock_table_mr;
            struct ibv_cq * _completion_queue;
            table_config * _table_config;
            struct ibv_wc *_wc;
            uint64_t _wr_id;


            // hash_locations  (*_location_function)(string, unsigned int);
            hash_locations  (*_location_function)(Key, unsigned int);

            vector<path_element> (RCuckoo::*_table_search_function)(vector<unsigned int> searchable_buckets);
            vector<path_element> a_star_insert_self(vector<unsigned int> searchable_buckets);
            vector<path_element> random_insert_self(vector<unsigned int> searchable_buckets);


            bool read_complete();
            bool all_locks_aquired();
            bool all_locks_released();
            VRMessage get_prior_locking_message();
            VRMessage get_current_locking_message();

    };
}

#endif
