#ifndef RDMA_CLIENT_LIB_H
#define RDMA_CLIENT_LIB_H

#define MAX_THREADS MAX_QPS
#define MULTI_CQ

#include "rdma_common.h"

typedef struct result_t {
    double err_code;
    double xput_ops;
    double xput_bps;
    double cq_poll_time_percent;        // in cpu cycles
    double cq_poll_count;
    double cq_empty_count;
} result_t;


typedef struct RDMAConnectionManagerArguments {
    int num_qps;
    int base_port;
    struct sockaddr_in * server_sockaddr;
} RDMAConnectionManagerArguments;

class RDMAConnectionManager {
    public:
        /* Source and Destination buffers, where RDMA operations source and sink */
        RDMAConnectionManager();
        // ~RDMAConnectionManager();
        RDMAConnectionManager(RDMAConnectionManagerArguments arg);
        /* These are basic RDMA resources */
        /* These are RDMA connection related resources */
        struct ibv_context **devices;
        struct rdma_event_channel *cm_event_channel;
        struct rdma_cm_id *cm_client_qp_id[MAX_QPS];
        struct ibv_pd *pd;
        struct ibv_qp_init_attr qp_init_attr;
        struct ibv_qp *client_qp[MAX_QPS];
        struct ibv_device_attr dev_attr;
        struct ibv_exp_qp_init_attr qp_init_attr_exp;

        /* These are memory buffers related resources */
        struct ibv_mr *client_qp_src_mr[MAX_QPS];
        struct ibv_mr *client_qp_dst_mr[MAX_QPS];
        struct ibv_mr *client_qp_metadata_mr[MAX_QPS];
        struct ibv_mr *server_qp_metadata_mr[MAX_QPS];
        struct rdma_buffer_attr client_qp_metadata_attr[MAX_QPS];
        struct rdma_buffer_attr server_qp_metadata_attr[MAX_QPS];


        result_t thread_results[MAX_THREADS];
        struct ibv_cq *client_cq_threads[MAX_THREADS];
        struct ibv_comp_channel *io_completion_channel_threads[MAX_THREADS];
        uint32_t thread_contexts[MAX_THREADS];


        struct ibv_send_wr client_send_wr, *bad_client_send_wr;
        struct ibv_recv_wr server_recv_wr, *bad_server_recv_wr;
        struct ibv_sge client_send_sge, server_recv_sge;


        //TODO move to an indivdidual connection
        int client_xchange_metadata_with_server(int qp_num, char* buffer, uint32_t buffer_size);

        void CheckDMSupported(struct ibv_context *ctx);
        void CheckGeneralExtendedAttributes(struct ibv_context *ctx);
        void CheckExtendedAttributes2(struct ibv_context *ctx);
        void CheckCapabilities();
        void CheckAdvancedTransport(struct ibv_context *ctx);

    private:
        bool _connections_initialized = false;
        int _num_qps;
        int _base_port;
        struct sockaddr_in * _server_sockaddr;
        int client_setup_shared_resources();
        int client_prepare_connection(struct sockaddr_in *s_addr, int qp_num, int port_num);
        int client_pre_post_recv_buffer(int qp_num);
        int client_connect_qp_to_server(int qp_num);
        int client_disconnect_and_clean(int qp_num);
        int client_clean();

};




/* List of ops that are being instrumented */
enum rdma_measured_op { RDMA_READ_OP, RDMA_WRITE_OP, RDMA_CAS_OP, RDMA_FAA_OP};
enum mem_reg_mode { 
    MR_MODE_PRE_REGISTER,               // Use a pre-registered buffer and use it for all transactions
    MR_MODE_PRE_REGISTER_WITH_ROTATE,   // Use a set of pre-registered buffers (for the same piece of memory) but rotate their usage
    MR_MODE_REGISTER_IN_DATAPTH,        // Register buffers in datapath as necessary
};

struct xput_thread_args {
    int thread_id;
    int core;
    int num_concur;
    struct ibv_cq *cq_ptr;
    int msg_size;
    enum rdma_measured_op rdma_op;      // rdma op to use i.e., read or write
    uint64_t start_cycles;
    struct ibv_mr **mr_buffers;           /* Make sure to deregister these local MRs before exiting */
    int num_lbuffers;
    RDMAConnectionManager * cm;
};

#define HIST_SIZE 32

/* Data transfer modes */
enum data_transfer_mode { 
    DTR_MODE_NO_GATHER,         // Assume that data is already gathered in a single big buffer
    DTR_MODE_CPU_GATHER,        // Assume that data is scattered but is gathered by CPU into a single big buffer before xmit
    DTR_MODE_NIC_GATHER,        // Assume that data is scattered but is gathered by NIC with a scatter-gather op during xmit
    DTR_MODE_PIECE_BY_PIECE,    // Assume that data is scattered but each piece is sent in a different rdma write op
};

#endif