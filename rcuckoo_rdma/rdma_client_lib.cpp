/*
 * An example RDMA client side code. 
 * Author: Animesh Trivedi 
 *         atrivedi@apache.org
 */

#include <sys/time.h>
#include <assert.h>
#include <stdexcept>
#include "rdma_common.h"
#include "rdma_client_lib.h"
#include <netinet/in.h>
#include <inttypes.h>
#include "log.h"

/* A fast but good enough pseudo-random number generator. Good enough for what? */
/* Courtesy of https://stackoverflow.com/questions/1640258/need-a-fast-random-generator-for-c */
unsigned long rand_xorshf96(void) {          //period 2^96-1
    static unsigned long x=123456789, y=362436069, z=521288629;
    unsigned long t;
    x ^= x << 16;
    x ^= x >> 5;
    x ^= x << 1;

    t = x;
    x = y;
    y = z;
    z = t ^ x ^ y;
    return z;
}


RDMAConnectionManager::RDMAConnectionManager() {

}

void RDMAConnectionManager::CheckDMSupported(struct ibv_context *ctx) {
  // thanks https://github.com/thustorage/Sherman/blob/main/src/rdma/Utility.cpp
  struct ibv_exp_device_attr attrs;

  attrs.comp_mask = IBV_EXP_DEVICE_ATTR_UMR;
  attrs.comp_mask |= IBV_EXP_DEVICE_ATTR_MAX_DM_SIZE;

  if (ibv_exp_query_device(ctx, &attrs)) {
    printf("Couldn't query device attributes: error %s\n", strerror(errno));
  }

  if (!(attrs.comp_mask & IBV_EXP_DEVICE_ATTR_MAX_DM_SIZE)) {
    fprintf(stderr, "Can not support device memory!\n");
    exit(-1);
  } else if (!(attrs.max_dm_size)) {
  } else {
    int kMaxDeviceMemorySize = attrs.max_dm_size;
    printf("The RNIC has %dKB device memory\n", kMaxDeviceMemorySize / 1024);
  }
}

void RDMAConnectionManager::CheckGeneralExtendedAttributes(struct ibv_context *ctx) {
    //https://github.com/redn-io/RedN/blob/2609ea622be38698445e03f2ef439143de7f12b0/src/rdma/connection.c#L234
    struct ibv_exp_device_attr attr;
    if(ibv_exp_query_device(ctx, &attr)){
        printf("ibv_exp_query_device() failed\n");
        exit(0);
    }
    

    printf("Maximum # of QPs: %d\n", attr.max_qp);
    printf("Maximum # of outstanding WRs: %d\n", attr.max_qp_wr);
    printf("Maximum # of outstanding Atoms/Rds: %d\n", attr.max_qp_rd_atom);
    printf("Maximum depth for Atoms/Rds: %d\n", attr.max_qp_init_rd_atom);
    printf("-- Supported features --\n");
    printf("Atomic BEndian replies: %s\n", attr.exp_atomic_cap & IBV_EXP_ATOMIC_HCA_REPLY_BE ? "YES":"NO");
    printf("Core-direct: %s\n", attr.exp_device_cap_flags & IBV_EXP_DEVICE_CROSS_CHANNEL ? "YES":"NO");
    printf("Collectives:\n");
    printf("  [int operations]\n");
    printf("	* ADD    -> %s\n", attr.calc_cap.int_ops & IBV_EXP_CALC_OP_ADD ? "YES":"NO");
    printf("	* BAND   -> %s\n", attr.calc_cap.int_ops & IBV_EXP_CALC_OP_BAND ? "YES":"NO");
    printf("	* BXOR   -> %s\n", attr.calc_cap.int_ops & IBV_EXP_CALC_OP_BXOR ? "YES":"NO");
    printf("	* BOR    -> %s\n", attr.calc_cap.int_ops & IBV_EXP_CALC_OP_BOR ? "YES":"NO");
    printf("	* MAXLOC -> %s\n", attr.calc_cap.int_ops & IBV_EXP_CALC_OP_MAXLOC ? "YES":"NO");
    printf("  [uint operations]\n");
    printf("	* ADD    -> %s\n", attr.calc_cap.uint_ops & IBV_EXP_CALC_OP_ADD ? "YES":"NO");
    printf("	* BAND   -> %s\n", attr.calc_cap.uint_ops & IBV_EXP_CALC_OP_BAND ? "YES":"NO");
    printf("	* BXOR   -> %s\n", attr.calc_cap.uint_ops & IBV_EXP_CALC_OP_BXOR ? "YES":"NO");
    printf("	* BOR    -> %s\n", attr.calc_cap.uint_ops & IBV_EXP_CALC_OP_BOR ? "YES":"NO");
    printf("	* MAXLOC -> %s\n", attr.calc_cap.uint_ops & IBV_EXP_CALC_OP_MAXLOC ? "YES":"NO");
    printf("  [fp operations]\n");
    printf("	* ADD    -> %s\n", attr.calc_cap.fp_ops & IBV_EXP_CALC_OP_ADD ? "YES":"NO");
    printf("	* BAND   -> %s\n", attr.calc_cap.fp_ops & IBV_EXP_CALC_OP_BAND ? "YES":"NO");
    printf("	* BXOR   -> %s\n", attr.calc_cap.fp_ops & IBV_EXP_CALC_OP_BXOR ? "YES":"NO");
    printf("	* BOR    -> %s\n", attr.calc_cap.fp_ops & IBV_EXP_CALC_OP_BOR ? "YES":"NO");
    printf("	* MAXLOC -> %s\n", attr.calc_cap.fp_ops & IBV_EXP_CALC_OP_MAXLOC ? "YES":"NO");

}

void RDMAConnectionManager::CheckAdvancedTransport(struct ibv_context *ctx) {
    //thansk -- https://github.com/lastweek/rdma_bench_dirty/blob/0060794d8456209c1af0d100046ed8a70f2acb22/drivers/libmlx5-1.0.2mlnx1/src/verbs.c#L1593

    struct ibv_exp_device_attr attr;
    if(ibv_exp_query_device(ctx, &attr)){
        printf("ibv_exp_query_device() failed\n");
        exit(0);
    }

    printf("UMR %s\n", attr.comp_mask & IBV_EXP_DEVICE_ATTR_UMR ? "YES":"NO");
    printf("ODP %s\n", attr.comp_mask & IBV_EXP_DEVICE_ATTR_ODP ? "YES":"NO");
    // printf("ODP MR %s\n", attr.comp_mask & IBV_EXP_DEVICE_ATTR_ODP_MR ? "YES":"NO");
    printf("Extended Atomics %s\n", attr.comp_mask & IBV_EXP_DEVICE_EXT_ATOMICS? "YES":"NO");
    printf("Extended Masked Atomics: %s\n", attr.comp_mask & IBV_EXP_DEVICE_EXT_MASKED_ATOMICS ? "YES":"NO");
    printf("Extended Capabilities %s\n", attr.comp_mask & IBV_EXP_DEVICE_ATTR_EXP_CAP_FLAGS ? "YES":"NO");
    printf("EXT ATOMICS %s\n", attr.comp_mask & IBV_EXP_DEVICE_ATTR_EXT_ATOMIC_ARGS ? "YES":"NO");

    printf("MASKED ATOMICS %s\n", attr.comp_mask & IBV_EXP_DEVICE_ATTR_MASKED_ATOMICS ? "YES":"NO");
    printf("CROSS CHANNEL %s\n", attr.exp_device_cap_flags & IBV_EXP_DEVICE_CROSS_CHANNEL ? "YES":"NO");

    attr.comp_mask &= IBV_EXP_DEVICE_ATTR_MASKED_ATOMICS;
    attr.comp_mask &= IBV_EXP_DEVICE_ATTR_EXP_CAP_FLAGS;
    attr.comp_mask &= IBV_EXP_DEVICE_ATTR_EXT_ATOMIC_ARGS;

	return;
}

static void print_caps_exp(uint64_t caps)
{
	uint64_t unknown_flags = ~(IBV_EXP_DEVICE_DC_TRANSPORT |
				   IBV_EXP_DEVICE_QPG |
				   IBV_EXP_DEVICE_UD_RSS |
				   IBV_EXP_DEVICE_UD_TSS |
				   IBV_EXP_DEVICE_CROSS_CHANNEL |
				   IBV_EXP_DEVICE_MR_ALLOCATE |
				   IBV_EXP_DEVICE_MR_ALLOCATE |
				   IBV_EXP_DEVICE_EXT_ATOMICS |
				   IBV_EXP_DEVICE_NOP |
				   IBV_EXP_DEVICE_UMR |
				   IBV_EXP_DEVICE_ODP |
				   IBV_EXP_DEVICE_VXLAN_SUPPORT |
				   IBV_EXP_DEVICE_RX_CSUM_TCP_UDP_PKT |
				   IBV_EXP_DEVICE_RX_CSUM_IP_PKT |
				   IBV_EXP_DEVICE_DC_INFO |
				   IBV_EXP_DEVICE_EXT_MASKED_ATOMICS |
				   IBV_EXP_DEVICE_RX_TCP_UDP_PKT_TYPE |
				   IBV_EXP_DEVICE_SCATTER_FCS |
				   IBV_EXP_DEVICE_WQ_DELAY_DROP |
				   IBV_EXP_DEVICE_PHYSICAL_RANGE_MR |
				   IBV_EXP_DEVICE_CAPI |
				   IBV_EXP_DEVICE_UMR_FIXED_SIZE |
				   IBV_EXP_DEVICE_PACKET_BASED_CREDIT_MODE);

	if (caps & IBV_EXP_DEVICE_DC_TRANSPORT)
		printf("\t\t\t\t\tEXP_DC_TRANSPORT\n");
	if (caps & IBV_EXP_DEVICE_QPG)
		printf("\t\t\t\t\tEXP_DEVICE_QPG\n");
	if (caps & IBV_EXP_DEVICE_UD_RSS)
		printf("\t\t\t\t\tEXP_UD_RSS\n");
	if (caps & IBV_EXP_DEVICE_UD_TSS)
		printf("\t\t\t\t\tEXP_UD_TSS\n");
	if (caps & IBV_EXP_DEVICE_CROSS_CHANNEL)
		printf("\t\t\t\t\tEXP_CROSS_CHANNEL\n");
	if (caps & IBV_EXP_DEVICE_MR_ALLOCATE)
		printf("\t\t\t\t\tEXP_MR_ALLOCATE\n");
	if (caps & IBV_EXP_DEVICE_SHARED_MR)
		printf("\t\t\t\t\tEXP_SHARED_MR\n");
	if (caps & IBV_EXP_DEVICE_EXT_ATOMICS)
		printf("\t\t\t\t\tEXT_ATOMICS\n");
	if (caps & IBV_EXP_DEVICE_NOP)
		printf("\t\t\t\t\tEXT_SEND NOP\n");
	if (caps & IBV_EXP_DEVICE_UMR)
		printf("\t\t\t\t\tEXP_UMR\n");
	if (caps & IBV_EXP_DEVICE_ODP)
		printf("\t\t\t\t\tEXP_ODP\n");
	if (caps & IBV_EXP_DEVICE_VXLAN_SUPPORT)
		printf("\t\t\t\t\tEXP_VXLAN_SUPPORT\n");
	if (caps & IBV_EXP_DEVICE_RX_CSUM_TCP_UDP_PKT)
		printf("\t\t\t\t\tEXP_RX_CSUM_TCP_UDP_PKT\n");
	if (caps & IBV_EXP_DEVICE_RX_CSUM_IP_PKT)
		printf("\t\t\t\t\tEXP_RX_CSUM_IP_PKT\n");
	if (caps & IBV_EXP_DEVICE_DC_INFO)
		printf("\t\t\t\t\tEXP_DC_INFO\n");
	if (caps & IBV_EXP_DEVICE_EXT_MASKED_ATOMICS)
		printf("\t\t\t\t\tEXP_MASKED_ATOMICS\n");
	if (caps & IBV_EXP_DEVICE_RX_TCP_UDP_PKT_TYPE)
		printf("\t\t\t\t\tEXP_RX_TCP_UDP_PKT_TYPE\n");
	if (caps & IBV_EXP_DEVICE_SCATTER_FCS)
		printf("\t\t\t\t\tEXP_SCATTER_FCS\n");
	if (caps & IBV_EXP_DEVICE_WQ_DELAY_DROP)
		printf("\t\t\t\t\tEXP_WQ_DELAY_DROP\n");
	if (caps & IBV_EXP_DEVICE_PHYSICAL_RANGE_MR)
		printf("\t\t\t\t\tEXP_PHYSICAL_RANGE_MR\n");
	if (caps & IBV_EXP_DEVICE_CAPI)
		printf("\t\t\t\t\tEXP_DEVICE_CAPI\n");
	if (caps & IBV_EXP_DEVICE_UMR_FIXED_SIZE)
		printf("\t\t\t\t\tEXP_UMR_FIXED_SIZE\n");
	if (caps & IBV_EXP_DEVICE_PACKET_BASED_CREDIT_MODE)
		printf("\t\t\t\t\tEXP_PACKET_BASED_CREDIT_MODE\n");
	if (caps & unknown_flags)
		printf("\t\t\t\t\tUnknown flags: 0x%" PRIX64 "\n", caps & unknown_flags);
}

void RDMAConnectionManager::CheckExtendedAttributes2(struct ibv_context *ctx) {
    //thansk -- https://github.com/emersonford/Freeflow/blob/8b807b7ca28afeea67407894bcbfdbbda30f1e87/libraries/libibverbs-1.2.1mlnx1/examples/devinfo.c#L299
    struct ibv_exp_device_attr device_attr;
    memset(&device_attr, 0, sizeof(device_attr));
	device_attr.comp_mask = 0xffffffff;
	device_attr.comp_mask_2 = IBV_EXP_DEVICE_ATTR_RESERVED_2 - 1;
    if(ibv_exp_query_device(ctx, &device_attr)){
        printf("ibv_exp_query_device() failed\n");
        exit(0);
    }
    if (strlen(device_attr.fw_ver))
		printf("\tfw_ver:\t\t\t\t%s\n", device_attr.fw_ver);
	// printf("\tnode_guid:\t\t\t%s\n", guid_str(device_attr.node_guid, buf));
	// printf("\tsys_image_guid:\t\t\t%s\n", guid_str(device_attr.sys_image_guid, buf));
	printf("\tvendor_id:\t\t\t0x%04x\n", device_attr.vendor_id);
	printf("\tvendor_part_id:\t\t\t%d\n", device_attr.vendor_part_id);
	printf("\thw_ver:\t\t\t\t0x%X\n", device_attr.hw_ver);
    print_caps_exp(device_attr.exp_device_cap_flags & ~(IBV_EXP_START_FLAG - 1));
}





void RDMAConnectionManager::CheckCapabilities() {
    printf("Checking Device Capabilities\n");
    // check if device memory is supported
    assert(devices[0]);
    CheckDMSupported(devices[0]);
    CheckAdvancedTransport(devices[0]);
    CheckGeneralExtendedAttributes(devices[0]);
    CheckExtendedAttributes2(devices[0]);
}


RDMAConnectionManager::RDMAConnectionManager(RDMAConnectionManagerArguments args) {

    _num_qps = args.num_qps;
    _base_port = args.base_port;
    _server_sockaddr = args.server_sockaddr;


    cm_event_channel = NULL;
    pd = NULL;
    client_send_wr, bad_client_send_wr = NULL;
    server_recv_wr, bad_server_recv_wr = NULL;

    if (_num_qps > MAX_QPS) {
        rdma_error("Failed to setup shared RDMA resourceds MAX QPS = %d. %d qp's were requested \n", MAX_QPS, _num_qps);
    }

    int ret;
    /* Setup shared resources */
    ret = client_setup_shared_resources();
    if (ret) { 
        rdma_error("Failed to setup shared RDMA resources , ret = %d \n", ret);
        throw std::runtime_error("Failed to setup shared RDMA resources");
    }

    #ifdef __LOG_INFO
    #endif

    /* Connect the local QPs to the ones on server. 
     * NOTE: Make sure to connect all QPs before moving to 
     * other activities that involve communication with the server as the 
     * server runs on single core and waits for connect request for all 
     * QPs before moving forward. */
    for(int i = 0; i < _num_qps; i++) {
        /* Each QP will try to connect to port numbers starting from base port */
        ret = client_prepare_connection(_server_sockaddr, i, _base_port + i);
        if (ret) { 
            ALERT("Connection Manager", "Failed to prepare client connection , ret = %d \n", ret);
            throw std::runtime_error("Failed to setup client connection");
        }

        ret = client_pre_post_recv_buffer(i); 
        if (ret) { 
            ALERT("Connection Manager", "Failed to post receive buffers client connection , ret = %d \n", ret);
            throw std::runtime_error("Failed to setup client connection");
        }
        
        ret = client_connect_qp_to_server(i);
        if (ret) { 
            ALERT("Connection Manager", "Failed to setup client connection , ret = %d \n", ret);
            throw std::runtime_error("Failed to setup client connection");
        }
        INFO("Connection Manager", "Successfully setup client connection %d", i);

        // Give server some time to prepare for the next QP
        // usleep(100);     // 100ms
        _connections_initialized = true;
    }
    SUCCESS("Connection Manager", "Successfully setup %d QPs", _num_qps);
    
}

// RDMAConnectionManager::~RDMAConnectionManager() {

//     printf("TODO actually make constructior this one is really bad and fails because of the disconeect call\n");
//     return;

//     int ret;
//     for (int i = 0; i < _num_qps; i++) {
//         printf("RDMA CONNECTION MANAGER DESTRUCTOR disconnecting qp %d \n", i);
//         ret = client_disconnect_and_clean(i);
//         if (ret)
//             rdma_error("Failed to cleanly disconnect qp %d \n", i);
//             throw std::runtime_error("Failed to cleanly disconnect qp");
//     }
//     ret = client_clean();
//     if (ret)
//         rdma_error("Failed to clean client resources\n");
//         throw std::runtime_error("Failed to clean client resources");
// }


/* This function prepares client side shared resources for all connections */
int RDMAConnectionManager::client_setup_shared_resources()
{
    int ret = -1;
    /* Get RDMA devices */
    devices = rdma_get_devices(&ret);
    if (ret == 0) {
        ALERT("Connection Manager", "No RDMA devices found\n");
        return -ENODEV;
    }
    SUCCESS("Connection Manager", "%d devices found, using the first one: %s\n", ret, devices[0]->device->name);
    for(int i = 0; i < ret; i++)    VERBOSE("Connection Manager", "Device %d: %s\n", i+1, devices[i]->device->name);



    CheckCapabilities();

    /* Create shared resources for all conections per device i.e., cq, pd, etc */
    /* Protection Domain (PD) is similar to a "process abstraction" 
     * in the operating system. All resources are tied to a particular PD. 
     * And accessing recourses across PD will result in a protection fault.
     */
    pd = ibv_alloc_pd(devices[0]);
    if (!pd) {
        ALERT("Connection Manager","Failed to alloc pd, errno: %d \n", -errno);
        return -errno;
    }
    VERBOSE("Connection Manager", "pd allocated at %p \n", pd);

    for(int i=0;i<MAX_THREADS;i++){
        INFO("Connection Manager", "allocing completion channel %d\n",i);
        /* Now we need a completion channel, were the I/O completion 
        * notifications are sent. Remember, this is different from connection 
        * management (CM) event notifications. 
        * A completion channel is also tied to an RDMA device
        */
        io_completion_channel_threads[i] = ibv_create_comp_channel(devices[0]);
        if (!io_completion_channel_threads[i]) {
            ALERT("Connection Manager","Failed to create IO completion event channel %d, errno: %d\n",
                    i,-errno);
            return -errno;
        }
        INFO("Connection Manager", "io completion channel @ %p\n",(void*)io_completion_channel_threads[i]);
    }
    SUCCESS("Connection Manager", "created %d io completion channels\n",MAX_THREADS);

    ret = ibv_query_device(devices[0], &dev_attr);    
    if (ret) {
        ALERT("Connection Manager", "Failed to get device info, errno: %d\n", -errno);
        return -errno;
    }
    SUCCESS("Connection Manager", "got device info. max qpe: %d, sge: %d, cqe: %d, max rd/at qp depth/outstanding: %d/%d max mr size: %lu\n", 
        dev_attr.max_qp_wr, dev_attr.max_sge, dev_attr.max_cqe, dev_attr.max_qp_init_rd_atom, dev_attr.max_qp_rd_atom, 
        dev_attr.max_mr_size);

    
    /*  Open a channel used to report asynchronous communication event */
    cm_event_channel = rdma_create_event_channel();
    if (!cm_event_channel) {
        ALERT("Connection Manager", "Creating cm event channel failed, errno: %s \n", strerror(errno));
        return -errno;
    }
    SUCCESS("Connection Manager", "RDMA CM event channel is created at : %p \n", cm_event_channel);

    INFO("Connection Manager", "Making a total of %d completion queues\n", MAX_THREADS);
    for(int i=0;i<MAX_THREADS;i++){
        thread_contexts[i]=i;
        client_cq_threads[i] = ibv_create_cq(devices[0] /* which device*/, 
            CQ_CAPACITY             /* maximum device capacity*/, 
            &thread_contexts[i]                    /* user context, not used here */,
            //io_completion_channel   /* which IO completion channel */, 
            io_completion_channel_threads[i]   /* which IO completion channel */, 
            //io_completion_channel_threads[i]   /* which IO completion channel */, 
            0                       /* signaling vector, not used here*/);
        if (!client_cq_threads[i]) {
            ALERT("Connection Manager", "Failed to create CQ %d, errno: %s \n", i, strerror(errno));
            return -errno;
        }
        VERBOSE("Connection Manager", "CQ created at %p with %d elements \n", (void*)client_cq_threads[i], client_cq_threads[i]->cqe);
        ret = ibv_req_notify_cq(client_cq_threads[i], 0);
        if (ret) {
            ALERT("Connection Manager", "Failed to request notifications, errno: %d\n", -errno);
            return -errno;
        }

    }

    return ret;
}

/* This function prepares client side connection for a QP */
int RDMAConnectionManager::client_prepare_connection(struct sockaddr_in *s_addr, int qp_num, int port_num)
{
    int ret = -1;
    struct rdma_cm_event *cm_event = NULL;
    /* rdma_cm_id is the connection identifier (like socket) which is used 
    * to define an RDMA connection. 
    */
    ret = rdma_create_id(cm_event_channel, &cm_client_qp_id[qp_num], 
            devices[0],
            RDMA_PS_TCP);
    if (ret) {
        ALERT("Connection Manager", "Creating cm id failed with errno: %d \n", -errno); 
        return -errno;
    }

    /* Resolve destination and optional source addresses from IP addresses  to
    * an RDMA address.  If successful, the specified rdma_cm_id will be bound
    * to a local device. */
    s_addr->sin_port = htons(port_num);
    ret = rdma_resolve_addr(cm_client_qp_id[qp_num], NULL, (struct sockaddr*) s_addr, 2000);
    if (ret) {
        ALERT("Connection Manager", "Failed to resolve address, errno: %d \n", -errno);
        return -errno;
    }
    VERBOSE("Connection Manager", "waiting for cm event: RDMA_CM_EVENT_ADDR_RESOLVED\n");
    ret  = process_rdma_cm_event(cm_event_channel, 
            RDMA_CM_EVENT_ADDR_RESOLVED,
            &cm_event);
    if (ret) {
        ALERT("Connection Manager","Failed to receive a valid event, ret = %d \n", ret);
        return ret;
    }
    /* we ack the event */
    ret = rdma_ack_cm_event(cm_event);
    if (ret) {
        ALERT("Connection Manager", "Failed to acknowledge the CM event, errno: %d\n", -errno);
        return -errno;
    }
    VERBOSE("Connection Manager","RDMA address is resolved \n");
    /* Resolves an RDMA route to the destination address in order to 
    * establish a connection */
    ret = rdma_resolve_route(cm_client_qp_id[qp_num], 2000);
    if (ret) {
        ALERT("Connection Manager", "Failed to resolve route, erno: %d \n", -errno);
        return -errno;
    }
    VERBOSE("Connection Manager","waiting for cm event: RDMA_CM_EVENT_ROUTE_RESOLVED\n");
    ret = process_rdma_cm_event(cm_event_channel, 
            RDMA_CM_EVENT_ROUTE_RESOLVED,
            &cm_event);
    if (ret) {
        ALERT("Connectiopn Manager","Failed to receive a valid event, ret = %d \n", ret);
        return ret;
    }
    /* we ack the event */
    ret = rdma_ack_cm_event(cm_event);
    if (ret) {
        rdma_error("Failed to acknowledge the CM event, errno: %d \n", -errno);
        return -errno;
    }
    INFO("Connection Manager", "Trying to connect QP %d to server at : %s port: %d \n", qp_num, 
        inet_ntoa(s_addr->sin_addr), ntohs(s_addr->sin_port));

    /* TODO: Get capacity from device */
    /* Now the last step, set up the queue pair (send, recv) queues and their capacity.
    * Set capacity to device limits (since we use only one qp and one application) 
    * This only sets the limits; this will let us play around with the actual numbers */
    bool experimental = true;
    if (!experimental) {
        bzero(&qp_init_attr, sizeof qp_init_attr);
        qp_init_attr.cap.max_recv_sge = MAX_SGE;    /* Maximum SGE per receive posting;*/
        qp_init_attr.cap.max_recv_wr = MAX_WR;      /* Maximum receive posting capacity; */
        qp_init_attr.cap.max_send_sge = MAX_SGE;    /* Maximum SGE per send posting;*/
        qp_init_attr.cap.max_send_wr = MAX_WR;      /* Maximum send posting capacity; */
        qp_init_attr.cap.max_inline_data = 128;      /* Maximum amount of inline data */
        qp_init_attr.qp_type = IBV_QPT_RC;                  /* QP type, RC = Reliable connection */


        /* We use same completion queue, but one can use different queues */
        #ifdef MULTI_CQ
        qp_init_attr.recv_cq = client_cq_threads[qp_num]; /* Where should I notify for receive completion operations */
        qp_init_attr.send_cq = client_cq_threads[qp_num]; /* Where should I notify for send completion operations */
        #else
        qp_init_attr.recv_cq = client_cq; /* Where should I notify for receive completion operations */
        qp_init_attr.send_cq = client_cq; /* Where should I notify for send completion operations */
        #endif
        ret = rdma_create_qp(cm_client_qp_id[qp_num], pd, &qp_init_attr);
        if (ret) {
            ALERT("Connection Manager","Failed to create QP, errno: %d \n", -errno);
            return -errno;
        }
        client_qp[qp_num] = cm_client_qp_id[qp_num]->qp;
        INFO("Connection Manager", "QP %d created at %p \n", qp_num, (void *)client_qp[qp_num]);
    } else {
        bzero(&qp_init_attr_exp, sizeof(qp_init_attr_exp));
        // qp_init_attr.qp_context = nullptr;
        qp_init_attr_exp.qp_type = IBV_QPT_RC;                  /* QP type, RC = Reliable connection */
        qp_init_attr_exp.sq_sig_all = 0;
        #ifdef MULTI_CQ
        assert(client_cq_threads[qp_num] != NULL);
        qp_init_attr_exp.recv_cq = client_cq_threads[qp_num]; /* Where should I notify for receive completion operations */
        qp_init_attr_exp.send_cq = client_cq_threads[qp_num]; /* Where should I notify for send completion operations */
        #else
        qp_init_attr_exp.recv_cq = client_cq; /* Where should I notify for receive completion operations */
        qp_init_attr_exp.send_cq = client_cq; /* Where should I notify for send completion operations */
        #endif
        qp_init_attr_exp.pd = pd;
        if (qp_init_attr_exp.qp_type == IBV_QPT_RC) {
            qp_init_attr_exp.comp_mask = IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS |
                            IBV_EXP_QP_INIT_ATTR_PD | IBV_EXP_QP_INIT_ATTR_ATOMICS_ARG;
            qp_init_attr_exp.max_atomic_arg = 32;
        } else {
            qp_init_attr_exp.comp_mask = IBV_EXP_QP_INIT_ATTR_PD;
        }
        qp_init_attr_exp.cap.max_send_wr = MAX_WR;      /* Maximum send posting capacity; */
        qp_init_attr_exp.cap.max_recv_wr = MAX_WR;      /* Maximum receive posting capacity; */
        qp_init_attr_exp.cap.max_send_sge = MAX_SGE;    /* Maximum SGE per send posting;*/
        qp_init_attr_exp.cap.max_recv_sge = MAX_SGE;    /* Maximum SGE per receive posting;*/
        qp_init_attr_exp.cap.max_inline_data = 128;      /* Maximum amount of inline data */

        // qp_init_attr_exp.exp_create_flags = IBV_EXP_QP_CREATE_ATOMIC_BE_REPLY;
        /* We use same completion queue, but one can use different queues */
        client_qp[qp_num] = ibv_exp_create_qp(devices[0], &qp_init_attr_exp);
        cm_client_qp_id[qp_num]->qp = client_qp[qp_num];
        if (!client_qp[qp_num]) {
            ALERT("Connection Manager", "Failed to EXP create QP, errno#: %d error %s\n", -errno, strerror(errno));
            return -errno;
        }
        // client_qp[qp_num] = cm_client_qp_id[qp_num]->qp;
        INFO("Connection Manager", "EXP QP %d created at %p \n", qp_num, (void *)client_qp[qp_num]);
    }

    return ret;
}

/* Pre-posts a receive buffer before calling rdma_connect () */
int RDMAConnectionManager::client_pre_post_recv_buffer(int qp_num)
{
    int ret = -1;
    server_qp_metadata_mr[qp_num] = rdma_buffer_register(pd,
            &server_qp_metadata_attr[qp_num],
            sizeof(server_qp_metadata_attr[qp_num]),
            (IBV_ACCESS_LOCAL_WRITE));
    if(!server_qp_metadata_mr[qp_num]){
        rdma_error("Failed to setup the server metadata mr , -ENOMEM\n");
        return -ENOMEM;
    }
    server_recv_sge.addr = (uint64_t) server_qp_metadata_mr[qp_num]->addr;
    server_recv_sge.length = (uint32_t) server_qp_metadata_mr[qp_num]->length;
    server_recv_sge.lkey = (uint32_t) server_qp_metadata_mr[qp_num]->lkey;
    /* now we link it to the request */
    bzero(&server_recv_wr, sizeof(server_recv_wr));
    server_recv_wr.sg_list = &server_recv_sge;
    server_recv_wr.num_sge = 1;
    ret = ibv_post_recv(client_qp[qp_num] /* which QP */,
            &server_recv_wr /* receive work request*/,
            &bad_server_recv_wr /* error WRs */);
    if (ret) {
        rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
        return ret;
    }
    debug("Receive buffer pre-posting is successful \n");
    return 0;
}

/* Connects a QP to an RDMA server QP */
int RDMAConnectionManager::client_connect_qp_to_server(int qp_num) 
{
    struct rdma_conn_param conn_param;
    struct rdma_cm_event *cm_event = NULL;
    int ret = -1;
    bzero(&conn_param, sizeof(conn_param));
    conn_param.initiator_depth = MAX_RD_AT_IN_FLIGHT;
    conn_param.responder_resources = MAX_RD_AT_IN_FLIGHT;
    conn_param.retry_count = 3; // if fail, then how many times to retry
    ret = rdma_connect(cm_client_qp_id[qp_num], &conn_param);
    if (ret) {
        ALERT("Connection Manager", "Failed to connect to remote host , errno: %d\n", -errno);
        return -errno;
    }
    VERBOSE("Connection Manager", "waiting for cm event: RDMA_CM_EVENT_ESTABLISHED on qp num %d\n", qp_num);
    // ret = process_rdma_cm_event(cm_event_channel, 
    //         RDMA_CM_EVENT_CONNECT_RESPONSE,
    //         &cm_event);
    // printf("received a cm event: RDMA_CM_CONNECT_RESPONSE\n");
    ret = process_rdma_cm_event(cm_event_channel, 
            RDMA_CM_EVENT_ESTABLISHED,
            &cm_event);
    if (ret) {
        ALERT("Connection Manager", "Failed to get cm event, ret = %d \n", ret);
           return ret;
    }
    ret = rdma_ack_cm_event(cm_event);
    if (ret) {
        ALERT("Connection Manager", "Failed to acknowledge cm event, errno: %d\n", 
                   -errno);
        return -errno;
    }
    INFO("Connection Manager", "The client qp %d is connected successfully \n", qp_num);
    return 0;
}

/* Set up buffers to exchange data with the server, on a given QP. The client sends its, and then receives
 * from the server. The client-side metadata on the server is _not_ used because
 * this program is client driven. But it shown here how to do it for the illustration
 * purposes.
 * buffer: register as client buffer if provided.
 * buffer_size: if buffer is not provided, allocate buffers of this size on both client and server. 
 */
int RDMAConnectionManager::client_xchange_metadata_with_server(int qp_num, char* buffer, uint32_t buffer_size)
{
    struct ibv_wc wc[2];
    int ret = -1;
    
    client_qp_src_mr[qp_num] = 
        buffer == NULL ? 
            rdma_buffer_alloc(pd,
                buffer_size,
                MEMORY_PERMISSION
                ) :
            rdma_buffer_register(pd,
                buffer,
                buffer_size,
                MEMORY_PERMISSION);;
    if(!client_qp_src_mr[qp_num]){
        rdma_error("Failed to register the first buffer, ret = %d \n", ret);
        return ret;
    }

    /* we prepare metadata for the first buffer */
    client_qp_metadata_attr[qp_num].address = (uint64_t) client_qp_src_mr[qp_num]->addr; 
    client_qp_metadata_attr[qp_num].length = client_qp_src_mr[qp_num]->length; 
    client_qp_metadata_attr[qp_num].stag.local_stag = client_qp_src_mr[qp_num]->lkey;
    /* now we register the metadata memory */
    client_qp_metadata_mr[qp_num] = rdma_buffer_register(pd,
            &client_qp_metadata_attr[qp_num],
            sizeof(client_qp_metadata_attr[qp_num]),
            IBV_ACCESS_LOCAL_WRITE);
    if(!client_qp_metadata_mr[qp_num]) {
        rdma_error("Failed to register the client metadata buffer, ret = %d \n", ret);
        return ret;
    }

    /* now we fill up SGE */
    client_send_sge.addr = (uint64_t) client_qp_metadata_mr[qp_num]->addr;
    client_send_sge.length = (uint32_t) client_qp_metadata_mr[qp_num]->length;
    client_send_sge.lkey = client_qp_metadata_mr[qp_num]->lkey;
    /* now we link to the send work request */
    bzero(&client_send_wr, sizeof(client_send_wr));
    client_send_wr.sg_list = &client_send_sge;
    client_send_wr.num_sge = 1;
    client_send_wr.opcode = IBV_WR_SEND;
    client_send_wr.send_flags = IBV_SEND_SIGNALED;
    /* Now we post it */
    ret = ibv_post_send(client_qp[qp_num], 
               &client_send_wr,
           &bad_client_send_wr);
    if (ret) {
        rdma_error("Failed to send client metadata, errno: %d \n", 
                -errno);
        return -errno;
    }
    /* at this point we are expecting 2 work completion. One for our 
     * send and one for recv that we will get from the server for 
     * its buffer information */
    ret = process_work_completion_events(io_completion_channel_threads[qp_num], 
            wc, 2);
    if(ret != 2) {
        rdma_error("We failed to get 2 work completions , ret = %d \n",
                ret);
        return ret;
    }
    debug("Server sent us buffer location and credentials for QP %d, showing \n", qp_num);
    show_rdma_buffer_attr(&server_qp_metadata_attr[qp_num]);

    return 0;
}




/* This function disconnects the RDMA connection from the server and cleans up 
 * all the resources, for a given QP
 */
int RDMAConnectionManager::client_disconnect_and_clean(int qp_num)
{
    struct rdma_cm_event *cm_event = NULL;
    int ret = -1;
    /* active disconnect from the client side */
    ret = rdma_disconnect(cm_client_qp_id[qp_num]);
    if (ret) {
        rdma_error("Failed to disconnect, errno: %d \n", -errno);
        //continuing anyways
    }
    ret = process_rdma_cm_event(cm_event_channel, 
            RDMA_CM_EVENT_DISCONNECTED,
            &cm_event);
    if (ret) {
        rdma_error("Failed to get RDMA_CM_EVENT_DISCONNECTED event, ret = %d\n",
                ret);
        //continuing anyways 
    }
    ret = rdma_ack_cm_event(cm_event);
    if (ret) {
        rdma_error("Failed to acknowledge cm event, errno: %d\n", 
                   -errno);
        //continuing anyways
    }
    /* Destroy QP */
    rdma_destroy_qp(cm_client_qp_id[qp_num]);

    /* Destroy client cm id */
    ret = rdma_destroy_id(cm_client_qp_id[qp_num]);
    if (ret) {
        rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
        // we continue anyways;
    }

    /* Destroy memory buffers */
    rdma_buffer_deregister(server_qp_metadata_mr[qp_num]);
    rdma_buffer_deregister(client_qp_metadata_mr[qp_num]);	
    rdma_buffer_deregister(client_qp_src_mr[qp_num]);	
    //printf("Client QP %d clean up is complete \n", qp_num);
    return 0;
}

/* This function destroys all the shared resources
 */
int RDMAConnectionManager::client_clean()
{
    int ret = -1;

    for (int i=0;i<MAX_THREADS;i++) {
        int ret = -1;
        /* Destroy CQ */
        ret = ibv_destroy_cq(client_cq_threads[i]);
        if (ret) {
            rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
            // we continue anyways;
        }
    }

    for (int i=0;i<MAX_THREADS;i++) {
        /* Destroy completion channel */
        ret = ibv_destroy_comp_channel(io_completion_channel_threads[i]);
        if (ret) {
            rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
            // we continue anyways;
        }
    }
    /* Destroy protection domain */
    ret = ibv_dealloc_pd(pd);
    if (ret) {
        rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
        // we continue anyways;
    }
    rdma_destroy_event_channel(cm_event_channel);
    rdma_free_devices(devices);
    printf("Client resource clean up is complete \n");
    return 0;
}
