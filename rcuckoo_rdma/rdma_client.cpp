// #include <infiniband/mlx5.h>
#include "rdma_common.h"
#include "rdma_client.h"
#include "rdma_client_lib.h"
// #include <infiniband/verbs_exp.h>
#include <infiniband/verbs.h>
#include <sys/time.h>
#include <assert.h>     /* assert */
 #include <byteswap.h>




static int finished_running_xput=0;

inline int get_buf_offset(int slot_num, int msg_size) {
    return slot_num * (msg_size + GLOBAL_GAP_INTEGER);
}

int get_buffer_size(int num_concur,int msg_size) {
    return num_concur *(get_buf_offset(1,msg_size) - get_buf_offset(0, msg_size));
}

/* Rdtsc blocks for time measurements */
unsigned cycles_low, cycles_high, cycles_low1, cycles_high1;
// static __inline__ unsigned long long rdtsc(void)
static __inline__ void rdtsc(void)
{
   __asm__ __volatile__ ("RDTSC\n\t"
            "mov %%edx, %0\n\t"
            "mov %%eax, %1\n\t": "=r" (cycles_high), "=r" (cycles_low)::
            "%rax", "rbx", "rcx", "rdx");
}

// static __inline__ unsigned long long rdtsc1(void)
static __inline__ void rdtsc1(void)
{
   __asm__ __volatile__ ("RDTSC\n\t"
            "mov %%edx, %0\n\t"
            "mov %%eax, %1\n\t": "=r" (cycles_high1), "=r" (cycles_low1)::
            "%rax", "rbx", "rcx", "rdx");
}

unsigned xcycles_low, xcycles_high, xcycles_low1, xcycles_high1;
// static __inline__ unsigned long long xrdtsc(void)
static __inline__ void xrdtsc(void)
{
   __asm__ __volatile__ ("RDTSC\n\t"
            "mov %%edx, %0\n\t"
            "mov %%eax, %1\n\t": "=r" (xcycles_high), "=r" (xcycles_low)::
            "%rax", "rbx", "rcx", "rdx");
}

// static __inline__ unsigned long long xrdtsc1(void)
static __inline__ void xrdtsc1(void)
{
   __asm__ __volatile__ ("RDTSC\n\t"
            "mov %%edx, %0\n\t"
            "mov %%eax, %1\n\t": "=r" (xcycles_high1), "=r" (xcycles_low1)::
            "%rax", "rbx", "rcx", "rdx");
}

/* This is our testing function */
int check_src_dst(char *src, char *dst) {
    return memcmp((void*) src, (void*) dst, strlen(src));
}

/* This function does the following for given QP:
 * 1) Prepare memory buffers for RDMA operations 
 * 1) RDMA write from src -> remote buffer 
 * 2) RDMA read from remote bufer -> dst
 */ 
static int client_remote_memory_ops(int qp_num, RDMAConnectionManager &cm) 
{

    struct ibv_sge client_send_sge;
    struct ibv_sge server_recv_sge;
    static struct ibv_send_wr client_send_wr, *bad_client_send_wr;
    static struct ibv_recv_wr server_recv_wr, *bad_server_recv_wr;
    static char *src , *dst; 
    struct ibv_wc wc;
    int ret = -1;
    // struct timeval start, end;
    // long ops_count = 0;
    // double duration = 0.0;
    // double throughput = 0.0;
    uint64_t start_cycles, end_cycles;

    cm.client_qp_dst_mr[qp_num] = rdma_buffer_register(cm.pd,
        dst,
        strlen(src), MEMORY_PERMISSION
        );
    if (!cm.client_qp_dst_mr[qp_num]) {
        rdma_error("We failed to create the destination buffer, -ENOMEM\n");
        return -ENOMEM;
    }
    /* Step 1: is to copy the local buffer into the remote buffer. We will 
     * reuse the previous variables. */
    /* now we fill up SGE */
    client_send_sge.addr = (uint64_t) cm.client_qp_src_mr[qp_num]->addr;
    client_send_sge.length = (uint32_t) cm.client_qp_src_mr[qp_num]->length;
    client_send_sge.lkey = cm.client_qp_src_mr[qp_num]->lkey;
    /* now we link to the send work request */
    bzero(&client_send_wr, sizeof(client_send_wr));
    client_send_wr.sg_list = &client_send_sge;
    client_send_wr.num_sge = 1;
    client_send_wr.opcode = IBV_WR_RDMA_WRITE;
    client_send_wr.send_flags = IBV_SEND_SIGNALED;
    /* we have to tell server side info for RDMA */
    client_send_wr.wr.rdma.rkey = cm.server_qp_metadata_attr[qp_num].stag.remote_stag;
    client_send_wr.wr.rdma.remote_addr = cm.server_qp_metadata_attr[qp_num].address;


    rdtsc();
    /* Now we post it */
    ret = ibv_post_send(cm.client_qp[qp_num], 
               &client_send_wr,
           &bad_client_send_wr);
    if (ret) {
        rdma_error("Failed to write client src buffer, errno: %d \n", 
                -errno);
        return -errno;
    }
    /* at this point we are expecting 1 work completion for the write */
    ret = process_work_completion_events(cm.io_completion_channel_threads[qp_num], &wc, 1);
    if(ret != 1) {
        rdma_error("We failed to get 1 work completions , ret = %d \n", ret);
        return ret;
    }
    rdtsc1();

    debug("Client side WRITE is complete \n");
    start_cycles = ( ((int64_t)cycles_high << 32) | cycles_low );
    end_cycles = ( ((int64_t)cycles_high1 << 32) | cycles_low1 );
    printf("Client side WRITE took %lf mu-sec\n", (end_cycles - start_cycles) * 1e6 / CPU_FREQ);

    /* Now we prepare a READ using same variables but for destination */
    client_send_sge.addr = (uint64_t) cm.client_qp_dst_mr[qp_num]->addr;
    client_send_sge.length = (uint32_t) cm.client_qp_dst_mr[qp_num]->length;
    client_send_sge.lkey = cm.client_qp_dst_mr[qp_num]->lkey;
    /* now we link to the send work request */
    bzero(&client_send_wr, sizeof(client_send_wr));
    client_send_wr.sg_list = &client_send_sge;
    client_send_wr.num_sge = 1;
    client_send_wr.opcode = IBV_WR_RDMA_READ;
    client_send_wr.send_flags = IBV_SEND_SIGNALED;
    /* we have to tell server side info for RDMA */
    client_send_wr.wr.rdma.rkey = cm.server_qp_metadata_attr[qp_num].stag.remote_stag;
    client_send_wr.wr.rdma.remote_addr = cm.server_qp_metadata_attr[qp_num].address;
    /* Now we post it */
    ret = ibv_post_send(cm.client_qp[qp_num], &client_send_wr, &bad_client_send_wr);
    if (ret) {
        rdma_error("Failed to read client dst buffer from the master, errno: %d \n", -errno);
        return -errno;
    }
    /* at this point we are expecting 1 work completion for the write */
    ret = process_work_completion_events(cm.io_completion_channel_threads[qp_num], &wc, 1);
    if(ret != 1) {
        rdma_error("We failed to get 1 work completions , ret = %d \n", ret);
        return ret;
    }
    debug("Client side READ is complete \n");
    
    // This buffer is local to this method, won't need it anymore.
    rdma_buffer_deregister(cm.client_qp_dst_mr[qp_num]);	
    return 0;
}


void set_wr_op_code(struct ibv_send_wr  &client_send_wr,  enum rdma_measured_op rdma_op) {
    switch (rdma_op) {
        case RDMA_READ_OP:
            client_send_wr.opcode = IBV_WR_RDMA_READ;
        break;
        case RDMA_WRITE_OP:
            client_send_wr.opcode = IBV_WR_RDMA_WRITE;
            client_send_wr.send_flags |= IBV_SEND_INLINE;          // NOTE: This tells the other NIC to send completion events
        break;
        case RDMA_CAS_OP:
            printf("setting cas in header\n");
            client_send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        break;
        case RDMA_FAA_OP:
            client_send_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
            //client_send_wr.send_flags |= IBV_EXP_SEND_EXT_ATOMIC_INLINE;
        break;
    }
}



void set_send_work_request(struct ibv_send_wr &client_send_wr, struct ibv_sge &client_send_sge, enum rdma_measured_op rdma_op, uint32_t remote_key, uint64_t remote_address){

    bzero(&client_send_wr, sizeof(client_send_wr));
    client_send_wr.sg_list = &client_send_sge;
    client_send_wr.num_sge = 1;

    set_wr_op_code(client_send_wr, rdma_op);
    client_send_wr.send_flags |= IBV_SEND_SIGNALED;          // NOTE: This tells the other NIC to send completion events

    /* we have to tell server side info for RDMA */
    client_send_wr.wr.rdma.rkey = remote_key;
    client_send_wr.wr.rdma.remote_addr = remote_address;
}


#define BATCH_SIZE 1024
typedef struct performance_statistics {
    uint64_t start_cycles;
    uint64_t end_cycles;
    uint64_t poll_count;
    uint64_t idle_count;
    uint64_t poll_time;
    uint64_t wr_posted;
    uint64_t wr_acked;
    struct timeval start;
    struct timeval end;
    uint32_t message_size;
} performance_statistics;

int bulk_poll(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc, performance_statistics *ps) {
    int n = 0;
    int ret = 0;
    do {
        n = ibv_poll_cq(cq, num_entries, wc);       // get upto num_concur entries
        if (n < 0) {
            printf("Failed to poll cq for wc due to %d\n", ret);
            rdma_error("Failed to poll cq for wc due to %d \n", ret);
            exit(1);
        }
        //todo put the poll count and idle count here 
        (ps->poll_count)++;
        if (n == 0) {
            (ps->idle_count)++;
            //todo deal with a global variable being used to break the polling here
            //we should deal with finished running xput elsewhere (hard to tell where though)
            if (finished_running_xput) {
                break;
            }
        }     
    } while (n < 1);
    return n;
}

void send_bulk(int n, int qp_num, RDMAConnectionManager *cm, struct ibv_send_wr *send_work_request_batch) {
    assert(n <= BATCH_SIZE);
    assert(n > 0);
    assert(send_work_request_batch);
    struct ibv_send_wr **bad_send_wr;
    for (int i = 0; i < n; i++) {
        send_work_request_batch[i].next=&(send_work_request_batch[i+1]);
    }
    send_work_request_batch[n-1].next=NULL;
    int ret = ibv_post_send(cm->client_qp[qp_num], 
            &(send_work_request_batch[0]),
            bad_send_wr);
    if (ret) {
        rdma_error("Failed to write client src buffer, errno: %d \n", -errno);
        exit(1);
    }
}


result_t get_thread_performance_results(performance_statistics *ps){ 
    /* Calculate duration. See if RDTSC timer agrees with regular ctime.
     * ctime is more accurate on longer timescales as rdtsc depends on cpu frequency which is not stable
     * but rdtsc is low overhead so we can use that from (roughly) keeping track of time while we use ctime 
     * to calculate numbers */
    result_t result;
    double duration_rdtsc = (ps->end_cycles - ps->start_cycles) / CPU_FREQ;
    double duration_ctime = (double)((ps->end.tv_sec - ps->start.tv_sec) + (ps->end.tv_usec - ps->start.tv_usec) * 1.0e-6);
    //printf("Duration as measured by RDTSC: %.2lf secs, by ctime: %.2lf secs\n", duration_rdtsc, duration_ctime);

    double goodput_pps = ps->wr_acked / duration_ctime;
    double goodput_bps = goodput_pps * ps->message_size * 8;
    //printf("Goodput = %.2lf Gbps. Sampled for %.2lf seconds\n", goodput_bps / 1e9, duration_ctime);
    
    /* Fill in result */
    result.xput_bps = goodput_bps / 1e9;
    result.xput_ops = goodput_pps;
    result.cq_poll_time_percent = ps->poll_time * 100.0 / CPU_FREQ / duration_rdtsc;
    result.cq_poll_count = ps->poll_count * 1.0 / ps->wr_acked;     // TODO: Is dividing by wr the right way to interpret this number?
    result.cq_empty_count = ps->idle_count * 1.0 / ps->wr_acked;     // TODO: Is dividing by wr the right way to interpret this number?
    return result;
}

uint64_t get_xput_thread_remote_address(uint64_t local_server_address, struct ibv_wc* wc, int message_size){
        union work_req_id wr_id;
        wr_id.val =  wc->wr_id;
        int slot_num = wr_id.s.window_slot;
        uint64_t remote_memory_address = local_server_address + get_buf_offset(slot_num,message_size);
        return remote_memory_address;
}

void check_work_completion_success(struct ibv_wc * wc){
    if (wc->status != IBV_WC_SUCCESS) {
        rdma_error("Work completion (WC) has error status: %d, %s at index %ld\n",  
            wc->status, ibv_wc_status_str(wc->status), wc->wr_id);
    }
}

void set_compare_and_swap_work_request(struct ibv_send_wr  &client_send_wr, uint64_t address, uint32_t rkey, uint64_t compare_add, uint64_t swap) {
    client_send_wr.wr.atomic.remote_addr = address;
    client_send_wr.wr.atomic.rkey = rkey;
    client_send_wr.wr.atomic.compare_add = compare_add;
    client_send_wr.wr.atomic.swap = swap;
}

void set_fetch_and_add_work_request(struct ibv_send_wr  &client_send_wr, uint64_t address, uint32_t rkey, uint64_t swap) {
    client_send_wr.wr.atomic.remote_addr = address;
    client_send_wr.wr.atomic.rkey = rkey;
    client_send_wr.wr.atomic.compare_add = 1ULL;
    client_send_wr.wr.atomic.swap = swap;
}

void set_scatter_gather_entry(struct ibv_sge &client_send_sge, uint64_t address, uint32_t length, uint32_t lkey) {
    client_send_sge.addr = address;
    client_send_sge.length = length;
    client_send_sge.lkey = lkey;
}

void * xput_thread(void * args) {
    struct xput_thread_args * targs = (struct xput_thread_args *)args;
    stick_this_thread_to_core(targs->core);

    //Unpack thread arguments
    int num_concur = targs->num_concur;
    struct ibv_cq *cq_ptr = targs->cq_ptr;
    int msg_size = targs->msg_size;
    enum rdma_measured_op rdma_op = targs->rdma_op;
    struct ibv_mr **mr_buffers=targs->mr_buffers;           /* Make sure to deregister these local MRs before exiting */
    RDMAConnectionManager * cm = targs->cm;
    int qp_num = targs->thread_id;

    //Initialize performance statistics
    performance_statistics ps;
    ps.start_cycles=targs->start_cycles;
    ps.message_size=msg_size;

    struct ibv_sge local_client_send_sge;
    set_scatter_gather_entry(local_client_send_sge, (uint64_t) mr_buffers[qp_num]->addr, (uint32_t) msg_size, mr_buffers[qp_num]->lkey);

    struct ibv_send_wr local_client_send_wr_batch[BATCH_SIZE];
    uint32_t remote_key = cm->server_qp_metadata_attr[0].stag.remote_stag;
    uint64_t local_server_address = cm->server_qp_metadata_attr[0].address;

    for (int i=0;i<BATCH_SIZE;i++) {
        set_send_work_request(local_client_send_wr_batch[i], local_client_send_sge, rdma_op, remote_key, local_server_address);
    }

    gettimeofday (&ps.start, NULL);
    rdtsc();
    ps.start_cycles = ( ((int64_t)cycles_high << 32) | cycles_low );


    struct ibv_wc* wc;
    int n, i;
    wc = (struct ibv_wc *) calloc (num_concur, sizeof(struct ibv_wc));
    do {
        /* Poll the completion queue for the completion event for the earlier write */
        n = bulk_poll(cq_ptr, num_concur, wc, &ps);

        for (i = 0; i < n; i++) {
            /* Check that it succeeded */
            check_work_completion_success(&(wc[i]));

            if (!finished_running_xput) {
                /* calculate the remote memory location of the request and then set each of the requests */

                uint64_t remote_memory_address = get_xput_thread_remote_address(local_server_address, &(wc[i]), msg_size);

                local_client_send_wr_batch[i].wr_id = wc[i].wr_id;              /* User-assigned id to recognize this WR on completion */
                local_client_send_wr_batch[i].wr.rdma.remote_addr = remote_memory_address;

                if (rdma_op == RDMA_CAS_OP) {
                    set_compare_and_swap_work_request(local_client_send_wr_batch[i], remote_memory_address, remote_key, 0, 0);
                }
                if (rdma_op == RDMA_FAA_OP) {
                    set_fetch_and_add_work_request(local_client_send_wr_batch[i], remote_memory_address, remote_key, 0);
                }

            }
        }

        //send all of the messages
        if (!finished_running_xput) {
            send_bulk(n, qp_num, cm, local_client_send_wr_batch);
            ps.wr_posted+=n;
        }
        ps.wr_acked += n;

    } while(!finished_running_xput);
    
    rdtsc1();
    ps.end_cycles = ( ((int64_t)cycles_high1 << 32) | cycles_low1 );
    gettimeofday (&ps.end, NULL);

    cm->thread_results[targs->thread_id]=get_thread_performance_results(&ps);
    return NULL;
}

void drain_all_concurrent_requests(int num_concur, struct ibv_cq *cq_ptr, struct ibv_wc* wc) {
    performance_statistics ps;
    int n = 0;
    while (n < num_concur) {
        int single_n = bulk_poll(cq_ptr, num_concur, wc, &ps);
        n += single_n;
        printf("QP %d: Draining concurrent requests %d/%d\n", wc->qp_num, n , num_concur);
    }
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
    // Debug::notifyError("Send with ATOMIC_CMP_AND_SWP failed.");
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


void * read_write_cas_test(void * args) {
    struct xput_thread_args * targs = (struct xput_thread_args *)args;
    stick_this_thread_to_core(targs->core);

    //Unpack thread arguments
    int num_concur = targs->num_concur;
    struct ibv_cq *cq_ptr = targs->cq_ptr;
    int msg_size = targs->msg_size;
    enum rdma_measured_op rdma_op = targs->rdma_op;
    struct ibv_mr **mr_buffers=targs->mr_buffers;           /* Make sure to deregister these local MRs before exiting */
    RDMAConnectionManager * cm = targs->cm;
    int qp_num = targs->thread_id;

    // //Initialize performance statistics
    performance_statistics ps;
    struct ibv_wc *wc = (struct ibv_wc *) calloc (num_concur, sizeof(struct ibv_wc));

    struct ibv_sge local_client_send_sge;
    set_scatter_gather_entry(local_client_send_sge, (uint64_t) mr_buffers[qp_num]->addr, (uint32_t) msg_size, mr_buffers[qp_num]->lkey);

    struct ibv_send_wr local_client_send_wr_batch[BATCH_SIZE];
    uint32_t remote_key = cm->server_qp_metadata_attr[0].stag.remote_stag;
    uint64_t local_server_address = cm->server_qp_metadata_attr[0].address;



    char local_write_value[8] = {'x','x','x','x','x','x','x','x'};
    // bzero(local_write_value, 8);
    memcpy(mr_buffers[qp_num]->addr, local_write_value, 8);

    rdma_measured_op first_write = RDMA_WRITE_OP;

    //Empty the buffer from the writes that occured earlier
    int n =0;
    drain_all_concurrent_requests(num_concur, cq_ptr, wc);

    //write
    sleep(1);
    local_client_send_wr_batch[0].wr_id = 1337;              /* User-assigned id to recognize this WR on completion */
    local_client_send_wr_batch[0].wr.rdma.remote_addr = local_server_address;
    set_send_work_request(local_client_send_wr_batch[0], local_client_send_sge, first_write, remote_key, local_server_address);
    printf("sending write request [%s]\n", local_write_value);
    send_bulk(1, qp_num, cm, local_client_send_wr_batch);
    printf("polling for write completion\n");
    bulk_poll(cq_ptr, 1, wc, &ps);
    printf("write work completion recieved qp %d wr_id %d\n", wc[0].qp_num, wc[0].wr_id);


    //read
    sleep(1);
    char local_overwrite_write_value[8] = {'X','b','c','d','e','f','g','\0'};
    memcpy(mr_buffers[qp_num]->addr, local_overwrite_write_value, 8);

    printf("We have sent the value and recived it, time to read that value back\n");
    char local_read[8];
    //Now read the value back
    rdma_measured_op read_op = RDMA_READ_OP;
    local_client_send_wr_batch[0].wr_id = 1337;              /* User-assigned id to recognize this WR on completion */
    local_client_send_wr_batch[0].wr.rdma.remote_addr = local_server_address;
    set_send_work_request(local_client_send_wr_batch[0], local_client_send_sge, read_op, remote_key, local_server_address);
    send_bulk(1, qp_num, cm, local_client_send_wr_batch);
    //Value sent
    bulk_poll(cq_ptr, 1, wc, &ps);
    memcpy(local_read, mr_buffers[qp_num]->addr, 8);
    printf("The value read back is %s\n", local_read);
    if (memcmp(local_read, local_write_value, 8) == 0) {
        printf("write then read was a success\n");
    } else {
        printf("write then read was a failure\n");
    }

    //Now CAS the value
    sleep(1);
    char local_cas_value[8];
    uint64_t compare_value = *(uint64_t *) local_write_value;
    char local_cas_write_value_new[8] = {'a','a','z','d','e','f','g','f'};
    bzero(local_cas_write_value_new, 8);
    uint64_t swap_value = *(uint64_t *) local_cas_write_value_new;

    rdma_measured_op cas_op = RDMA_CAS_OP;
    local_client_send_wr_batch[0].wr_id = 0;              /* User-assigned id to recognize this WR on completion */
    // local_client_send_wr_batch[0].wr.rdma.remote_addr = local_server_address;
    set_send_work_request(local_client_send_wr_batch[0], local_client_send_sge, cas_op, remote_key, local_server_address);
    set_compare_and_swap_work_request(local_client_send_wr_batch[0], local_server_address, remote_key, compare_value, swap_value);

    send_bulk(1, qp_num, cm, local_client_send_wr_batch);
    //Value sent
    bulk_poll(cq_ptr, 1, wc, &ps);
    printf("CAS operation completed\n");

    //Doing the masked CAS now
    printf("preparing masked cas\n");
    sleep(1);

    compare_value = 0x000000000000FFFF;
    char masked_cas_value[8] = {'m','a','s','k','c','a','s','s'};
    uint64_t mask = 0xFFFFFFFFFFFF0000;


    swap_value = *(uint64_t*) masked_cas_value;
    // swap_value = bswap_64(swap_value);
    printf("sending masked CAS\n");
    rdmaCompareAndSwapMask(
        cm->client_qp[qp_num], 
        (uint64_t)mr_buffers[qp_num]->addr, 
        local_server_address, 
        compare_value, 
        swap_value, 
        mr_buffers[qp_num]->lkey, 
        remote_key, 
        mask, 
        true);
    printf("polling for masked cas response\n") ;
    bulk_poll(cq_ptr, 1, wc, &ps);

    printf("Second Masked Cas");
    sleep(1);

    char compare_value_2[8] = {'D','D','s','k','c','a','s','s'};
    char masked_cas_value_2[8] = {'N','O','M','A','S','K','s','s'};
    swap_value=(*(uint64_t*)masked_cas_value_2);
    mask = 0xFFFFFFFFFFFF0000;

    rdmaCompareAndSwapMask(
        cm->client_qp[qp_num], 
        (uint64_t)mr_buffers[qp_num]->addr, 
        local_server_address, 
        *(uint64_t*)compare_value_2, 
        swap_value, 
        mr_buffers[qp_num]->lkey, 
        remote_key, 
        mask, 
        true);
    printf("polling for masked cas response\n") ;
    bulk_poll(cq_ptr, 1, wc, &ps);



    printf("maksed cas response recived\n");
    printf("wc status is %d\n", wc[0].status);
    printf("wc opcode is %d\n", wc[0].opcode);
    printf("wc wr_id is %d\n", wc[0].wr_id);
    printf("wc vendor_err is %d\n", wc[0].vendor_err);
    printf("maksed cas response recived\n");


    printf("The value read back from the cas is %s\n", mr_buffers[qp_num]->addr);
    return NULL;

}



/* Measures throughput for RDMA READ/WRITE ops for a specified message size and number of concurrent messages (i.e., requests in flight) */
/* Returns xput in ops/sec */
static result_t measure_xput(
    uint32_t msg_size,                  // payload size
    int num_concur,                     // number of requests in flight; use 1 for RTT measurements
    enum rdma_measured_op rdma_op,      // rdma op to use i.e., read or write
    enum mem_reg_mode mr_mode,          // mem reg mode; see comments for each "enum mem_reg_mode" 
    int num_lbuffers,                   // number of pre-registed buffers to rotate between; only valid for MR_MODE_PRE_REGISTER_WITH_ROTATE
    int num_qps,                         // num of QPs to use
    RDMAConnectionManager &cm          // connection manager
    ) { 
    int ret = -1, n, i;
    struct ibv_wc* wc;
    uint64_t start_cycles, end_cycles;
    struct timeval      start; //, end;
    struct ibv_mr **mr_buffers = NULL;           /* Make sure to deregister these local MRs before exiting */
    result_t result;
    union work_req_id wr_id;
    int qp_num = 0;


    struct ibv_sge client_send_sge;
    struct ibv_sge server_recv_sge;
    struct ibv_send_wr client_send_wr, *bad_client_send_wr;
    struct ibv_recv_wr server_recv_wr, *bad_server_recv_wr;

    
    /* Learned that the limit for number of outstanding requests for READ and ATOMIC ops 
     * is very less; while there is no such limit for WRITES. The connection parameters initiator_depth
     * and responder_resources determine such limit, which are again limited by hardware device 
     * attributes max_qp_rd_atom and max_qp_init_rd_atom. Why is there such a limit?  */
    /* In any case, for now, disallow request for concurrency more than the limit */
    if ((rdma_op == RDMA_READ_OP || rdma_op == RDMA_CAS_OP || rdma_op == RDMA_FAA_OP) && num_concur > (MAX_RD_AT_IN_FLIGHT * num_qps)) {
        rdma_error("Device cannot support more than %d outstnading READs (num_concur=%d, qps %d)\n", 
            MAX_RD_AT_IN_FLIGHT, num_concur,num_qps);
        result.err_code = -EOVERFLOW;
        return result;
    }

    mr_mode=mr_mode; //to avoid warning

    /* Allocate client buffers to read/write from (use the same piece of underlying memory) */
    mr_buffers = (struct ibv_mr**) malloc(num_lbuffers * sizeof(struct ibv_mr*));
    size_t buf_size = get_buffer_size(num_concur,msg_size);
    mr_buffers[0] = rdma_buffer_alloc(cm.pd,
        buf_size, MEMORY_PERMISSION
            
            );
    if (!mr_buffers[0]) {
        rdma_error("We failed to create the destination buffer, -ENOMEM\n");
        result.err_code = -ENOMEM;
        return result;
    }
    debug("Buffer registered (%3d): addr = %p, length = %ld, handle = %d, lkey = %d, rkey = %d\n", 0,
        mr_buffers[0]->addr, mr_buffers[0]->length, mr_buffers[0]->handle, mr_buffers[0]->lkey, mr_buffers[0]->rkey);

    if (num_lbuffers > 1) {
        /* Register rest of the buffers using same piece of memory */
        for (i = 1; i < num_lbuffers; i++) {
            mr_buffers[i] = rdma_buffer_register(cm.pd,
                mr_buffers[0]->addr,
                buf_size, MEMORY_PERMISSION
                    );

            if (!mr_buffers[i]) {
                rdma_error("Registering buffer %d failed\n", i);
                result.err_code = -ENOMEM;
                return result;
            }
            
            debug("Buffer registered (%3d): addr = %p, length = %ld, handle = %d, lkey = %d, rkey = %d\n", i,
                mr_buffers[i]->addr, mr_buffers[i]->length, mr_buffers[i]->handle, mr_buffers[i]->lkey, mr_buffers[i]->rkey);
        }
    }

    gettimeofday (&start, NULL);
    rdtsc();
    start_cycles = ( ((int64_t)cycles_high << 32) | cycles_low );

    // //Sanity starts here
    for (int i = 0; i < num_concur; i++)
        memset((uint8_t*)mr_buffers[0]->addr + (i*msg_size), 'a' + i%26, msg_size);


    /* Post until number of max requests in flight is hit */
    uint32_t remote_key = cm.server_qp_metadata_attr[0].stag.remote_stag;
    char *buf_ptr = (char * )mr_buffers[0]->addr;
    int	buf_offset = 0, slot_num = 0;
    int buf_num = 0;
    uint64_t wr_posted = 0;


    for (int i = 0; i < num_concur; i++) {
        /* it is safe to reuse client_send_wr object after post_() returns */
        uint64_t remote_memory_address= cm.server_qp_metadata_attr[0].address + buf_offset;
        set_scatter_gather_entry(client_send_sge, (uint64_t) buf_ptr, msg_size, mr_buffers[buf_num]->lkey);
        set_send_work_request(client_send_wr, client_send_sge, rdma_op, remote_key, remote_memory_address);
        wr_id.s.qp_num=qp_num;
        wr_id.s.window_slot=slot_num;
        client_send_wr.wr_id = wr_id.val;                  /* User-assigned id to recognize this WR on completion */

        if (rdma_op == RDMA_CAS_OP) {
            set_compare_and_swap_work_request(client_send_wr, remote_memory_address, remote_key, 0, 0);
        }

        if (rdma_op == RDMA_FAA_OP) {
            set_fetch_and_add_work_request(client_send_wr, remote_memory_address, remote_key, 0);
        }

        int ret = ibv_post_send(cm.client_qp[qp_num], 
                &client_send_wr,
                &bad_client_send_wr);
        if (ret) {
            rdma_error("Failed to write client src buffer, errno: %d \n", -errno);
            result.err_code = -errno;
            return result;
        }
        	
        wr_posted++;
        slot_num    = (slot_num + 1) % GLOBAL_KEYS;
        buf_offset = get_buf_offset(slot_num,msg_size);
        buf_num     = (buf_num + 1) % num_lbuffers;
	    buf_ptr     = (char *)((char *)(mr_buffers[buf_num]->addr) + buf_offset);     /* We can always use mr_buffers[0] as all buffers point to same memory */
        qp_num      = (qp_num + 1) % num_qps;

    }

    void * context = NULL;
    struct ibv_cq *local_client_cq_threads[MAX_THREADS];
    for (int i=0;i<num_qps;i++){
        int ret = ibv_get_cq_event(cm.io_completion_channel_threads[i], &local_client_cq_threads[i], &context);
        //printf("Local CQ %p\n",local_client_cq_threads[i]);
        if (ret) {
            rdma_error("Failed to get next CQ event due to %d \n", -errno);
            result.err_code = -errno;
            return result;
        }
        ret = ibv_req_notify_cq(local_client_cq_threads[i], 0);
        if (ret) {
            rdma_error("Failed to request further notifications %d \n", -errno);
            result.err_code = -errno;
            return result;
        }
    }

    int32_t total_threads = num_qps;
    pthread_t threadId[MAX_THREADS];
    struct xput_thread_args targs[MAX_THREADS];
    for (int i=0;i<total_threads;i++){
        targs[i].thread_id=i;
        //targs[i].core=4+(2*i);
        targs[i].core=(2*i);
        targs[i].num_concur=num_concur;
        targs[i].cq_ptr=cm.client_cq_threads[i];
        //targs[i].cq_ptr=cq_ptr;
        targs[i].msg_size = msg_size;
        targs[i].rdma_op = rdma_op;
        targs[i].num_lbuffers = num_lbuffers;
        targs[i].start_cycles = start_cycles;
        targs[i].mr_buffers = mr_buffers;
        targs[i].cm = &cm;
        finished_running_xput=0;
        // pthread_create(&threadId[i], NULL, &xput_thread, (void*)&targs[i]);
        pthread_create(&threadId[i], NULL, &read_write_cas_test, (void*)&targs[i]);
        //Setupt next itt
    }

    const uint64_t minimum_duration_secs = 10;  /* number of seconds to run the experiment for at least */
    do {
        rdtsc1();
        end_cycles = ( ((int64_t)cycles_high1 << 32) | cycles_low1 );
        if ((end_cycles - start_cycles) >= minimum_duration_secs * CPU_FREQ) {
            //printf("Time is up, sending kill signal to threads");
            finished_running_xput = 1;
        }
        sleep(1);

    } while(!finished_running_xput);
    
    for (int i=0;i<total_threads;i++){
        pthread_join(threadId[i],NULL);
    }

    sleep(1);
    wc = (struct ibv_wc *) calloc (num_concur, sizeof(struct ibv_wc));
    uint32_t unfinished_requests=0;
    for (int i=0;i<total_threads;i++){
        do {
            //printf("Thread %d, CQ %p\n",targs->thread_id,cq_ptr);
            n = ibv_poll_cq(cm.client_cq_threads[i], num_concur, wc);       // get upto num_concur entries
            //printf("exit poll\n");
            if (n < 0) {
                printf("Failed to poll cq for wc due to %d\n", ret);
                rdma_error("Failed to poll cq for wc due to %d \n", ret);
                exit(1);
            }
            unfinished_requests+=n;
            if (n == 0) {
                break;
            }     
        } while (n < 1);
    }



    /* Similar to connection management events, we need to acknowledge CQ events */
    //ibv_ack_cq_events(local_client_cq_threads[0], 1 /* we received one event notification. This is not number of WC elements */);
    for (int i=0;i<total_threads;i++){
        ibv_ack_cq_events(local_client_cq_threads[i], 64 /* we received one event notification. This is not number of WC elements */);
    }


    /* Deregister local MRs */
    for (i = 0; i < num_lbuffers; i++)
        rdma_buffer_deregister(mr_buffers[i]);

    result_t meta_result;
    meta_result.cq_empty_count=0;
    meta_result.cq_poll_count=0;
    meta_result.xput_bps=0;
    meta_result.xput_ops=0;
    for (i=0;i<total_threads;i++){
        meta_result.cq_empty_count+=cm.thread_results[i].cq_empty_count;
        meta_result.cq_poll_count+=cm.thread_results[i].cq_poll_count;
        meta_result.xput_bps+=cm.thread_results[i].xput_bps;
        meta_result.xput_ops+=cm.thread_results[i].xput_ops;
    }

    return meta_result;
}


int main(int argc, char **argv) {
    struct sockaddr_in server_sockaddr;
    int ret, option;
    int base_port = DEFAULT_RDMA_PORT;
    bzero(&server_sockaddr, sizeof server_sockaddr);
    server_sockaddr.sin_family = AF_INET;
    server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    /* buffers are NULL */
    const char* dummy_text = "HELLO";
    char * dst = NULL;
    char * src = NULL; 
    int simple = 0, i;
    int rtt_flag = 0, xput_flag = 0;
    int num_concur = 1;
    int num_mbuf = 1;
    int msg_size = 0;
    int write_to_file = 0;
    char outfile[200] ;
    int num_qps = 1;

    /* Parse Command Line Arguments */
    static const struct option options[] = {
        {.name = "simple", .has_arg = no_argument, .flag=NULL, .val = 's'},
        {.name = "rtt", .has_arg = no_argument, .flag=NULL, .val = 'r'},
        {.name = "xput", .has_arg = no_argument, .flag=NULL, .val = 'x'},
        {.name = "concur", .has_arg = required_argument, .flag=NULL, .val = 'c'},
        {.name = "buffers", .has_arg = required_argument, .flag=NULL, .val = 'b'},
        {.name = "msgsize", .has_arg = required_argument, .flag=NULL, .val = 'm'},
        {.name = "out", .has_arg = required_argument, .flag=NULL, .val = 'o'},
        {.name = "qps", .has_arg = required_argument, .flag=NULL, .val = 'q'},
        {}
    };
    while ((option = getopt_long(argc, argv, "sa:p:rxc:b:m:o:q:", options, NULL)) != -1) {
        switch (option) {
            case 's':
                /* run the basic example to test connection */
                simple = 1;
                src = (char *) calloc(strlen(dummy_text) , 1);
                if (!src) {
                    rdma_error("Failed to allocate memory : -ENOMEM\n");
                    return -ENOMEM;
                }
                /* Copy the passes arguments */
                memcpy(src, dummy_text, strlen(dummy_text));
                dst = (char * )calloc(strlen(dummy_text), 1);
                if (!dst) {
                    rdma_error("Failed to allocate destination memory, -ENOMEM\n");
                    free(src);
                    return -ENOMEM;
                }
                break;
            case 'a':
                /* remember, this overwrites the port info */
                ret = get_addr(optarg, (struct sockaddr*) &server_sockaddr);
                if (ret) {
                    rdma_error("Invalid IP \n");
                    return ret;
                }
                break;
            case 'p':
                /* passed port to listen on */
                base_port = strtol(optarg, NULL, 0); 
                break;
            case 'r':
                /* Run RTT measurement (for all msg sizes) */
                rtt_flag = 1;     
                break;
            case 'x':
                /* Run xput measurement */
                xput_flag = 1;
                break;
            case 'c':
                /* concurrency i.e., number of requests in flight */
                /* num_concur < 0: invalid; = 0: vary from 1 to 128; > 0: use the number. Default is 1 */
                num_concur = strtol(optarg, NULL, 0); 
                if (num_concur < 0 || num_concur > MAX_WR) {
                    rdma_error("Invalid num_concur. Should be between 0 and %d\n", MAX_WR);
                    return -1;
                }
                break;  
            case 'b':
                /* number of MR buffers to use */
                /* num_mbuf < 0: invalid; = 0: vary from 1 to 256; > 0: use the number. Default is 1 */
                num_mbuf = strtol(optarg, NULL, 0); 
                if (num_mbuf < 0 || num_mbuf > MAX_MR) {
                    rdma_error("Invalid num_mbuf. Should be between 1 and %d\n", MAX_MR);
                    return -1;
                }
                break; 
            case 'm':
                /* input payload size to use */
                /* msg_size < 0: invalid; = 0: vary from 64 to 4096; > 0: use the number. Default is 0 (vary) */
                msg_size = strtol(optarg, NULL, 0); 
                if (msg_size < 0 || msg_size > 4096) {
                    rdma_error("Invalid msg_size. Should be between 1 and %d\n", 4096);
                    return -1;
                }
                break;
            case 'q':
                /* input number of queue pairs */
                /* qp_num < 0: invalid; = 0: vary from 1 to MAX_QPS; > 0: use the number. Default is 1 qp */
                num_qps = strtol(optarg, NULL, 0); 
                if (num_qps < 0 || num_qps > MAX_QPS) {
                    rdma_error("Invalid qps. Should be between 1 and %d\n", MAX_QPS);
                    return -1;
                }
                break;  
            case 'o':
                /* output file to write to */
                write_to_file = 1;
                strcpy(outfile, optarg);
                break;
            default:
                usage();
                break;
            }
        }

    RDMAConnectionManagerArguments args;
    args.num_qps = num_qps;
    args.server_sockaddr = &server_sockaddr;
    args.base_port = base_port;

    RDMAConnectionManager cm(args);
    printf("RDMAConnectionManager created\n");


    /* Connection now set up and ready to play */
    if (simple) {
        /* If asked for a simple program, run the original example for all qps */
        for (i = 0; i < num_qps; i++) {
            ret = cm.client_xchange_metadata_with_server(i, src, strlen(src));
            if (ret) {
                rdma_error("Failed to setup client connection , ret = %d \n", ret);
                return ret;
            }

            ret = client_remote_memory_ops(i, cm);
            if (ret) {
                rdma_error("Failed to finish remote memory ops, ret = %d \n", ret);
                return ret;
            }
                
            if (check_src_dst(src, dst)) {
                rdma_error("src and dst buffers do not match \n");
            } else {
                printf("...\nSUCCESS, source and destination buffers match \n");
            }
        }
    }
    else {   
        /* Set up a buffer with enough size on server, we don't need to do this for every qp since we only need 
         * one server-side buffer that we can reuse for all QPs but we do it anyway since server expects it */
        /* Once metadata is exchanged, server-side buffer metadata would be saved in server_qp_metadata_attr[*] */
            printf("excanging metadata with server\n");
        for (i = 0; i < num_qps; i++) {
            #ifdef USE_DEVICE_MEMORY
            ret = client_xchange_metadata_with_server(i, NULL, (DEVICE_MEMORY_KB / (num_qps*2)));      // Relies on the server running CX5 TODO query remote device for mapped memory
            #else
            ret = cm.client_xchange_metadata_with_server(i, NULL, 1024*1024*64);      // 1 MB
            #endif
            if (ret) {
                rdma_error("Failed to setup client connection , ret = %d \n", ret);
                return ret;
            }
        }
            printf("done excanging metadata with server");
        
        // int min_num_concur = num_concur == 0 ? 1 : num_concur;
        // int max_num_concur = num_concur == 0 ? 256 : num_concur;        /* Empirically found that anything above this number does not matter for single core */

        int min_msg_size = msg_size == 0 ? 64 : msg_size;
        int max_msg_size = msg_size == 0 ? 4096 : msg_size;
        int msg_size_incr = 64;

        // int min_num_mbuf = num_mbuf == 0 ? 1 : num_mbuf;
        // int max_num_mbuf = num_mbuf == 0 ? MAX_MR : num_mbuf;
        // int max_num_mbuf = num_mbuf == 0 ? 1e6 : num_mbuf;

        /* Get roundtrip latencies for ops */
        if (rtt_flag) {
            // printf("RTT for %d B WRITES: %0.2lf mu-secs\n", 64, measure_rtt(64, RDMA_WRITE_OP));        // WRITE RTT
            // printf("RTT for %d B READS : %0.2lf mu-secs\n", 64, measure_rtt(64, RDMA_READ_OP));         // READ RTT

            /* We can also get them using the xput method with 1 request in flight; results agreed with above method. */
            // printf("RTT for %d B WRITES: %0.2lf mu-secs\n", 64, 1e6 / measure_xput(64, 1, RDMA_WRITE_OP) );     // WRITE RTT
            // printf("RTT for %d B READS : %0.2lf mu-secs\n", 64, 1e6 / measure_xput(64, 1, RDMA_READ_OP));       // READ RTT


            /* Generate RTT numbers for different msg sizes */
            FILE *fptr;
            if (write_to_file) {
                printf("Writing output to: %s\n", outfile);
                fptr = fopen(outfile, "w");
                fprintf(fptr, "msg size,write,read\n");
            }
            printf("=========== RTTs =============\n");
            printf("msg size,write,read\n");
            for (msg_size = min_msg_size; msg_size <= max_msg_size; msg_size += msg_size_incr) {
                double wrtt = 1e6 / measure_xput(msg_size, 1, RDMA_WRITE_OP, MR_MODE_PRE_REGISTER_WITH_ROTATE, num_mbuf, num_qps, cm).xput_ops;
                double rrtt = 1e6 / measure_xput(msg_size, 1, RDMA_READ_OP, MR_MODE_PRE_REGISTER_WITH_ROTATE, num_mbuf, num_qps, cm).xput_ops;
                printf("%d,%.2lf,%.2lf\n", msg_size, wrtt, rrtt);
                if (write_to_file) {
                    fprintf(fptr, "%d,%.2lf,%.2lf\n", msg_size, wrtt, rrtt);
                    fflush(fptr);
                }
            }
            if (write_to_file)  fclose(fptr);
        }

        /* Get xputs for ops */
        if (xput_flag) {
            uint32_t msg_size = 8;
            uint32_t num_concur = 1;
            // max_reqs_in_flight = 20;
            // printf("Goodput for %3d B READS (%3d QPE) : %0.2lf gbps\n", msg_size, max_reqs_in_flight, 
            //     measure_xput(msg_size, max_reqs_in_flight, RDMA_READ_OP) * msg_size * 8 / 1e9);       // READ Xput
            // printf("Goodput for %3d B WRITES (%3d QPE) : %0.2lf gbps\n", msg_size, max_reqs_in_flight, 
            //     measure_xput(msg_size, max_reqs_in_flight, RDMA_WRITE_OP) );       // WRITE Xput

            FILE *fptr;
            if (write_to_file) {
                printf("Writing output to: %s\n", outfile);
                fptr = fopen(outfile, "w");
                fprintf(fptr,"msg size,concur,write_ops,write,read_ops,read,cas_ops,cas,faa_ops,faa\n");
            }
            printf("=========== Xput =============\n");
            printf("msg size,concur,write_ops,write,read_ops,read,cas_ops,cas,faa_ops,faa,keys\n");
            num_mbuf=num_qps;
            //for (num_concur =128; num_concur <=1024; num_concur *= 2) {
            //for (num_concur = num_qps; num_concur <=num_qps*(MAX_RD_AT_IN_FLIGHT); num_concur*=2) {
            /*
            num_concur=num_qps*(MAX_RD_AT_IN_FLIGHT);
            double casput_ops = measure_xput(msg_size, num_concur, RDMA_CAS_OP, MR_MODE_PRE_REGISTER, num_mbuf, num_qps).xput_ops;
            double casput_gbps = casput_ops * msg_size * 8 / 1e9;
            printf("%d,%d,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%d\n", msg_size, num_concur, 0.0,0.0,0.0,0.0,casput_ops,casput_gbps,GLOBAL_KEYS);
            */



            num_concur=92;
            //Set this value if we only want one key, it controls what happens in measure_xput.
            //GLOBAL_KEYS=1024;
            //for (GLOBAL_GAP_INTEGER=0;GLOBAL_GAP_INTEGER<1024;GLOBAL_GAP_INTEGER+=8) {
            //for (GLOBAL_KEYS=1;GLOBAL_KEYS<num_concur;GLOBAL_KEYS++) {
            //for (GLOBAL_KEYS=1;GLOBAL_KEYS<16;GLOBAL_KEYS++) {
                //double rput_ops =  measure_xput(msg_size, num_concur, RDMA_READ_OP, MR_MODE_PRE_REGISTER, num_mbuf, num_qps).xput_ops;

            //This is the primer to set up the middle box
            //num_qps=2;
            for (num_qps=1;num_qps<24;num_qps++){
            //for (num_concur=1;num_concur<=16;num_concur*=2){
            //for (num_concur = num_qps; num_concur <=num_qps*(MAX_RD_AT_IN_FLIGHT); num_concur+=num_qps) {
                num_concur=372*num_qps;
                //double rput_ops =  measure_xput(msg_size, num_concur, RDMA_READ_OP, MR_MODE_PRE_REGISTER, num_mbuf, num_qps).xput_ops;
                double wput_ops = measure_xput(msg_size, num_concur, RDMA_WRITE_OP, MR_MODE_PRE_REGISTER, num_mbuf, num_qps, cm).xput_ops; 
                //double casput_ops = measure_xput(msg_size, num_concur, RDMA_CAS_OP, MR_MODE_PRE_REGISTER, num_mbuf, num_qps).xput_ops;
                //double faa_ops = measure_xput(msg_size, num_concur, RDMA_FAA_OP, MR_MODE_PRE_REGISTER, num_mbuf, num_qps).xput_ops;
                double rput_ops =1;
                double casput_ops = 1;
                double faa_ops = 1;
                double wput_gbps = wput_ops * msg_size * 8 / 1e9;
                double rput_gbps = rput_ops * msg_size * 8 / 1e9;
                double casput_gbps = casput_ops * msg_size * 8 / 1e9;
                double faa_gbps = casput_ops * msg_size * 8 / 1e9;
                printf("%d,%d,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%d\n", msg_size, num_concur, wput_ops, wput_gbps, rput_ops, rput_gbps,casput_ops,casput_gbps,faa_ops,faa_gbps,GLOBAL_KEYS);
                if (write_to_file) {
                    fprintf(fptr,"%d,%d,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%d\n", msg_size, num_concur, wput_ops, wput_gbps, rput_ops, rput_gbps,casput_ops,casput_gbps,faa_ops,faa_gbps,GLOBAL_GAP_INTEGER);
                    fflush(fptr);
                
                }
            }
            if (write_to_file)  fclose(fptr);
        }

    }

}

void usage() {
    printf("Usage:\n");
    printf("rdma_client: [-a <server_addr>] [-p <server_port>] [-s/--simple]\n");
    printf("	--simple: runs a simple ping to test RDMA connection\n");
    printf("(default IP is 127.0.0.1 and port is %d)\n", DEFAULT_RDMA_PORT);
    exit(1);
}