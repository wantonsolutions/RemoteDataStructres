#include "rdma_helper.h"
#include "rdma_common.h"

namespace rdma_helper {
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
                                uint32_t remoteRKey, uint64_t mask, bool singal, uint64_t wr_ID) {
        struct ibv_sge sg;
        struct ibv_exp_send_wr wr;
        struct ibv_exp_send_wr *wrBad;
        fillSgeWr(sg, wr, source, 8, lkey);

        wr.next = NULL;
        wr.exp_opcode = IBV_EXP_WR_EXT_MASKED_ATOMIC_CMP_AND_SWP;
        wr.exp_send_flags = IBV_EXP_SEND_EXT_ATOMIC_INLINE;
        wr.wr_id = wr_ID;

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
}