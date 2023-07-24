#pragma once
#ifndef RDMA_HELPER_H
#define RDMA_HELPER_H

#include <infiniband/verbs.h>

namespace rdma_helper {

    int bulk_poll(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc);

    // for RC & UC
    bool rdmaRead(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t size,
                uint32_t lkey, uint32_t remoteRKey, bool signal, uint64_t wrID);

    // for RC & UC
    bool rdmaCompareAndSwap(ibv_qp *qp, uint64_t source, uint64_t dest,
                            uint64_t compare, uint64_t swap, uint32_t lkey,
                            uint32_t remoteRKey, bool signal, uint64_t wrID);

    bool rdmaCompareAndSwapMask(ibv_qp *qp, uint64_t source, uint64_t dest,
                                uint64_t compare, uint64_t swap, uint32_t lkey,
                                uint32_t remoteRKey, uint64_t mask, bool singal, uint64_t wr_ID);


    bool rdmaReadExp(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t size,
        uint32_t lkey, uint32_t remoteRKey, bool signal, uint64_t wrID);
}

#endif