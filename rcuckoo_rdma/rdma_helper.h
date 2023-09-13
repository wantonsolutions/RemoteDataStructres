#pragma once
#ifndef RDMA_HELPER_H
#define RDMA_HELPER_H

#include <infiniband/verbs.h>

namespace rdma_helper {

    int bulk_poll(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc);

    void send_bulk(int n, ibv_qp * qp, struct ibv_exp_send_wr *send_work_request_batch);

    // for RC & UC
    bool rdmaRead(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t size,
                uint32_t lkey, uint32_t remoteRKey, bool signal, uint64_t wrID);



    void setRdmaCompareAndSwap(struct ibv_sge * sg, struct ibv_send_wr * wr, ibv_qp *qp, uint64_t source, uint64_t dest,
            uint64_t compare, uint64_t swap, uint32_t lkey,
            uint32_t remoteRKey, bool signal, uint64_t wrID);

    // for RC & UC
    bool rdmaCompareAndSwap(ibv_qp *qp, uint64_t source, uint64_t dest,
                            uint64_t compare, uint64_t swap, uint32_t lkey,
                            uint32_t remoteRKey, bool signal, uint64_t wrID);

    void setRdmaCompareAndSwapExp(struct ibv_sge * sg, struct ibv_exp_send_wr * wr, ibv_qp *qp, uint64_t source, uint64_t dest,
        uint64_t compare, uint64_t swap, uint32_t lkey,
        uint32_t remoteRKey, bool signal, uint64_t wrID);

    bool rdmaCompareAndSwapExp(ibv_qp *qp, uint64_t source, uint64_t dest,
        uint64_t compare, uint64_t swap, uint32_t lkey,
        uint32_t remoteRKey, bool signal, uint64_t wrID);



    void setRdmaCompareAndSwapMask(struct ibv_sge* sg, struct ibv_exp_send_wr *wr, ibv_qp *qp, uint64_t source, uint64_t dest,
                                uint64_t compare, uint64_t swap, uint32_t lkey,
                                uint32_t remoteRKey, uint64_t mask, bool singal, uint64_t wr_ID);

    bool rdmaCompareAndSwapMask(ibv_qp *qp, uint64_t source, uint64_t dest,
                                uint64_t compare, uint64_t swap, uint32_t lkey,
                                uint32_t remoteRKey, uint64_t mask, bool singal, uint64_t wr_ID);


    void setRdmaReadExp(struct ibv_sge * sg, struct ibv_exp_send_wr * wr, uint64_t source, uint64_t dest, uint64_t size,
        uint32_t lkey, uint32_t remoteRKey, bool signal, uint64_t wrID);


    bool rdmaReadExp(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t size,
        uint32_t lkey, uint32_t remoteRKey, bool signal, uint64_t wrID);


    void setRdmaWriteExp(struct ibv_sge * sg, struct ibv_exp_send_wr * wr, uint64_t source, uint64_t dest, uint64_t size,
        uint32_t lkey, uint32_t remoteRKey, int32_t imm, bool signal, uint64_t wrID);
}

#endif