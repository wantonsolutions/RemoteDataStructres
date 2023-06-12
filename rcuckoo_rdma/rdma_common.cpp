/*
 * Implementation of the common RDMA functions. 
 *
 * Authors: Animesh Trivedi
 *          atrivedi@apache.org 
 */

#include "rdma_common.h"
#define __STDC_FORMAT_MACROS 1
#include <inttypes.h>


void show_rdma_cmid(struct rdma_cm_id *id)
{
	if(!id){
		rdma_error("Passed ptr is NULL\n");
		return;
	}
	printf("RDMA cm id at %p \n", (void * )id);
	if(id->verbs && id->verbs->device)
		printf("dev_ctx: %p (device name: %s) \n", (void *)id->verbs, 
			id->verbs->device->name);
	if(id->channel)
		printf("cm event channel %p\n", (void *)id->channel);
	printf("QP: %p, port_space %x, port_num %u \n", (void *)id->qp, 
			id->ps,
			id->port_num);
}

void show_rdma_buffer_attr(struct rdma_buffer_attr *attr){
	if(!attr){
		rdma_error("Passed attr is NULL\n");
		return;
	}
	printf("---------------------------------------------------------\n");
	printf("buffer attr, addr: %p , len: %u , stag : 0x%x \n", 
			(void*) attr->address, 
			(unsigned int) attr->length,
			attr->stag.local_stag);
	printf("---------------------------------------------------------\n");
}

struct ibv_mr* rdma_buffer_alloc(struct ibv_pd *pd, uint32_t size,
    enum ibv_access_flags permission) 
{
	struct ibv_mr *mr = NULL;
	if (!pd) {
		rdma_error("Protection domain is NULL \n");
		return NULL;
	}
	void *buf = calloc(1, size);
	if (!buf) {
		rdma_error("failed to allocate buffer, -ENOMEM\n");
		return NULL;
	}
	debug("Buffer allocated: %p , len: %u \n", buf, size);
	mr = rdma_buffer_register(pd, buf, size, permission);
	if(!mr){
		free(buf);
	}
	return mr;
}


struct ibv_mr *rdma_buffer_register(struct ibv_pd *pd, 
		void *addr, uint32_t length, 
		enum ibv_access_flags permission)
{
	struct ibv_mr *mr = NULL;
	if (!pd) {
		rdma_error("Protection domain is NULL, ignoring \n");
		return NULL;
	}
	mr = ibv_reg_mr(pd, addr, length, permission);
	if (!mr) {
		rdma_error("Failed to create mr on buffer, errno: %d \n", -errno);
		return NULL;
	}
	debug("Registered: %p , len: %u , stag: 0x%x \n", 
			mr->addr, 
			(unsigned int) mr->length, 
			mr->lkey);
	return mr;
}

void print_dev_attributes(struct ibv_device_attr_ex * attr) {
	printf("Device attributes\n");
	printf("todo after switching to V4.9 this seems to not work\n");
	printf("print attributes better");
	// printf("Fetch ADD %X\n",attr->pci_atomic_caps.fetch_add);
	// printf("SWAP %X\n",attr->pci_atomic_caps.swap);
	// printf("CAS %X\n",attr->pci_atomic_caps.compare_swap);
}

struct ibv_mr* rdma_buffer_alloc_dm(struct ibv_pd *pd, uint32_t size, enum ibv_access_flags permission) {
	
	// struct ibv_exp_alloc_dm_attr dm_attr = {0};
	struct ibv_dm             *dm = {0};
	struct ibv_mr			  *mr ={0};

	struct ibv_device_attr_ex attrx;

	/////////////////////////////////////////////////////  
	/////////////////////////////////////////////////////  
	/////////////////////////////////////////////////////  
	/////////////////////////////////////////////////////  
	//https://docs.nvidia.com/networking/display/MLNXOFEDv494170/Programming
// struct ibv_exp_dm             *dm;
// struct ibv_mr                 *mr;
// struct ibv_exp_alloc_dm_attr dm_attr = {0};
// struct ibv_exp_memcpy_dm_attr cpy_attr = {0};
// struct ibv_exp_reg_mr_in mr_in = { .pd = my_pd,
//                                .addr = 0,
//                                .length = packet_size,
//                                .exp_access = IBV_EXP_ACCESS_LOCAL_WRITE,
//                                .create_flags = 0};
 
//  /* Device memory allocation request */
//  dm_attr.length = packet_size;
//  dm = ibv_exp_alloc_dm(context, &dm_attr);
 
//  /* Device memory registration as memory region */
//  mr_in.dm = dm;
//  mr_in.comp_mask = IBV_EXP_REG_MR_DM;
//  mr = ibv_exp_reg_mr(&mr_in);
 
//  cpy_attr.memcpy_dir = IBV_EXP_DM_CPY_TO_DEVICE;
//  cpy_attr.host_addr = (void *)my_packet_buffer;
//  cpy_attr.length = packet_size;
//  cpy_attr.dm_offset = 0;
//  ibv_exp_memcpy_dm(dm, &cpy_attr);
 
// struct ibv_sge list = {
//          .addr      = 0,
//          .length    = packet_size,
//          .lkey      = mr->lkey memory region */
// };
// struct ibv_send_wr wr = {
//           .wr_id      = my_wrid,
//           .sg_list    = &list,
//           .num_sge    = 1,
//           .opcode     = IBV_WR_SEND,
//           .send_flags = IBV_SEND_SIGNALED,
// };
// struct ibv_send_wr *bad_wr;
 
// ibv_post_send(my_qp, &wr, &bad_wr);
	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////



//This is what used to work not using device memory for now.
	// if (ibv_query_device_ex(pd->context, NULL, &attrx)) {
	// 	printf("unable to query device for device memory alloc\n");
	// 	return NULL;
	// }

	// printf("max alloc size %ld\n",attrx.orig_attr.);
	// if (!attrx.max_dm_size) {
	// 	printf("Device doesn't support dm allocation\n");
	// 	return NULL;
	// }

	// if (attrx.max_dm_size < size) {
	// 	rdma_error("Size of %u larger than max alloc %ld",size,attrx.max_dm_size);
	// 	return NULL;
	// }

	// if (attrx.max_dm_size != DEVICE_MEMORY_KB) {
	// 	printf("Defined value for max device memory not the same real: %ld static %d, are you running on CX5?\n",attrx.max_dm_size,DEVICE_MEMORY_KB);
	// }

	// print_dev_attributes(&attrx);

	// dm_attr.length = size;
	// dm = ibv_alloc_dm(pd->context, &dm_attr);


	// if (!dm) {
	// 	printf("Unable to allocate device memory\n");
	// 	return NULL;
	// }

	// // a bit of a c++ switcheroo for bullshit reasons
	// unsigned int p_sub = (unsigned int)permission;
	// p_sub = p_sub | IBV_ACCESS_ZERO_BASED;
	// permission = (enum ibv_access_flags)p_sub;

	// mr = ibv_reg_dm_mr(pd,dm,0,size,permission);

	// if (!mr) {
	// 	//rdma_error("Unable to register device memory\n");
	// 	printf("Unable to register device memory\n");
	// 	return NULL;
	// }

	// return mr;


		/** Stolen from sherman
		 * https://github.com/thustorage/Sherman/blob/main/src/rdma/Resource.cpp
		 ibv_mr *createMemoryRegionOnChip(uint64_t mm, uint64_t mmSize,
                                 RdmaContext *ctx) {

		// struct ibv_exp_alloc_dm_attr dm_attr;
		// memset(&dm_attr, 0, sizeof(dm_attr));
		// dm_attr.length = mmSize;
		// struct ibv_exp_dm *dm = ibv_exp_alloc_dm(ctx->ctx, &dm_attr);
		// if (!dm) {
		// 	Debug::notifyError("Allocate on-chip memory failed");
		// 	return nullptr;
		// }

		// struct ibv_exp_reg_mr_in mr_in;
		// memset(&mr_in, 0, sizeof(mr_in));
		// mr_in.pd = ctx->pd, mr_in.addr = (void *)mm, mr_in.length = mmSize,
		// mr_in.exp_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
		// 					IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC,
		// mr_in.create_flags = 0;
		// mr_in.dm = dm;
		// mr_in.comp_mask = IBV_EXP_REG_MR_DM;
		// struct ibv_mr *mr = ibv_exp_reg_mr(&mr_in);
		// if (!mr) {
		// 	Debug::notifyError("Memory registration failed");
		// 	return nullptr;
		// }

		// // init zero
		// char *buffer = (char *)malloc(mmSize);
		// memset(buffer, 0, mmSize);

		// struct ibv_exp_memcpy_dm_attr cpy_attr;
		// memset(&cpy_attr, 0, sizeof(cpy_attr));
		// cpy_attr.memcpy_dir = IBV_EXP_DM_CPY_TO_DEVICE;
		// cpy_attr.host_addr = (void *)buffer;
		// cpy_attr.length = mmSize;
		// cpy_attr.dm_offset = 0;
		// ibv_exp_memcpy_dm(dm, &cpy_attr);

		// free(buffer);

		// return mr;
		// }
		**/ //End stolen from sherman


}

// struct ibv_mr *rdma_buffer_register_dm(struct ibv_pd *pd, 
// 		void *addr, uint32_t length, 
// 		enum ibv_access_flags permission)
// {

// }

void rdma_buffer_free(struct ibv_mr *mr) 
{
	if (!mr) {
		rdma_error("Passed memory region is NULL, ignoring\n");
		return ;
	}
	void *to_free = mr->addr;
	rdma_buffer_deregister(mr);
	debug("Buffer %p free'ed\n", to_free);
	free(to_free);
}

void rdma_buffer_deregister(struct ibv_mr *mr) 
{
	if (!mr) { 
		rdma_error("Passed memory region is NULL, ignoring\n");
		return;
	}
	debug("Deregistered: %p , len: %u , stag : 0x%x \n", 
			mr->addr, 
			(unsigned int) mr->length, 
			mr->lkey);
	ibv_dereg_mr(mr);
}

int process_rdma_cm_event(struct rdma_event_channel *echannel, 
		enum rdma_cm_event_type expected_event,
		struct rdma_cm_event **cm_event)
{
	int ret = 1;
	ret = rdma_get_cm_event(echannel, cm_event);
	if (ret) {
		rdma_error("Failed to retrieve a cm event, errno: %d \n",
				-errno);
		return -errno;
	}
	/* lets see, if it was a good event */
	if(0 != (*cm_event)->status){
		rdma_error("CM event has non zero status: %d\n", (*cm_event)->status);
		ret = -((*cm_event)->status);
		/* important, we acknowledge the event */
		rdma_ack_cm_event(*cm_event);
		return ret;
	}
	/* if it was a good event, was it of the expected type */
	if ((*cm_event)->event != expected_event) {
		rdma_error("Unexpected event received: %s [ expecting: %s ]", 
				rdma_event_str((*cm_event)->event),
				rdma_event_str(expected_event));
		/* important, we acknowledge the event */
		rdma_ack_cm_event(*cm_event);
		return -1; // unexpected event :(
	}
	debug("A new %s type event is received \n", rdma_event_str((*cm_event)->event));
	/* The caller must acknowledge the event */
	return ret;
}


int process_work_completion_events (struct ibv_comp_channel *comp_channel, 
		struct ibv_wc *wc, int max_wc)
{
	struct ibv_cq *cq_ptr = NULL;
	void *context = NULL;
	int ret = -1, i, total_wc = 0;
       /* We wait for the notification on the CQ channel */
	ret = ibv_get_cq_event(comp_channel, /* IO channel where we are expecting the notification */ 
		       &cq_ptr, /* which CQ has an activity. This should be the same as CQ we created before */ 
		       &context); /* Associated CQ user context, which we did set */
       if (ret) {
	       rdma_error("Failed to get next CQ event due to %d \n", -errno);
	       return -errno;
       }
       /* Request for more notifications. */
       ret = ibv_req_notify_cq(cq_ptr, 0);
       if (ret){
	       rdma_error("Failed to request further notifications %d \n", -errno);
	       return -errno;
       }
       /* We got notification. We reap the work completion (WC) element. It is 
	* unlikely but a good practice it write the CQ polling code that 
       * can handle zero WCs. ibv_poll_cq can return zero. Same logic as 
       * MUTEX conditional variables in pthread programming.
	*/
       total_wc = 0;
       do {
	       ret = ibv_poll_cq(cq_ptr /* the CQ, we got notification for */, 
		       max_wc - total_wc /* number of remaining WC elements*/,
		       wc + total_wc/* where to store */);
	       if (ret < 0) {
		       rdma_error("Failed to poll cq for wc due to %d \n", ret);
		       /* ret is errno here */
		       return ret;
	       }
	       total_wc += ret;
       } while (total_wc < max_wc); 
       debug("%d WC are completed \n", total_wc);

       /* Now we check validity and status of I/O work completions */
       for( i = 0 ; i < total_wc ; i++) {
	       if (wc[i].status != IBV_WC_SUCCESS) {
		       rdma_error("Work completion (WC) has error status: %d, %s at index %d", 
				       wc[i].status, ibv_wc_status_str(wc[i].status), i);
		       /* return negative value */
		       return -(wc[i].status);
	       }
       }
       /* Similar to connection management events, we need to acknowledge CQ events */
       ibv_ack_cq_events(cq_ptr, 
		       1 /* we received one event notification. This is not 
		       number of WC elements */);
       return total_wc; 
}


/* Code acknowledgment: rping.c from librdmacm/examples */
int get_addr(char *dst, struct sockaddr *addr)
{
	struct addrinfo *res;
	int ret = -1;
	ret = getaddrinfo(dst, NULL, NULL, &res);
	if (ret) {
		rdma_error("getaddrinfo failed - invalid hostname or IP address\n");
		return ret;
	}
	memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in));
	freeaddrinfo(res);
	return ret;
}



int stick_this_thread_to_core(int core_id) {
   int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
   if (core_id < 0 || core_id >= num_cores)
      return EINVAL;

   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   pthread_t current_thread = pthread_self();    
   return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}


