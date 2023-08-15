/*
 * Implementation of the common RDMA functions. 
 *
 * Authors: Animesh Trivedi
 *          atrivedi@apache.org 
 */

#include "rdma_common.h"
#define __STDC_FORMAT_MACROS 1
#include <inttypes.h>

#include <string>
#include "log.h"
#include <unordered_map>

using namespace std;


struct sockaddr_in server_address_to_socket_addr(string server_address) {
	struct sockaddr_in server_sockaddr;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	char * address = new char[server_address.length() + 1];
	strcpy(address, server_address.c_str());
	int ret = get_addr(address, (struct sockaddr*) &server_sockaddr);
	if (ret != 0) {
		printf("Error: get_addr failed\n");
		exit(1);
	} else {
		printf("get_addr succeeded\n");
		printf("connecting to %s\n", inet_ntoa(server_sockaddr.sin_addr));
	}
	return server_sockaddr;
}

unordered_map<string, string> gen_config() {
    unordered_map<string, string> config;
    int table_size = 1024 * 1024 * 10;
    // int table_size = 1024 * 1024;
    // int table_size = 1024 * 10;
    // int table_size = 256;
    // int table_size = 1024;
    // int table_size = 256;
    // int table_size = 1024 * 2;
    int entry_size = 8;
    int bucket_size = 8;
    int memory_size = entry_size * table_size;
    int buckets_per_lock = 1;
    int locks_per_message = 64;
    int read_threshold_bytes = 256;


    config["bucket_size"] = to_string(bucket_size);
    config["entry_size"] = to_string(entry_size);
    config["indexes"] = to_string(table_size);
    config["read_threshold_bytes"] = to_string(read_threshold_bytes);
    config["buckets_per_lock"] = to_string(buckets_per_lock);
    config["locks_per_message"] = to_string(locks_per_message);
    config["memory_size"] = to_string(memory_size);
    config["deterministic"]="True";
    config["workload"]="ycsb-w";
    config["id"]="0";
    config["search_function"]="a_star";
    config["location_function"]="dependent";

    // Client State Machine Arguements
    int total_inserts = 1;
    int max_fill = 50;
	int prime_fill = 30;
    int num_clients = 24;
	// int num_clinets = 1;
    config["total_inserts"]=to_string(total_inserts);
    config["total_requests"]=to_string(total_inserts);
    config["max_fill"]=to_string(max_fill);
	config["prime_fill"]=to_string(prime_fill);
    config["num_clients"]=to_string(num_clients);

    // RDMA Engine Arguments
    config["server_address"]="192.168.1.12";
    config["base_port"] = "20886";
    return config;
}



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
		rdma_error("addr: %p , len: %u , perm: %d \n", addr, length, permission);
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


// /** Stolen from sherman
on_chip_memory_attr createMemoryRegionOnChip(uint64_t mm, uint64_t mmSize,
                                 ibv_pd *pd, ibv_context *ctx) {

	struct ibv_exp_alloc_dm_attr dm_attr;
	memset(&dm_attr, 0, sizeof(dm_attr));
	dm_attr.length = mmSize;
	struct ibv_exp_dm *dm = ibv_exp_alloc_dm(ctx, &dm_attr);
	if (!dm) {
		rdma_error("Allocate on-chip memory failed\n");
		exit(0);
	}

	struct ibv_exp_reg_mr_in mr_in;
	memset(&mr_in, 0, sizeof(mr_in));
	mr_in.pd = pd;
	mr_in.addr = (void *)mm;
	mr_in.length = mmSize;
	// mr_in.exp_access = MEMORY_PERMISSION | IBV_ACCESS_ZERO_BASED;
	// mr_in.exp_access = IBV_ACCESS_ZERO_BASED;
	// mr_in.exp_access = IBV_ACCESS_ZERO_BASED | IBV_ACCESS_LOCAL_WRITE;
	// mr_in.exp_access = IBV_EXP_ACCESS_LOCAL_WRITE | IBV_EXP_ACCESS_REMOTE_READ | IBV_EXP_ACCESS_REMOTE_WRITE | IBV_EXP_ACCESS_REMOTE_ATOMIC; // | IBV_ACCESS_ZERO_BASED; // | IBV_EXP_ACCESS_MW_BIND | IBV_EXP_ACCESS_ZERO_BASED;
	mr_in.exp_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC; // | IBV_ACCESS_ZERO_BASED; // | IBV_EXP_ACCESS_MW_BIND | IBV_EXP_ACCESS_ZERO_BASED;

	// mr_in.exp_access |= IBV_EXP_ACCESS_MW_ZERO_BASED;
	
	//IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC | IBV_EXP_ACCESS_MW_ZERO_BASED; // | IBV_ACCESS_MW_BIND | IBV_ACCESS_ZERO_BASED;

	
	
	mr_in.create_flags = 0;
	mr_in.dm = dm;
	mr_in.comp_mask = IBV_EXP_REG_MR_DM;
	struct ibv_mr *mr = ibv_exp_reg_mr(&mr_in);
	if (!mr) {
		rdma_error("Register on-chip memory failed\n");
		exit(0);
	}

	// init zero
	char *buffer = (char *)malloc(mmSize);
	memset(buffer, 0, mmSize);

	struct ibv_exp_memcpy_dm_attr cpy_attr;
	memset(&cpy_attr, 0, sizeof(cpy_attr));
	cpy_attr.memcpy_dir = IBV_EXP_DM_CPY_TO_DEVICE;
	cpy_attr.host_addr = (void *)buffer;
	cpy_attr.length = mmSize;
	cpy_attr.dm_offset = 0;
	ibv_exp_memcpy_dm(dm, &cpy_attr);

	free(buffer);

	on_chip_memory_attr attr;
	attr.addr = (void *)mm;
	attr.length = mmSize;
	attr.mr = mr;
	attr.dm = dm;

	printf("allocated on chip memory %s\n", attr.to_string().c_str());
	return attr;
}

string on_chip_memory_attr::to_string(){
	string output = "";
	output += "addr: " + std::to_string((uint64_t) addr);
	output += "\nlength: " + std::to_string((uint64_t)length);
	output += "\nmr: addr " + std::to_string((uint64_t) mr->addr);
	output += "\nmr: handel " + std::to_string(mr->handle);
	output += "\nmr: length " + std::to_string((uint64_t)mr->length);
	output += "\nmr: lkey " + std::to_string(mr->lkey);
	output += "\nmr: rkey " + std::to_string(mr->rkey);
	output += "\ndm: context " + std::to_string((uint64_t) dm->context);
	output += "\ndm: handle " + std::to_string(dm->handle);
	output += "\ndm: length " + std::to_string(dm->comp_mask);


	return output;

}
		// **/ //End stolen from sherman


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
		ALERT("RDMA Common", "Failed to retrieve a cm event, errno: %d \n",
				-errno);
		return -errno;
	}
	/* lets see, if it was a good event */
	if(0 != (*cm_event)->status){
		ALERT("RDMA Common", "CM event has non zero status: %d\n", (*cm_event)->status);
		ret = -((*cm_event)->status);
		/* important, we acknowledge the event */
		rdma_ack_cm_event(*cm_event);
		return ret;
	}
	/* if it was a good event, was it of the expected type */
	if ((*cm_event)->event != expected_event) {
		ALERT("RDMA Common", "Unexpected event received: %s [ expecting: %s ]", 
				rdma_event_str((*cm_event)->event),
				rdma_event_str(expected_event));
		/* important, we acknowledge the event */
		rdma_ack_cm_event(*cm_event);
		return -1; // unexpected event :(
	}
	VERBOSE("RDMA Common", "A new %s type event is received \n", rdma_event_str((*cm_event)->event));
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


