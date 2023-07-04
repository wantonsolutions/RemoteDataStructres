#include "../virtual_rdma.h"
#include "../cuckoo.h"
#include "../rdma_engine.h"
#include "../state_machines.h"
#include <unordered_map>


using namespace std;
using namespace cuckoo_rdma_engine;
using namespace cuckoo_rcuckoo;

unordered_map<string, string> gen_config() {
    unordered_map<string, string> config;
    int table_size = 1024 * 32;
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
    int max_fill = 90;
    int num_clients = 1;
    config["total_inserts"]=to_string(total_inserts);
    config["total_requests"]=to_string(total_inserts);
    config["max_fill"]=to_string(max_fill);
    config["num_clients"]=to_string(num_clients);

    // RDMA Engine Arguments
    config["num_qps"]="1";
    config["server_address"]="192.168.1.12";
    config["base_port"] = "20886";

    return config;

}

int main(){
    printf("testing cuckoo!\n");
    unordered_map<string, string> config = gen_config();

    RCuckoo rcuck = RCuckoo(config);
    printf("done rcuck init\n");

    RDMA_Engine client_1 = RDMA_Engine(config, &rcuck);
    client_1.start();


    //now we call the engine

}