import orchestrator
import json

# def write_config_to_json_file(config, filename):
#     with open(filename, 'w') as outfile:
#         json.dump(config, outfile)

config=dict()


table_size = 1024 * 1024 * 10
#int table_size = 1024 * 1024;
#int table_size = 1024 * 10;
#int table_size = 256;
#int table_size = 1024;
#int table_size = 256;
#int table_size = 1024 * 2;
entry_size = 8
bucket_size = 8
memory_size = entry_size * table_size
buckets_per_lock = 1
locks_per_message = 64
read_threshold_bytes = 256



config["bucket_size"] = str(bucket_size)
config["entry_size"] = str(entry_size)
config["indexes"] = str(table_size)
config["read_threshold_bytes"] = str(read_threshold_bytes)
config["buckets_per_lock"] = str(buckets_per_lock)
config["locks_per_message"] = str(locks_per_message)
config["memory_size"] = str(memory_size)
config["deterministic"]="True"
config["workload"]="ycsb-w"
config["id"]=str(0)
config["search_function"]="a_star"
config["location_function"]="dependent"

#Client State Machine Arguements
total_inserts = 1
max_fill = 90
num_clients = 24
#num_clinets = 1;
config["total_inserts"]=str(total_inserts)
config["total_requests"]=str(total_inserts)
config["max_fill"]=str(max_fill)
config["num_clients"]=str(num_clients)

#RDMA Engine Arguments
config["server_address"]="192.168.1.12"
config["base_port"] = "20886"


clients = [1, 2, 4 , 8, 16, 24]
for c in clients:
    lconfig = config.copy()
    lconfig["num_clients"] = str(c)
    orch = orchestrator.orchestrator(lconfig)
    orch.transfer_configs_to_nodes(lconfig, "remote_config.json")
    orch.sanity_check()
    orch.kill()
    # orch.build()
    # # orch.build(clean=True)
    # orch.sync()
    orch.run_rdma_benchmark()
    orch.kill()