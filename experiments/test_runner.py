import orchestrator
import data_management as dm
import plot_cuckoo as plot_cuckoo
import datetime
import git

# def write_config_to_json_file(config, filename):
#     with open(filename, 'w') as outfile:
#         json.dump(config, outfile)

def plot_general_stats_last_run(dirname=""):
    stats, directory = dm.load_statistics(dirname=dirname)
    print("plot general stats")
    plot_names = [
        "general_stats",
        "cas_success_rate",
        "read_write_ratio",
        # #dont uncomment this one ####"request_success_rate",
        "rtt_per_operation",
        "bytes_per_operation",
        "messages_per_operation",
        ### add a memory collection function first ### "fill_factor",
        # "throughput_approximation",
        "throughput",
        "retry_breakdown"
        ]
    plot_cuckoo.multi_plot_runs(stats, plot_names, directory)

config=dict()


# table_size = 1024 * 1024 * 10
table_size = 1024 * 1024 * 2
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


#maditory fields added to prevent breakage
config["description"] = "This is a test run and a manditory field"
config["name"] = "test_run"
config["state_machine"] = "cuckoo"
config['date']=datetime.datetime.now().strftime("%Y-%m-%d")
config['commit']=git.Repo(search_parent_directories=True).head.object.hexsha
config['hash_factor']=str(999999999)

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

config["trials"] = 1


def debug_exp(config):
    clients = [1, 2, 4, 8, 16, 24]
    runs = []
    for c in clients:
        lconfig = config.copy()
        lconfig["num_clients"] = str(c)
        stats = orchestrator.run_trials(lconfig)
        runs.append(stats)
    # print(runs)
    dm.save_statistics(runs)

# debug_exp(config)
plot_general_stats_last_run()