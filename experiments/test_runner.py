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
        "fill_factor",
        # "throughput_approximation",
        "throughput",
        "latency_per_operation",
        "retry_breakdown",
        ]
    plot_cuckoo.multi_plot_runs(stats, plot_names, directory)

config=dict()


# table_size = 1024 * 1024 * 10
# table_size = 1024 * 1024
table_size = 1024 * 1024 * 10
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
config["memory_size"] = str(memory_size)

config["read_threshold_bytes"] = str(read_threshold_bytes)
config["buckets_per_lock"] = str(buckets_per_lock)
config["locks_per_message"] = str(locks_per_message)
config["deterministic"]="True"
config["workload"]="ycsb-w"
config["id"]=str(0)
config["search_function"]="a_star"
config["location_function"]="dependent"

#Client State Machine Arguements
total_inserts = 1
max_fill = 50
prime_fill = 10
num_clients = 1
#num_clinets = 1;
config["total_inserts"]=str(total_inserts)
config["total_requests"]=str(total_inserts)
config["max_fill"]=str(max_fill)
config["prime"]="true"
config["prime_fill"]=str(prime_fill)
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

def fill_factor_single_client(config):
    fill = [10,20,30,40,50,60,70,80,90]
    table_size = 1024 * 1024
    clients = 1

    runs = []
    config["table_size"] = str(table_size)
    config["num_clients"]=str(clients)
    config["prime"]="true"
    for f in fill:
        lconfig = config.copy()
        lconfig["prime_fill"] = str(f-10)
        lconfig["max_fill"] = str(f)
        stats = orchestrator.run_trials(lconfig)
        runs.append(stats)
    # print(runs)
    dm.save_statistics(runs)

def client_fill_to_50_exp(config):
    clients = [1, 2, 4, 8, 16, 24]
    # clients = [4, 8, 16, 24]
    runs = []
    config["prime"]="false"
    config["max_fill"]="50"
    for c in clients:
        lconfig = config.copy()
        lconfig["num_clients"] = str(c)
        stats = orchestrator.run_trials(lconfig)
        runs.append(stats)
    # print(runs)
    dm.save_statistics(runs)

def table_size_contention(config):
    fill =50
    entry_size = 8
    config["prime_fill"] = str(fill-10)
    config["max_fill"] = str(fill)

    table_size = [1024 * 128, 1024 * 256, 1024 * 512, 1024 * 1024, 1024 * 1024 * 2, 1024 * 1024 * 4, 1024 * 1024 * 8]
    table_size = [1024 * 128, 1024 * 256,1024 * 512]
    clients = 24
    runs = []
    config["num_clients"]=str(clients)
    config["prime"]="true"
    for t in table_size:
        memory_size = entry_size * t
        lconfig = config.copy()
        print("table size: " + str(t))
        lconfig["table_size"] = str(t)
        lconfig["indexes"] = str(t)
        lconfig["memory_size"] = str(memory_size)
        stats = orchestrator.run_trials(lconfig)
        runs.append(stats)
    # print(runs)
    dm.save_statistics(runs)





# debug_exp(config)
# fill_factor_single_client(config)
# table_size_contention(config)
client_fill_to_50_exp(config)
plot_general_stats_last_run()