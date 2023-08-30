import warnings

from cryptography.utils import CryptographyDeprecationWarning
with warnings.catch_warnings():
    warnings.filterwarnings('ignore', category=CryptographyDeprecationWarning)
import paramiko
from scp import SCPClient

import json
import tempfile
from tqdm import tqdm
import copy
import numpy as np
import time

from experiments import data_management as dm

import threading
import plot_fusee as pf

# import asyncio use this eventually when you want to parallelize the builds

cx5_numa_node_from_host = {
    'yak-00.sysnet.ucsd.edu': 0,
    'yak-01.sysnet.ucsd.edu': 0,
    'yeti-00.sysnet.ucsd.edu': 1,
    'yeti-01.sysnet.ucsd.edu': 1,
    'yeti-04.sysnet.ucsd.edu': 1,
    'yeti-05.sysnet.ucsd.edu': 1,
    'yeti-08.sysnet.ucsd.edu': 1,
}


class ssh_wrapper:
    def __init__(self, username,hostname):
        # print("setting up ssh connection to", hostname)
        self.username = username
        self.hostname = hostname
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #ensure that we don't need a password to connect
        self.ssh.connect(self.hostname, username=self.username)
        self.scp = SCPClient(self.ssh.get_transport())
        self.verbose = False

    def get(self, remote_path, local_path):
        self.scp.get(remote_path, local_path)

    def put(self, local_path, remote_path):
        self.scp.put(local_path,remote_path )

    def set_verbose(self, verbose):
        self.verbose = verbose


    def run_cmd(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        stdout_result =stdout.read()
        stderr_result = stderr.read()
        print("stdout: ", stdout_result.decode("utf-8"))
        print("stderr: ", stderr_result.decode("utf-8"))

        error_code = stdout.channel.recv_exit_status()
        return
        if (self.verbose) or (error_code != 0):
            stdout_result =stdout.read()
            stderr_result = stderr.read()
            print("stdout: ", stdout_result.decode("utf-8"))
            print("stderr: ", stderr_result.decode("utf-8"))
            return (error_code, stdout_result, stderr_result)

    def get_mlx5_ip(self):
        stdin, stdout, stderr = self.ssh.exec_command(
            "ifconfig -a | grep enp -A 1 | tail -1 | awk '{print $2}'"
        )
        return stdout.read().decode("utf-8").strip()

    def sanity_check_mlx5(self):
        ip = self.get_mlx5_ip()
        if ip == "":
            print("ERROR: mlx5 ip is empty")
            exit()
        # print("Sanity Check: mlx5 ip is", ip)

    def __del__(self):
        self.ssh.close()

    def pull(self):
        self.run_cmd('git pull')

class Orchestrator:

    latency_memory_program_name = "ycsb_test_server"
    latency_client_program_name = "latency_test_client"

    throughput_memory_program_name = "ycsb_test_server"
    throughput_client_program_name = "micro_test_multi_client"

    ycsb_throughput_memory_program_name = "ycsb_test_server"
    ycsb_throughput_client_program_name = "ycsb_test_multi_client"


    server_name = 'yak-00.sysnet.ucsd.edu'
    # client_name = 'yak-01.sysnet.ucsd.edu' 
    client_names = [
    'yeti-00.sysnet.ucsd.edu',
    'yeti-01.sysnet.ucsd.edu',
    'yeti-04.sysnet.ucsd.edu', 
    'yeti-05.sysnet.ucsd.edu', 
    'yeti-08.sysnet.ucsd.edu']
    # client_name = 'yeti-05.sysnet.ucsd.edu' 
    config = dict()
    def __init__(self):
        self.server = ssh_wrapper('ssgrant', self.server_name)
        self.build_server = ssh_wrapper('ssgrant', 'yak-01.sysnet.ucsd.edu')
        self.clients=[]

        for client_name in self.client_names:
            self.clients.append(ssh_wrapper('ssgrant', client_name))

        # self.client = ssh_wrapper('ssgrant', self.client_name)

        #pointer to all of the nodes
        self.all_nodes = [self.server] + self.clients + [self.build_server]

        self.build_location = self.build_server
        self.project_directory = '/usr/local/home/ssgrant/RemoteDataStructres/systems/FUSEE'
        self.sync_dependents = [self.server] + self.clients




    def collect_stats(self):
        temp_dir = tempfile.TemporaryDirectory()
        # print(temp_dir.name)

        client_stat_filename = 'client_statistics.json'
        remote_client_stat_filename = 'rcuckoo_rdma/statistics/'+client_stat_filename
        local_client_stat_filename = temp_dir.name+'/'+client_stat_filename
        self.client.get(remote_client_stat_filename, local_client_stat_filename)

        #we need to get the memory json

        f = open(local_client_stat_filename, 'r')
        client_stat = json.load(f)
        temp_dir.cleanup()
        return client_stat

    def set_verbose(self, verbose):
        for node in self.all_nodes:
            node.set_verbose(verbose)

    def kill(self):
        self.server.run_cmd('echo iwicbV15 | sudo -S killall ' + self.latency_memory_program_name)
        self.server.run_cmd('echo iwicbV15 | sudo -S killall ' + self.throughput_memory_program_name)
        self.server.run_cmd('echo iwicbV15 | sudo -S killall ' + self.ycsb_throughput_memory_program_name)

        for client in self.clients:
            client.run_cmd('echo iwicbV15 | sudo -S killall ' + self.latency_client_program_name)
            client.run_cmd('echo iwicbV15 | sudo -S killall ' + self.throughput_client_program_name)
            client.run_cmd('echo iwicbV15 | sudo -S killall ' + self.ycsb_throughput_client_program_name)


    # def build(self, pull=False, clean=False):
    #     print("Starting Build on", self.build_location.hostname)

    #     if pull:
    #         print("Pulling from git")
    #         self.build_location.pull()

    #     if clean:
    #         print("Cleaning First on", self.build_location.hostname)
    #         self.build_location.run_cmd(
    #             'cd ' + self.project_directory + ';'
    #             'make clean;')

    #     threads = 30
    #     print("Building on ...", self.build_location.hostname, "with", threads, "threads")
    #     self.build_location.run_cmd(
    #         'cd ' + self.project_directory + ';'
    #         'make -j ' + str(threads) + ';')


    def setup(self):
        #do hugeppages on the server
        #200gb of 2mb pages
        self.server.run_cmd('echo iwicbV15 | sudo -S hugeadm --pool-pages-min 2MB:102400')
        for client in self.clients:
            # client.run_cmd('echo iwicbV15 | sudo hugeadm --pool-pages-min 2MB:16384')
            client.run_cmd('echo iwicbV15 | sudo -S hugeadm --pool-pages-min 2MB:65536')

    def sync(self):
        for dep in self.sync_dependents:
            print("Syncing from", dep.hostname)
            dep.run_cmd(
                'cd rcuckoo_rdma;'
                'rsync -r ' + self.build_location.hostname + ':' + self.project_directory + '/*;')

    def sanity_check(self):
        for node in self.all_nodes:
            node.sanity_check_mlx5()


    def run_latency_test(self):
        print("Running Latency Test")
        print("Starting server")

        #run the actual experiment
        self.kill()
        command=("cd " + self.project_directory + ";"
        "cd build;" 
        "numactl -N 0 -m 0 ./ycsb-test/" +self.latency_memory_program_name + " 0 &")
        "echo server started"
        thr = threading.Thread(target=self.server.run_cmd,args=(command,), kwargs={})
        thr.start()
        print("Starting client")

        numa_node = cx5_numa_node_from_host[self.client_name]
        client_command=("cd " + self.project_directory + ";"
        "cd build/micro-test;"
        "numactl -N " + str(numa_node) +" -m "+ str(numa_node) +" ./" + self.latency_client_program_name + " ../client_config.json &")

        thr2 = threading.Thread(target=self.client.run_cmd,args=(client_command,), kwargs={})
        thr2.start()

        sleeptime=10
        print("Sleeping for ",sleeptime," seconds to let the test run")
        for i in tqdm(range(sleeptime)):
            time.sleep(1)
        print("Killing client and server")
        self.kill()

    def run_throughput_test(self,config):
        print("Running Throughput Test " + str(config))
        print("Starting server")

        self.generate_and_send_configs(config)

        #run the actual experiment
        self.kill()
        command=("cd " + self.project_directory + ";"
        "cd build;" 
        "numactl -N 0 -m 0 ./ycsb-test/" +self.throughput_memory_program_name + " 0 &")
        "echo server started"

        thr = threading.Thread(target=self.server.run_cmd,args=(command,), kwargs={})
        print("Starting Server")
        thr.start()

        threads_per_client = self.get_threads_per_client(config)
        client_threads=[]
        for i, client in enumerate(self.clients):
            numa_node = cx5_numa_node_from_host[self.client_names[i]]
            client_command=("cd " + self.project_directory + ";"
            "cd build/micro-test;"
            "CLIENTS="+str(threads_per_client)+";"
            'rm results/$CLIENTS.tput;'
            # "yes | numactl -N "+ str(numa_node) +" -m "+ str(numa_node) +" ./" + self.throughput_client_program_name + ' ../client_config_orc.json $CLIENTS > results/"$CLIENTS".tput')
            "yes |  ./" + self.throughput_client_program_name + ' ../client_config_orc.json $CLIENTS > results/"$CLIENTS".tput')
            client_thread = threading.Thread(target=client.run_cmd,args=(client_command,), kwargs={})
            client_threads.append(client_thread)

        print("Starting Clients")
        for client_thread in client_threads:
            client_thread.start()
        print("waiting for clients to join") 
        for client_thread in client_threads:
            client_thread.join()

        # sleeptime=30
        # print("Sleeping for ",sleeptime," seconds to let the test run")
        # for i in tqdm(range(sleeptime)):
        #     time.sleep(1)
        print("Killing client and server")
        self.kill()

    def run_ycsb_throughput_test(self,config):
        print("Running YCSB Throughput Test " + str(config))
        print("Starting server")

        self.generate_and_send_configs(config)
        #run the actual experiment
        self.kill()
        command=("cd " + self.project_directory + ";"
        "cd build;" 
        # "numactl -N 0 -m 0 ./ycsb-test/" +self.ycsb_throughput_memory_program_name + " 0 &")
        "numactl ./ycsb-test/" +self.ycsb_throughput_memory_program_name + " 0 &")
        "echo server started"
        thr = threading.Thread(target=self.server.run_cmd,args=(command,), kwargs={})
        thr.start()

        threads_per_client = self.get_threads_per_client(config)
        client_threads=[]
        for i, client in enumerate(self.clients):
            numa_node = cx5_numa_node_from_host[self.client_names[i]]
            output_file = "results/"+config["workload"] + '_'+str(threads_per_client)+".tput"
            client_command=("cd " + self.project_directory + ";"
            "cd build/ycsb-test;"
            " echo "+str(self.client_names[i])+"; "
            "CLIENTS="+str(threads_per_client)+";"
            'rm -f '+output_file +';'
            # "yes | numactl -N "+ str(numa_node) +" -m "+ str(numa_node) + " ./" + self.ycsb_throughput_client_program_name + ' ../client_config_orc.json '+ config["workload"]+ ' $CLIENTS > '+output_file)
            "yes |  ./" + self.ycsb_throughput_client_program_name + ' ../client_config_orc.json '+ config["workload"]+ ' $CLIENTS > '+output_file)

            print(client_command)
            client_thread = threading.Thread(target=client.run_cmd,args=(client_command,), kwargs={})
            client_threads.append(client_thread)
        
        print("Starting client")
        for client_thread in client_threads:
            client_thread.start()
        print("waiting for clients to join")    
        timeout_time = 120
        for i, client_thread in enumerate(client_threads):
            print("waiting for client ",i, "for ",timeout_time," seconds")
            client_thread.join(timeout=timeout_time)
            if client_thread.is_alive():
                print("Client ",i, "timed out")
            timeout_time = 10

        
        # sleeptime=30
        # print("Sleeping for ",sleeptime," seconds to let the test run")
        # for i in tqdm(range(sleeptime)):
        #     time.sleep(1)
        print("Killing client and server")
        self.kill()

    def collect_latency_stats(self):

        temp_dir = tempfile.TemporaryDirectory()
        # print(temp_dir.name)
        insert_latency_filename = 'insert_lat-1rp.txt'
        read_latency_filename = 'search_lat-1rp.txt'
        update_latency_filename = 'update_lat-1rp.txt'
        remote_latency_directory = self.project_directory +'/build/micro-test/results'
        local_client_stat_directory = temp_dir.name
        self.client.get(remote_latency_directory+"/"+insert_latency_filename, local_client_stat_directory+"/"+insert_latency_filename)
        self.client.get(remote_latency_directory+"/"+read_latency_filename, local_client_stat_directory+"/"+read_latency_filename)
        self.client.get(remote_latency_directory+"/"+update_latency_filename, local_client_stat_directory+"/"+update_latency_filename)    


        #we need to get the memory json
        insert_data = np.loadtxt(local_client_stat_directory+"/"+insert_latency_filename)
        read_data = np.loadtxt(local_client_stat_directory+"/"+read_latency_filename)
        update_data = np.loadtxt(local_client_stat_directory+"/"+update_latency_filename)
        json_output = dict()

        json_output["insert_latency"] = insert_data.tolist()
        json_output["read_latency"] = read_data.tolist()
        json_output["update_latency"] = update_data.tolist()
        json_return = json.dumps(json_output)
        return json_return

    def get_operation_tput(self,lines, operation):
        search_query = operation + " tpt"
        for line in lines:
            # we are searching for strings of the form
            # 'search tpt: 296874 ops/s'
            # the output is big and dirty so we are just searching for a specific line
            if search_query in line:
                return int(line.split(" ")[2])

    def get_ycsb_tput(self, lines):
        for line in lines:
            if "tpt" in line:
                return int(line.split(" ")[1])

        print("EXPERIMENT FAILED, unable to find a result")
        return 0

    def collect_ycsb_throughput_stats(self,config):
        temp_dir = tempfile.TemporaryDirectory()
        # print(temp_dir.name)
        threads_per_client = self.get_threads_per_client(config)

        throughput_filename = str(config['workload']) + "_" + str(threads_per_client)+".tput"
        remote_directory = self.project_directory +'/build/ycsb-test/results'
        local_client_stat_directory = temp_dir.name

        client_tput=0
        for i, client in enumerate(self.clients):
            local_filename=local_client_stat_directory+"/"+throughput_filename
            remote_filename=remote_directory+"/"+throughput_filename
            client.get(remote_filename, local_filename)
            with open(local_filename) as file:
                lines = [line.rstrip() for line in file]

            client_tput += self.get_ycsb_tput(lines)
        print(client_tput)

        return client_tput



    def collect_throughput_stats(self,config):
        temp_dir = tempfile.TemporaryDirectory()
        # print(temp_dir.name)
        threads_per_client = self.get_threads_per_client(config)
        throughput_filename = str(threads_per_client)+".tput"
        remote_latency_directory = self.project_directory +'/build/micro-test/results'
        local_client_stat_directory = temp_dir.name

        client_tputs = []
        for i, client in enumerate(self.clients):
            local_filename=local_client_stat_directory+"/"+"client_"+str(i)+"_"+throughput_filename
            remote_filename=remote_latency_directory+"/"+throughput_filename
            client.get(remote_filename, local_filename)
            with open(local_filename) as file:
                lines = [line.rstrip() for line in file]


            tputs = dict()
            for operation in config["operations"]:
                tput = self.get_operation_tput(lines, operation)
                # print(operation, tput)
                tputs[operation] = tput

            client_tputs.append(tputs)

        return client_tputs

    def open_reference_config(self, filename):
        try: 
            f = open(filename, 'r')
            config = json.load(f)
        except:
            print("Error opening config file", filename)
            exit(0)
        return config

    def gb_to_bytes(self, gb):
        return gb * (1024) * (1024) * (1024)

    def get_threads_per_client(self,config):
        threads_per_client = int(config["clients"] / len(self.clients))
        if (threads_per_client * len(self.clients) != config["clients"]):
            print("ERROR: number of clients must be divisible by the number of machines. clients:", config["clients"], "machines:", len(self.clients))
            exit(0)
        return threads_per_client


    def generate_and_send_configs(self, config):

        #read in the server config
        server_config = self.open_reference_config("server_config.json")
        client_config = self.open_reference_config("client_config.json")

        #calculate the server data len
        server_memory = 100
        # server_memory = int(config["clients"]) / 2
        server_config["server_data_len"] = self.gb_to_bytes(server_memory)
        client_config["server_data_len"] = self.gb_to_bytes(server_memory)

        #calculate the number of threads per client
        threads_per_client = self.get_threads_per_client(config)
        client_config["num_coroutines"] = threads_per_client

        #create per client configurations
        client_configs=[]
        index = 0
        num_servers = 1
        for client in self.clients:
            copy_config = copy.deepcopy(client_config)
            copy_config["server_id"] = num_servers + (index * threads_per_client)
            client_configs.append(copy_config)
            index += 1

        # write the files out and then send them to the cluster
        temp_dir = tempfile.TemporaryDirectory()
        filename =temp_dir.name+"/server_config.json"
        with open(filename, 'w') as outfile:
            json.dump(server_config, outfile, indent=4)
        #note this file must be called server_config.json, it's hard coded
        self.server.put(filename, self.project_directory+"/build/server_config.json")

        i=0
        for conf in client_configs:
            filename = temp_dir.name+"/client_config_"+str(i)+".json"
            with open(filename, 'w') as outfile:
                json.dump(conf, outfile, indent=4)
            self.clients[i].put(filename, self.project_directory+"/build/client_config_orc.json")
            i += 1

        temp_dir.cleanup()







def run_latency_trial():
    orch = Orchestrator()
    orch.run_latency_test()
    result = orch.collect_latency_stats()
    dm.save_statistics(result,"data/fusee_latency")

def consolidate_tput_results(clients, operations, tputs):
    results = dict() 
    for op in operations:
        tput = []
        for i, v in enumerate(clients):
            tput.append(tputs[i][op])
        results[op] = tput
    results["clients"] = clients
    return results

def consolidate_multi_machine_tput_results(clients, operations, tputs):
    results = dict() 
    for op in operations:
        tput = []
        for i, measurement in enumerate(tputs):
            agg_tput = 0
            for j, machine in enumerate(measurement):
                if machine[op] != None:
                    agg_tput += machine[op]
                else :
                    agg_tput += 0
            tput.append(agg_tput)
        results[op] = tput
    results["clients"] = clients
    return results
        
        

def run_throughput_trial(data_file="data/fusee_throughput"):
    orch = Orchestrator()

    orch.setup()
    config = dict()
    # clients = [1,2,4,8]
    clients = [2,4,8,16,32,48, 64, 80]
    # clients = [80]
    # clients = [2,4]
    run_tputs = []
    operations = ["insert", "search", "update", "delete"]
    config["operations"] = operations
    for client in clients:
        config["clients"] = client
        orch.run_throughput_test(config)
        tputs = orch.collect_throughput_stats(config)
        run_tputs.append(tputs)

    # results = consolidate_tput_results(clients,operations,run_tputs)
    print(run_tputs)
    results = consolidate_multi_machine_tput_results(clients,operations,run_tputs)
    print(results)
    dm.save_statistics(results,data_file)

def run_ycsb_throughput_trial(data_file="data/fusee_ycsb"):
    orch = Orchestrator()
    orch.setup()

    config = dict()
    # clients = [1,2,4,8]

    # clients = [2,4,8,16,32,48, 64, 80]
    # clients = [200]
    clients = [5, 10, 20, 40, 80, 160, 200]
    # clients = [60, 96, 120]
    # workloads = ["workloada", "workloadb", "workloadc", "workloadd", "workloadupd100"]
    workloads = ["workloada", "workloadb", "workloadc", "workloadd"]
    # workloads = ["workloadb"]
    all_results = dict()
    for workload in workloads:
        results = []
        for client in clients:
            config["workload"] = workload
            config["clients"] = client
            orch.run_ycsb_throughput_test(config)
            result = orch.collect_ycsb_throughput_stats(config)
            results.append(result)
        all_results[workload] = results
    all_results['clients'] = clients
    print(all_results)
    dm.save_statistics(all_results,data_file)

def run_many_ycsb_trials(trials=5):
    for i in range(trials):
        run_ycsb_throughput_trial(data_file="data/fusee_ycsb_"+str(i))

            # dm.save_statistics(result,"data/fusee_throughput_"+workload)

#run throughput trial
# run_throughput_trial()
# pf.plot_tput()

#get raw operation latency numbers
# run_latency_trial()
# pf.plot_latency()

#run ycsb throughput trial
# run_ycsb_throughput_trial()
# pf.plot_ycsb()

run_many_ycsb_trials(10)


    