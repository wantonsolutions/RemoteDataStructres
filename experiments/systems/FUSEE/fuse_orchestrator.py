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

    def set_verbose(self, verbose):
        self.verbose = verbose

    def run_cmd(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        # stdout_result =stdout.read()
        # stderr_result = stderr.read()
        # print("stdout: ", stdout_result.decode("utf-8"))
        # print("stderr: ", stderr_result.decode("utf-8"))

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



    server_name = 'yak-00.sysnet.ucsd.edu'
    client_name = 'yak-01.sysnet.ucsd.edu' 
    config = dict()
    def __init__(self):
        self.server = ssh_wrapper('ssgrant', self.server_name)
        self.client = ssh_wrapper('ssgrant', self.client_name)

        #pointer to all of the nodes
        self.all_nodes = [self.client, self.server]

        self.build_location = self.client
        self.project_directory = '/usr/local/home/ssgrant/RemoteDataStructres/systems/FUSEE'
        self.sync_dependents = [self.server]




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
        self.client.run_cmd('echo iwicbV15 | sudo -S killall ' + self.latency_client_program_name)

        self.server.run_cmd('echo iwicbV15 | sudo -S killall ' + self.throughput_memory_program_name)
        self.client.run_cmd('echo iwicbV15 | sudo -S killall ' + self.throughput_client_program_name)


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

        client_command=("cd " + self.project_directory + ";"
        "cd build/micro-test;"
        "numactl -N 0 -m 0 ./" + self.latency_client_program_name + " ../client_config.json &")

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

        #run the actual experiment
        self.kill()
        command=("cd " + self.project_directory + ";"
        "cd build;" 
        "numactl -N 0 -m 0 ./ycsb-test/" +self.throughput_memory_program_name + " 0 &")
        "echo server started"
        thr = threading.Thread(target=self.server.run_cmd,args=(command,), kwargs={})
        thr.start()
        print("Starting client")

        client_command=("cd " + self.project_directory + ";"
        "cd build/micro-test;"
        "CLIENTS="+str(config['clients'])+";"
        'rm results/$CLIENTS.tput;'
        "yes | numactl -N 0 -m 0 ./" + self.throughput_client_program_name + ' ../client_config.json $CLIENTS > results/"$CLIENTS".tput')
        thr2 = threading.Thread(target=self.client.run_cmd,args=(client_command,), kwargs={})
        thr2.start()
        thr2.join()
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


    def collect_throughput_stats(self,config):
        temp_dir = tempfile.TemporaryDirectory()
        # print(temp_dir.name)
        throughput_filename = str(config['clients'])+".tput"
        remote_latency_directory = self.project_directory +'/build/micro-test/results'
        local_client_stat_directory = temp_dir.name
        self.client.get(remote_latency_directory+"/"+throughput_filename, local_client_stat_directory+"/"+throughput_filename)
        with open(local_client_stat_directory+"/"+throughput_filename) as file:
            lines = [line.rstrip() for line in file]


        tputs = dict()
        for operation in config["operations"]:
            tput = self.get_operation_tput(lines, operation)
            # print(operation, tput)
            tputs[operation] = tput

        return tputs



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
        
        

def run_throughput_trial():
    orch = Orchestrator()
    config = dict()
    # clients = [1,2,4,8]
    clients = [1,2,3,4,5,6,7,8]
    run_tputs = []
    operations = ["insert", "search", "update", "delete"]
    config["operations"] = operations
    for client in clients:
        config["clients"] = client
        orch.run_throughput_test(config)
        tputs = orch.collect_throughput_stats(config)
        run_tputs.append(tputs)

    results = consolidate_tput_results(clients,operations,run_tputs)
    print(results)
    dm.save_statistics(results,"data/fusee_throughput")


    

    # config["clients"] = 1
    # orch.run_throughput_test(config)
    # result = orch.collect_throughput_stats()
    # dm.save_statistics(result,"data/fusee_throughput")


run_throughput_trial()
pf.plot_tput()

#get raw operation latency numbers
# run_latency_trial()
# pf.plot_latency()


    