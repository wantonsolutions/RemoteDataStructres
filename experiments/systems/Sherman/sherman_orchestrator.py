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
import plot_sherman as ps

# import asyncio use this eventually when you want to parallelize the builds

cx5_numa_node_from_host = {
    'yak-00.sysnet.ucsd.edu': 0,
    'yak-01.sysnet.ucsd.edu': 0,
    'yeti-00.sysnet.ucsd.edu': 1,
    'yeti-01.sysnet.ucsd.edu': 1,
    'yeti-02.sysnet.ucsd.edu': 1,
    'yeti-03.sysnet.ucsd.edu': 1,
    'yeti-04.sysnet.ucsd.edu': 1,
    'yeti-05.sysnet.ucsd.edu': 1,
    'yeti-06.sysnet.ucsd.edu': 1,
    'yeti-07.sysnet.ucsd.edu': 1,
    'yeti-08.sysnet.ucsd.edu': 1,
    'yeti-09.sysnet.ucsd.edu': 1,
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

    def set_verbose(self, verbose):
        self.verbose = verbose

    def run_cmd(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        stdout_result =stdout.read()
        stderr_result = stderr.read()
        print(self.hostname, "stdout: ", stdout_result.decode("utf-8"))
        print(self.hostname, "stderr: ", stderr_result.decode("utf-8"))

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

    program_name = "benchmark"

    #here is there is really no concept of client and
    #server. There is a master node, node 0, but no client
    #and server, all machiens are just nodes.
    # server_name = 'yak-00.sysnet.ucsd.edu'
    # client_name = 'yak-01.sysnet.ucsd.edu' 

    # node_names = [
    # 'yeti-00.sysnet.ucsd.edu',
    # 'yeti-01.sysnet.ucsd.edu',
    # 'yeti-04.sysnet.ucsd.edu', 
    # 'yeti-05.sysnet.ucsd.edu', 
    # 'yeti-08.sysnet.ucsd.edu'
    # ]
    node_names = [
    'yeti-00.sysnet.ucsd.edu',
    'yeti-01.sysnet.ucsd.edu',
    'yeti-02.sysnet.ucsd.edu',
    'yeti-03.sysnet.ucsd.edu',
    'yeti-04.sysnet.ucsd.edu', 
    'yeti-05.sysnet.ucsd.edu', 
    # 'yeti-06.sysnet.ucsd.edu', 
    # 'yeti-07.sysnet.ucsd.edu', 
    'yeti-08.sysnet.ucsd.edu',
    'yeti-09.sysnet.ucsd.edu', 
    ]

    master_node_index = 0

    config = dict()
    def __init__(self):
        #pointer to all of the nodes
        self.all_nodes = []
        self.follower_nodes = []

        for node_name in self.node_names:
            self.all_nodes.append(ssh_wrapper('ssgrant', node_name))

        for i in range(1, len(self.all_nodes)):
            self.follower_nodes.append(self.all_nodes[i])

        self.master = self.all_nodes[self.master_node_index]
        self.build_location = ssh_wrapper('ssgrant', 'yak-01.sysnet.ucsd.edu')
        self.project_directory = '/usr/local/home/ssgrant/RemoteDataStructres/systems/Sherman'
        self.sync_dependents = self.all_nodes




    def collect_stats(self):
        temp_dir = tempfile.TemporaryDirectory()
        # print(temp_dir.name)

        client_stat_filename = 'client_statistics.json'
        remote_client_stat_filename = 'rcuckoo_rdma/statistics/'+client_stat_filename
        local_client_stat_filename = temp_dir.name+'/'+client_stat_filename
        self.master.get(remote_client_stat_filename, local_client_stat_filename)

        #we need to get the memory json

        f = open(local_client_stat_filename, 'r')
        client_stat = json.load(f)
        temp_dir.cleanup()
        return client_stat

    def set_verbose(self, verbose):
        for node in self.all_nodes:
            node.set_verbose(verbose)

    def kill(self):
        for node in self.all_nodes:
            node.run_cmd('echo iwicbV15 | sudo -S killall ' + self.program_name)


    def sync(self):
        for dep in self.sync_dependents:
            print("Syncing from", dep.hostname)
            dep.run_cmd(
                'cd ' + self.project_directory + '/build;'
                'rsync -r ' + self.build_location.hostname + ':' + self.project_directory + '/build/* ./;')

    def sanity_check(self):
        for node in self.all_nodes:
            node.sanity_check_mlx5()

    def workload_to_read_percentage(self, workload):
        wl = dict()
        wl["workloada"]=50
        wl["workloadb"]=95
        wl["workloadc"]=100
        wl["workloadupd100"]=0
        if workload not in wl:
            print("ERROR: workload not found")
            exit()
        return wl[workload]


    def run_ycsb_test(self,config):


        clients_per_node = int(config["clients"] / len(self.all_nodes))
        read_percentage = self.workload_to_read_percentage(config["workload"])
        node_count = len(self.all_nodes)
        if (clients_per_node < 1):
            print("ERROR: not enough nodes to run this experiment")
            exit()
        elif (clients_per_node * len(self.all_nodes) != config["clients"]):
            print("ERROR: clients must be a multiple of the number of nodes")
            exit()
        

        print("Running YCSB Throughput Test " + str(config))
        print("Starting server")



        #run the actual experiment
        self.kill()
        master_command=("cd " + self.project_directory + ";"
        "cd build;"
        "rm -f statistics.txt;"
        "./restartMemc.sh;"
        "./benchmark "+str(node_count)+ " " + str(read_percentage) + " " + str(clients_per_node) + " > master_node.log"
        )

        threads = []
        print(master_command)
        thr = threading.Thread(target=self.master.run_cmd,args=(master_command,), kwargs={})
        thr.start()
        threads.append(thr)


        time.sleep(5)
        for node in self.follower_nodes: 
            print("Starting client")
            command=("cd " + self.project_directory + ";"
            "cd build;" 
            "stdbuf -oL ./benchmark "+str(node_count)+ " " + str(read_percentage) + " " + str(clients_per_node) + " > client_node.log"
            )
            print(command)

            thr2 = threading.Thread(target=node.run_cmd,args=(command,), kwargs={})
            thr2.start()
            threads.append(thr2)

        sleeptime=60
        print("Sleeping for ",sleeptime," seconds to let the test run")
        for i in tqdm(range(sleeptime)):
            time.sleep(1)
        print("Killing client and server")
        self.kill()


    def collect_ycsb_stats(self):
        temp_dir = tempfile.TemporaryDirectory()
        # print(temp_dir.name)
        throughput_filename = "statistics.txt"
        remote_latency_directory = self.project_directory +'/build'
        local_client_stat_directory = temp_dir.name
        self.master.get(remote_latency_directory+"/"+throughput_filename, local_client_stat_directory+"/"+throughput_filename)
        with open(local_client_stat_directory+"/"+throughput_filename) as file:
            lines = [line.rstrip() for line in file]

        return lines[len(lines)-1]



def consolidate_tput_results(clients, operations, tputs):
    results = dict() 
    for op in operations:
        tput = []
        for i, v in enumerate(clients):
            tput.append(tputs[i][op])
        results[op] = tput
    results["clients"] = clients
    return results

def result_array_to_dict(results):
    dict_results = dict()
    for r in results:
        vals = r.split(",")
        for v in vals:
            key, value = v.split(":")
            if key in dict_results:
                l = dict_results[key]
                l.append(value)
                dict_results[key]=l
            else:
                dict_results[key] = [value]
    return dict_results

def run_ycsb_trial(datadir="data/sherman_ycsb"):
    orch = Orchestrator()
    config = dict()
    # orch.kill()
    orch.sync()

    # clients = [2,4,8]
    workloads = ["workloada","workloadb","workloadc","workloadupd100"]

    # clients = [4,8,16,32,64,128,200]
    # clients = [5,10,20,40,80,160,200]
    # clients = [250, 300, 400]
    # clients = [280]

    client_count = 8
    multiple = 40
    client_count = client_count * multiple
    clients = [client_count]
    workloads = ["workloada"]

    all_results= dict()
    for workload in workloads:
        results = []
        for client in clients:
            config["clients"] = client
            config["workload"] = workload
            orch.run_ycsb_test(config)
            result = orch.collect_ycsb_stats()
            results.append(result)
        all_results[workload] = result_array_to_dict(results)
        print(result_array_to_dict(results))
    all_results['clients'] = clients
    print(all_results)
    dm.save_statistics(all_results,datadir)


run_ycsb_trial("data/sherman_ycsb_zipf")
ps.plot_ycsb()
# ps.plot_latency()



    