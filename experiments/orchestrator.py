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

import threading

import time

# import asyncio use this eventually when you want to parallelize the builds


class rcuckoo_ssh_wrapper:
    def __init__(self, username,hostname):
        # print("setting up ssh connection to", hostname)
        self.username = username
        self.hostname = hostname
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #ensure that we don't need a password to connect
        # print("connecting to ", hostname, "as", username)
        self.ssh.connect(self.hostname, username=self.username)
        self.scp = SCPClient(self.ssh.get_transport())
        self.verbose = False

    def get(self, remote_path, local_path):
        try:
            self.scp.get(remote_path, local_path)
        except:
            print("ERROR: file not found")
            return False
        return True

    def set_verbose(self, verbose):
        self.verbose = verbose

    def run_cmd(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        # stdout_result =stdout.read()
        # stderr_result = stderr.read()
        # print("stdout: ", stdout_result.decode("utf-8"))
        # print("stderr: ", stderr_result.decode("utf-8"))

        error_code = stdout.channel.recv_exit_status()
        if (self.verbose) or (error_code != 0):
            # print("Error code is not zero: ", error_code)
            # print("ERROR:!")
            # print("STDOUT:")
            stdout_result =stdout.read()
            # print(result.decode("utf-8"))
            # print("STDERR:")
            stderr_result = stderr.read()
            # print(result.decode("utf-8"))
            return (error_code, stdout_result, stderr_result)

    def get_mlx5_ip(self):
        stdin, stdout, stderr = self.ssh.exec_command(
            "dev=`ibdev2netdev | awk '{print $5}'`; ifconfig -a | grep $dev -A 1 | tail -1 | awk '{print $2}'"
        )
        return stdout.read().decode("utf-8").strip()

    def sanity_check_mlx5(self):
        ip = self.get_mlx5_ip()
        if ip == "":
            print("ERROR: mlx5 ip is empty")
            exit()
        print("Sanity Check: mlx5 ip is", ip)

    def __del__(self):
        self.ssh.close()

    def pull(self):
        self.run_cmd('git pull')

class Orchestrator:

    memory_program_name = "rdma_memory_server"
    client_program_name = "cuckoo_client"
    config_name = "remote_config.json"
    statistics_file = "statistics/statistics.json"
    server_name = 'yak-00.sysnet.ucsd.edu'
    build_server_name = 'yak-01.sysnet.ucsd.edu'
    # client_name = 'yak-01.sysnet.ucsd.edu' 
    client_names = [
    'yeti-00.sysnet.ucsd.edu',
    'yeti-01.sysnet.ucsd.edu',
    'yeti-02.sysnet.ucsd.edu',
    'yeti-03.sysnet.ucsd.edu',
    'yeti-04.sysnet.ucsd.edu',
    'yeti-05.sysnet.ucsd.edu', 
    'yeti-06.sysnet.ucsd.edu',
    'yeti-07.sysnet.ucsd.edu',
    'yeti-08.sysnet.ucsd.edu',
    'yeti-09.sysnet.ucsd.edu',
    ]
    config = dict()
    def __init__(self, conf):
        self.server = rcuckoo_ssh_wrapper('ssgrant', self.server_name)
        self.build_server = rcuckoo_ssh_wrapper('ssgrant', self.build_server_name)
        self.clients=[]
        for client_name in self.client_names:
            self.clients.append(rcuckoo_ssh_wrapper('ssgrant', client_name))

        #pointer to all of the nodes
        self.all_nodes = [self.server] + self.clients + [self.build_server]

        self.build_location = self.build_server
        self.project_directory = '/usr/local/home/ssgrant/RemoteDataStructres/rcuckoo_rdma'
        self.sync_dependents = [self.server] + self.clients


    def validate_run(self):
        # print("Here we should check that the JSON responses make sense, and that the run was successful")
        # print("At the moment validate run does nothing")
        self=self


    def transfer_configs_to_nodes(self, config, config_name):
        self.config_name = config_name
        for node in self.all_nodes:
            node.run_cmd(
                'cd rcuckoo_rdma/configs;'
                'echo \'' + json.dumps(config) + '\' > ' + config_name + ';')

    def get_threads_per_machine(self,config):
        threads_per_client = int(int(config["num_clients"]) / len(self.clients))
        if (threads_per_client * len(self.clients) != int(config["num_clients"])):
            print("ERROR: number of clients must be divisible by the number of machines. clients:", config["num_clients"], "machines:", len(self.clients))
            exit(0)
        return threads_per_client

    def generate_and_send_configs(self, config, config_name):
        #start by checking the config

        print(
            "workload:", config["workload"], 
            "cores:", config["num_clients"], 
            "fill:", config["max_fill"] ,
            "lpm:", config["locks_per_message"],
            "lock size:", config["buckets_per_lock"],
            "search", config["search_function"],
            )

        threads_per_machine = self.get_threads_per_machine(config)
        # print("threads per machine:", threads_per_machine)

        if not "base_port" in config:
            default_base_port=8500
            print("WARNING: base_port should be specified in the config using default ", default_base_port)
            config["base_port"] = str(default_base_port)

        memory_server_addr = self.server.get_mlx5_ip()
        # print("memory server address:", memory_server_addr)
        if memory_server_addr == "":
            print("ERROR: mlx5 ip is empty (how):", memory_server_addr)
            exit()
        if not "server_address" in config:
            print("WARNING: server_address should be specified in the config using default ", memory_server_addr)
            config["server_address"] = memory_server_addr

        if config["server_address"] != memory_server_addr:
            print("ERROR: server_address in config does not match mlx5 ip")
            exit()

        ##at this point we should be good to generate the configs
        #send the memory configuration to the memory server

        self.server.run_cmd(
            'cd rcuckoo_rdma/configs;'
            'echo \'' + json.dumps(config) + '\' > ' + config_name + ';')

        #send the client configuration to the clients
        base_port = int(config["base_port"])
        starting_id = 0
        config["global_clients"] = config["num_clients"]
        config["num_clients"] = threads_per_machine
        for client in self.clients:
            config["base_port"] = str(base_port)
            config["starting_id"] = str(starting_id)
            client.run_cmd(
                'cd rcuckoo_rdma/configs;'
                'echo \'' + json.dumps(config) + '\' > ' + config_name + ';')
            base_port += threads_per_machine
            starting_id += threads_per_machine




    def collect_stats(self):
        temp_dir = tempfile.TemporaryDirectory()
        # print(temp_dir.name)

        master_stats = dict()
        client_stat_filename = 'client_statistics.json'
        remote_client_stat_filename = self.project_directory + '/statistics/'+client_stat_filename
        local_client_stat_filename = temp_dir.name+'/'+client_stat_filename
        success = False
        for i, client in enumerate(self.clients):
            success = client.get(remote_client_stat_filename, local_client_stat_filename)

            if not success:
                print("ERROR: failed to get client statistics for client ", i, " from ", client.hostname)
                continue
            f = open(local_client_stat_filename, 'r')
            client_stat = json.load(f)
            if i ==0:
                success = True
                master_stats = client_stat
                print("master stats:", master_stats["system"])
            else:
                clients = master_stats["clients"]
                t_clients = client_stat["clients"]
                clients.extend(t_clients)
                master_stats["clients"] = clients

            f.close()
        temp_dir.cleanup()
        return master_stats, success

    def set_verbose(self, verbose):
        for node in self.all_nodes:
            node.set_verbose(verbose)

    def kill(self):
        self.server.run_cmd('echo iwicbV15 | sudo -S killall ' + self.memory_program_name)
        for client in self.clients:
            client.run_cmd('echo iwicbV15 | sudo -S killall ' + self.client_program_name)


    def build(self, config):
        print("Starting Build on", self.build_location.hostname)

        threads = 30
        print("Building on ...", self.build_location.hostname, "with", threads, "threads")

        define_string = ""

        if "value_size" in config:
            define_string += " export VALUE_SIZE=" + str(config["value_size"]) + ";"

        self.build_location.run_cmd(
            'cd ' + self.project_directory + ';'
            + define_string +
            'make -B -j ;')

    def sync(self):
        sync_command=(
                'cd rcuckoo_rdma;'
                'rsync -a ' + self.build_location.hostname + ':' + self.project_directory + '/* ./;')
        
        sync_threads=[]
        for dep in self.sync_dependents:
            sync_thread = threading.Thread(target=dep.run_cmd, args=(sync_command,))
            sync_threads.append(sync_thread)
        
        for sync_thread in sync_threads:
            # print("Syncing from", dep.hostname)
            sync_thread.start()

        for sync_thread in sync_threads:
            sync_thread.join()

        print("done syncing")
            # dep.run_cmd(
            #     'cd rcuckoo_rdma;'
            #     'rsync -a ' + self.build_location.hostname + ':' + self.project_directory + '/* ./;')

    def sanity_check(self):
        for node in self.all_nodes:
            node.sanity_check_mlx5()

    def setup_huge(self):
        #do hugeppages on the server
        #200gb of 2mb pages
        # print("setting up huge pages on server")
        self.server.run_cmd('echo iwicbV15 | sudo -S hugeadm --pool-pages-min 2MB:102400')
        for client in self.clients:
            # client.run_cmd('echo iwicbV15 | sudo hugeadm --pool-pages-min 2MB:16384')
            # print("setting up huge pages on client", client.hostname)
            client.run_cmd('echo iwicbV15 | sudo -S hugeadm --pool-pages-min 2MB:65536')

    def run(self):
        # print("Starting RDMA Benchmark")

        server_command=('cd rcuckoo_rdma;'
            'stdbuf -oL ./'+ self.memory_program_name + ' ' + 'configs/' + self.config_name + ' > memserver.out 2>&1 &')
        server_thread = threading.Thread(target=self.server.run_cmd, args=(server_command,))
        server_thread.start()

        sleeptime=15
        print("sleeping for ", sleeptime, " seconds")
        for i in tqdm(range(sleeptime)):
            # print("Waiting for server to start")
            time.sleep(1)


            # 'export MLX5_SINGLE_THREADED=1;'
        client_threads=[]
        client_command=(
                'cd rcuckoo_rdma;'
                'rm -f statistics/client_statistics.json;'
                'LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes stdbuf -oL ./' + self.client_program_name + ' ' + 'configs/' + self.config_name + ' > client.out 2>&1;'
                'cat client.out;'
        )
        for client in self.clients:
            print("Starting experiment on:", client.hostname)
            client_thread = threading.Thread(target=client.run_cmd, args=(client_command,))
            client_threads.append(client_thread)

        # print("Starting all clients")
        for client_thread in client_threads:
            # print("starting client thread", client_thread)
            client_thread.start()
            time.sleep(1)
        timeout = 120
        print("Waiting for all {} clients to finish..".format(len(client_threads)))
        for i, client_thread in enumerate(client_threads):
            print("Waiting for client", i)
            client_thread.join(timeout=timeout)
            timeout=5
        print("All clients finished")

        time.sleep(15)

        self.kill()


        # time.sleep(5)

def fix_stats(stats, config):
    print("fixing the number of clients")

    #make sure that the total number of clients is correct
    clients = stats["clients"]
    for i, client in enumerate(clients):
        clients[i]["client_id"] = str(i)
        clients[i]["stats"]["client_id"] = str(i)
        clients[i]["stats"]["num_clients"] = len(clients)
    stats["clients"] = clients

    stats["config"]["num_clients"] = len(clients)
    return stats


def boot(config):
    boot_orch = Orchestrator(config)
    boot_orch.sanity_check()
    boot_orch.setup_huge()
    boot_orch.sync()
    boot_orch.kill()
    del boot_orch

def build(config):
    build_orch = Orchestrator(config)
    build_orch.build(config)
    del build_orch

    
def run_trials(config):
    runs = []
    trials = config['trials']


    for i in tqdm(range(trials)):
        c=copy.copy(config)
        orch = Orchestrator(c)
        #prepare for the run
        # orch.transfer_configs_to_nodes(config, "remote_config.json")
        orch.generate_and_send_configs(c, "remote_config.json")
        orch.kill()
        stats_collected = False
        try:
            orch.run()
        except Exception as e:
            print("Run ", i, " failed with exception -- we gotta fix this", e)
            exit()

        
        if not stats_collected:
            orch.validate_run()
            stats, success = orch.collect_stats()
            if not success:
                print("ERROR: failed to get stats for trial ", i)
                continue
            stats = fix_stats(stats, c)
        runs.append(stats)
        del orch
    return runs
