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

# import asyncio use this eventually when you want to parallelize the builds


class rcuckoo_ssh_wrapper:
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

    memory_program_name = "rdma_memory_server"
    client_program_name = "cuckoo_client"
    config_name = "remote_config.json"
    statistics_file = "statistics/statistics.json"
    server_name = 'yak-00.sysnet.ucsd.edu'
    client_name = 'yak-01.sysnet.ucsd.edu' 
    config = dict()
    def __init__(self, conf):
        self.server = rcuckoo_ssh_wrapper('ssgrant', self.server_name)
        self.client = rcuckoo_ssh_wrapper('ssgrant', self.client_name)

        #pointer to all of the nodes
        self.all_nodes = [self.client, self.server]

        self.build_location = self.client
        self.project_directory = '/usr/local/home/ssgrant/RemoteDataStructres/rcuckoo_rdma'
        self.sync_dependents = [self.server]

        self.queue_pairs = 24

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
        self.server.run_cmd('echo iwicbV15 | sudo -S killall ' + self.memory_program_name)
        self.client.run_cmd('echo iwicbV15 | sudo -S killall ' + self.client_program_name)


    def build(self, pull=False, clean=False):
        print("Starting Build on", self.build_location.hostname)

        if pull:
            print("Pulling from git")
            self.build_location.pull()

        if clean:
            print("Cleaning First on", self.build_location.hostname)
            self.build_location.run_cmd(
                'cd ' + self.project_directory + ';'
                'make clean;')

        threads = 30
        print("Building on ...", self.build_location.hostname, "with", threads, "threads")
        self.build_location.run_cmd(
            'cd ' + self.project_directory + ';'
            'make -j ' + str(threads) + ';')

    def sync(self):
        for dep in self.sync_dependents:
            print("Syncing from", dep.hostname)
            dep.run_cmd(
                'cd rcuckoo_rdma;'
                'rsync -r ' + self.build_location.hostname + ':' + self.project_directory + '/*;')

    def sanity_check(self):
        for node in self.all_nodes:
            node.sanity_check_mlx5()


    def run(self):
        # print("Starting RDMA Benchmark")
        self.server.run_cmd(
            'cd rcuckoo_rdma;'
            './'+ self.memory_program_name + ' ' + 'configs/' + self.config_name + ' > memserver.out 2>&1 &')

        # print("Server is started with queue pairs", self.queue_pairs)

            # 'export MLX5_SINGLE_THREADED=1;'
        self.client.run_cmd(
            'cd rcuckoo_rdma;'
            'LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes ./' + self.client_program_name + ' ' + 'configs/' + self.config_name + ' > client.out 2>&1;'
            'cat client.out;'
        )
            # './rdma_client -a ' + server_ip + ' -p 20886 -q '+str(self.queue_pairs)+ ' -x;'

    
def run_trials(config):
    runs = []
    trials = config['trials']
    for i in tqdm(range(trials)):
        c=copy.copy(config)
        orch = Orchestrator(c)
        #prepare for the run
        orch.transfer_configs_to_nodes(config, "remote_config.json")
        orch.sanity_check()
        orch.kill()
        stats_collected = False
        try:
            orch.run()
            orch.kill()
        except Exception as e:
            print("Run ", i, " failed with exception -- we gotta fix this", e)
            exit()

        
        if not stats_collected:
            orch.validate_run()
            stats = orch.collect_stats()
        runs.append(stats)
        del orch
    return runs
