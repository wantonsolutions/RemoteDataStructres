import warnings
from cryptography.utils import CryptographyDeprecationWarning
with warnings.catch_warnings():
    warnings.filterwarnings('ignore', category=CryptographyDeprecationWarning)
    import paramiko

# import asyncio use this eventually when you want to parallelize the builds


class rcuckoo_ssh_wrapper:
    def __init__(self, username,hostname):
        print("setting up ssh connection to", hostname)
        self.username = username
        self.hostname = hostname
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #ensure that we don't need a password to connect
        self.ssh.connect(self.hostname, username=self.username)
        self.verbose = False

    def set_verbose(self, verbose):
        self.verbose = verbose

    def run_cmd(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)

        error_code = stdout.channel.recv_exit_status()
        if (self.verbose) or (error_code != 0):
            print("Error code is not zero: ", error_code)
            print("ERROR:!")
            print("STDOUT:")
            result =stdout.read()
            print(result.decode("utf-8"))
            print("STDERR:")
            result = stderr.read()
            print(result.decode("utf-8"))

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
        print("Sanity Check: mlx5 ip is", ip)

    def __del__(self):
        self.ssh.close()

    def pull(self):
        self.run_cmd('git pull')

class orchestrator:

    memory_program_name = "rdma_memory_server"
    client_program_name = "./test/test_cuckoo"
    def __init__(self, config):
        self.server = rcuckoo_ssh_wrapper('ssgrant', 'yak-00.sysnet.ucsd.edu')
        self.client = rcuckoo_ssh_wrapper('ssgrant', 'yak-01.sysnet.ucsd.edu')

        #pointer to all of the nodes
        self.all_nodes = [self.client, self.server]

        self.build_location = self.client
        self.project_directory = '/usr/local/home/ssgrant/RemoteDataStructres/rcuckoo_rdma'
        self.sync_dependents = [self.server]

        self.queue_pairs = 24

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


    def run_rdma_benchmark(self):
        print("Starting RDMA Benchmark")
        self.server.run_cmd(
            'cd rcuckoo_rdma;'
            './'+ self.memory_program_name + ' > memserver.out 2>&1 &')

        print("Server is started with queue pairs", self.queue_pairs)

        server_ip = self.server.get_mlx5_ip()
        self.client.run_cmd(
            'cd rcuckoo_rdma;'
            '' + self.client_program_name + ''
            # './rdma_client -a ' + server_ip + ' -p 20886 -q '+str(self.queue_pairs)+ ' -x;'
        )
