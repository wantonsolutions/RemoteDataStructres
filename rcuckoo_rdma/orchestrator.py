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

    def __del__(self):
        self.ssh.close()

    def pull(self):
        self.run_cmd('git pull')

class orchestrator:
    def __init__(self):
        self.client = rcuckoo_ssh_wrapper('ssgrant', 'yak-00.sysnet.ucsd.edu')
        self.server = rcuckoo_ssh_wrapper('ssgrant', 'yak-01.sysnet.ucsd.edu')

        #pointer to all of the nodes
        self.all_nodes = [self.client, self.server]

        self.build_location = self.client
        self.project_directory = '/usr/local/home/ssgrant/RemoteDataStructres/rcuckoo_rdma'
        self.sync_dependents = [self.server]

    def set_verbose(self, verbose):
        for node in self.all_nodes:
            node.set_verbose(verbose)

    def kill(self):
        self.client.run_cmd('killall rcuckoo_rdma')
        self.server.run_cmd('killall rcuckoo_rdma')


    def build(self, clean=False):
        print("Starting Build on", self.build_location.hostname)

        print("Pulling from git")
        self.build_location.pull()

        if clean:
            print("Cleaning First on", self.build_location.hostname)
            self.build_location.run_cmd(
                'cd ' + self.project_directory + ';'
                'make clean;')

        threads = 40
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



orch = orchestrator()
orch.build(clean=True)
orch.sync()