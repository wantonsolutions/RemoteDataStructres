#this file is the entry point for RDMA cuckoo, and other experiments
# from datetime import date

# print("Hello Rcuckoo!", date.today())

import chash as cpp_hash
import os
import sys

import simulator.hash as py_hash

# def import_rmem_simulator():
#     # home_directory = os.path.expanduser( '~' )
#     sys.path.insert(1, '/usr/local/home/ssgrant/RemoteDataStructres/')

# import_rmem_simulator()
# import rmem_simulator.hash as py_hash


import time
start_time = time.time()
cpp_hash.ten_k_hashes()
t1 = time.time() - start_time
print("--- cpp_hash %s seconds ---" % (t1))

start_time = time.time()
py_hash.ten_k_hashes()
t2 = time.time() - start_time
print("--- py_hash %s seconds ---" % (t2))

print("speed up factor: ", t2/t1)

