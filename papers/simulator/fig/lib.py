import sys
import os

def import_rmem_simulator():
    home_directory = os.path.expanduser( '~' )
    sys.path.insert(1, home_directory + '/RemoteDataStructres/rmem_simulator')
