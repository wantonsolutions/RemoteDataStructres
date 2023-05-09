import sys
import os

def import_rmem_simulator():
    home_directory = os.path.expanduser( '~' )
    sys.path.insert(1, home_directory + '/RemoteDataStructres/rmem_simulator')

def get_config():
    import_rmem_simulator()
    import simulator as sim
    config = sim.default_config()
    config['description'] = "--"
    config['name'] = "--"
    return config
