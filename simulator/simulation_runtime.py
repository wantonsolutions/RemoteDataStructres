import logging
from time import sleep
import json
import datetime
from collections import deque
import git
from tqdm import tqdm
import random

from . import log
from . import tables
from . import cuckoo
from . import rcuckoo_basic
from . import search
from . import state_machines
from . import hash
from . import race


# import log
# import tables
# import cuckoo
# import rcuckoo_basic
# import search


import logging
logger = logging.getLogger('root')

def messages_append_or_extend(messages, message):
    if message != None:
        if isinstance(message, list):
            messages.extend(message)
        else:
            messages.append(message)
    return messages

class Node:
    def __init__(self, config):
        self.logger = logging.getLogger("root")
        self.steps=0

    def log_prefix(self):
        return "{:<9}".format(str(self))

    def info(self, message):
        self.logger.info("[" + self.log_prefix() + "] " + message)

    def debug(self, message):
        self.logger.debug("[" + self.log_prefix() + "] " + message)

    def warning(self, message):
        self.logger.warning("[" + self.log_prefix() + "] " + message)

    def critical(self, message):
        self.logger.critical("[" + self.log_prefix() + "] " + message)

    def increment_step(self):
        self.steps += 1

    def get_steps(self):
        return self.steps

class Client(Node):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.client_id = config['client_id']

        self.bucket_size = config['bucket_size']
        #create the index structure
        index_func = config['index_init_function']
        index_args = config['index_init_args']

        self.debug("Index Function: " + str(index_func.__name__))
        self.debug("Index Args: " + str(index_args)+"\n")
        self.index = index_func(**index_args)

        state_machine_args = config['state_machine_args'] 
        state_machine_args['table'] = self.index
        state_machine_args['id'] = self.client_id
        state_machine_args['num_clients'] = config['num_clients']
        state_machine_args['read_threshold_bytes'] = config['read_threshold_bytes']
        state_machine_args['deterministic'] = config['deterministic']
        state_machine_args['buckets_per_lock'] = config['buckets_per_lock']
        state_machine_args['locks_per_message'] = config['locks_per_message']
        state_machine_args['search_function'] = config['search_function']
        state_machine_args['location_function'] = config['location_function']
        state_machine_args['workload'] = config['workload']
        state_machine = config['state_machine']
        self.critical(str(state_machine))
        self.state_machine = state_machine(state_machine_args)

    def __str__(self):
        return "Client: " + str(self.client_id)

    def clear_statistics(self):
        self.state_machine.clear_statistics()

    def state_machine_step(self, message=None):
        self.increment_step()
        messages = []
        if message == None:
            m = self.state_machine.fsm()
            messages = messages_append_or_extend(messages, m)
        else:
            m = self.state_machine.fsm(message)
            messages = messages_append_or_extend(messages, m)

        #for now send all of the message to memory
        for i in range(len(messages)):
            messages[i].payload["src"] = self.client_id
            messages[i].payload["dest"] = "memory"
        return messages


class Memory(Node):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.memory_id = config['memory_id']
        self.bucket_size = config['bucket_size']

        #create the index structure
        self.debug("Initializing memory Index")
        index_func = config['index_init_function']
        index_args = config['index_init_args']
        self.debug("Index Function: " + str(index_func.__name__))
        self.debug("Index Args: " + str(index_args)+"\n")
        self.index = index_func(**index_args)

        state_machine_args = config['state_machine_args']
        state_machine_args['table'] = self.index
        state_machine = config['state_machine']


        self.state_machine = state_machine(state_machine_args)

    
    def __str__(self):
        return "Memory: " + str(self.memory_id)

    def state_machine_step(self, message):
        self.increment_step()
        responses = []
        response = self.state_machine.fsm(message)
        responses = messages_append_or_extend(responses, response)
        for i in range(len(responses)):
            if(responses[i] != None):
                responses[i].payload['src'] = message.payload['dest']
                responses[i].payload['dest'] = message.payload['src']
        return responses

    def clear_statistics(self):
        self.state_machine.clear_statistics()

class Switch(Node):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.switch_id = config['switch_id']
    
    def __str__(self):
        return "Switch"

    def process_message(self, message):
        self.increment_step()
        # p = message.payload
        # dest = p["dest"]
        # src = p["src"]
        # self.info("src: "+str(src) + ", dest: " + str(dest) + ", func: " + str(p["function"].__name__))
        return message


class Simulator(Node):


    def __init__(self, config):
        self.logger = logging.getLogger('root')
        self.step=0
        self.config_set = False
        self.config = config
        self.client_list = []
        self.memory = None
        self.switch = None
        self.in_flight_messages = 0
        self.client_switch_channel=deque()
        self.switch_client_channel=deque()
        self.switch_memory_channel=deque()
        self.memory_switch_channel=deque()

    def drop_all_messages(self):    
        self.client_switch_channel=deque()
        self.switch_client_channel=deque()
        self.switch_memory_channel=deque()
        self.memory_switch_channel=deque()
    
    def __str__(self):
        return "Simulator"

    def client_event(self, client_id, message=None):
        #deliver messages to client
        client = self.client_list[client_id]
        if message != None:
            self.in_flight_messages -=1
            messages = client.state_machine_step(message)
        else:
            #generate client messages and send to switch
            messages = client.state_machine_step()

        if messages is not None:
            self.in_flight_messages += len(messages)
            self.client_switch_channel.extend(messages)


    def client_events(self):
        #generate messages from the client
        for i in range(len(self.client_list)):
            self.client_event(i)

        #send messages to the clients
        while len(self.switch_client_channel) > 0:
            sm = self.switch_client_channel.popleft()
            client_id = sm.payload["dest"]
            self.client_event(client_id, sm)


    def switch_client_events(self):
        #send a client messages to the switch
        while len(self.client_switch_channel) > 0:
            cm = self.client_switch_channel.popleft()
            sm = self.switch.process_message(cm)
            self.switch_memory_channel.append(sm)

    def switch_memory_events(self):
        #send memory message through the switch
        while len(self.memory_switch_channel) > 0:
            mm = self.memory_switch_channel.popleft()
            sm = self.switch.process_message(mm)
            self.switch_client_channel.append(sm)

    def switch_events(self):
        self.switch_client_events()
        self.switch_memory_events()

    def memory_event(self):
        #send switch messages to the memory
        while len(self.switch_memory_channel) > 0:
            sm = self.switch_memory_channel.popleft()
            self.in_flight_messages -=1
            mm = self.memory.state_machine_step(sm)
            self.in_flight_messages += len(mm)
            self.memory_switch_channel.extend(mm)

    def no_events(self):
        return (self.in_flight_messages == 0)

    def clients_complete(self):
        for i in range(len(self.client_list)):
            # if self.client_list[i].state_machine.state != "complete":
            #     return False
            if not self.client_list[i].state_machine.complete:
                return False
        return True


    #this is mostly a debugging function for a single client
    def deterministic_simulation_step(self):
        # self.warning("Step: " + str(self.step))
        self.client_events()
        self.switch_events()
        self.memory_event()
        self.switch_events()
        self.client_events()
        self.step = self.step+1

    def random_single_simulation_step(self):
        # self.warning("Step: " + str(self.step))
        value = int(random.random() * 10000)
        selection = value % 3
        clients = 0
        switch = 1
        memory = 2
        bottom = int(value / 3)
        #client events
        if selection == clients:
            #deliver or generate
            d_or_g = bottom % 2
            if d_or_g == 0:
                #deliver
                if len(self.switch_client_channel) > 0:
                    sm = self.switch_client_channel.popleft()
                    client_id = sm.payload["dest"]
                    self.client_event(client_id, sm)
            else:
                #generate
                client = int(random.random() * len(self.client_list))
                self.client_event(client)
        elif selection == switch:
            c_or_m = bottom % 2
            if c_or_m == 0:
                if len(self.client_switch_channel) > 0:
                    cm = self.client_switch_channel.popleft()
                    sm = self.switch.process_message(cm)
                    self.switch_memory_channel.append(sm)
            else:
                if len(self.memory_switch_channel) > 0:
                    mm = self.memory_switch_channel.popleft()
                    sm = self.switch.process_message(mm)
                    self.switch_client_channel.append(sm)

        elif selection == memory:
            self.memory_event()

        self.step = self.step+1

    def set_config(self):
        bucket_size=self.config['bucket_size']
        entry_size=self.config['entry_size']
        indexes=self.config['indexes']
        buckets_per_lock=self.config['buckets_per_lock']
        memory_size=entry_size * indexes
        index_init_function=tables.Table
        index_init_args={'memory_size': memory_size, 'bucket_size': bucket_size, 'buckets_per_lock': buckets_per_lock}
        self.deterministic = self.config['deterministic']

        #initialize hash function
        hash.set_factor(self.config['hash_factor'])


        #initialize clients
        for i in range(self.config['num_clients']):
            client_config = {'client_id': i}
            client_config['num_clients'] = self.config['num_clients']

            client_config['bucket_size'] = bucket_size
            client_config['index_init_function'] = tables.Table
            client_config['index_init_args'] = index_init_args

            # client_config['state_machine']=lockless_a_star_insert_only_state_machine
            client_config['state_machine']=self.config["state_machine"]
            client_config['state_machine_args']={'total_inserts': indexes * 20}
            client_config['read_threshold_bytes']=self.config['read_threshold_bytes']
            client_config['buckets_per_lock']=buckets_per_lock
            client_config['locks_per_message']=self.config['locks_per_message']
            client_config['search_function']=self.config['search_function']
            client_config['location_function']=self.config['location_function']
            client_config['workload']=self.config['workload']
            client_config['deterministic']=self.config['deterministic']

            c = Client(client_config)

            self.client_list.append(c)

        #initialize memory
        memory_config = {'memory_id': 0}
        memory_config['bucket_size'] = bucket_size
        memory_config['index_init_function'] = tables.Table
        memory_config['index_init_args'] = index_init_args

        memory_config['state_machine']=state_machines.basic_memory_state_machine
        memory_config['state_machine_args']={'max_fill': self.config['max_fill']}
        self.memory = Memory(memory_config)

        #initialize switch
        switch_config = {'switch_id': 0}
        self.switch = Switch(switch_config)
        self.config['state_machine']=state_machines.get_state_machine_name(self.config['state_machine'])

        print(self.config)
        self.config_set = True


    def run(self):

        if self.config_set == False:
            self.set_config()
        #run simulation
        for i in range(self.config['num_steps']):
            if self.deterministic:
                self.deterministic_simulation_step()
            else:
                self.random_single_simulation_step()

            if self.no_events() == True and self.clients_complete() == True:
                break

    def validate_run(self):
        #check that there are no duplicates in the table
        if self.memory.index.contains_duplicates():
            duplicates = self.memory.index.get_duplicates()
            self.critical("Memory contains duplicates")
            self.critical(str(duplicates))
            return False

        #check to make sure that all of the values inserted are actually in the hash table
        for i in range(len(self.client_list)):
            client = self.client_list[i]
            for j in range(len(client.state_machine.completed_inserts)):
                value = client.state_machine.completed_inserts[j]
                if self.memory.index.contains(value) == False:
                    self.critical("Memory does not contain value: " + str(value) + "inserted by " + str(client.client_id))
                    return False

    def clear_statistics(self):
        for i in range(len(self.client_list)):
            self.client_list[i].clear_statistics()
        self.memory.clear_statistics()
        self.memory.index.unlock_all()

        self.drop_all_messages()

    def set_workload(self, workload):
        self.config['workload'] = workload
        for i in range(len(self.client_list)):
            self.client_list[i].state_machine.set_workload(workload)


    def set_max_fill(self, fill):
        self.memory.state_machine.max_fill = fill
        self.config['max_fill'] = fill
    
    def reset_step_max(self, max_steps):
        self.config['num_steps'] = max_steps
        self.step=0

    def collect_stats(self):
        statistics = dict()

        #simulator stats
        statistics['config'] = dict()
        statistics['config'] = self.config
        statistics['simulator'] = dict()
        statistics['simulator']['steps'] = self.step

        #memory stats
        statistics['memory'] = dict()
        fill = self.memory.index.get_fill_percentage()
        statistics['memory']['fill'] = fill
        statistics['memory']['steps'] = self.memory.get_steps()
        self.memory.index.print_table()

        statistics['hash'] = dict()
        statistics['hash']['factor'] = hash.get_factor()

        #client stats
        statistics['clients'] = []


        for i in range(len(self.client_list)):
            client = self.client_list[i]
            client_stats = dict()
            client_stats['client_id'] = client.client_id
            client_stats['stats'] = client.state_machine.get_stats()
            client_stats['steps'] = client.get_steps()
            statistics['clients'].append(client_stats)

        #TODO start here after lunch, we are calculating the path lenght and number of messages
        #assosiated with each of the inserts, as well as the range on the table.

        return statistics

     
def default_config():
    config = dict()
    config['num_clients']=1
    config['num_steps']=1000000
    config['trials']=1
    config['deterministic']=False

    #table settings
    config['bucket_size']=4
    config['entry_size']=8
    config['indexes']=32
    config['read_threshold_bytes']=config['bucket_size'] * config['entry_size']

    #memory settings
    config["max_fill"]=100.0

    #default is global locking
    config['buckets_per_lock']=1
    config['locks_per_message']=64
    config['workload']="ycsb-w"
    config['hash_factor']=hash.get_factor()
    config['state_machine']=rcuckoo_basic.rcuckoo_basic
    config['search_function']="a_star"
    config['location_function']="dependent"

    config['date']=datetime.datetime.now().strftime("%Y-%m-%d")
    config['commit']=git.Repo(search_parent_directories=True).head.object.hexsha


    return config

def run_trials(config):
    runs = []
    trials = config['trials']
    for i in tqdm(range(trials)):
        c=config.copy()
        sim = Simulator(c)
        sim.run()
        try:
            sim.run()
        except Exception as e:
            print(e)
            stats = sim.collect_stats()
            sim.validate_run()
        sim.validate_run()
        stats = sim.collect_stats()
        runs.append(stats)
    return runs


def fill_then_run(config, fill_to, max_fill, run_steps=1000000000):
    c=config.copy()
    c['max_fill'] = fill_to
    original_workload = c['workload']
    c['workload'] = 'ycsb-w'
    sim = Simulator(c)
    able_to_fill=False
    try:
        sim.run()
    except Exception as e:
        #we trigger a fill rate exit based on this. so if we don't have this happen we should exit
        able_to_fill=True

    if not able_to_fill:
        print("unable to fill")
        return exit(0)

    print("Table filled to ", fill_to, " starting measurement")
    sim.clear_statistics()
    sim.reset_step_max(run_steps)
    sim.set_workload(original_workload)
    sim.set_max_fill(max_fill)

    try:
        sim.run()
    except Exception as e:
        print(e)

    stats = sim.collect_stats()
    return stats

def fill_then_run_trials(config, fill_to, max_fill, max_steps=1000000000):
    runs = []
    trials = config['trials']
    for i in tqdm(range(trials)):
        runs.append(fill_then_run(config, fill_to, max_fill, max_steps))
    return runs



def main():

    runs = []
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")
    config = default_config()
    config['indexes'] = 16
    config['num_clients'] = 1
    config['num_steps'] = 5000000
    config['read_threshold_bytes'] = 256
    config["buckets_per_lock"] = 1
    config["locks_per_message"] = 64
    config["trials"] = 1
    config["state_machine"]=cuckoo.rcuckoo
    # config["state_machine"]=race.race
    config['workload']='ycsb-w'
    log.set_debug()



    # simulator = Simulator(config)
    # simulator.run()
    # if not simulator.validate_run():
    #     logger.error("Simulation failed check the logs")
    # simulator.collect_stats()
    runs.append(run_trials(config))

if __name__ == "__main__":
    main()








