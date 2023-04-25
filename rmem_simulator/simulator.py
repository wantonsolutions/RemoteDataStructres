import log
import logging
from cuckoo import *
from hash import *
from time import sleep
import json
import datetime
from collections import deque
import git

class Node:
    def __init__(self, config):
        self.logger = logging.getLogger("root")

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
        state_machine_args['buckets_per_lock'] = config['buckets_per_lock']
        state_machine_args['locks_per_message'] = config['locks_per_message']
        state_machine = config['state_machine']
        self.critical(str(state_machine))
        self.state_machine = state_machine(state_machine_args)

    def __str__(self):
        return "Client: " + str(self.client_id)


    def state_machine_step(self, message=None):
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
        responses = []
        response = self.state_machine.fsm(message)
        responses = messages_append_or_extend(responses, response)
        for i in range(len(responses)):
            if(responses[i] != None):
                responses[i].payload['src'] = message.payload['dest']
                responses[i].payload['dest'] = message.payload['src']
        return responses

class Switch(Node):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.switch_id = config['switch_id']
    
    def __str__(self):
        return "Switch"

    def process_message(self, message):
        # p = message.payload
        # dest = p["dest"]
        # src = p["src"]
        # self.info("src: "+str(src) + ", dest: " + str(dest) + ", func: " + str(p["function"].__name__))
        return message


class Simulator(Node):
    def __init__(self, config):
        self.logger = logging.getLogger('root')
        self.step=0
        self.config = config
        self.client_list = []
        self.memory = None
        self.switch = None
        self.in_flight_messages = 0
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
            sm = self.switch_client_channel.pop()
            client_id = sm.payload["dest"]
            self.client_event(client_id, sm)


    def switch_client_events(self):
        #send a client messages to the switch
        while len(self.client_switch_channel) > 0:
            cm = self.client_switch_channel.pop()
            sm = self.switch.process_message(cm)
            self.switch_memory_channel.append(sm)

    def switch_memory_events(self):
        #send memory message through the switch
        while len(self.memory_switch_channel) > 0:
            mm = self.memory_switch_channel.pop()
            sm = self.switch.process_message(mm)
            self.switch_client_channel.append(sm)

    def switch_events(self):
        self.switch_client_events()
        self.switch_memory_events()

    def memory_event(self):
        #send switch messages to the memory
        while len(self.switch_memory_channel) > 0:
            sm = self.switch_memory_channel.pop()
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
        top = value % 3
        bottom = int(value / 3)
        #client events
        if top == 0:
            #deliver or generate
            d_or_g = bottom % 2
            if d_or_g == 0:
                #deliver
                if len(self.switch_client_channel) > 0:
                    sm = self.switch_client_channel.pop()
                    client_id = sm.payload["dest"]
                    self.client_event(client_id, sm)
            else:
                #generate
                client = int(random.random() * len(self.client_list))
                self.client_event(client)
        elif top == 1:
            c_or_m = bottom % 2
            if c_or_m == 0:
                if len(self.client_switch_channel) > 0:
                    cm = self.client_switch_channel.pop()
                    sm = self.switch.process_message(cm)
                    self.switch_memory_channel.append(sm)
            else:
                if len(self.memory_switch_channel) > 0:
                    mm = self.memory_switch_channel.pop()
                    sm = self.switch.process_message(mm)
                    self.switch_client_channel.append(sm)

        elif top == 2:
            self.memory_event()

        self.step = self.step+1


    def run(self):

        bucket_size=self.config['bucket_size']
        entry_size=self.config['entry_size']
        indexes=self.config['indexes']
        buckets_per_lock=self.config['buckets_per_lock']
        memory_size=entry_size * indexes
        index_init_function=Table
        index_init_args={'memory_size': memory_size, 'bucket_size': bucket_size, 'buckets_per_lock': buckets_per_lock}

        #initialize hash function
        hash.set_factor(self.config['hash_factor'])


        #initialize clients
        for i in range(self.config['num_clients']):
            client_config = {'client_id': i}
            client_config['num_clients'] = self.config['num_clients']

            client_config['bucket_size'] = bucket_size
            client_config['index_init_function'] = Table
            client_config['index_init_args'] = index_init_args

            # client_config['state_machine']=lockless_a_star_insert_only_state_machine
            client_config['state_machine']=self.config["state_machine"]
            client_config['state_machine_args']={'total_inserts': indexes * 20}
            client_config['read_threshold_bytes']=self.config['read_threshold_bytes']
            client_config['buckets_per_lock']=buckets_per_lock
            client_config['locks_per_message']=self.config['locks_per_message']

            c = Client(client_config)

            self.client_list.append(c)

        #initialize memory
        memory_config = {'memory_id': 0}
        memory_config['bucket_size'] = bucket_size
        memory_config['index_init_function'] = Table
        memory_config['index_init_args'] = index_init_args

        memory_config['state_machine']=basic_memory_state_machine
        memory_config['state_machine_args']={}
        self.memory = Memory(memory_config)

        #initialize switch
        switch_config = {'switch_id': 0}
        self.switch = Switch(switch_config)


        self.config['state_machine']=get_state_machine_name(self.config['state_machine'])

        #run simulation
        for i in range(self.config['num_steps']):
            # self.deterministic_simulation_step()
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

    def collect_stats(self):
        statistics = dict()

        #simulator stats
        statistics['config'] = dict()
        statistics['config'] = self.config

        #memory stats
        statistics['memory'] = dict()
        fill = self.memory.index.get_fill_percentage()
        statistics['memory']['fill'] = fill
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
            statistics['clients'].append(client_stats)

        #TODO start here after lunch, we are calculating the path lenght and number of messages
        #assosiated with each of the inserts, as well as the range on the table.

        return statistics

     
def default_config():
    config = dict()
    config['num_clients']=1
    config['num_steps']=1000000
    config['trials']=1

    #table settings
    config['bucket_size']=4
    config['entry_size']=8
    config['indexes']=32
    config['read_threshold_bytes']=config['bucket_size'] * config['entry_size']

    #default is global locking
    config['buckets_per_lock']=1
    config['locks_per_message']=1
    config['hash_factor']=hash.DEFAULT_FACTOR
    config['state_machine']=rcuckoo

    config['date']=datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    config['commit']=git.Repo(search_parent_directories=True).head.object.hexsha


    return config


def main():
    #test_hashes()
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")
    config = default_config()
    simulator = Simulator(config)
    simulator.run()
    if not simulator.validate_run():
        logger.error("Simulation failed check the logs")
    simulator.collect_stats()

if __name__ == "__main__":
    main()








