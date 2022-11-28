from cuckoo import *
from hash import *


class Client:
    def __init__(self, config):
        self.config = config
        self.client_id = config['client_id']

        self.bucket_size = config['bucket_size']
        #create the index structure
        print(self.log_str() +  "Init Index")
        index_func = config['index_init_function']
        index_args = config['index_init_args']

        print("Index Function: " + str(index_func.__name__))
        print("Index Args: " + str(index_args)+"\n")
        self.index = index_func(**index_args)

        state_machine_args = config['state_machine_init_args'] 
        state_machine_args['table'] = self.index
        state_machine_init = config['state_machine_init']
        self.state_machine = state_machine_init(state_machine_args)

    def __str__(self):
        return "Client: " + str(self.client_id)

    def log_str(self):
        return "["+ str(self) + "] "

    def state_machine_step(self, message=None):
        print(self.log_str() + "state machine step")
        messages = []
        if message == None:
            print("no message for client " + str(self.client_id))
            m = self.state_machine.fsm()
            messages = messages_append_or_extend(messages, m)
        else:
            print("Client " + str(self.client_id) + " received message: " + str(message) + "\n")
            m = self.state_machine.fsm(message)
            messages = messages_append_or_extend(messages, m)

        #for now send all of the message to memory
        for i in range(len(messages)):
            messages[i].payload["src"] = self.client_id
            messages[i].payload["dest"] = "memory"
        return messages


class Memory:
    def __init__(self, config):
        self.config = config
        self.memory_id = config['memory_id']
        self.bucket_size = config['bucket_size']

        #create the index structure
        print("Initializing memory Index")
        index_func = config['index_init_function']
        index_args = config['index_init_args']
        print("Index Function: " + str(index_func.__name__))
        print("Index Args: " + str(index_args)+"\n")
        self.index = index_func(**index_args)

        state_machine_args = config['state_machine_init_args']
        state_machine_args['table'] = self.index
        state_machine_init = config['state_machine_init']
        self.state_machine = state_machine_init(state_machine_args)

    
    def __str__(self):
        return "Memory: " + str(self.memory_id)

    def state_machine_step(self, message):
        print("Memory: " + str(message) + "\n")
        responses = []
        response = self.state_machine.fsm(message)
        responses = messages_append_or_extend(responses, response)
        print("responses")
        print(responses)
        for i in range(len(responses)):
            if(responses[i] != None):
                responses[i].payload['src'] = message.payload['dest']
                responses[i].payload['dest'] = message.payload['src']
                print("Turning around message " + str(responses[i]))
        return responses

class Switch:
    def __init__(self, config):
        self.config = config
        self.switch_id = config['switch_id']
    
    def __str__(self):
        return "Switch: " + str(self.switch_id)

    def process_message(self, message):
        print("Switch: " + str(message) + "\n")
        return message


class Simulator:
    def __init__(self, config):
        self.step=0
        self.config = config
        self.client_list = []
        self.memory = None
        self.switch = None
        self.client_switch_channel=[]
        self.switch_client_channel=[]
        self.switch_memory_channel=[]
        self.memory_switch_channel=[]
    
    
    def __str__(self):
        return "Simulator"

    def client_event(self, client_id, message=None):
        #deliver messages to client
        client = self.client_list[client_id]
        if message != None:
            messages = client.state_machine_step(message)
        else:
            #generate client messages and send to switch
            messages = client.state_machine_step()

        if messages is not None:
            self.client_switch_channel.extend(messages)


    def client_events(self):
        #generate messages from the client
        for i in range(len(self.client_list)):
            self.client_event(i)

        #send messages to the clients
        while len(self.switch_client_channel) > 0:
            sm = self.switch_client_channel.pop()
            print(sm)
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
            mm = self.memory.state_machine_step(sm)
            self.memory_switch_channel.extend(mm)


    #this is mostly a debugging function for a single client
    def deterministic_simulation_step(self):
        print("Step: " + str(self.step))
        self.client_events()
        self.switch_events()
        self.memory_event()
        self.switch_events()
        self.client_events()
        self.step = self.step+1

    def run(self):

        bucket_size=8
        memory_size=1024
        index_init_function=generate_bucket_cuckoo_hash_index
        index_init_args={'memory_size': memory_size, 'bucket_size': bucket_size}


        #initialize clients
        for i in range(self.config['num_clients']):
            client_config = {'client_id': i}

            client_config['bucket_size'] = bucket_size
            client_config['index_init_function'] = index_init_function
            client_config['index_init_args'] = index_init_args

            client_config['state_machine_init']=basic_insert_state_machine
            client_config['state_machine_init_args']={'total_inserts': 1}

            c = Client(client_config)

            self.client_list.append(c)

        #initialize memory
        memory_config = {'memory_id': 0}
        memory_config['bucket_size'] = bucket_size
        memory_config['index_init_function'] = index_init_function
        memory_config['index_init_args'] = index_init_args

        memory_config['state_machine_init']=basic_memory_state_machine
        memory_config['state_machine_init_args']={}
        self.memory = Memory(memory_config)

        #initialize switch
        switch_config = {'switch_id': 0}
        self.switch = Switch(switch_config)

        #run simulation
        for i in range(self.config['num_steps']):
            self.deterministic_simulation_step()


def test_hashes():
    for value in range(1000):
        print("value: " + str(value) + " hash: " + str(hash_locations(value, 1024)))
    print("exiting after test")
    exit(0)


def main():
    #test_hashes()
    config = {'num_clients': 1, 'num_steps': 1}
    simulator = Simulator(config)
    simulator.run()

if __name__ == "__main__":
    main()








