from cuckoo import *

class Message:
    def __init__(self, config):
        self.payload = dict()
        for key, value in config.items():
            self.payload[key] = value
    
    def __str__(self):
        v = ""
        for key, value in self.payload.items():
            v += str(key) + ":" + str(value) + "\n"
        return v[:len(v)-1]

class Client:
    def __init__(self, config):
        self.config = config
        self.client_id = config['client_id']

    def __str__(self):
        return "Client: " + str(self.client_id)

    def process_message(self, message):
        print("Client " + str(self.client_id) + " received message: " + str(message) + "\n")

    def generate_message(self):
        type = "ping"
        sender = self.client_id
        dest = "memory"
        message = Message({'type': type, 'src': sender, 'dest': dest})
        print("Client " + str(self.client_id) + " generated message: " + str(message) + "\n")
        return message


class Memory:
    def __init__(self, config):
        self.config = config
        self.memory_id = config['memory_id']
        self.bucket_size = config['bucket_size']

        #create the index structure
        print("Initializing memory Index")
        index_func = config['memory_index_function']
        index_args = config['memory_index_args']
        print("Index Function: " + str(index_func.__name__))
        print("Index Args: " + str(index_args)+"\n")
        self.index = index_func(**index_args)        

    
    def __str__(self):
        return "Memory: " + str(self.memory_id)

    def process_message(self, message):
        print("Memory: " + str(message) + "\n")
        #turn message around with ping
        tmp = message.payload['src']
        message.payload['src'] = message.payload['dest']
        message.payload['dest'] = tmp
        message.payload['type'] = "pong"
        return message

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

    def client_egress_event(self, client_id):
        #generate client messages
        c = self.client_list[client_id]
        messages = c.generate_message()
        self.client_switch_channel.append(messages)

    def switch_client_event(self):
        #send a client messages to the switch
        cm = self.client_switch_channel.pop()
        sm = self.switch.process_message(cm)
        self.switch_memory_channel.append(sm)

    def memory_event(self):
        #send switch messages to the memory
        sm = self.switch_memory_channel.pop()
        mm = self.memory.process_message(sm)
        self.memory_switch_channel.append(mm)

    def switch_memory_event(self):
        #send memory message through the switch
        mm = self.memory_switch_channel.pop()
        sm = self.switch.process_message(mm)
        self.switch_client_channel.append(sm)

    def client_ingress_event(self):
        #send switch messages to the client
        sm = self.switch_client_channel.pop()
        message_recipient = sm.payload["dest"]
        self.client_list[message_recipient].process_message(sm)


    def deterministic_simulation_step(self):
        self.client_egress_event(0)
        self.switch_client_event()
        self.memory_event()
        self.switch_memory_event()
        self.client_ingress_event()

    def run(self):

        bucket_size=8
        memory_size=1024
        index_gen_function=generate_bucket_cuckoo_hash_index
        index_gen_args={'memory_size': memory_size, 'bucket_size': bucket_size}


        #initialize clients
        for i in range(self.config['num_clients']):
            client_config = {'client_id': i}

            client_config['bucket_size'] = bucket_size
            client_config['client_index_function'] = index_gen_function
            client_config['client_index_args'] = index_gen_args
            c = Client(client_config)

            self.client_list.append(c)

        #initialize memory
        memory_config = {'memory_id': 0}
        memory_config['bucket_size'] = bucket_size
        memory_config['memory_index_function'] = index_gen_function
        memory_config['memory_index_args'] = index_gen_args

        self.memory = Memory(memory_config)

        #initialize switch
        switch_config = {'switch_id': 0}
        self.switch = Switch(switch_config)

        #run simulation
        for i in range(self.config['num_steps']):
            self.deterministic_simulation_step()


def main():
    config = {'num_clients': 1, 'num_steps': 1}
    simulator = Simulator(config)
    simulator.run()

if __name__ == "__main__":
    main()








