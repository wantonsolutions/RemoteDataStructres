class Client:
    def __init__(self, config):
        self.config = config
        self.client_id = config['client_id']
    def __str__(self):
        return "Client: " + str(self.client_id)

    def process_messages(self, inbound_messages):
        for message in inbound_messages:
            print("Client " + str(self.client_id) + " received message: " + str(message))

    def generate_messages(self):
        outbound_messages = []
        message = "ping"
        outbound_messages.append(message)
        return outbound_messages

class Memory:
    def __int__(self, config):
        self.config = config
        self.memory_id = config['memory_id']
    
    def __str__(self):
        return "Memory: " + str(self.memory_id)

    def process_messages(self, inbound_messages):
        outbound_messages = []
        counter = 0
        for message in inbound_messages:
            print(message)
            om = "message " + str(counter) + "processed"
            counter += 1
            outbound_messages.append(om)
        return outbound_messages

class Switch:
    def __init__(self, config):
        self.config = config
        self.switch_id = config['switch_id']
    
    def __str__(self):
        return "Switch: " + str(self.switch_id)

    def process_messages(self, inbound_messages):
        outbound_messages = []
        counter = 0
        for message in inbound_messages:
            print(message)
            om = "message " + str(counter) + "processed"
            counter += 1
            outbound_messages.append(om)
        return outbound_messages

class Message:
    def __init__(self, config):
        self.payload = dict()

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

    def client_egress_events(self):
        #generate client messages
        for c in self.client_list:
            messages = c.generate_messages()
            self.client_switch_channel.append(messages)

    def switch_client_events(self):
        #send a client messages to the switch
        cm = self.client_switch_channel.pop()
        sm = self.switch.process_messages(cm)
        self.switch_memory_channel.append(sm)

    def memory_events(self):
        #send switch messages to the memory
        sm = self.switch_memory_channel.pop()
        mm = self.memory.process_messages(mm)
        self.memory_switch_channel.append(mm)

    def switch_memory_events(self):
        #send memory message through the switch
        mm = self.memory_switch_channel.pop()
        sm = self.switch.process_messages(sm)
        self.switch_client_channel.append(sm)

    def client_ingress_events(self):
        #send switch messages to the client
        sm = self.switch_client_channel.pop()
        message_recipient = sm.payload["client_id"]
        self.client_list[message_recipient].process_messages(sm)


    def simulation_step(self):

        self.client_egress_events()
        self.switch_client_events()
        self.memory_events()
        self.switch_memory_events()
        self.client_ingress_events()










