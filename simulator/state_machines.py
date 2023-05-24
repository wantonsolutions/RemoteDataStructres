# from cuckoo import *
# import cuckoo
import search
import random
import virtual_rdma as vrdma

import logging
logger = logging.getLogger('root')

class request():
    def __init__(self, request_type, key, value=None):
        self.request_type = request_type
        self.key = key
        self.value = value

    def __str__(self):
        return "Request: " + self.request_type + " Key: " + str(self.key) + " Value: " + str(self.value)

    def __repr__(self):
        return self.__str__()

def get_state_machine_name(state_machine_class_pointer):
    str_name = str(state_machine_class_pointer)
    back = str_name.split(".")[1]
    front = back.split("'")[0]
    return front

#cuckoo protocols
class state_machine:
    def clear_statistics(self):
        self.complete = False
        self.inserting = False
        self.reading = False

        #state machine statistics
        self.total_bytes = 0
        self.read_bytes = 0
        self.write_bytes = 0
        self.cas_bytes = 0
        self.total_reads = 0
        self.total_writes = 0
        self.total_cas = 0
        self.total_cas_failures = 0

        #todo add to a subclass
        #insert stats
        self.insert_path_lengths = []
        self.index_range_per_insert = []
        self.current_insert_messages = 0
        self.messages_per_insert = []
        self.completed_inserts = []
        self.completed_insert_count = 0
        self.failed_inserts = []
        self.failed_insert_count = 0
        self.insert_operation_bytes = 0
        self.insert_operation_messages = 0
        #track rtt
        self.current_insert_rtt = 0
        self.insert_rtt = []
        self.insert_rtt_count = 0

        #read stats
        self.current_read_messages = 0
        self.messages_per_read = []
        self.completed_reads = []
        self.completed_read_count = 0
        self.failed_reads = []
        self.failed_read_count = 0
        self.read_operation_bytes = 0
        self.read_operation_messages = 0
        self.current_read_rtt = 0
        self.read_rtt = []
        self.read_rtt_count = 0

    def __init__(self, config):
        self.logger = logging.getLogger("root")
        self.config = config
        self.clear_statistics()
        #spiritually these are the high level states of the state machine
        #these are considered less configurable than the self.state variable I use in the children

    def complete_read_stats(self, success, read_value):
        if success:
            self.completed_read_count += 1
            self.completed_reads.append(read_value)
        else:
            self.failed_read_count += 1
            self.failed_reads.append(read_value)
        self.messages_per_read.append(self.current_read_messages)
        self.read_rtt.append(self.current_read_rtt)
        self.read_rtt_count += self.current_read_rtt

        #clear for next time
        self.current_read_messages = 0
        self.current_read_rtt=0

    def complete_insert_stats(self, success):
        if success:
            self.completed_inserts.append(self.current_insert_value)
            self.completed_insert_count += 1
        else:
            self.failed_inserts.append(self.current_insert_value)
            self.failed_insert_count += 1
        self.messages_per_insert.append(self.current_insert_messages)
        self.insert_rtt.append(self.current_insert_rtt)
        self.insert_rtt_count += self.current_insert_rtt
        self.current_insert_rtt = 0
        self.current_insert_messages = 0


    def get_stats(self):
        stats = dict()
        stats["total_bytes"] = self.total_bytes
        stats["read_bytes"] = self.read_bytes
        stats["write_bytes"] = self.write_bytes
        stats["cas_bytes"] = self.cas_bytes
        stats["total_reads"] = self.total_reads
        stats["total_writes"] = self.total_writes
        stats["total_cas"] = self.total_cas
        stats["total_cas_failures"] = self.total_cas_failures

        #todo add to a subclass 
        stats["insert_path_lengths"] = self.insert_path_lengths
        stats["index_range_per_insert"] = self.index_range_per_insert
        stats["messages_per_insert"] = self.messages_per_insert
        stats["completed_inserts"] = self.completed_inserts
        stats["completed_insert_count"] = self.completed_insert_count
        stats["failed_inserts"] = self.failed_inserts
        stats["failed_insert_count"] = self.failed_insert_count
        stats["insert_operation_bytes"] = self.insert_operation_bytes
        stats["insert_operation_messages"] = self.insert_operation_messages
        stats["insert_rtt"] = self.insert_rtt
        stats["insert_rtt_count"] = self.insert_rtt_count


        stats["messages_per_read"] = self.messages_per_read
        stats["completed_reads"] = self.completed_reads
        stats["completed_read_count"] = self.completed_read_count
        stats["failed_reads"] = self.failed_reads
        stats["failed_read_count"] = self.failed_read_count
        stats["read_operation_bytes"] = self.read_operation_bytes
        stats["read_operation_messages"] = self.read_operation_messages
        stats["read_rtt"] = self.read_rtt
        stats["read_rtt_count"] = self.read_rtt_count
        return stats

    def update_message_stats(self, messages):

        if messages == None:
            return

        if not isinstance(messages, list):
            messages = [messages]

        for message in messages:

            size = vrdma.message_to_bytes(message)
            if self.inserting:
                self.current_insert_messages += 1
                self.insert_operation_bytes += size
                self.insert_operation_messages +=1
            elif self.reading:
                self.current_read_messages +=1
                self.read_operation_bytes += size
                self.read_operation_messages +=1

            self.total_bytes += size
            t = vrdma.message_type(message)
            payload = message.payload
            if t == "read":
                self.total_reads += 1
                self.read_bytes += size
            elif t == "read_response":
                self.read_bytes += size
            elif t == "cas" or t == "masked_cas":
                self.total_cas += 1
                self.cas_bytes += size
            elif t == "cas_response" or t == "masked_cas_response":
                self.cas_bytes += size
                if payload["function_args"]["success"] == False:
                    self.total_cas_failures += 1
            elif t == "write":
                self.total_writes += 1
                self.write_bytes += size
            elif t == "write_response":
                self.write_bytes += size
            else:
                print("Unknown message type! " + str(message))
                exit(1)

    
    def fsm(self, message = None):
        #caclulate statistics
        self.update_message_stats(message)
        #return fsm_wapper
        output_message = self.fsm_logic(message)
        if __debug__:
            if isinstance(output_message, list):
                for m in output_message:
                    self.warning("FSM: " + str(message) + " -> " + str(m))
            else:
                self.warning("FSM: " + str(message) + " -> " + str(output_message))

        self.update_message_stats(output_message)
        return output_message

    def fsm_logic(self, messages=None):
        print("state machine top level overload this")


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

workload_write_percentage={
    "ycsb-a": 50,
    "ycsb-b": 5,
    "ycsb-c": 0,
    "ycsb-w": 100,
}

class client_workload_driver():
    def __init__(self, config):
        self.set_workload(config["workload"])
        self.total_requests=config["total_requests"]
        self.client_id=config['id']
        self.num_clients=config['num_clients']
        self.deterministic=config['deterministic']

        if self.deterministic:
            self.random_factor = 1
        else:
            self.random_factor = int(random.random() * 100) + 1
        #todo add a line for having a workload passed in as a file
        self.completed_requests=0
        self.completed_puts=0
        self.completed_gets=0
        self.last_request=None

    def get_stats(self):
        stats = dict()
        stats["completed_requests"] = self.completed_requests
        stats["completed_puts"] = self.completed_puts
        stats["completed_gets"] = self.completed_gets
        stats["workload"] = self.workload
        stats["total_requests"] = self.total_requests
        stats["client_id"] = self.client_id
        stats["num_clients"] = self.num_clients
        return stats

    def record_last_request(self):
        request = self.last_request
        self.completed_requests += 1
        if request == None:
            return
        if request.request_type == "put":
            self.completed_puts += 1
        elif request.request_type == "get":
            self.completed_gets += 1
        else:
            print("Unknown request type! " + str(request))
            exit(1)

    def unique_insert(self, insert, client_id, total_clients, factor):
        return (insert * total_clients * factor) + client_id

    def unique_get(self, get, client_id, total_clients, factor):
        return (get * total_clients * factor) + client_id

    def next_put(self):
        next_value = self.unique_insert(self.completed_puts, self.client_id, self.num_clients, self.random_factor)
        req = request("put", next_value, next_value)
        self.last_request = req
        return req

    def next_get(self):
        if self.deterministic:
            index = self.completed_puts - 1
        else:
            if self.completed_puts <= 1:
                index = 0
            else:
                index = int(random.random()*1000000) % (self.completed_puts-1)

        next_value = self.unique_get(index, self.client_id, self.num_clients, self.random_factor)
        req = request("get", next_value)
        self.last_request = req
        return req

    def set_workload(self, workload):
        if not workload in workload_write_percentage:
            print("Unknown workload! " + workload)
            exit(1)
        self.workload = workload


    def gen_next_operation(self, workload):
        percentage = workload_write_percentage[workload]
        # if int(random.random() * 100) < percentage:
        if ((self.completed_requests * 1337) % 100) < percentage:
            return "put"
        else:
            return "get"

    def next(self):
        self.record_last_request()
        if self.completed_requests >= self.total_requests:
            return None
        op = self.gen_next_operation(self.workload)
        if op == "put":
            return self.next_put()
        elif op == "get":
            return self.next_get()


class client_state_machine(state_machine):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.total_inserts = config["total_inserts"]
        self.id = config["id"]
        self.table = config["table"]
        self.state="idle"


        #read state machine
        self.current_read_key = None
        self.outstanding_read_requests = 0
        self.read_values_found = 0
        self.read_values = []
        self.duplicates_found = 0

        workload_config = {
            "workload": config["workload"],
            "total_requests": self.total_inserts,
            "id": self.id,
            "num_clients": config["num_clients"],
            "deterministic": config["deterministic"],
        }
        self.workload_config=workload_config
        self.workload_driver = client_workload_driver(workload_config)

    def clear_statistics(self):
        self.state="idle"
        #read state machine
        self.current_read_key = None
        self.outstanding_read_requests = 0
        self.read_values_found = 0
        self.read_values = []
        self.duplicates_found = 0
        return super().clear_statistics()

    def set_workload(self, workload):
        self.workload_driver.set_workload(workload)
        self.config["workload"] = workload


    def begin_read(self, messages):
        self.outstanding_read_requests = len(messages)
        self.read_values_found = 0
        self.read_values = []
        self.state = "reading"
        self.reading=True
        return messages

    def read_complete(self):
        return self.outstanding_read_requests ==  0

    def read_successful(self, key):
        if self.read_values_found == 0:
            success = False
            self.info("Read Failed: " + str(key)) if __debug__ else None
        elif self.read_values_found == 1:
            success = True
            self.info("Read Complete: " + str(self.read_values)) if __debug__ else None
        elif self.read_values_found > 1:
            success = True
            self.duplicates_found = self.duplicates_found + 1
            self.info("Read Complete: " + str(self.read_values) + " Duplicate Found") if __debug__ else None
            #todo we likely need a tie breaker here
        return success


    #return true if the read is complete
    def wait_for_read_messages_fsm(self, message, key):
        if message != None and message_type(message) == "read_response":

            #unpack and check the response for a valid read
            args = unpack_read_response(message)
            fill_local_table_with_read_response(self.table, args)
            read = args["read"]

            keys_found = keys_contained_in_read_response(key, read)
            self.read_values_found = self.read_values_found + keys_found
            self.read_values.extend(get_entries_from_read(key, read))
            self.outstanding_read_requests = self.outstanding_read_requests - 1


        complete = self.read_complete()
        success = self.read_successful(key)
        return complete, success


    def general_idle_fsm(self, message):
        if message != None:
            self.critical("Idle state machine received a message" + str(message))
            return None
        #get the next operations
        req = self.workload_driver.next()
        self.info("Workload Request: " + str(req)) if __debug__ else None
        if req == None:
            self.complete=True
            self.info("Workload Complete")
            return None
        elif req.request_type == "put":

            self.current_insert_value = req.key
            self.inserting=True
            return self.put()

        elif req.request_type == "get":
            if req.key < 0:
                return None
            self.current_read_key = req.key
            self.reading=True
            return self.get()
        else:
            raise Exception("No generator for request : " + str(req.request_type))

    def __str__(self):
        return "Client " + str(self.id)

    def get_stats(self):
        stats = super().get_stats()
        stats["workload_stats"] = self.workload_driver.get_stats()
        return stats

    def get(self):
        self.critical("Get method should be overwritten")

    def put(self):
        self.critical("put method should be overwritten")



class basic_memory_state_machine(state_machine):
    def __init__(self,config):
        super().__init__(config)
        self.table = config["table"]
        self.state = "memory... state not being used"
        self.max_fill = config["max_fill"]


    def __str__(self):
        return "Memory"
    
    def fsm_logic(self, message=None):

        if self.table.get_fill_percentage() * 100 > self.max_fill:
            raise Exception("Table has reached max fill rate")
        if message == None:
            return None

        args = message.payload["function_args"]
        if message.payload["function"] == vrdma.read_table_entry:
            self.info("Read: " + "Bucket: " + str(args["bucket_id"]) + " Offset: " + str(args["bucket_offset"]) + " Size: " + str(args["size"]))  if __debug__ else None
            read = vrdma.read_table_entry(self.table, **args)
            response = Message({"function":vrdma.fill_table_with_read, "function_args":{"read":read, "bucket_id":args["bucket_id"], "bucket_offset":args["bucket_offset"], "size":args["size"]}})
            self.info("Read Response: " +  str(response.payload["function_args"]["read"])) if __debug__ else None
            return response

        if message.payload["function"] == vrdma.cas_table_entry:
            self.info("CAS: " + "Bucket: " + str(args["bucket_id"]) + " Offset: " + str(args["bucket_offset"]) + " Old: " + str(args["old"]) + " New: " + str(args["new"])) if __debug__ else None
            success, value = vrdma.cas_table_entry(self.table, **args)
            response = vrdma.Message({"function":vrdma.fill_table_with_cas, "function_args":{"bucket_id":args["bucket_id"], "bucket_offset":args["bucket_offset"], "value":value, "success":success}})

            # self.table.print_table()  if __debug__ else None

            rargs=response.payload["function_args"]
            self.info("Read Response: " +  "Success: " + str(rargs["success"]) + " Value: " + str(rargs["value"])) if __debug__ else None
            return response

        if message.payload["function"] == vrdma.masked_cas_lock_table:
            #self.info("Masked CAS in Memory: "+ str(args["lock_index"]) + " Old: " + str(args["old"]) + " New: " + str(args["new"]) + " Mask: " + str(args["mask"]))
            success, value = vrdma.masked_cas_lock_table(self.table.lock_table, **args)
            response = vrdma.Message({"function": vrdma.fill_lock_table_masked_cas, "function_args":{"lock_index":args["lock_index"], "success":success, "value": value, "mask":args["mask"]}})
            return response
            
        else:
            self.logger.warning("MEMORY: unknown message type " + str(message))