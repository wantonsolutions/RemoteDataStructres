# import hash
from tqdm import tqdm
import json

from . import state_machines
from . import virtual_rdma as vrdma

import logging
logger = logging.getLogger('root')

CAS_SIZE = 64

#########A star search

#an entry is a physical entry in the hash table index.
class entry:
    def __init__(self, key):
        self.key = key
    def __str__(self):
        return("Key: " + str(self.key))

class rcuckoo(state_machines.client_state_machine):
    def __init__(self, config):
        super().__init__(config)
        # self.table = config["table"]

        index_func = config['index_init_function']
        index_args = config['index_init_args']
        self.debug("Table Function: " + str(index_func.__name__))
        self.debug("Table Args: " + str(index_args)+"\n")
        self.table = index_func(**index_args)

        #inserting and locking
        # self.current_insert_value = None
        # self.search_path = []
        # self.search_path_index = 0
        self.clear_statistics()

        self.buckets_per_lock = config['buckets_per_lock']
        self.locks_per_message = config['locks_per_message']

        self.read_threshold_bytes = config['read_threshold_bytes']
        assert self.read_threshold_bytes >= self.table.row_size_bytes(), "read threshold is smaller than a single row in the table (this is not allowed)"
        # self.locks_held = []
        # self.current_locking_messages = []
        # self.current_lock_read_messages = []
        # self.locking_message_index = 0
        self.search_module = config['search_module']
        self.hash_module = config['hash_module']

        self.set_search_function(config)
        self.set_location_function(config)

    
    def clear_statistics(self):
        super().clear_statistics()
        self.current_insert_value = None
        self.search_path = []
        self.search_path_index = 0
        self.locks_held = []
        self.current_locking_messages = []
        self.current_lock_read_messages = []
        self.locking_message_index = 0

    def set_search_function(self, config):
        search_function = config['search_function']
        if search_function == "a_star":
            self.table_search_function = self.a_star_insert_self
        elif search_function == "random":
            self.table_search_function = self.random_insert_self
        else:
            raise Exception("unknown search function")

    def set_location_function(self, config):
        location_function = config['location_function']
        if location_function == "dependent":
            self.location_function = self.hash_module.rcuckoo_hash_locations
        elif location_function == "independent":
            self.location_function = self.hash_module.rcuckoo_hash_locations_independent
        else:
            raise Exception("unknown location function")

    def get(self):
        self.current_read_rtt +=1
        messages = vrdma.read_threshold_message(self.location_function, self.current_read_key, self.read_threshold_bytes, self.table.get_row_count(), self.table.row_size_bytes())
        return self.begin_read(messages)

    def put(self):
        self.current_insert_rtt += 1
        return self.search()

    def all_locks_aquired(self):
        return self.locking_message_index == len(self.current_locking_messages) and len(self.locks_held) > 0

    def all_locks_released(self):
        return len(self.locks_held) == 0

    def get_current_locking_message(self):
        return self.current_locking_messages[self.locking_message_index]

    def get_current_locking_message_with_covering_read(self):
        lock_message = self.current_locking_messages[self.locking_message_index]
        read_message = vrdma.get_covering_read_from_lock_message(lock_message, self.buckets_per_lock, self.table.row_size_bytes())
        self.outstanding_read_requests += 1
        return [lock_message, read_message]

    def receieve_successful_locking_message(self, message):
        lock_indexes = vrdma.lock_message_to_lock_indexes(message)
        self.info("aquired locks: " + str(lock_indexes)) if __debug__ else None
        self.locks_held.extend(lock_indexes)
        #make unique
        self.locks_held = list(set(self.locks_held))
        self.locking_message_index = self.locking_message_index + 1

    def receive_successful_unlocking_message(self, message):
        unlocked_locks = vrdma.lock_message_to_lock_indexes(message)
        self.info("released locks: " + str(unlocked_locks)) if __debug__ else None
        for lock in unlocked_locks:
            self.locks_held.remove(lock)
        self.locking_message_index = self.locking_message_index + 1


    def aquire_locks(self):
        #get the unique set of buckets and remove -1 (the root) from the search path
        self.locking_message_index = 0

        buckets = self.search_module.search_path_to_buckets(self.search_path)
        self.info("gather locks for buckets: " + str(buckets)) if __debug__ else None
        lock_messages = vrdma.get_lock_messages(buckets, self.buckets_per_lock, self.locks_per_message)
        self.current_locking_messages = vrdma.masked_cas_lock_table_messages(lock_messages)
        return self.get_current_locking_message_with_covering_read()

    def release_locks_batched(self):
        self.locking_message_index = 0
        buckets = vrdma.lock_indexes_to_buckets(self.locks_held, self.buckets_per_lock)
        buckets.sort(reverse=True)
        unlock_messages = vrdma.get_unlock_messages(buckets, self.buckets_per_lock, self.locks_per_message)
        self.current_locking_messages = vrdma.masked_cas_lock_table_messages(unlock_messages)
        return self.current_locking_messages

    def aquire_locks_with_reads_fsm(self, message):
        if vrdma.message_type(message) == "masked_cas_response":
            if message.payload["function_args"]["success"] == True:
                self.receieve_successful_locking_message(message)
            #1) retransmit if we did not make process
            #2) or just select the next message
            if not self.all_locks_aquired():
                self.current_insert_rtt += 1
                return self.get_current_locking_message_with_covering_read()

        if vrdma.message_type(message) == "read_response":
            #unpack and check the response for a valid read
            args = vrdma.unpack_read_response(message)
            vrdma.fill_local_table_with_read_response(self.table, args)
            self.outstanding_read_requests = self.outstanding_read_requests - 1
            #enter the critical section if we have all of the locks
            #we also want to have all of the reads completed (for now)
            if self.all_locks_aquired() and self.read_complete():
                return self.begin_insert()
        return None

    def release_locks_fsm(self, message):
        if vrdma.message_type(message) == "masked_cas_response":
            if message.payload["function_args"]["success"] == False:
                self.critical("What the fuck is happening I failed to release a lock")
                self.critical(str(message))
                raise Exception("Failed to unlock after aquiring lock")
            
            #successful response
            self.receive_successful_unlocking_message(message)
            if self.all_locks_released():
                if self.state == "inserting":
                    return self.complete_insert()
                if self.state == "release_locks_try_again":
                    return self.search()
            return None

    def a_star_insert_self(self, limit_to_buckets=None):
        return self.search_module.bucket_cuckoo_a_star_insert(self.table, self.location_function, self.current_insert_value, limit_to_buckets)

    def random_insert_self(self, limit_to_buckets=None):
        return self.search_module.bucket_cuckoo_random_insert(self.table, self.location_function, self.current_insert_value, limit_to_buckets)

    def table_search_function(self, limit_to_buckets=None):
        # return self.a_star_insert_self(limit_to_buckets)
        print("This function should be overloaded with a search function for example", str(self.a_star_insert_self))
        return
        raise Exception("This function should be overloaded with a search function for example", str(self.a_star_insert_self))

    def search(self, message=None):
        assert message == None, "there should be no message passed to search"

        self.search_path=self.table_search_function()
        if len(self.search_path) == 0:
            self.info("Search Failed: " + str(self.current_insert_value) + "| unable to continue, client " + str(self.id) + " is done")
            self.complete=True
            self.state = "idle"
            return None
        self.info("Current insert value " + str(self.current_insert_value)) if __debug__ else None
        self.info("Successful local search for [key: " + str(self.current_insert_value) + "] [path: " +self.search_module.path_to_string(self.search_path) + "]") if __debug__ else None
        # self.info("Printing table in function search() Table:") if __debug__ else None
        # self.table.print_table() if __debug__ else None
        self.state="aquire_locks"
        return self.aquire_locks()


    def begin_insert(self):
        self.state = "inserting"
        #todo there are going to be cases where this fails because

        search_buckets=vrdma.lock_indexes_to_buckets(self.locks_held, self.buckets_per_lock)
        self.info("locked buckets: " + str(search_buckets)) if __debug__ else None
        self.search_path=self.table_search_function(search_buckets)
        if len(self.search_path) == 0:
            self.info("Second Search Failed: " + str(self.current_insert_value) + "| unable to continue, client " + str(self.id) + " is done") if __debug__ else None
            self.current_insert_rtt += 1
            return self.retry_insert()
        self.search_path_index = len(self.search_path) - 1

        insert_messages = vrdma.gen_cas_messages(self.search_path)
        unlock_messages = self.release_locks_batched()

        insert_messages.extend(unlock_messages)

        self.current_insert_rtt+=1
        return insert_messages

    def complete_insert_stats(self, success):
        self.insert_path_lengths.append(len(self.search_path))
        self.index_range_per_insert.append(self.search_module.path_index_range(self.search_path))

        #clear for next insert
        return super().complete_insert_stats(success)

    def complete_insert(self):
        self.info("Insert Complete: " + str(self.current_insert_value)) if __debug__ else None
        self.state= "idle"
        self.inserting=False
        self.complete_insert_stats(success=True)
        return None

    def retry_insert(self):
        self.state="release_locks_try_again"
        return self.release_locks_batched()


    def insert_cas_fsm(self, message):
        args = vrdma.unpack_cas_response(message)
        vrdma.fill_local_table_with_cas_response(self.table, args)

        #If CAS failed, try the insert a second time.
        success = args["success"]
        if not success:
            self.info("CAS Failed: " + str(self.current_insert_value)) if __debug__ else None
            self.warning("FAILED CAS MESSAGE: " + str(message))
            self.warning("FAILED CAS MESSAGE PAYLOAD: " + str(message.payload))
            raise Exception("Failed Cas on Insert path -- we should not have failures here")

        #Step down the search path a single index
        self.search_path_index -= 1
        if self.search_path_index == 0:
            self.debug("Insertion Path Complete") if __debug__ else None
        elif self.search_path_index < 0:
            raise Exception("Error we have gone too far down the search path, too many received CAS messages")
        
        #just return None we are only using this method to receieve messages
        return None

    def insert_and_release_fsm(self, message):
        #there should be a message, otherwise don't do anything
        if message == None:
            return None
        
        if vrdma.message_type(message) == "cas_response":
            return self.insert_cas_fsm(message)

        if vrdma.message_type(message) == "masked_cas_response":
            return self.release_locks_fsm(message)


    def idle_fsm(self,message):
        return self.general_idle_fsm(message)

    def read_fsm(self,message):
        complete, success = self.wait_for_read_messages_fsm(self.table, message, self.current_read_key)
        if complete:
            self.state="idle"
            self.complete_read_stats(success, self.current_read_key)
            self.reading=False
        return None

    def fsm_logic(self, message = None):

        if self.complete and self.state == "idle":
            return None

        if self.state== "idle":
            return self.idle_fsm(message)

        if self.state == "reading":
            return self.read_fsm(message)

        if self.state == "aquire_locks":
            return self.aquire_locks_with_reads_fsm(message)

        if self.state == "release_locks_try_again":
            return self.release_locks_fsm(message)

        if self.state == "inserting":
            return self.insert_and_release_fsm(message)


    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)