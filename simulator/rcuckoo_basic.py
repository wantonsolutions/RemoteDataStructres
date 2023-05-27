from . import state_machines
from . import search
from . import hash
from . import virtual_rdma as vrdma

class rcuckoo_basic(state_machines.client_state_machine):
    def __init__(self, config):
        super().__init__(config)

        #inserting and locking
        self.current_insert_value = None
        self.search_path = []
        self.search_path_index = 0

        self.buckets_per_lock = config['buckets_per_lock']
        self.locks_per_message = config['locks_per_message']

        self.read_threshold_bytes = config['read_threshold_bytes']
        assert self.read_threshold_bytes >= self.table.row_size_bytes(), "read threshold is smaller than a single row in the table (this is not allowed)"
        self.locks_held = []
        self.current_locking_messages = []
        self.locking_message_index = 0


    def get(self):
        messages = vrdma.read_threshold_message(self.current_read_key, self.read_threshold_bytes, self.table.get_row_count(), self.table.row_size_bytes())
        return self.begin_read(messages)

    def put(self):
        return self.search()

    def all_locks_aquired(self):
        return self.locking_message_index == len(self.current_locking_messages) and len(self.locks_held) > 0

    def all_locks_released(self):
        return len(self.locks_held) == 0

    def get_current_locking_message(self):
        return self.current_locking_messages[self.locking_message_index]

    def receieve_successful_locking_message(self, message):
        lock_indexes = vrdma.lock_message_to_lock_indexes(message)
        self.info("aquired locks: " + str(lock_indexes))
        self.locks_held.extend(lock_indexes)
        #make unique
        self.locks_held = list(set(self.locks_held))
        self.locking_message_index = self.locking_message_index + 1

    def receive_successful_unlocking_message(self, message):
        unlocked_locks = vrdma.lock_message_to_lock_indexes(message)
        self.info("released locks: " + str(unlocked_locks))
        for lock in unlocked_locks:
            self.locks_held.remove(lock)
        self.locking_message_index = self.locking_message_index + 1

    def search(self, message=None):
        assert message == None, "there should be no message passed to search"

        self.search_path=search.bucket_cuckoo_a_star_insert(self.table, hash.rcuckoo_hash_locations, self.current_insert_value)
        if len(self.search_path) == 0:
            self.info("Search Failed: " + str(self.current_insert_value) + "| unable to continue, client " + str(self.id) + " is done")
            self.complete=True
            self.state = "idle"
            return None

        self.info("Search complete aquiring locks")
        self.state="aquire_locks"
        return self.aquire_locks()


    def aquire_locks(self):
        #get the unique set of buckets and remove -1 (the root) from the search path
        self.locking_message_index = 0
        buckets = search.search_path_to_buckets(self.search_path)
        self.info("gather locks for buckets: " + str(buckets))
        lock_messages = vrdma.get_lock_messages(buckets, self.buckets_per_lock, self.locks_per_message)
        self.current_locking_messages = vrdma.masked_cas_lock_table_messages(lock_messages)
        return self.get_current_locking_message()

    def release_locks(self):
        self.locking_message_index = 0
        buckets = vrdma.lock_indexes_to_buckets(self.locks_held, self.buckets_per_lock)
        buckets.sort(reverse=True)
        unlock_messages = vrdma.get_unlock_messages(buckets, self.buckets_per_lock, self.locks_per_message)
        self.current_locking_messages = vrdma.masked_cas_lock_table_messages(unlock_messages)
        return self.get_current_locking_message()

    def aquire_locks_fsm(self, message):
        if vrdma.message_type(message) == "masked_cas_response":
            if message.payload["function_args"]["success"] == True:
                self.receieve_successful_locking_message(message)
                #enter the critical section if we have all of the locks
                if self.all_locks_aquired():
                    self.state = "critical_section"
                    return None
            #1) retransmit if we did not make process
            #2) or just select the next message
            return self.get_current_locking_message()
        return None

    def release_locks_fsm(self, message):
        if vrdma.message_type(message) == "masked_cas_response":
            if message.payload["function_args"]["success"] == False:
                self.critical("What the fuck is happening I failed to release a lock")
                exit(1)
            
            #successful response
            self.receive_successful_unlocking_message(message)
            if self.all_locks_released():
                if self.state == "release_locks":
                    self.state = "idle"
                    self.inserting=False
                    return None
                if self.state == "release_locks_try_again":
                    return self.search()
            else:
                return self.get_current_locking_message()

    def begin_insert(self):
        self.state = "inserting"
        #todo there are going to be cases where this fails because 
        self.search_path_index = len(self.search_path)-1
        return vrdma.next_cas_message(self.search_path, self.search_path_index)

    def complete_insert_stats(self, success):
        self.insert_path_lengths.append(len(self.search_path))
        self.index_range_per_insert.append(search.path_index_range(self.search_path))
        self.messages_per_insert.append(self.current_insert_messages)

        #clear for next insert
        self.current_insert_messages = 0
        return super().complete_insert_stats(success)

    def finish_insert_and_release_locks(self, success):
        self.complete_insert_stats(success)
        return self.release_locks()

    def complete_insert(self):
        self.info("Insert Complete: " + str(self.current_insert_value))
        self.state="release_locks"
        return self.finish_insert_and_release_locks(success=True)

    def fail_insert(self):
        self.info("Insert Failed: " + str(self.current_insert_value))
        self.state="release_locks_try_again"
        return self.finish_insert_and_release_locks(success=False)



    def undo_last_cas_fsm(self, message):
        if message == None:
            return None

        args = vrdma.unpack_cas_response(message)
        vrdma.fill_local_table_with_cas_response(self.table, args)

        #If CAS failed, try the insert a second time.
        success = args["success"]
        if not success:
            raise Exception("Failed to undo last cas -- this is a really bad scenario")
        
        self.debug("Last CAS was undone, now we are just trying again")

        #this insertion was a failure
        return self.fail_insert()

    def insert_fsm(self, message):
        #there should be a message, otherwise don't do anything
        if message == None:
            return None

        args = vrdma.unpack_cas_response(message)
        vrdma.fill_local_table_with_cas_response(self.table, args)

        #If CAS failed, try the insert a second time.
        success = args["success"]
        if not success:
            self.debug("Insert Failed: " + str(self.current_insert_value) + "| trying again")
            self.debug("failed insert path: " + str(self.search_path))
            self.debug("failed insert path index: " + str(self.search_path_index) + "of  " + str(len(self.search_path) -1 ))
            self.debug("failed cas: " + str(message))
            self.debug("cas element: " + str(self.search_path[self.search_path_index]))

            #if we have not issued a successful CAS yet there is nothing to backtrack to.
            if self.search_path_index == len(self.search_path)-1:
                self.debug("Insertion has failed but no harm was done, trying again")
                return self.fail_insert()
            else:
                self.debug("Insertion has failed, but we have issued a CAS, we can backtrack to repair damage")
                self.state = "undo_last_cas"
                return vrdma.undo_last_cas_message(self.search_path, self.search_path_index)

        #Step down the search path a single index
        self.search_path_index -= 1
        if self.search_path_index <= 0:
            return self.complete_insert()
        else:
            return vrdma.next_cas_message(self.search_path, self.search_path_index)

    def idle_fsm(self,message):
        return self.general_idle_fsm(message)

    def read_fsm(self,message):
        complete, success = self.wait_for_read_messages_fsm(message, self.current_read_key)
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
            return self.aquire_locks_fsm(message)

        if self.state == "release_locks" or self.state == "release_locks_try_again":
            return self.release_locks_fsm(message)

        if self.state == "critical_section":
            return self.begin_insert()

        if self.state == "inserting":
            return self.insert_fsm(message)

        if self.state == "undo_last_cas":
            return self.undo_last_cas_fsm(message)


    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)