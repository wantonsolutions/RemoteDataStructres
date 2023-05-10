from cuckoo import *

class race(client_state_machine):
    def __init__(self, config):
        super().__init__(config)

    def idle_fsm(self, message):
        return self.general_idle_fsm(message)


    def begin_insert(self, messages):
        self.outstanding_read_requests = len(messages)
        self.read_values_found = 0
        self.read_values = []
        self.state = "inserting-read-first"
        return messages

    def begin_insert_second_read(self):
        messages = race_messages(self.current_insert_value, self.table.table_size, self.table.row_size_bytes())
        self.outstanding_read_requests = len(messages)
        self.read_values_found = 0
        self.read_values = []
        self.state = "inserting-read-second"
        return messages

    def begin_extent_read(self):
        #todo read an extent, this currently just reads the table again
        messages = race_message_read_key_location(self.current_insert_value, self.table.table_size, self.table.row_size_bytes(),0)
        self.outstanding_read_requests = len(messages)
        self.read_values_found = 0
        self.read_values = []
        self.state = "reading-extent"
        return messages

    def put(self):
        messages = race_messages(self.current_insert_value, self.table.table_size, self.table.row_size_bytes())
        self.current_insert_rtt+=1
        return self.begin_insert(messages)

    def get(self):
        messages = race_messages(self.current_read_key, self.table.table_size, self.table.row_size_bytes())
        self.current_read_rtt+=1
        return self.begin_read(messages)

    def first_read_fsm(self,message):
        complete, success = self.wait_for_read_messages_fsm(message, self.current_read_key)
        if complete:
            self.debug("Race Reading Complete! success:" + str(success) + " key: " +str(self.current_read_key))
            self.current_read_rtt+=1
            return self.begin_extent_read()
        return None
        #     self.state="idle"
        #     self.complete_read_stats(success, self.current_read_key)
        #     self.reading=False
        # return None

    
    def extent_read_fsm(self,message):
        complete, success = self.wait_for_read_messages_fsm(message, self.current_read_key)
        if complete:
            self.debug("Race Extent Reading Complete! success:" + str(success) + " key: " +str(self.current_read_key))
            self.state="idle"
            self.complete_read_stats(success, self.current_read_key)
            self.reading=False
        return None

    def get_power_of_two_cas_location(self,bucket_0,bucket_1):
        if self.table.bucket_has_empty(bucket_0) or self.table.bucket_has_empty(bucket_1):
            fill_0 = self.table.get_first_empty_index(bucket_0)
            fill_1 = self.table.get_first_empty_index(bucket_1)
            if fill_0 <= fill_1:
                return bucket_0, fill_0
            else:
                return bucket_1, fill_1
        return -1, -1


    def power_of_two_cas_location(self, key, table_size):
        locations = hash.race_hash_locations(key, table_size)
        bucket_0, overflow_0 = locations[0]
        bucket_1, overflow_1 = locations[1]

        bucket, offset = self.get_power_of_two_cas_location(bucket_0, bucket_1) 
        if bucket != -1:
            return bucket, offset

        #both buckets are full moving to overflow buckets
        self.info("both buckets are full moving to overflow buckets")
        bucket, offset = self.get_power_of_two_cas_location(overflow_0, overflow_1)
        if bucket != -1:
            return bucket, offset

        return -1, -1

    def complete_insert_stats(self, success):
        super().complete_insert_stats(success)
    
    def insert_fsm(self, message):
        #there should be a message, otherwise don't do anything
        if message == None:
            return None

        args = unpack_cas_response(message)
        fill_local_table_with_cas_response(self.table, args)

        #If CAS failed, try the insert a second time.
        success = args["success"]
        #increment the rtt fdor both cases
        self.current_insert_rtt+=1
        if not success:
            self.debug("Insert Failed: " + str(self.current_insert_value) + "| trying again")
            #try again
            return self.insert_cas()
        else:
            return self.begin_insert_second_read()

    def insert_cas(self):
        bucket, offset = self.power_of_two_cas_location(self.current_insert_value, self.table.table_size)
        if bucket == -1:
            self.complete=True
            self.state = "idle"
            return None

        self.info("Inserting: " + str(self.current_insert_value) + " at bucket: " + str(bucket) + " offset: " + str(offset))
        self.state="inserting"
        return cas_table_entry_message(bucket , offset, None, self.current_insert_value)


    def first_read_insert_fsm(self, message):
        complete, success = self.wait_for_read_messages_fsm(message, self.current_insert_value)
        if complete:
            self.info("Race Reading Complete - first insert!: " + str(success) + " " + str(self.current_insert_value))
            if success:
                self.critical("we found a duplicate value in the table, so we are not going to insert")
                self.state="idle"
                self.complete_insert_stats(False)
                self.inserting=False
                return None
            else:
                self.current_insert_rtt+=1
                return self.insert_cas()
    

    def second_read_insert_fsm(self, message):
        complete, success = self.wait_for_read_messages_fsm(message, self.current_insert_value)
        if complete:
            self.info("Race Reading Complete - second insert!: " + str(success) + " " + str(self.current_insert_value))
            if success:
                #TODO check for duplicates
                # self.critical("Insertion complete (todo check for duplicates")
                self.state="idle"
                self.complete_insert_stats(True)
                self.inserting=False
                return None
            else:
                raise Exception("We should never get here, because we should have already inserted the value (FAILED INSERT)")


    def fsm_logic(self, message = None):

        if self.complete:
            return None

        if self.state== "idle":
            return self.idle_fsm(message)

        if self.state == "reading":
            return self.first_read_fsm(message)

        if self.state == "reading-extent":
            return self.extent_read_fsm(message)

        if self.state == "inserting-read-first":
            return self.first_read_insert_fsm(message)

        if self.state == "inserting":
            return self.insert_fsm(message)

        if self.state == "inserting-read-second":
            return self.second_read_insert_fsm(message)

        #todo re-read to check for duplicates
        # if self.state == "inserting-read-second":
        #     return self.first_read_insert_second(message)


        return None


class rcuckoo(client_state_machine):
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
        messages = read_threshold_message(self.current_read_key, self.read_threshold_bytes, self.table.table_size, self.table.row_size_bytes())
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
        lock_indexes = lock_message_to_lock_indexes(message)
        self.info("aquired locks: " + str(lock_indexes))
        self.locks_held.extend(lock_indexes)
        #make unique
        self.locks_held = list(set(self.locks_held))
        self.locking_message_index = self.locking_message_index + 1

    def receive_successful_unlocking_message(self, message):
        unlocked_locks = lock_message_to_lock_indexes(message)
        self.info("released locks: " + str(unlocked_locks))
        for lock in unlocked_locks:
            self.locks_held.remove(lock)
        self.locking_message_index = self.locking_message_index + 1

    def search(self, message=None):
        assert message == None, "there should be no message passed to search"

        self.search_path=bucket_cuckoo_a_star_insert(self.table, hash.rcuckoo_hash_locations, self.current_insert_value)
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
        buckets = search_path_to_buckets(self.search_path)
        self.info("gather locks for buckets: " + str(buckets))
        lock_messages = get_lock_messages(buckets, self.buckets_per_lock, self.locks_per_message)
        self.current_locking_messages = masked_cas_lock_table_messages(lock_messages)
        return self.get_current_locking_message()

    def release_locks(self):
        self.locking_message_index = 0
        buckets = lock_indexes_to_buckets(self.locks_held, self.buckets_per_lock)
        buckets.sort(reverse=True)
        unlock_messages = get_unlock_messages(buckets, self.buckets_per_lock, self.locks_per_message)
        self.current_locking_messages = masked_cas_lock_table_messages(unlock_messages)
        return self.get_current_locking_message()

    def aquire_locks_fsm(self, message):
        if message_type(message) == "masked_cas_response":
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
        if message_type(message) == "masked_cas_response":
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
        return next_cas_message(self.search_path, self.search_path_index)

    def complete_insert_stats(self, success):
        self.insert_path_lengths.append(len(self.search_path))
        self.index_range_per_insert.append(path_index_range(self.search_path))
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

        args = unpack_cas_response(message)
        fill_local_table_with_cas_response(self.table, args)

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

        args = unpack_cas_response(message)
        fill_local_table_with_cas_response(self.table, args)

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
                return undo_last_cas_message(self.search_path, self.search_path_index)

        #Step down the search path a single index
        self.search_path_index -= 1
        if self.search_path_index <= 0:
            return self.complete_insert()
        else:
            return next_cas_message(self.search_path, self.search_path_index)

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
        if message.payload["function"] == read_table_entry:
            self.info("Read: " + "Bucket: " + str(args["bucket_id"]) + " Offset: " + str(args["bucket_offset"]) + " Size: " + str(args["size"]))  if __debug__ else None
            read = read_table_entry(self.table, **args)
            response = Message({"function":fill_table_with_read, "function_args":{"read":read, "bucket_id":args["bucket_id"], "bucket_offset":args["bucket_offset"], "size":args["size"]}})
            self.info("Read Response: " +  str(response.payload["function_args"]["read"])) if __debug__ else None
            return response

        if message.payload["function"] == cas_table_entry:
            self.info("CAS: " + "Bucket: " + str(args["bucket_id"]) + " Offset: " + str(args["bucket_offset"]) + " Old: " + str(args["old"]) + " New: " + str(args["new"])) if __debug__ else None
            success, value = cas_table_entry(self.table, **args)
            response = Message({"function":fill_table_with_cas, "function_args":{"bucket_id":args["bucket_id"], "bucket_offset":args["bucket_offset"], "value":value, "success":success}})

            # self.table.print_table()  if __debug__ else None

            rargs=response.payload["function_args"]
            self.info("Read Response: " +  "Success: " + str(rargs["success"]) + " Value: " + str(rargs["value"])) if __debug__ else None
            return response

        if message.payload["function"] == masked_cas_lock_table:
            #self.info("Masked CAS in Memory: "+ str(args["lock_index"]) + " Old: " + str(args["old"]) + " New: " + str(args["new"]) + " Mask: " + str(args["mask"]))
            success, value = masked_cas_lock_table(self.table.lock_table, **args)
            response = Message({"function": fill_lock_table_masked_cas, "function_args":{"lock_index":args["lock_index"], "success":success, "value": value, "mask":args["mask"]}})
            return response
            
        else:
            self.logger.warning("MEMORY: unknown message type " + str(message))