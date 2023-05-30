from . import state_machines
from . import virtual_rdma as vrdma
from . import hash

class race(state_machines.client_state_machine):
    def __init__(self, config):
        super().__init__(config)

        self.table = config["table"]

    def idle_fsm(self, message):
        return self.general_idle_fsm(message)


    def begin_insert(self, messages):
        self.outstanding_read_requests = len(messages)
        self.read_values_found = 0
        self.read_values = []
        self.state = "inserting-read-first"
        return messages

    def begin_insert_second_read(self):
        messages = vrdma.race_messages(self.current_insert_value, self.table.get_row_count(), self.table.row_size_bytes())
        self.outstanding_read_requests = len(messages)
        self.read_values_found = 0
        self.read_values = []
        self.state = "inserting-read-second"
        return messages

    def begin_extent_read(self):
        #todo read an extent, this currently just reads the table again
        messages = vrdma.race_message_read_key_location(self.current_insert_value, self.table.get_row_count(), self.table.row_size_bytes(),0)
        self.outstanding_read_requests = len(messages)
        self.read_values_found = 0
        self.read_values = []
        self.state = "reading-extent"
        return messages

    def put(self):
        messages = vrdma.race_messages(self.current_insert_value, self.table.get_row_count(), self.table.row_size_bytes())
        self.current_insert_rtt+=1
        return self.begin_insert(messages)

    def get(self):
        messages = vrdma.race_messages(self.current_read_key, self.table.get_row_count(), self.table.row_size_bytes())
        self.current_read_rtt+=1
        return self.begin_read(messages)

    def first_read_fsm(self,message):
        complete, success = self.wait_for_read_messages_fsm(self.table, message, self.current_read_key)
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
        complete, success = self.wait_for_read_messages_fsm(self.table, message, self.current_read_key)
        if complete:
            self.debug("Race Extent Reading Complete! success:" + str(success) + " key: " +str(self.current_read_key))
            self.state="idle"
            self.critical("we are assuming all reads are successful in race for extents") if __debug__ else None
            success=True
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

        args = vrdma.unpack_cas_response(message)
        vrdma.fill_local_table_with_cas_response(self.table, args)

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
        bucket, offset = self.power_of_two_cas_location(self.current_insert_value, self.table.get_row_count())
        if bucket == -1:
            self.complete=True
            self.state = "idle"
            return None

        self.info("Inserting: " + str(self.current_insert_value) + " at bucket: " + str(bucket) + " offset: " + str(offset))
        self.state="inserting"
        return vrdma.cas_table_entry_message(bucket , offset, None, self.current_insert_value)


    def first_read_insert_fsm(self, message):
        complete, success = self.wait_for_read_messages_fsm(self.table, message, self.current_insert_value)
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
        complete, success = self.wait_for_read_messages_fsm(self.table, message, self.current_insert_value)
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