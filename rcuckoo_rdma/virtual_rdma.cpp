#include "virtual_rdma.h"
#include <string.h>
#include <algorithm>
#include <iostream>
#include "log.h"
#include "search.h"
#include "util.h"

using namespace std;
using namespace cuckoo_search;
namespace cuckoo_virtual_rdma {

    Request::Request(operation op, Key key, Value value){
        this->op = op;
        this->key = key;
        this->value = value;
    } 

    Request::Request(){
        this->op = GET;
        memset(key.bytes, 0, KEY_SIZE);
        memset(value.bytes, 0, VALUE_SIZE);
    }

    string Request::to_string(){
        string s = "Request: {";
        s += "Op: ";
        switch (op) {
            case GET:
                s += "GET";
                break;
            case PUT:
                s += "PUT";
                break;
            case DELETE:
                s += "DELETE";
                break;
            case NO_OP:
                s += "NO_OP";
                break;
        }
        s += " Key: " + key.to_string();
        s += " Value: " + value.to_string();
        s += "}";
        return s;
    }

    string VRMaskedCasData::to_string(){
        string s = "VRMaskedCasData: {";
        s += "min_lock_index: " + std::to_string(min_lock_index);
        s += " mask: " + uint64t_to_bin_string(mask);
        s += " new_value: " + uint64t_to_bin_string(new_value);
        s += " old: " + uint64t_to_bin_string(old);
        s += " min_set_lock: " + std::to_string(min_set_lock);
        s += " max_set_lock: " + std::to_string(max_set_lock);
        s += "}";
        return s;
    }

    string VRReadData::to_string() {
        string s = "VRReadData: {";
        s += "row: " + std::to_string(row);
        s += " offset: " + std::to_string(offset);
        s += " size: " + std::to_string(size);
        s += "}";
        return s;
    }

    string VRCasData::to_string() {
        string s = "VRCasData: {";
        s += "row " + std::to_string(row);
        s += " offset" + std::to_string(offset);
        s += " old_value: " + uint64t_to_bin_string(old);
        s += " new_value: " + uint64t_to_bin_string(new_value);
        return s;
    }


    message_type VRMessage::get_message_type(){
        if (function == "cas_table_entry") {
            return CAS_REQUEST;
        } else if (function == "fill_table_with_cas"){
            return CAS_RESPONSE;
        } else if (function == "masked_cas_lock_table") {
            return MASKED_CAS_REQUEST;
        } else if (function == "fill_lock_table_masked_cas") {
            return MASKED_CAS_RESPONSE;
        } else if (function == "read_table_entry") {
            return READ_REQUEST;
        } else if (function == "fill_table_with_read") {
            return READ_RESPONSE;
        } else if (function == "no_op" || function == "") {
            return NO_OP_MESSAGE;
        } else {
            ALERT("Error", "unknown function (%s)\n", function.c_str());
            return NO_OP_MESSAGE;
            // exit(1);
        }
    }

    VRMessage& VRMessage::operator=(const VRMessage& other){
        this->function = other.function;
        this->function_args = other.function_args;
        return *this;
    }

    string message_type_to_function_string(message_type type){
        switch(type){
            case READ_REQUEST:
                return "read_table_entry";
            case READ_RESPONSE:
                return "fill_table_with_read";
            // case WRITE_REQUEST:
            //     return "write_table_entry";
            // case WRITE_RESPONSE:
            //     return "fill_table_with_write";
            case CAS_REQUEST:
                return "cas_table_entry";
            case CAS_RESPONSE:
                return "fill_table_with_cas";
            case MASKED_CAS_REQUEST:
                return "masked_cas_lock_table";
            case MASKED_CAS_RESPONSE:
                return "fill_lock_table_masked_cas";
            case NO_OP_MESSAGE:
                return "no_op";
            default:
                return "broken int type";
                // printf("Error unknown message type %d\n", type);
                // exit(1);
        }
    }


    uint32_t VRMessage::get_message_size_bytes() {
        uint32_t size = 0;
        size += header_size(get_message_type());
        if (function_args.find("size") != function_args.end()) {
            size += stoi(function_args["size"]);
        }
        return size;
    }

    string VRMessage::to_string(){
        string s = "";
        s += "Message Type:";
        s += message_type_to_function_string(get_message_type());
        s += "(int type)" + message_type_to_function_string(type);
        s += " Function Arguments {";
        for (auto const& x : function_args) {
            s += x.first + ":" + x.second + " ";
        }
        s += "}";
        return s;
    }




    uint32_t header_size(message_type type){
        switch(type){
            case READ_REQUEST:
                return READ_REQUEST_SIZE;
            case READ_RESPONSE:
                return READ_RESPONSE_SIZE;
            case WRITE_REQUEST:
                return WRITE_REQUEST_SIZE;
            case WRITE_RESPONSE:
                return WRITE_RESPONSE_SIZE;
            case CAS_REQUEST:
                return CAS_REQUEST_SIZE;
            case CAS_RESPONSE:
                return CAS_RESPONSE_SIZE;
            case MASKED_CAS_REQUEST:
                return MASKED_CAS_REQUEST_SIZE;
            case MASKED_CAS_RESPONSE:
                return MASKED_CAS_RESPONSE_SIZE;
            default:
                printf("error unknown enum %d (string attempt %s)\n", message_type_to_function_string(type).c_str());
                exit(0);
        }
    }

    unordered_map<string,string> unpack_read_read_response(VRMessage &msg) {
        assert (msg.get_message_type() == READ_RESPONSE);
        return msg.function_args;
    }

    void fill_table_with_read(Table &table, uint32_t bucket_id, uint32_t bucket_offset, uint32_t size, vector<Entry> &entries) {
        table.assert_operation_in_table_bound(bucket_id, bucket_offset, size);
        uint32_t total_indexes = size / sizeof(Entry);
        assert (entries.size() == total_indexes);

        uint32_t base = bucket_id * table.get_buckets_per_row() + bucket_offset;

        for (int i=0; i<total_indexes; i++) {
            uint32_t bucket = table.absolute_index_to_bucket_index(base + i);
            uint32_t offset = table.absolute_index_to_bucket_offset(base + i);
            table.set_entry(bucket, offset, entries[i]);
        }
        return;

    }

    vector<Entry> read_table_entry(Table &table, uint32_t bucket_id, uint32_t bucket_offset, uint32_t size) {
        table.assert_operation_in_table_bound(bucket_id, bucket_offset, size);
        uint32_t total_indexes = size / sizeof(Entry);
        vector<Entry> entries;
        uint32_t base = bucket_id * table.get_buckets_per_row() + bucket_offset;

        for (int i=0; i<total_indexes; i++) {
            uint32_t bucket = table.absolute_index_to_bucket_index(base + i);
            uint32_t offset = table.absolute_index_to_bucket_offset(base + i);
            entries.push_back(table.get_entry(bucket, offset));
        }
        return entries;
    }


    CasOperationReturn cas_table_entry(Table &table, uint32_t bucket_id, uint32_t bucket_offset, uint64_t old, uint64_t new_value) {
        Entry e = table.get_entry(bucket_id, bucket_offset);
        uint64_t ret_val = e.get_as_uint64_t();
        bool success = false;
        Entry new_entry;

        SUCCESS("virtual_rdma", "cas compairision table %08llx == guess %08llx (success=%d -- row %06d, col %06d)\n", ret_val, old, ret_val == old, bucket_id, bucket_offset);

        if (ret_val == old) {
            SUCCESS("virtual_rdma", "success cas compairision table %08llx == guess %08llx (success=%d -- row %06d, col %06d)\n", ret_val, old, ret_val == old, bucket_id, bucket_offset);
            new_entry.set_as_uint64_t(new_value);
            table.set_entry(bucket_id, bucket_offset, new_entry);
            success = true;
        } 
        return CasOperationReturn(success, ret_val);
    }

    CasOperationReturn masked_cas_lock_table(Table &table, uint32_t lock_index, uint64_t old, uint64_t new_value, uint64_t mask) {
        return table.lock_table_masked_cas(lock_index, old, new_value, mask);
    }

    vector<string> split(const string &str, const string &delim)
    {
        vector<string> tokens;
        size_t prev = 0, pos = 0;
        do
        {
            pos = str.find(delim, prev);
            if (pos == string::npos)
                pos = str.length();
            string token = str.substr(prev, pos - prev);
            if (!token.empty())
                tokens.push_back(token);
            prev = pos + delim.length();
        } while (pos < str.length() && prev < str.length());
        return tokens;
    }

    vector<Entry> decode_entries_from_string(string str_entries) {
        vector<Entry> entries;
        vector<string> entries_str = split(str_entries, ",");
        for (int i=0; i<entries_str.size(); i++) {
            vector<string> kv = split(entries_str[i], ":");
            Entry e = Entry(kv[0], kv[1]);
            entries.push_back(e);
        }
        return entries;
    }

    string encode_entries_to_string(vector<Entry> &entries) {
        string entries_str;
        for (int i=0; i<entries.size(); i++) {
            entries_str += entries[i].to_string();
            if (i != entries.size() - 1) {
                entries_str += ",";
            }
        }
        return entries_str;
    }

    void fill_local_table_with_read_response(Table &table, unordered_map<string,string> &args) {
        try {
            uint32_t bucket_id = stoi(args["bucket_id"]);
            uint32_t bucket_offset = stoi(args["bucket_offset"]);
            uint32_t size = stoi(args["size"]);
            string read = args["read"];
            //This parsing assumes the encoding
            //key1:value1,key2:value2,key3:value3
            vector<Entry> entries = decode_entries_from_string(args["read"]);
            fill_table_with_read(table, bucket_id, bucket_offset, size, entries);
        } catch (exception &e) {
            printf("error in fill_local_table_with_read_response %s\n", e.what());
            exit(1);
        }
        return;
    }

    unsigned int get_lock_message_lock_index(VRMessage lock_message) {
            unsigned int base_index = stoi(lock_message.function_args["lock_index"]);
            // base_index = base_index * 8; //convert bit alignment to byte alignment for ease of manipulation
            return base_index;

    }

    unsigned int lock_message_to_lock_indexes(VRMaskedCasData lock_message, unsigned int * lock_indexes) {
        // uint64_t mask = reverse_uint64_t(lock_message.mask);
        uint64_t mask = __builtin_bswap64(lock_message.mask);
        unsigned int base_index = lock_message.min_lock_index * 8;
        unsigned int total_locks = 0;

        uint64_t one = 1;
        for (uint64_t i=0; i<BITS_IN_MASKED_CAS; i++) {
            if (mask & (one << i)) {
                // lock_indexes.push_back(base_index + i);
                lock_indexes[total_locks] = base_index + i;
                total_locks++;
            }
        }
        // ALERT("lock_message to indexes","appending a total of %d locks\n", lock_indexes.size());
        return total_locks;

    }

    vector<unsigned int> lock_message_to_lock_indexes(VRMessage lock_message) {
        vector<unsigned int> lock_indexes;
        try { 
            unsigned int base_index = get_lock_message_lock_index(lock_message);
            base_index = base_index * 8; //convert bit alignment to byte alignment for ease of manipulation
            // string mast_str = lock_message.function_args["mask"];
            // printf("Mask String: %s\n", mast_str.c_str());

            uint64_t mask = bin_string_to_uint64_t(lock_message.function_args["mask"]);
            mask = reverse_uint64_t(mask);
            uint64_t one = 1;
            for (uint64_t i=0; i<64; i++) {
                if (mask & (one << i)) {
                    lock_indexes.push_back(base_index + i);
                }
            }
        } catch (exception &e) {
            printf("error in lock_message_to_lock_indexes %s\n", e.what());
            exit(1);
        }
        return lock_indexes;

    }

    int keys_contained_in_read_response(const Key &key, const vector<Entry> &entries) {
        int count = 0;
        for (int i=0; i<entries.size(); i++) {
            if (entries[i].key == key) {
                count++;
            }
        }
        return count;
    }

    unsigned int single_read_size_bytes(hash_locations buckets, unsigned int row_size_bytes) {
        return (buckets.distance() + 1) * row_size_bytes;
    }

    
    void lock_indexes_to_buckets(vector<unsigned int> &buckets, vector<unsigned int>& lock_indexes, unsigned int buckets_per_lock) {
        buckets.clear();
        // const uint8_t bits_in_byte = 8;
        //Translate locks by multiplying by the buckets per lock
        for (int i=0; i<lock_indexes.size(); i++) {
            unsigned int lock_index = lock_indexes[i];
            unsigned int bucket = lock_index * buckets_per_lock;
            //fill in buckets that are covered by the lock
            //This is every bucket which is covered by the buckets per lock
            for(int j=0; j<buckets_per_lock; j++) {
                buckets.push_back(bucket + j);
            }
        }
    }

    vector<unsigned int> get_unique_lock_indexes(vector<unsigned int> buckets, unsigned int buckets_per_lock) {
        vector<unsigned int> buckets_chunked_by_lock;
        for (int i=0; i<buckets.size(); i++) {
            unsigned int lock_index = buckets[i] / buckets_per_lock;
            buckets_chunked_by_lock.push_back(lock_index);
        }
        std::sort(buckets_chunked_by_lock.begin(), buckets_chunked_by_lock.end());
        buckets_chunked_by_lock.erase(std::unique(buckets_chunked_by_lock.begin(), buckets_chunked_by_lock.end()), buckets_chunked_by_lock.end());
        return buckets_chunked_by_lock;
    }

    unsigned int byte_aligned_index(unsigned int index) {
        return (index / 8) * 8;
    }

    unsigned int sixty_four_aligned_index(unsigned int index) {
        return (index / 64) * 64;
    }

    unsigned int get_min_sixty_four_aligned_index(vector<unsigned int> &indexes) {
        unsigned int min_index = indexes[0];
        for (int i=1; i<indexes.size(); i++) {
            if (indexes[i] < min_index) {
                printf("new min index\n");
                min_index = indexes[i];
            }
        }
        return sixty_four_aligned_index(min_index);
    }

    #define BITS_PER_BYTE 8

    vector<vector<unsigned int>> break_lock_indexes_into_chunks(vector<unsigned int> lock_indexes, unsigned int locks_per_message) {
        vector<vector<unsigned int>> lock_indexes_chunked;
        vector<unsigned int> current_chunk;
        unsigned int min_lock_index;
        unsigned int bits_in_uint64_t = sizeof(uint64_t) * BITS_PER_BYTE;
        for (int i=0; i<lock_indexes.size(); i++) {
            if(current_chunk.size() == 0) {
                min_lock_index = sixty_four_aligned_index(lock_indexes[i]);
                VERBOSE("break_lock_indexes_into_chunks", "Min lock index: %u origingal %u\n", min_lock_index, lock_indexes[i]);
            }

            if(((lock_indexes[i] - min_lock_index) < bits_in_uint64_t) && (current_chunk.size() < locks_per_message)) {
                VERBOSE("pushing to chunk", "Pushing %u to chunk\n", lock_indexes[i]);
                current_chunk.push_back(lock_indexes[i]);
            } else {
                VERBOSE("pushing to new chunk", "Pushing %u to new chunk\n", lock_indexes[i]);
                lock_indexes_chunked.push_back(current_chunk);
                current_chunk.clear();
                min_lock_index = sixty_four_aligned_index(lock_indexes[i]);
                current_chunk.push_back(lock_indexes[i]);
            }
        }
        if(current_chunk.size() > 0) {
            lock_indexes_chunked.push_back(current_chunk);
        }
        return lock_indexes_chunked;
    }

    void break_lock_indexes_into_chunks_fast(unsigned int* lock_indexes, unsigned int lock_indexes_size, unsigned int locks_per_message, vector<vector<unsigned int>>& locks_chunked) {
        unsigned int min_lock_index;
        const unsigned int bits_in_uint64_t = sizeof(uint64_t) * BITS_PER_BYTE;
        unsigned int current_chunk_index=0;
        unsigned int top_level_index=0;

        assert(lock_indexes_size > 0);
        assert(lock_indexes_size < MAX_LOCKS);
        assert(lock_indexes);
        assert(locks_per_message > 0);

        //Make sure that we have enough space to store the chunks
        locks_chunked.resize(lock_indexes_size);
        locks_chunked[top_level_index].resize(locks_per_message);
        for(int i=0; i<lock_indexes_size; i++) {
            if(current_chunk_index == 0) {
                min_lock_index = sixty_four_aligned_index(lock_indexes[i]);
                VERBOSE("break_lock_indexes_into_chunks", "Min lock index: %u origingal %u\n", min_lock_index, lock_indexes[i]);
            }
            if (((lock_indexes[i] - min_lock_index) < bits_in_uint64_t) && (current_chunk_index < locks_per_message)) {
                VERBOSE("pushing to chunk", "Pushing %u to chunk [%d][%d]\n", lock_indexes[i], top_level_index, current_chunk_index);
                locks_chunked[top_level_index][current_chunk_index] = lock_indexes[i];
                VERBOSE("after setting lock", "locks_chunked[%d][%d] = %u\n", top_level_index, current_chunk_index, locks_chunked[top_level_index][current_chunk_index]);
                current_chunk_index++;
            } else {
                VERBOSE("pushing to new chunk", "Pushing %u to new chunk\n", lock_indexes[i]);
                //Fit the vector without reallocating it.

                VERBOSE("RESIZE", "Resizing [%d] to %d\n", top_level_index, current_chunk_index);
                locks_chunked[top_level_index].resize(current_chunk_index);
                top_level_index++;
                locks_chunked[top_level_index].resize(locks_per_message);

                current_chunk_index=0;
                min_lock_index = sixty_four_aligned_index(lock_indexes[i]);
                locks_chunked[top_level_index][current_chunk_index] = lock_indexes[i];
                current_chunk_index++;
            }
        }
        VERBOSE("final RESIZE", "Resizing [%d] to %d\n", top_level_index, current_chunk_index);
        VERBOSE("final RESIZE", "top level size before %d\n", locks_chunked[top_level_index].size());
        locks_chunked[top_level_index].resize(current_chunk_index);
        //This line is serious bullshit
        // locks_chunked[top_level_index].resize(current_chunk_index, locks_chunked[top_level_index][current_chunk_index-1]);


        VERBOSE("final RESIZE", "Resizing top level to %d\n", top_level_index + 1);
        locks_chunked.resize(top_level_index + 1);

        VERBOSE("Final print", "-------");
        for(int i=0;i<locks_chunked.size();i++){
            for(int j=0;j<locks_chunked[i].size();j++){
                VERBOSE("Final print", "locks_chunked[%d][%d] = %u\n", i, j, locks_chunked[i][j]);
            }
        }


        return;

    }

    void break_lock_indexes_into_chunks_fast_context(LockingContext &context) {
        unsigned int min_lock_index;
        const unsigned int bits_in_uint64_t = sizeof(uint64_t) * BITS_PER_BYTE;
        unsigned int chunk_index=0;

        assert(context.lock_indexes_size > 0);
        assert(context.lock_indexes_size < MAX_LOCKS);
        assert(context.lock_indexes);
        assert(context.locks_per_message > 0);

        //Make sure that we have enough space to store the chunks
        // context.fast_lock_chunks.resize(context.lock_indexes_size);
        vector<unsigned int> * current_chunk = &context.fast_lock_chunks[chunk_index];
        current_chunk->clear();
        min_lock_index = sixty_four_aligned_index(context.lock_indexes[0]);
        for(int i=0; i<context.lock_indexes_size; i++) {
            unsigned int lock = context.lock_indexes[i];
            if (
            ((lock - min_lock_index) < bits_in_uint64_t) &&
            (current_chunk->size() < context.locks_per_message)) [[likely]]{
                current_chunk->push_back(lock);
            } else {
                //Fit the vector without reallocating it.
                chunk_index++;
                current_chunk = &context.fast_lock_chunks[chunk_index];
                current_chunk->clear();

                min_lock_index = sixty_four_aligned_index(lock);
                current_chunk->push_back(lock);
            }
        }

        context.number_of_chunks=chunk_index + 1;
        return;

    }


    void lock_chunks_to_masked_cas_data(vector<vector<unsigned int>> lock_chunks, vector<VRMaskedCasData> &masked_cas_data) {
        // vector<VRMaskedCasData> masked_cas_data;
        masked_cas_data.clear();
        assert(masked_cas_data.size() == 0);
        for (int i=0; i<lock_chunks.size(); i++) {
            VRMaskedCasData mcd;
            uint64_t lock = 0;
            uint64_t one = 1;
            unsigned int min_index = get_min_sixty_four_aligned_index(lock_chunks[i]);
            for (int j=0; j<lock_chunks[i].size(); j++) {
                unsigned int normal_index = lock_chunks[i][j] - min_index;
                lock |= (uint64_t)(one << normal_index);
                VERBOSE("lock_chunks_to_masked_cas", "normalized index %u\n", normal_index);
            }

            VERBOSE("lock_chunks_to_masked_cas", "lock is %s", uint64t_to_bin_string(lock).c_str());
            lock = reverse_uint64_t(lock);
            mcd.min_set_lock = lock_chunks[i][0] - min_index;
            mcd.max_set_lock = lock_chunks[i][lock_chunks[i].size()-1] - min_index;
            mcd.min_lock_index = min_index / 8;
            mcd.old = 0;
            mcd.new_value = lock;
            mcd.mask = lock;
            VERBOSE("lock_chunks_to_masked_cas", "pushing back masked cas data %s", mcd.to_string().c_str());
            masked_cas_data.push_back(mcd);
        }
        VERBOSE("lock_chunks_to_masked_cas", "returning %d masked cas data", masked_cas_data.size());
        return;
    }

    void lock_chunks_to_masked_cas_data_context(LockingContext &context) {
        // vector<VRMaskedCasData> masked_cas_data;
        context.lock_list.clear();
        for (int i=0; i<context.number_of_chunks; i++) {
            VRMaskedCasData mcd;
            uint64_t lock = 0;
            uint64_t one = 1;
            unsigned int min_index = get_min_sixty_four_aligned_index(context.fast_lock_chunks[i]);
            // unsigned int min_index = sixty_four_aligned_index(context.fast_lock_chunks[i][0]);
            for (int j=0; j<context.fast_lock_chunks[i].size(); j++) {
                unsigned int normal_index = context.fast_lock_chunks[i][j] - min_index;
                lock |= (uint64_t)(one << normal_index);
            }

            //TODO for performance we can probably use bswap instead of the swap I built
            // lock = reverse_uint64_t(lock);
            lock = __builtin_bswap64(lock);
            mcd.min_set_lock = context.fast_lock_chunks[i][0] - min_index;
            mcd.max_set_lock = context.fast_lock_chunks[i][context.fast_lock_chunks[i].size()-1] - min_index;
            mcd.min_lock_index = min_index / 8;
            mcd.old = 0;
            mcd.new_value = lock;
            mcd.mask = lock;
            // printf("pushing mcd %d %s\n", i, mcd.to_string().c_str());
            context.lock_list.push_back(mcd);
        }
        VERBOSE("lock_chunks_to_masked_cas", "returning %d masked cas data", context.lock_list.size());
        return;
    }

    void unlock_chunks_to_masked_cas_data(vector<vector<unsigned int>> lock_chunks, vector<VRMaskedCasData> &masked_cas_data) { 
        VERBOSE("unlock_chunks_to_masked_cas_data", "ENTRY");
        lock_chunks_to_masked_cas_data(lock_chunks, masked_cas_data);
        for (int i=0; i<masked_cas_data.size(); i++) {
            masked_cas_data[i].old = masked_cas_data[i].new_value;
            masked_cas_data[i].new_value = 0;
        }
        return;
    }

    void unlock_chunks_to_masked_cas_data_context(LockingContext &context) { 
        VERBOSE("unlock_chunks_to_masked_cas_data", "ENTRY");
        lock_chunks_to_masked_cas_data_context(context);
        for (int i=0; i<context.lock_list.size(); i++) {
            context.lock_list[i].old = context.lock_list[i].new_value;
            context.lock_list[i].new_value = 0;
        }
        return;
    }

    vector<VRMaskedCasData> get_lock_or_unlock_list(vector<unsigned int> buckets, unsigned int buckets_per_lock, unsigned int locks_per_message, bool locking) {
        assert(locks_per_message <= 64);
        vector<unsigned int> unique_lock_indexes = get_unique_lock_indexes(buckets, buckets_per_lock);
        vector<vector<unsigned int>> lock_chunks = break_lock_indexes_into_chunks(unique_lock_indexes, locks_per_message);
        vector<VRMaskedCasData> masked_cas_data;
        if (locking) {
            lock_chunks_to_masked_cas_data(lock_chunks, masked_cas_data);
        } else {
            unlock_chunks_to_masked_cas_data(lock_chunks, masked_cas_data);
        }
        return masked_cas_data;
    }


    void get_lock_or_unlock_list_fast(vector<unsigned int> buckets, vector<vector<unsigned int>> & fast_lock_chunks, vector<VRMaskedCasData> &masked_cas_data, unsigned int buckets_per_lock, unsigned int locks_per_message, bool locking) {
        assert(locks_per_message <= 64);
        // vector<vector<unsigned int>> fast_lock_chunks;
        masked_cas_data.clear();
        if (fast_lock_chunks.size() == 0) {
            fast_lock_chunks.resize(MAX_LOCKS);
            for (int i=0; i<MAX_LOCKS; i++) {
                fast_lock_chunks[i].resize(locks_per_message);
            }
        }
        assert(buckets.size() <= MAX_LOCKS);
        unsigned int unique_lock_indexes[MAX_LOCKS];

        //Lets make sure that all the buckets are allready sorted
        //I'm 90% sure this will get optimized out in the end
        for (int i=0; i<buckets.size()-1; i++) {
            assert(buckets[i] < buckets[i+1]);
        }
        unsigned int unique_lock_count = get_unique_lock_indexes_fast(buckets, buckets_per_lock, unique_lock_indexes, MAX_LOCKS);
        break_lock_indexes_into_chunks_fast(unique_lock_indexes, unique_lock_count, locks_per_message, fast_lock_chunks);

        // #define CHECK_WITH_OLD_UNLOCK 
        #ifdef CHECK_WITH_OLD_UNLOCK
        //Do the same thing as the old method
        vector<unsigned int> unique_lock_indexes_slow = get_unique_lock_indexes(buckets, buckets_per_lock);
        assert(unique_lock_count == unique_lock_indexes_slow.size());
        for (int i=0; i<unique_lock_count; i++) {
            assert(unique_lock_indexes[i] == unique_lock_indexes_slow[i]);
        }
        //Now we need to make sure that everything is okay for testing purposes
        vector<vector<unsigned int>> test_locks_chunked = break_lock_indexes_into_chunks(unique_lock_indexes_slow, locks_per_message);
        //Test one way
        for (int i = 0; i < test_locks_chunked.size(); i++) {
            for (int j = 0; j < test_locks_chunked[i].size(); j++) {
                printf("1) test_locks_chunked[%d][%d] = %u\n", i, j, test_locks_chunked[i][j]);
                printf("1)      locks_chunked[%d][%d] = %u\n", i, j, fast_lock_chunks[i][j]);
                if (test_locks_chunked[i][j] != fast_lock_chunks[i][j]) {
                    printf("ERROR: lock chunking failed\n");
                    exit(1);
                }
            }
        }
        //Test the other way

        for (int i=0; i<fast_lock_chunks.size(); i++) {
            for (int j=0; j<fast_lock_chunks[i].size(); j++) {
                printf("2) test_locks_chunked[%d][%d] = %u\n", i, j, test_locks_chunked[i][j]);
                printf("2)      locks_chunked[%d][%d] = %u\n", i, j, fast_lock_chunks[i][j]);
                if (test_locks_chunked[i][j] != fast_lock_chunks[i][j]) {
                    printf("ERROR: lock chunking failed 2\n");
                    exit(1);
                }
            }
        }
        #endif

        // vector<vector<unsigned int>> lock_chunks = break_lock_indexes_into_chunks(unique_lock_indexes_slow, locks_per_message);
        if (locking) {
            lock_chunks_to_masked_cas_data(fast_lock_chunks, masked_cas_data);
        } else {
            unlock_chunks_to_masked_cas_data(fast_lock_chunks, masked_cas_data);
        }
        //Masked cas data should be filled at this point

    }

    void get_lock_or_unlock_list_fast_context(LockingContext & context){
        assert(context.locks_per_message <= 64);
        // vector<vector<unsigned int>> fast_lock_chunks;
        context.lock_list.clear();
        if (context.fast_lock_chunks.size() == 0) {
            context.fast_lock_chunks.resize(MAX_LOCKS);
            for (int i=0; i<MAX_LOCKS; i++) {
                context.fast_lock_chunks[i].resize(context.locks_per_message);
            }
        }
        // if(context.buckets.size() > MAX_LOCKS){
        //     ALERT("get-lock-unlock","what the heck, trying to lock %d buckets locking=%d",context.buckets.size(),context.locking);
        // }
        // assert(context.buckets.size() <= MAX_LOCKS);
        unsigned int unique_lock_indexes[MAX_LOCKS];

        //Lets make sure that all the buckets are allready sorted
        //I'm 90% sure this will get optimized out in the end
        // for (int i=0; i<context.buckets.size()-1; i++) {
        //     assert(context.buckets[i] < context.buckets[i+1]);
        // }
        unsigned int unique_lock_count = get_unique_lock_indexes_fast(context.buckets, context.buckets_per_lock, unique_lock_indexes, MAX_LOCKS);
        // break_lock_indexes_into_chunks_fast(unique_lock_indexes, unique_lock_count, context.locks_per_message, context.fast_lock_chunks);
        context.lock_indexes_size = unique_lock_count;
        context.lock_indexes = unique_lock_indexes;
        break_lock_indexes_into_chunks_fast_context(context);

        // #define CHECK_WITH_OLD_UNLOCK 
        #ifdef CHECK_WITH_OLD_UNLOCK
        //Do the same thing as the old method
        vector<unsigned int> unique_lock_indexes_slow = get_unique_lock_indexes(buckets, buckets_per_lock);
        assert(unique_lock_count == unique_lock_indexes_slow.size());
        for (int i=0; i<unique_lock_count; i++) {
            assert(unique_lock_indexes[i] == unique_lock_indexes_slow[i]);
        }
        //Now we need to make sure that everything is okay for testing purposes
        vector<vector<unsigned int>> test_locks_chunked = break_lock_indexes_into_chunks(unique_lock_indexes_slow, locks_per_message);
        //Test one way
        for (int i = 0; i < test_locks_chunked.size(); i++) {
            for (int j = 0; j < test_locks_chunked[i].size(); j++) {
                printf("1) test_locks_chunked[%d][%d] = %u\n", i, j, test_locks_chunked[i][j]);
                printf("1)      locks_chunked[%d][%d] = %u\n", i, j, fast_lock_chunks[i][j]);
                if (test_locks_chunked[i][j] != fast_lock_chunks[i][j]) {
                    printf("ERROR: lock chunking failed\n");
                    exit(1);
                }
            }
        }
        //Test the other way

        for (int i=0; i<fast_lock_chunks.size(); i++) {
            for (int j=0; j<fast_lock_chunks[i].size(); j++) {
                printf("2) test_locks_chunked[%d][%d] = %u\n", i, j, test_locks_chunked[i][j]);
                printf("2)      locks_chunked[%d][%d] = %u\n", i, j, fast_lock_chunks[i][j]);
                if (test_locks_chunked[i][j] != fast_lock_chunks[i][j]) {
                    printf("ERROR: lock chunking failed 2\n");
                    exit(1);
                }
            }
        }
        #endif

        // vector<vector<unsigned int>> lock_chunks = break_lock_indexes_into_chunks(unique_lock_indexes_slow, locks_per_message);
        if (context.locking) {
            lock_chunks_to_masked_cas_data_context(context);
            // lock_chunks_to_masked_cas_data(context.fast_lock_chunks, context.lock_list);
        } else {
            unlock_chunks_to_masked_cas_data_context(context);
        }
        //Masked cas data should be filled at this point

    }

    unsigned int get_unique_lock_indexes_fast(vector<unsigned int> &buckets, unsigned int buckets_per_lock, unsigned int *unique_buckets, unsigned int unique_buckets_size)
    {
        // assert(unique_buckets_size >= buckets.size());
        assert(unique_buckets);

        unique_buckets[0] = buckets[0] / buckets_per_lock;
        unsigned int unique_index = 1;
        for (int i=1; i< buckets.size(); i++){
            unsigned int lock_index = buckets[i] / buckets_per_lock;
            if (unique_buckets[unique_index-1] == lock_index) [[unlikely]]{
                continue;
            } 
            unique_buckets[unique_index] = lock_index;
            unique_index++;
            assert(unique_index < MAX_LOCKS);
        }
        VERBOSE("unique locks", "found a total of %d unique lock indexes out of %d buckets\n", unique_index, buckets.size());
        return unique_index;
    }

    vector<VRMaskedCasData> get_lock_list(vector<unsigned int> buckets, unsigned int buckets_per_lock, unsigned int locks_per_message) {
        bool get_lock_messages = true;
        return get_lock_or_unlock_list(buckets,buckets_per_lock,locks_per_message, get_lock_messages);
    }

    vector<VRMaskedCasData> get_unlock_list(vector<unsigned int> buckets, unsigned int buckets_per_lock, unsigned int locks_per_message) {
        bool get_lock_messages = false;
        return get_lock_or_unlock_list(buckets,buckets_per_lock,locks_per_message, get_lock_messages);
    }

    void get_lock_list_fast(vector<unsigned int> &buckets,vector<vector<unsigned int>> &fast_lock_chunks, vector<VRMaskedCasData> &lock_list, unsigned int buckets_per_lock, unsigned int locks_per_message) {
        bool get_lock_messages = true;
        // return get_lock_or_unlock_list(buckets,buckets_per_lock,locks_per_message, get_lock_messages);
        get_lock_or_unlock_list_fast(buckets, fast_lock_chunks, lock_list, buckets_per_lock,locks_per_message, get_lock_messages);
    }

    void get_unlock_list_fast(vector<unsigned int> &buckets,vector<vector<unsigned int>> &fast_lock_chunks, vector<VRMaskedCasData> &lock_list, unsigned int buckets_per_lock, unsigned int locks_per_message) {
        bool get_lock_messages = false;
        // return get_lock_or_unlock_list(buckets,buckets_per_lock,locks_per_message, get_lock_messages);
        get_lock_or_unlock_list_fast(buckets,fast_lock_chunks, lock_list, buckets_per_lock,locks_per_message, get_lock_messages);
    }

    void get_lock_list_fast_context(LockingContext &context){
        context.locking=true;
        get_lock_or_unlock_list_fast_context(context);
    }

    void get_unlock_list_fast_context(LockingContext &context){
        context.locking=false;
        get_lock_or_unlock_list_fast_context(context);
    }

    VRMessage read_request_message(unsigned int start_bucket, unsigned int offset, unsigned int size) {
        VRMessage message;
        message.function = message_type_to_function_string(READ_REQUEST);
        message.function_args["bucket_id"] = to_string(start_bucket);
        message.function_args["bucket_offset"] = to_string(offset);
        message.function_args["size"] = to_string(size);
        return message;
    }

    VRReadData read_request_data(unsigned int start_bucket, unsigned int offset, unsigned int size) {
        VRReadData message;
        message.size = size;
        message.row = start_bucket;
        message.offset = offset;
        return message;
    }

    vector<VRMessage> multi_bucket_read_message(hash_locations buckets, unsigned int row_size_bytes) {
        vector<VRMessage> messages;
        unsigned int min_bucket = buckets.min_bucket();
        unsigned int size = single_read_size_bytes(buckets, row_size_bytes);
        VRMessage message = read_request_message(min_bucket, 0, size);
        messages.push_back(message);
        return messages;
    }

    void multi_bucket_read_message(vector<VRReadData> & messages, hash_locations buckets, unsigned int row_size_bytes) {
        messages.clear();
        unsigned int min_bucket = buckets.min_bucket();
        unsigned int size = single_read_size_bytes(buckets, row_size_bytes);
        VRReadData message = read_request_data(min_bucket, 0, size);
        messages.push_back(message);
    }

    VRMessage single_bucket_read_message(unsigned int bucket, unsigned int row_size_bytes){
        return read_request_message(bucket, 0, row_size_bytes);
    }

    vector<VRMessage> single_bucket_read_messages(hash_locations buckets, unsigned int row_size_bytes){
        vector<VRMessage> messages;
        messages.push_back(single_bucket_read_message(buckets.primary, row_size_bytes));
        messages.push_back(single_bucket_read_message(buckets.secondary, row_size_bytes));
        return messages;

    }

    void single_bucket_read_messages(vector<VRReadData> & messages, hash_locations buckets, unsigned int row_size_bytes){
        messages.clear();
        messages.push_back(read_request_data(buckets.primary, 0, row_size_bytes));
        messages.push_back(read_request_data(buckets.secondary, 0, row_size_bytes));
    }

    vector<VRMessage> read_threshold_message(hash_locations (*location_function)(Key, unsigned int), Key key, unsigned int read_threshold_bytes,unsigned int table_size,unsigned int row_size_bytes) {
        hash_locations buckets = location_function(key.to_string(), table_size);
        VERBOSE("read_threshold_message", "buckets are %s", buckets.to_string().c_str());
        vector<VRMessage> messages;
        if (single_read_size_bytes(buckets, row_size_bytes) <= read_threshold_bytes) {
            messages = multi_bucket_read_message(buckets, row_size_bytes);
        } else {
            messages = single_bucket_read_messages(buckets, row_size_bytes);
        }
        return messages;
    }

    void read_theshold_message(vector<VRReadData> & messages, hash_locations (*location_function)(Key, unsigned int), Key key, unsigned int read_threshold_bytes,unsigned int table_size,unsigned int row_size_bytes){
        hash_locations buckets = location_function(key, table_size);
        VERBOSE("read_threshold_message", "buckets are %s", buckets.to_string().c_str());
        if (single_read_size_bytes(buckets, row_size_bytes) <= read_threshold_bytes) {
            multi_bucket_read_message(messages, buckets, row_size_bytes);
        } else {
            single_bucket_read_messages(messages, buckets, row_size_bytes);
        }
    }

    VRMessage create_masked_cas_message_from_lock_list(VRMaskedCasData masked_cas_data) {
        VRMessage message;
        message.function = message_type_to_function_string(MASKED_CAS_REQUEST);
        message.function_args["lock_index"] = to_string(masked_cas_data.min_lock_index);



        message.function_args["old"] = uint64t_to_bin_string(masked_cas_data.old);
        message.function_args["new"] = uint64t_to_bin_string(masked_cas_data.new_value);
        message.function_args["mask"] = uint64t_to_bin_string(masked_cas_data.mask);
        return message;
    }

    vector<VRMessage> create_masked_cas_messages_from_lock_list(vector<VRMaskedCasData> masked_cas_list) {
        VERBOSE("create_masked_cas_messages_from_lock_list", "ENTRY");
        vector<VRMessage> messages;
        for (int i=0; i<masked_cas_list.size(); i++) {
            messages.push_back(create_masked_cas_message_from_lock_list(masked_cas_list[i]));
        }
        VERBOSE("create_masked_cas_messages_from_lock_list", "EXIT");
        return messages;
    }

    VRMessage get_covering_read_from_lock_message(VRMessage lock_message, unsigned int buckets_per_lock, unsigned int row_size_bytes) {

        vector<unsigned int> lock_indexes = lock_message_to_lock_indexes(lock_message);
        unsigned int min_index = *min_element(lock_indexes.begin(), lock_indexes.end());
        unsigned int max_index = *max_element(lock_indexes.begin(), lock_indexes.end());
        // printf("base_index: %d min_index: %d, max_index: %d\n", base_index, min_index, max_index);
        VERBOSE("get_covering_read", "min_bucket: %d, max_bucket: %d\n", min_index, max_index);

        hash_locations buckets;
        buckets.primary = (min_index) * buckets_per_lock;
        buckets.secondary = (max_index) * buckets_per_lock + (buckets_per_lock - 1);

        VRMessage read_message = multi_bucket_read_message(buckets, row_size_bytes)[0];
        return read_message;
    }

    VRReadData get_covering_read_from_lock(VRMaskedCasData masked_cas, unsigned int buckets_per_lock, unsigned int row_size_bytes) {
        #define BITS_PER_BYTE 8
        VRReadData read_data;
        hash_locations buckets;


        buckets.primary = (masked_cas.min_set_lock) * buckets_per_lock;
        buckets.secondary = (masked_cas.max_set_lock) * buckets_per_lock + (buckets_per_lock - 1);



        read_data.size= single_read_size_bytes(buckets, row_size_bytes);
        read_data.row = (masked_cas.min_set_lock + (BITS_PER_BYTE * masked_cas.min_lock_index)) * buckets_per_lock;
        // read_data.row = buckets.primary;
        read_data.offset = 0;
        // ALERT("get_covering_read_from_lock", "min_bucket: %d, max_bucket: %d size %d\n", buckets.primary, buckets.secondary, read_data.size);
        return read_data;
    }

    void get_covering_reads_from_lock_list(vector<VRMaskedCasData> &masked_cas_list, vector<VRReadData> &read_data_list, unsigned int buckets_per_lock, unsigned int row_size_bytes) {
        read_data_list.clear();
        for (int i=0; i<masked_cas_list.size(); i++) {
            read_data_list.push_back(get_covering_read_from_lock(masked_cas_list[i], buckets_per_lock, row_size_bytes));
        }
        return;
    }


    VRMessage cas_table_entry_message(unsigned int bucket_index, unsigned int bucket_offset, Key old, Key new_value) {
        VRMessage cas_message;
        cas_message.function = message_type_to_function_string(CAS_REQUEST);
        cas_message.function_args["bucket_id"] = to_string(bucket_index);
        cas_message.function_args["bucket_offset"] = to_string(bucket_offset);
        cas_message.function_args["old"] = old.to_string();
        cas_message.function_args["new"] = new_value.to_string();
        return cas_message;
    }

    VRCasData cas_table_entry_cas_data(unsigned int bucket_index, unsigned int bucket_offset, Key old, Key new_value) {
        VRCasData cas_message;
        cas_message.row = bucket_index;
        cas_message.offset = bucket_offset;
        cas_message.old = old.to_uint64_t();
        cas_message.new_value = new_value.to_uint64_t();
        return cas_message;
    }

    VRMessage next_cas_message(vector<path_element> search_path, unsigned int index) {
        path_element insert_pe = search_path[index];
        path_element copy_pe = search_path[index+1];
        return cas_table_entry_message(insert_pe.bucket_index, insert_pe.offset, insert_pe.key, copy_pe.key);
    }

    VRCasData next_cas_data(vector<path_element> search_path, unsigned int index) {
        path_element insert_pe = search_path[index];
        path_element copy_pe = search_path[index+1];
        return cas_table_entry_cas_data(insert_pe.bucket_index, insert_pe.offset, insert_pe.key, copy_pe.key);
    }

    vector<VRMessage> gen_cas_messages(vector<path_element> search_path) {
        vector<VRMessage> cas_messages;
        for (int i=0; i<search_path.size()-1; i++) {
            cas_messages.push_back(next_cas_message(search_path,i));
        }
        INFO("gen_cas_message", "Generated %d cas messages", cas_messages.size());
        return cas_messages;
    }

    void gen_cas_data(vector<path_element>& search_path, vector<VRCasData>& cas_messages) {

        cas_messages.clear();
        for (int i=0; i<search_path.size()-1; i++) {
            cas_messages.push_back(next_cas_data(search_path,i));
        }
        INFO("gen_cas_message", "Generated %d cas messages", cas_messages.size());
    }

    /*------------------------------------ Real RDMA functions ---------------------------- */
}