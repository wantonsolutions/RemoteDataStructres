#include "../virtual_rdma.h"
#include <vector>

#include <iostream>

using namespace std;
using namespace cuckoo_virtual_rdma;

int main() {
    unsigned int memory_size = 1024;
    unsigned int bucket_size = 8;
    unsigned int buckets_per_lock = 1;
    vector<unsigned int> buckets = {1};

    //Translate a single lock
    vector<unsigned int> lock_indexes = get_unique_lock_indexes(buckets, buckets_per_lock);
    for (auto i : lock_indexes){
        cout << i << endl;
    }

    vector<VRMaskedCasData> masked_cas_data = get_lock_or_unlock_list(buckets, buckets_per_lock, 1, true);

    for (auto i : masked_cas_data){
        cout << "Masked Cas Data: " << i.to_string() << endl;
    }

    vector<VRMessage> masked_cas_messages = create_masked_cas_messages_from_lock_list(masked_cas_data);

    for (auto i : masked_cas_messages){
        cout << "Masked Cas Message: " << i.to_string() << endl;
    }

    vector<unsigned int> lock_indexes2 = {1};

    for (auto i : lock_indexes2){
        cout << "byte aligned index: " <<  i << "->" << byte_aligned_index(i) << endl;
    }
    vector<vector<unsigned int>> lock_indexes_list = break_lock_indexes_into_chunks(lock_indexes2, buckets_per_lock);
    for (auto i : lock_indexes_list){
        cout << "Lock Indexes: ";
        for (auto j : i){
            cout << j << " ";
        }
        cout << endl;
    }
    vector<VRMaskedCasData> masked_cas_data_2  = lock_chunks_to_masked_cas_data(lock_indexes_list);

    for (auto i : masked_cas_data_2){
        cout << "Masked Cas Data: " << i.to_string() << endl;
    }

    cout << "done!" << endl;




}