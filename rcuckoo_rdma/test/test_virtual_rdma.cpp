#include "../util.h"
#include "../virtual_rdma.h"
#include <vector>
#include <cassert>
#include <iostream>

using namespace std;
using namespace cuckoo_virtual_rdma;

static const unsigned int buckets_per_lock = 1;
static const unsigned int locks_per_message = 64;

string s(const char * c) {
    return string(c);
}

bool test_0() {
    printf("basic test, can we lock the first index\n");
    vector<unsigned int> buckets = {0};
    vector<VRMaskedCasData> masked_cas_data = get_lock_list(buckets, buckets_per_lock, locks_per_message);

    assert(masked_cas_data.size() == 1);
    VRMaskedCasData mcd = masked_cas_data[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("10000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    if (uint64t_to_bin_string(mcd.mask) == outcome) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

bool test_1() {
    printf("basic test, can we lock the first two indexes\n");
    vector<unsigned int> buckets = {0,1};
    vector<VRMaskedCasData> masked_cas_data = get_lock_list(buckets, buckets_per_lock, locks_per_message);

    assert(masked_cas_data.size() == 1);
    VRMaskedCasData mcd = masked_cas_data[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("11000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    if (uint64t_to_bin_string(mcd.mask) == outcome) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

bool test_2 () {
    printf("basic test, get a mask with an index of 1 shifted\n");
    vector<unsigned int> buckets = {8};
    vector<VRMaskedCasData> masked_cas_data = get_lock_list(buckets, buckets_per_lock, locks_per_message);

    assert(masked_cas_data.size() == 1);
    VRMaskedCasData mcd = masked_cas_data[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("10000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    int min_index = 1;
    if (uint64t_to_bin_string(mcd.mask) == outcome && mcd.min_lock_index == min_index) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

bool test_3 () {
    printf("basic test, get a mask with an index of 1 shifted\n");
    vector<unsigned int> buckets = {47, 30, 19};
    vector<VRMaskedCasData> masked_cas_data = get_lock_list(buckets, buckets_per_lock, locks_per_message);

    assert(masked_cas_data.size() == 1);
    VRMaskedCasData mcd = masked_cas_data[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("00010000") + s("00000010") + s("00000000") + s("000000001") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    int min_index = 1;
    if (uint64t_to_bin_string(mcd.mask) == outcome && mcd.min_lock_index == min_index) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

int main() {

    test_0();
    test_1();
    test_2();
    test_3();

    //Translate a single lock
    // vector<unsigned int> lock_indexes = get_unique_lock_indexes(buckets, buckets_per_lock);
    // for (auto i : lock_indexes){
    //     cout << "unique lock indexes " << i << endl;
    // }

    // vector<VRMaskedCasData> masked_cas_data = get_lock_or_unlock_list(buckets, buckets_per_lock, locks_per_message, true);

    // for (auto i : masked_cas_data){
    //     cout << "Masked Cas Data: " << i.to_string() << endl;
    // }

    // vector<VRMessage> masked_cas_messages = create_masked_cas_messages_from_lock_list(masked_cas_data);

    // for (auto i : masked_cas_messages){
    //     cout << "Masked Cas Message: " << i.to_string() << endl;
    // }

    // vector<unsigned int> lock_indexes2 = {0,1,10};
    // vector<unsigned int> lock_indexes2 = {10};

    // for (auto i : lock_indexes2){
    //     cout << "byte aligned index: " <<  i << "->" << byte_aligned_index(i) << endl;
    // }
    // vector<vector<unsigned int>> lock_indexes_list = break_lock_indexes_into_chunks(lock_indexes2, locks_per_message);
    // cout << "number of chunks " << lock_indexes_list.size() << endl;
    // for (auto i : lock_indexes_list){
    //     cout << "Lock Indexes: ";
    //     for (auto j : i){
    //         cout << j << " ";
    //     }
    //     cout << "end index " << endl;
    // }
    // vector<VRMaskedCasData> masked_cas_data_2  = lock_chunks_to_masked_cas_data(lock_indexes_list);

    // for (auto i : masked_cas_data_2){
    //     cout << "Masked Cas Data: " << i.to_string() << endl;
    // }

    // cout << "done!" << endl;

}