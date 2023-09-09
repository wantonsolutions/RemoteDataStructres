#include "../util.h"
#include "../virtual_rdma.h"
#include <vector>
#include <cassert>
#include <iostream>
#include <chrono>

using namespace std;
using namespace cuckoo_virtual_rdma;

static const unsigned int buckets_per_lock = 1;
static const unsigned int locks_per_message = 64;

string s(const char * c) {
    return string(c);
}

bool test_0() {
    string test_name = "basic test, can we lock the first index";
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

bool test_0_fast_context() {

    LockingContext context;
    context.locks_per_message = 64;
    context.buckets_per_lock = 1;
    context.buckets = {0};
    get_lock_list_fast_context(context);

    assert(context.lock_list.size() == 1);
    VRMaskedCasData mcd = context.lock_list[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("00000001") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    if (uint64t_to_bin_string(mcd.mask) == outcome) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

bool test_1() {
    string test_name = "basic test, can we lock the first two indexes";
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

bool test_1_fast_context() {

    LockingContext context;
    context.locks_per_message = 64;
    context.buckets_per_lock = 1;
    context.buckets = {0,1};
    get_lock_list_fast_context(context);

    assert(context.lock_list.size() == 1);
    VRMaskedCasData mcd = context.lock_list[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("00000011") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    if (uint64t_to_bin_string(mcd.mask) == outcome) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

bool test_2 () {
    string test_name ="basic test, get a mask with an index of 1 shifted";
    vector<unsigned int> buckets = {8};
    vector<VRMaskedCasData> masked_cas_data = get_lock_list(buckets, buckets_per_lock, locks_per_message);

    assert(masked_cas_data.size() == 1);
    VRMaskedCasData mcd = masked_cas_data[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("00000000") + s("10000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    int min_index = 0;
    if (uint64t_to_bin_string(mcd.mask) == outcome && mcd.min_lock_index == min_index) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

bool test_2_fast_context() {

    LockingContext context;
    context.locks_per_message = 64;
    context.buckets_per_lock = 1;
    context.buckets = {8};
    get_lock_list_fast_context(context);

    assert(context.lock_list.size() == 1);
    VRMaskedCasData mcd = context.lock_list[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("00000000") + s("00000001") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    if (uint64t_to_bin_string(mcd.mask) == outcome) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}


bool test_3 () {
    string test_name = "basic test, get a mask with an index of 8 shifted";
    vector<unsigned int> buckets = {64};
    vector<VRMaskedCasData> masked_cas_data = get_lock_list(buckets, buckets_per_lock, locks_per_message);

    assert(masked_cas_data.size() == 1);
    VRMaskedCasData mcd = masked_cas_data[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("10000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    int min_index = 8;
    if (uint64t_to_bin_string(mcd.mask) == outcome && mcd.min_lock_index == min_index) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

bool test_3_fast_context() {

    LockingContext context;
    context.locks_per_message = 64;
    context.buckets_per_lock = 1;
    context.buckets = {64};
    get_lock_list_fast_context(context);

    assert(context.lock_list.size() == 1);
    VRMaskedCasData mcd = context.lock_list[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("00000001") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    if (uint64t_to_bin_string(mcd.mask) == outcome) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

bool test_4(){
    string test_name = "make sure that we get two masked cas when the buckets are 64 apart";
    vector<unsigned int> buckets = {0,64};
    vector<VRMaskedCasData> masked_cas_data = get_lock_list(buckets, buckets_per_lock, locks_per_message);

    assert(masked_cas_data.size() == 2);
    VRMaskedCasData mcd_0 = masked_cas_data[0];
    VRMaskedCasData mcd_1 = masked_cas_data[1];
    printf("Masked Cas Data 0 : %s\n", mcd_0.to_string().c_str());
    printf("Masked Cas Data 1 : %s\n", mcd_1.to_string().c_str());

    string outcome_0  = s("10000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    int min_index_0 = 0;

    string outcome_1  = s("10000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    int min_index_1 = 8;
    if (
        uint64t_to_bin_string(mcd_0.mask) == outcome_0 && mcd_0.min_lock_index == min_index_0 &&
        uint64t_to_bin_string(mcd_1.mask) == outcome_1 && mcd_1.min_lock_index == min_index_1
        ) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

bool test_4_fast_context() {

    LockingContext context;
    context.locks_per_message = 64;
    context.buckets_per_lock = 1;
    context.buckets = {0,64};
    get_lock_list_fast_context(context);

    assert(context.lock_list.size() == 2);

    VRMaskedCasData mcd_0 = context.lock_list[0];
    VRMaskedCasData mcd_1 = context.lock_list[1];
    printf("Masked Cas Data 0 : %s\n", mcd_0.to_string().c_str());
    printf("Masked Cas Data 1 : %s\n", mcd_1.to_string().c_str());

    string outcome_0  = s("00000001") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    int min_index_0 = 0;

    string outcome_1  = s("00000001") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    int min_index_1 = 8;
    if (
        uint64t_to_bin_string(mcd_0.mask) == outcome_0 && mcd_0.min_lock_index == min_index_0 &&
        uint64t_to_bin_string(mcd_1.mask) == outcome_1 && mcd_1.min_lock_index == min_index_1
        ) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }

}

bool compare_test() {

    vector<unsigned int> buckets = {1,2,5,7,9};
    // buckets.push_back(0);

    vector<vector<unsigned int>> fast_lock_chunks;
    vector<VRMaskedCasData> lock_list;

    LockingContext context;
    context.locks_per_message = locks_per_message;
    context.buckets_per_lock = buckets_per_lock;
    context.fast_lock_chunks = vector<vector<unsigned int>>();
    // context.lock_list = vector<VRMaskedCasData>();
    context.buckets = buckets;
    const int itterations = 10000;
    for (int i = 0; i < itterations; i++) { 
        buckets.clear();
        for (int j = 0; j < 5; j++) {
            buckets.push_back(rand() % 100);
        }
        sort(buckets.begin(), buckets.end());
        for(int j = 0; j < buckets.size(); j++) {
            if (j > 0 && buckets[j] <= buckets[j-1]) {
                buckets[j] = buckets[j-1] + 1;
            }
        }
        // for (int j = 0; j < buckets.size(); j++) {
        //     printf("%d ", buckets[j]);
        // }
        // printf("\n");
        context.buckets = buckets;
        context.lock_list.clear();
        lock_list.clear();
        get_lock_list_fast(buckets,fast_lock_chunks,lock_list,buckets_per_lock,locks_per_message);
        get_lock_list_fast_context(context);
        // printf("lock list size %d context lock list size %d\n", lock_list.size(), context.lock_list.size());
        for (int j = 0; j < lock_list.size(); j++) {
            if (lock_list[j].mask != context.lock_list[j].mask || lock_list[j].min_lock_index != context.lock_list[j].min_lock_index) {
                printf("failure at itteration %d\n", i);
                printf("mcd: %s\n", lock_list[j].to_string().c_str());
                printf("mcd_context: %s\n", context.lock_list[j].to_string().c_str());
                return false;
            }
        }
    }
    printf("success!\n");

}

bool perftest() {
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;
    const int itterations = 100000000;



    vector<unsigned int> buckets = {0};
    vector<vector<unsigned int>> fast_lock_chunks;
    vector<VRMaskedCasData> lock_list;

    // auto t0 = high_resolution_clock::now();
    // for (int i = 0; i < itterations; i++) {
    //     get_lock_list_fast(buckets,fast_lock_chunks,lock_list,buckets_per_lock,locks_per_message);
    // }
    // auto t1 = high_resolution_clock::now();
    // auto duration_0 = duration_cast<milliseconds>( t1 - t0 ).count();
    // cout << "duration_0: " << duration_0 << endl;

    // auto t2 = high_resolution_clock::now();
    // for (int i = 0; i < itterations; i++) {
    //     get_lock_list_fast(buckets,fast_lock_chunks,lock_list,buckets_per_lock,locks_per_message);
    // }
    // auto t3 = high_resolution_clock::now();
    // auto duration_2 = duration_cast<milliseconds>( t3 - t2 ).count();
    // cout << "duration_2: " << duration_2 << endl;

    auto t4 = high_resolution_clock::now();
    LockingContext context;
    context.locks_per_message = locks_per_message;
    context.buckets_per_lock = buckets_per_lock;
    context.buckets = buckets;
    for (int i = 0; i < itterations; i++) {
        get_lock_list_fast_context(context);
    }
    auto t5 = high_resolution_clock::now();
    auto duration_3 = duration_cast<milliseconds>( t5 - t4 ).count();
    cout << "duration_3: " << duration_3 << endl;



    // get_lock_list(buckets, buckets_per_lock, locks_per_message);
    // cout << "time improvement" << float(duration_0)/float(duration_2) << "x" << endl;
}

// bool test_3 () {
//     printf("basic test, get a mask with an index of 1 shifted\n");
//     vector<unsigned int> buckets = {47, 30, 19};
//     vector<VRMaskedCasData> masked_cas_data = get_lock_list(buckets, buckets_per_lock, locks_per_message);

//     assert(masked_cas_data.size() == 1);
//     VRMaskedCasData mcd = masked_cas_data[0];
//     printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

//     string outcome  = s("00010000") + s("00000010") + s("00000000") + s("000000001") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
//     int min_index = 1;
//     if (uint64t_to_bin_string(mcd.mask) == outcome && mcd.min_lock_index == min_index) {
//         printf("success\n");
//         return true;
//     } else {
//         printf("failure\n");
//         return false;
//     }
// }

// bool test_4() {
//     printf("test to ensure that we get 64 bit aligned CAS ");


bool test_0b_fast_context() {
    printf("two buckets per lock first index");
    LockingContext context;
    context.locks_per_message = 64;
    context.buckets_per_lock = 2;
    context.buckets = {0};
    get_lock_list_fast_context(context);

    assert(context.lock_list.size() == 1);
    VRMaskedCasData mcd = context.lock_list[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("00000001") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    if (uint64t_to_bin_string(mcd.mask) == outcome) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

bool test_1b_fast_context() {
    printf("two buckets per lock second index");
    LockingContext context;
    context.locks_per_message = 64;
    context.buckets_per_lock = 2;
    context.buckets = {1};
    get_lock_list_fast_context(context);

    assert(context.lock_list.size() == 1);
    VRMaskedCasData mcd = context.lock_list[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("00000001") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    if (uint64t_to_bin_string(mcd.mask) == outcome) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

bool test_2b_fast_context() {
    printf("two buckets per lock third index");
    LockingContext context;
    context.locks_per_message = 64;
    context.buckets_per_lock = 2;
    context.buckets = {2};
    get_lock_list_fast_context(context);

    assert(context.lock_list.size() == 1);
    VRMaskedCasData mcd = context.lock_list[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("00000010") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    if (uint64t_to_bin_string(mcd.mask) == outcome) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}

bool test_3b_fast_context() {
    printf("two buckets per lock second byte");
    LockingContext context;
    context.locks_per_message = 64;
    context.buckets_per_lock = 2;
    context.buckets = {16};
    get_lock_list_fast_context(context);

    assert(context.lock_list.size() == 1);
    VRMaskedCasData mcd = context.lock_list[0];
    printf("Masked Cas Data: %s\n", mcd.to_string().c_str());

    string outcome  = s("00000000") + s("00000001") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000") + s("00000000");
    if (uint64t_to_bin_string(mcd.mask) == outcome) {
        printf("success\n");
        return true;
    } else {
        printf("failure\n");
        return false;
    }
}


int main() {

    test_0();
    test_0_fast_context();
    test_1_fast_context();
    test_2_fast_context();
    test_3_fast_context();
    test_4_fast_context();

    test_0b_fast_context();
    test_1b_fast_context();
    test_2b_fast_context();
    test_3b_fast_context();
    // test_1();
    // test_2();
    // test_3();
    // test_4();
    // perftest();
    // compare_test();
    // test_3();

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