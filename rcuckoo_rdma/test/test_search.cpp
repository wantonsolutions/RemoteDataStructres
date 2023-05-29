#include "../search.h"
#include <iostream>

using namespace std;
using namespace cuckoo_search;

int test_0() {
    //Test 0 make sure we return the correct result on a basic path
    vector<path_element> path;
    for (int i=0; i < 5; i++){
        path_element pe;
        pe.bucket_index=i;
        pe.table_index=i%2;
        path.push_back(pe);
    }
    vector<unsigned int> expected_result = {0, 1, 2, 3, 4};
    vector<unsigned int> buckets = search_path_to_buckets(path);
    if (buckets == expected_result){
        return 0;
    } else {
        return 1;
    }
}

int test_1() {
    //Test 1 make sure we return the correct result on a basic path
    vector<path_element> path;
    for (int i=0; i < 5; i++){
        path_element pe;
        pe.bucket_index=i;
        pe.table_index=i%2;
        path.push_back(pe);
        path.push_back(pe);
    }
    vector<unsigned int> expected_result = {0, 1, 2, 3, 4};
    vector<unsigned int> buckets = search_path_to_buckets(path);
    if (buckets == expected_result){
        return 0;
    } else {
        return 1;
    }
}

int main() {
    vector<int (*)()> tests = {test_0, test_1};
    for (int i=0; i < tests.size(); i++){
        int result = tests[i]();
        if (result == 0){
            cout << "Test " << i << " passed" << endl;
        } else {
            cout << "Test " << i << " failed" << endl;
        }
    }

    cout << "Hello Search Test!" << endl;
}