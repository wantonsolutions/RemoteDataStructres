#include "../hash.h"
#include <iostream>
#include <algorithm>


int mean(std::vector<int> distances) {
    int sum = 0;
    for (int i = 0; i < distances.size(); i++) {
        sum += distances[i];
    }
    // printf("sum: %d\n", sum);
    if (distances.size() == 0) {
        return 0;
    } else {
        return sum / distances.size();
    }
}

int median(std::vector<int> distances) {
    std::sort(distances.begin(), distances.end());
    int middle = distances.size() / 2;
    return distances[middle];
}

const std::vector<std::pair<float,int>> median_distances {
{1.000000,1},
{1.200000,1},
{1.300000,1},
{1.400000,1},
{1.500000,1},
{1.600000,1},
{1.700000,1},
{1.800000,1},
{1.900000,3},
{2.000000,3},
{2.000000,3},
{2.100000,3},
{2.200000,3},
{2.300000,5},
{2.400000,5},
{2.500000,7},
{2.600000,9},
{2.700000,11},
{2.800000,13},
{2.900000,17},
{3.000000,21},
{3.100000,25},
{3.200000,33},
{3.300000,41},
{3.400000,51},
{3.500000,65},
{3.600000,87},
{3.700000,107},
{3.800000,147},
{3.900000,187},
{4.000000,247},
};

int calculate_mean_distance() {
    std::vector<int> distances;
    int measures = 10000;
    int table_size = 50000;
    for (int i = 0; i < measures; i++) {
        int key_value = i + 32000;
        string key = std::to_string(key_value);
        // printf("hashing key %s\n", key.c_str());
        hash_locations h1 = rcuckoo_hash_locations(key, table_size);
        // printf("h1: %d %d\n", h1.primary, h1.secondary);
        if (h1.primary > h1.secondary) {
            // printf("primary is greater than secondary\n");
            continue;
        }
        int distance = h1.secondary - h1.primary;
        distances.push_back(distance);
    }

    return median(distances);
}

void calculate_mean_distances() {
    std::vector<float> factors { 
        1.0, 1.2, 1.3, 1.4, 1.5, 1.6,1.7, 1.8, 1.9, 2.0,
        2.0,2.1,2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9,
        3.0, 3.1, 3.2, 3.3, 3.4, 3.5,3.6, 3.7, 3.8, 3.9, 4.0};
    for (int i = 0; i < factors.size(); i++) {
        // printf("setting factor %f\n ", factors[i]);
        set_factor(factors[i]);
        int mean_distance = calculate_mean_distance();
        printf("{%f,%d},\n", factors[i], mean_distance);
    }
}

void most_basic_test() {
    cout << "Hello Hash Test!" << endl;
    hash_locations h1 = rcuckoo_hash_locations("hello", 100);
    cout << "h1: " << h1.primary << " " << h1.secondary << endl;
}


int main() {
    calculate_mean_distances();
    // set_factor(2.1);
    // int mean_distance = calculate_mean_distance();
    // printf("mean distance: %d\n", mean_distance);
    
}