#pragma once
#ifndef CONFIG_H
#define CONFIG_H

using namespace std;
#include <string>

const string SERVER_TABLE_CONFIG_KEY = "server_table_config";

typedef struct table_config {
    string to_string() {
        return "table_address: " + std::to_string(table_address) + "\n" +
            "table_size_bytes: " + std::to_string(table_size_bytes) + "\n" +
            "num_rows: " + std::to_string(num_rows) + "\n" +
            "remote_key: " + std::to_string(remote_key) + "\n" +
            "buckets_per_row: " + std::to_string(buckets_per_row) + "\n" +
            "entry_size_bytes: " + std::to_string(entry_size_bytes) + "\n" +
            "lock_table_address: " + std::to_string(lock_table_address) + "\n" +
            "lock_table_size_bytes: " + std::to_string(lock_table_size_bytes) + "\n";
    }
    uint64_t table_address;
    int table_size_bytes;
    int num_rows;
    int remote_key;
    int buckets_per_row;
    int entry_size_bytes;
    uint64_t lock_table_address;
    int lock_table_size_bytes;
} table_config;

#endif