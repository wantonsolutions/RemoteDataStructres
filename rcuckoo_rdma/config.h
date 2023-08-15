#pragma once
#ifndef CONFIG_H
#define CONFIG_H

using namespace std;
#include <string>
#include <unordered_map>
#include <vector>

const string SERVER_TABLE_CONFIG_KEY = "server_table_config";
const string EXPERIMENT_CONTROL_KEY = "experiment_control";
const string MEMORY_STATS_KEY = "memory_stats";

typedef struct table_config {
    string to_string() {
        return "table_address: " + std::to_string(table_address) + "\n" +
            "table_size_bytes: " + std::to_string(table_size_bytes) + "\n" +
            "num_rows: " + std::to_string(num_rows) + "\n" +
            "remote_key: " + std::to_string(remote_key) + "\n" +
            "buckets_per_row: " + std::to_string(buckets_per_row) + "\n" +
            "entry_size_bytes: " + std::to_string(entry_size_bytes) + "\n" +
            "lock_table_address: " + std::to_string(lock_table_address) + "\n" +
            "lock_table_size_bytes: " + std::to_string(lock_table_size_bytes) + "\n" +
            "lock_table_key: " + std::to_string(lock_table_key) + "\n";
    }
    uint64_t table_address;
    int table_size_bytes;
    int num_rows;
    int remote_key;
    int buckets_per_row;
    int entry_size_bytes;
    uint64_t lock_table_address;
    int lock_table_key;
    int lock_table_size_bytes;
} table_config;

typedef struct experiment_control {
    string to_string(){
        return "experiment_start: " + std::to_string(experiment_start) + "\n" +
            "experiment_stop: " + std::to_string(experiment_stop) + "\n" +
            "priming_complete: " + std::to_string(priming_complete) + "\n";
    }
    bool experiment_start;
    bool experiment_stop;
    bool priming_complete;
} experiment_control;

typedef struct memory_stats {
    bool finished_run;
    float fill;
} memory_stats;


unordered_map<string, string> read_config_from_file(string config_filename);
void write_statistics(
    unordered_map<string, string> config, 
    unordered_map<string,string> system_stats, 
    vector<unordered_map<string,string>> client_stats,
    unordered_map<string,string> memory_stats
    );

#endif