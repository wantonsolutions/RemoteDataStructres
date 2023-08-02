#include "config.h"
#include <jsoncpp/json/json.h>
#include <iostream>
#include <fstream>
#include "log.h"

#include <unordered_map>


inline bool file_exists (const std::string& name) {
    ifstream f(name.c_str());
    return f.good();
}

unordered_map<string, string> read_config_from_file(string config_filename){

    if(!file_exists(config_filename)){
        printf("ERROR: config file %s does not exist\n", config_filename.c_str());
        exit(1);
    }

    printf("Client Input Config: %s\n",config_filename.c_str());

    ifstream ifs(config_filename);
    Json::Reader reader;
    Json::Value obj;
    reader.parse(ifs, obj); // reader can also read strings

    unordered_map<string, string> config;
    for(Json::Value::iterator it = obj.begin(); it != obj.end(); ++it) {
        printf("%-30s : %s\n", it.key().asString().c_str(), it->asString().c_str());
        // cout << it.key().asString() << "  " << it->asString() <<endl;
        config[it.key().asString()] = it->asString();
    }
    return config;
}

void write_json_statistics_to_file(string filename, Json::Value statistics) {
    try{
        std::ofstream file_id;
        file_id.open(filename);
        Json::StyledWriter styledWriter;
        file_id << styledWriter.write(statistics);
        file_id.close();
    } catch (exception& e) {
        ALERT("WRITE STATS", "ERROR: could not write statistics to file %s\n", filename.c_str());
        ALERT("WRITE STATS", "ERROR: %s\n", e.what());
        exit(1);
    }

}

void write_statistics(
    unordered_map<string, string> config, 
    unordered_map<string,string> system_stats, 
    vector<unordered_map<string,string>> client_stats,
    unordered_map<string, string> memory_statistics
    ) {

    Json::Value client_json;

    //Write the system stats to the json output
    Json::Value system_stats_json;
    for (auto it = system_stats.begin(); it != system_stats.end(); it++) {
        system_stats_json[it->first] = it->second;
    }
    client_json["system"] = system_stats_json;

    //Write the config to the json output
    Json::Value config_json;
    for (auto it = config.begin(); it != config.end(); it++) {
        config_json[it->first] = it->second;
    }
    client_json["config"] = config_json;

    //Take care of the client statistics
    Json::Value client_vector (Json::arrayValue);
    for (int i = 0; i < client_stats.size(); i++) {
        Json::Value client_stats_json;
        client_stats_json["client_id"] = client_stats[i]["client_id"];
        for (auto it = client_stats[i].begin(); it != client_stats[i].end(); it++) {
            client_stats_json["stats"][it->first] = it->second;
        }
        client_vector.append(client_stats_json);
        // client_json.append(client_stats_json);
    }
    client_json["clients"] = client_vector;

    Json::Value memory_stats_json;
    for (auto it = memory_statistics.begin(); it != memory_statistics.end(); it++) {
        memory_stats_json[it->first] = it->second;
    }
    client_json["memory"] = memory_stats_json;
    write_json_statistics_to_file("statistics/client_statistics.json", client_json);
}