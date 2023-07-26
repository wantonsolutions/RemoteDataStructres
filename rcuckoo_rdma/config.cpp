#include "config.h"

#include <jsoncpp/json/json.h>
#include <iostream>
#include <fstream>

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

    cout << "reading config file " << config_filename << endl;

    ifstream ifs(config_filename);
    Json::Reader reader;
    Json::Value obj;
    reader.parse(ifs, obj); // reader can also read strings

    unordered_map<string, string> config;
    for(Json::Value::iterator it = obj.begin(); it != obj.end(); ++it) {
        cout << it.key() << "  " << it->asString() <<endl;
        config[it.key().asString()] = it->asString();
    }
    return config;

}