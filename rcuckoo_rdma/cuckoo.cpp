// #include "state_machines.h"
#include "cuckoo.h"
#include <string>

using namespace std;
using namespace cuckoo_state_machines;
namespace cuckoo_rcuckoo {
    RCuckoo::RCuckoo() : Client_State_Machine() {
        printf("RCUCKOO no args, get outta here\n");
        // _config = unordered_map<string, string>();
        // clear_statistics();
    }

    RCuckoo::RCuckoo(unordered_map<string, string> config) : Client_State_Machine(config) {
        printf("rcuckoo with args, get outta here\n");
        //do something with config
        // clear_statistics();
    }

    string RCuckoo::get_state_machine_name() {
        return "RCuckoo";
    }


}