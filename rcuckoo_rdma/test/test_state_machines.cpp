#include <stdio.h>
#include "../state_machines.h"
#include "../tables.h"
#include "../virtual_rdma.h"

using namespace cuckoo_state_machines;
using namespace cuckoo_tables;

int main(){
    printf("hello state machines\n");
    printf("initializing a request\n");
    Request r = Request();
    printf("%s\n", r.to_string().c_str());
    Request r2 = Request(PUT, Key(), Value());
    printf("%s\n", r2.to_string().c_str());

    printf("initializing a state machine\n");
    State_Machine sm = State_Machine();

}