#include "../virtual_rdma.h"
#include "../cuckoo.h"
#include "../rdma_common.h"
#include "../rdma_engine.h"
#include "../state_machines.h"
#include <unordered_map>


using namespace std;
using namespace cuckoo_rdma_engine;
using namespace cuckoo_rcuckoo;


int main(){
    printf("testing cuckoo!\n");
    unordered_map<string, string> config = gen_config();

    RDMA_Engine client_1 = RDMA_Engine(config);

    printf("starting client 0\n");
    client_1.start();


    //now we call the engine

}