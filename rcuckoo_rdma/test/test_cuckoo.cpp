#include "../virtual_rdma.h"
#include "../cuckoo.h"
#include "../rdma_engine.h"


using namespace std;
using namespace cuckoo_rdma_engine;
using namespace cuckoo_rcuckoo;

int main(){
    printf("testing cuckoo!\n");
    RCuckoo rcuck = RCuckoo();
    printf("done rcuck init");
}