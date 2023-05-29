#include "../tables.h"
#include <iostream>

int main() {
    using namespace std;
    unsigned int memory_size = 1024;
    unsigned int bucket_size = 8;
    unsigned int buckets_per_lock = 1;
    cuckoo_tables::Table table = cuckoo_tables::Table(memory_size, bucket_size, buckets_per_lock);
    cout << "Hello Table Test!" << endl;
    table.print_table();
}