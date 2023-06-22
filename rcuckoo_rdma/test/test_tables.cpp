#include "../tables.h"
#include <iostream>
#include <string>

int main() {
    using namespace std;
    unsigned int memory_size = 1024;
    unsigned int bucket_size = 8;
    unsigned int buckets_per_lock = 1;
    cuckoo_tables::Table table = cuckoo_tables::Table(memory_size, bucket_size, buckets_per_lock);
    cout << "Hello Table Test!" << endl;
    table.print_table();


    string s_key_1 = "01000000";
    Key key_1 = Key(s_key_1);
    cout << "Skey: " << s_key_1 << " Key 1: " << key_1.to_string() << endl;

}