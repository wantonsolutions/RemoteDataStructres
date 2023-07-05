#include "../memcached.h"
#include <iostream>
#include <string>

using namespace std;

typedef struct memcached_test_struct {
    int a;
    bool b;
    char c[50];
} memcached_test_struct;

int main() {
    printf("Hello Memcached\n");
    // strncpy(MEMCACHED_IP,"137.110.222.47",sizeof(MEMCACHED_IP));
    // printf("MEMCACHED_IP = %s\n",MEMCACHED_IP);
    string key = "stewbert";
    string value = "camille";


    printf("attempting to serialize a struct\n");
    memcached_test_struct *test_struct = (memcached_test_struct *)malloc(sizeof(memcached_test_struct));
    test_struct->a = 1;
    test_struct->b = true;
    strncpy(test_struct->c,"hello",sizeof(test_struct->c));
    memcached_publish(key.c_str(),(void *)test_struct,sizeof(memcached_test_struct));

    memcached_test_struct *response_struct;
    int response_struct_len = memcached_get_published(key.c_str(),(void **)&response_struct);
    printf("response_struct of size %d : %d %d %s\n",response_struct_len,response_struct->a,response_struct->b,response_struct->c);

    table_config *config = (table_config *)malloc(sizeof(table_config));
    config->table_address = 1;
    config->table_size_bytes = 2;
    config->buckets_per_row = 3;
    config->entry_size_bytes = 4;
    config->lock_table_address = 5;
    config->lock_table_size_bytes = 6;
    memcached_pubish_table_config(config);

    table_config *response_config = memcached_get_table_config();
    printf("response_config: %s\n",response_config->to_string().c_str());

}