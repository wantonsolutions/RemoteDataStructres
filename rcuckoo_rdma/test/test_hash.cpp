#include "../hash.h"
#include <iostream>

int main() {
    cout << "Hello Hash Test!" << endl;

    hash_locations h1 = rcuckoo_hash_locations("hello", 100);
    cout << "h1: " << h1.primary << " " << h1.secondary << endl;

}