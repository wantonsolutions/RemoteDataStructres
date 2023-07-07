#include <cassert>
#include <string>
#include "util.h"

using namespace std;

uint8_t reverse(uint8_t b) {
    b = (b & 0xF0) >> 4 | (b & 0x0F) << 4;
    b = (b & 0xCC) >> 2 | (b & 0x33) << 2;
    b = (b & 0xAA) >> 1 | (b & 0x55) << 1;
    return b;
}

uint64_t reverse_uint64_t(uint64_t n) {
    uint64_t r = 0;
    for (int i=0; i<8; i++) {
        r = (r << 8) | reverse(n & 0xff);
        n >>= 8;
    }
    return r;
}

string uint64t_to_bin_string(uint64_t num){
    string s = "";
    for (int i = 0; i < 64; i++){
        if (num & 1){
            s.insert(0, "1");
        } else {
            s.insert(0, "0");
        }
        num = num >> 1;
    }
    return s;
}

uint64_t bin_string_to_uint64_t(string s){
    // printf("converting %s to uint64_t\n", s.c_str());
    uint64_t num = 0;
    for (int i = 0; i < 64; i++){
        num = num << 1;
        if (s[i] == '1'){
            num = num | 1;
        }
    }
    // printf("converted to %llx\n", num);
    return num;
}