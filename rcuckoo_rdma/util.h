#pragma once
#ifndef _UTIL_H_
#define _UTIL_H_

#include <string>
using namespace std;

string uint64t_to_bin_string(uint64_t num);
uint64_t bin_string_to_uint64_t(string s);
uint8_t reverse(uint8_t b);
uint64_t reverse_uint64_t(uint64_t n);

#endif