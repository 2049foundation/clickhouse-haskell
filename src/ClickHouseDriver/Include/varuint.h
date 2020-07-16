#ifndef __VARUINT__
#define __VARUINT__

#include<stdio.h>
#include<string.h>
#include<stdlib.h>

const char * write_varint(u_int16_t number);
u_int16_t read_varint(char * istr, size_t size);
void test_func(char * istr);
size_t count_read(char * istr, size_t size);

#endif