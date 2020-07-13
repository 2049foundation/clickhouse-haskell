#ifndef __VARUINT__
#define __VARUINT__

#include<stdio.h>
#include<string.h>

const char * write_varint(size_t number);
size_t read_varint(const char * istr, size_t size);


#endif