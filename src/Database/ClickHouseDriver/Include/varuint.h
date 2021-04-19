#ifndef __VARUINT__
#define __VARUINT__

#include<stdio.h>
#include<string.h>
#include<stdlib.h>

const char * write_varint(u_int16_t number);
size_t* read_varint(u_int16_t cont ,char * istr, size_t size);

#endif