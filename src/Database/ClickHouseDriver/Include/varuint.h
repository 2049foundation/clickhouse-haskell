#ifndef __VARUINT__
#define __VARUINT__

#include<stdio.h>
#include<string.h>
#include<stdlib.h>

/**
 * @param __uint16_t
 * @return turned the number into string buffer. 
 **/
const char * write_varint(__uint16_t number);

/**
 * @param __uint16_t intermediate result to continue, which takes an uint16
 * @param char* string buffer, which takes an char pointer 
 * @param size_t size of the string buffer, which takes size_t
 * @return the read varuint from string buffer combined with offset information. 
 **/
__uint32_t read_varint(__uint16_t cont,char *istr, size_t size);

#endif