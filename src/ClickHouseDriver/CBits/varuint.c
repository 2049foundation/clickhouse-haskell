#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include "varuint.h"

/**
 * Encode integer using LEB128
*/


const char * write_varint(size_t number){
    char * buf = malloc(sizeof(char) * 32);
   // memset(buf, '\0', sizeof(char) * 32);
    size_t i = 0;
    unsigned char towrite;

    for(;;){
        towrite = number & 0x7f;
        number >>= 7;
        if(number){
            buf[i++] = towrite | 0x80;
        }
        else{
            buf[i] = towrite;
            break;
        }
    }
    return buf;
}

size_t read_varint(const char * istr, size_t size){
    const char * end = istr + size;
    size_t x = 0;

    for(size_t i = 0; i < 9; ++i){
        int byte = *istr;
        ++istr;
        x |= (byte & 0x7F) << (7 * i);
        if(!(byte & 0x80))
            return x;
    }
    return x;
}
