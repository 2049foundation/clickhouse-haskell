#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include "varuint.h"

/**
 * Encode integer using LEB128
*/
#define MAX 65535

const char * write_varint(u_int16_t number){
    char * buf = malloc(sizeof(char) * 32);
   // memset(buf, '\0', sizeof(char) * 32);
    u_int16_t i = 0;
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

u_int16_t read_varint(char * istr, size_t size){
    const char * end = istr + size;
    u_int16_t x = 0;
    int byte;
    for(size_t i = 0; i < 9; ++i){
        byte = *istr;
        ++istr;
        x |= (byte & 0x7F) << (7 * i);
        if(!(byte & 0x80))
            break;
    }
    return x;
}

size_t count_read(char * istr, size_t size){
    const char * end = istr + size;
    size_t n = 0;
    int byte;
    for(size_t i = 0; i < 9; ++i){
        byte = * istr;
        ++istr;
        ++n;
        if(!(byte & 0x80))
            break;
    }
    return n;
}

void test_func(char * istr){
    *(istr + 1) = 'k';
    istr = istr + 1;
    printf("%s\n", istr);
}
