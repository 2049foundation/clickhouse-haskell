#include<stdio.h>
#include<stdint.h>
#include<math.h>
#include "bigint.h"

double word128_division(__int64_t hi, __int64_t lo, int scale){
    __int128_t i_128 = 0;
    if (hi > UINT64_MAX){
        i_128 = UINT64_MAX - hi;
        i_128 = i_128 << 64;
        i_128 = -i_128;
        i_128 = i_128 - (UINT64_MAX - lo) - 1;
    }
    else{
        i_128 = hi;
        i_128 = i_128 << 64;
        i_128 += lo;
    }
    i_128 /= scale;
    return i_128;
}

__int64_t low_bits_128(double x, int scale){
    __int128_t i128 = x * pow(10, scale);
    return i128 & UINT64_MAX;
}

__int64_t hi_bits_128(double x, int scale){
    __int128_t i128 = x * pow(10, scale);
    return (i128 >> 64) & UINT64_MAX;
}

__int64_t low_bits_negative_128(double x, int scale){
    __int128_t i128 = x * pow(10, scale);
    return UINT64_MAX - (i128 & UINT64_MAX) + 1;
}

__int64_t hi_bits_negative_128(double x, int scale){
    __int128_t i128 = x * pow(10, scale);
    return UINT64_MAX - ((i128 >> 64) & UINT64_MAX);
}