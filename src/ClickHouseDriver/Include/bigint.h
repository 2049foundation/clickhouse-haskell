#ifndef __BIG_INT__
#define __BIG_INT__
#include<stdio.h>

double word128_division(__int64_t hi, __int64_t lo, int scale);

__int64_t low_bits_128(double,int);
__int64_t hi_bits_128(double,int);

__int64_t low_bits_negative_128(double,int);
__int64_t hi_bits_negative_128(double,int);

#endif