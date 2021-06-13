#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "varuint.h"

/**
 *Encode integer using LEB128
 */


const char *write_varint(__uint16_t number)
{
	char *ostr = malloc(sizeof(char) *32);
	char *ptr = ostr;
	memset(ostr, '\0', sizeof(char) *32);
	for (size_t i = 0; i < 9; ++i)
	{
		__uint8_t byte = number &0x7F;
		if (number > 0x7F)
			byte |= 0x80;
		*ptr = byte;
		++ptr;
		number >>= 7;
		if (!number)
			return ostr;
	}
	return ostr;
}

__uint32_t read_varint(__uint16_t cont,char *istr, size_t size)
{
	const char *end = istr + size;
	int byte;
	__uint16_t n = 0;
	for (size_t i = 0; i < 9; ++i)
	{
		if(istr == end){
			return 0;
		}
		byte = *istr;
		++istr;
		++n;
		cont |= (byte & 0x7F) << (7 * i);
		if (!(byte & 0x80))
			break;
	}
	__uint32_t res = (cont << 16) | n;
 	return res;
}