#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "varuint.h"

/**
 *Encode integer using LEB128
 */

const char *write_varint(u_int16_t number)
{
	char *ostr = malloc(sizeof(char) *32);
	char *ptr = ostr;
	memset(ostr, '\0', sizeof(char) *32);
	for (size_t i = 0; i < 9; ++i)
	{
		u_int8_t byte = number &0x7F;
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

u_int16_t read_varint(char *istr, size_t size)
{
	const char *end = istr + size;
	u_int16_t x = 0;
	int byte;
	for (size_t i = 0; i < 9; ++i)
	{
		byte = *istr;
		++istr;
		x |= (byte & 0x7F) << (7 *i);
		if (!(byte & 0x80))
			break;
	}
	return x;
}

size_t count_read(char *istr, size_t size)
{
	const char *end = istr + size;
	size_t n = 0;
	int byte;
	for (size_t i = 0; i < 9; ++i)
	{
		byte = *istr;
		++istr;
		++n;
		if (!(byte & 0x80))
			break;
	}
	return n;
}