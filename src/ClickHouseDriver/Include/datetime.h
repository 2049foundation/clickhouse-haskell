#ifndef __CK_DATE_TIME__
#define __CK_DATE_TIME__

#include<time.h>

char * convert_time(time_t original_time, char * timezone, size_t length);
time_t parse_time(char * time_string, char * timezone, size_t length1, size_t length2);
time_t convert_time_from_int32(time_t time);

#endif
