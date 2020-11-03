#include<time.h>
#include<stdio.h>
#include<stdlib.h>
#include "datetime.h"

char * convert_time(time_t time, char * timezone){
    struct tm mytm = {0};
    mytm.tm_isdst = 1;
    char * buf = malloc(sizeof(char) * 30);
    putenv(timezone);
    tzset();
    localtime_r(&time, &mytm);
    strftime(buf, 50, "%a, %d %b %Y %H:%M:%S %z(%Z)", &mytm);
    return buf;
}