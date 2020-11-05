#include<time.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>
#include "datetime.h"

char * convert_time(time_t time, char * timezone, size_t length){
    struct tm mytm = {0};
    mytm.tm_isdst = 1;
    timezone[length] = '\0';
    char * buf = malloc(sizeof(char) * 30);
    putenv(timezone);
    tzset();
    localtime_r(&time, &mytm);
    strftime(buf, 50, "%a, %d %b %Y %H:%M:%S %z(%Z)", &mytm);
    return buf;
}

char * convert_time64(time_t time, char * timezone, size_t length, size_t scale){
    struct tm my_tm = {0};
    my_tm.tm_isdst = 1;
    char * buf = malloc(sizeof(char) * 40);
    putenv(timezone);
    tzset();
    int exp = pow(10, scale);
    float scaled = time / exp;
    localtime_r(&scaled, &my_tm);
    return buf;
}

time_t parse_time(char * time_string, char * timezone, size_t length1, size_t length2){
    struct tm my_tm = {0};
    my_tm.tm_isdst = 0;
    timezone[length1] = '\0';
    time_string[length2] = '\0';
    time_t result;
    putenv(timezone);
    tzset();
    strptime(time_string, "%d %b %Y %H:%M:%S", &my_tm);
}

time_t convert_time_from_int32(time_t time){
    struct tm* gmt = gmtime(&time);
    time_t converted = mktime(gmt);
    return converted;
}