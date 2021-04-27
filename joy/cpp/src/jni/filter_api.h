#ifndef __FILTER_API_H__
#define __FILTER_API_H__

#include "../operator/filter/filter.cpp"
#include <string>
#include <iostream>

// vector struct
typedef struct FilterVector{
    int64_t vecAddress;
    int32_t vecCount;
} FilterVector;

int64_t filterCompile(
    string filterExpression,
    int64_t inputType,
    int32_t vecCount);

int32_t* createInputVec(
    int64_t address,
    int32_t count);

#endif