#ifndef __FILTER_API_H__
#define __FILTER_API_H__

#include "../operator/filter/filter.h"

// vector struct
typedef struct FilterVector{
    int64_t vecAddress;
    int32_t vecCount;
} FilterVector;

int32_t* createIntVec(int64_t address, int32_t count);
int64_t* createLongVec(int64_t address, int32_t count);
double* createFloatVec(int64_t address, int32_t count);

int64_t filterCompile(
    std::string filterExpression,
    int64_t inputType,
    int32_t vecCount);

int32_t filterExecute(
    int64_t filterPtr,
    int64_t *inputData,
    int64_t inputTypes,
    int32_t vecCount,
    int64_t selectedRowsAddr,
    int32_t rowNumber);

int32_t filterExecuteV1(
    int64_t filterPtr,
    int64_t *inputData,
    int64_t inputTypes,
    int32_t vecCount,
    int32_t rowNumber,
    int64_t *projectVecAddress,
    int32_t *projectIdx,
    int32_t projectVecCount);

#endif