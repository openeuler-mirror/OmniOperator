#ifndef __FILTER_API_H__
#define __FILTER_API_H__

#include "../operator/filter/filter.h"
#include "../util/debug.h"

int64_t filterCompile(
    std::string filterExpression,
    int32_t *inputTypes,
    int32_t vecCount);

int32_t filterExecute(
    int64_t filterPtr,
    int64_t *inputData,
    int32_t *inputTypes,
    int32_t vecCount,
    int32_t rowNumber,
    int64_t *projectVecAddress,
    int32_t *projectIdx,
    int32_t projectVecCount);

#endif