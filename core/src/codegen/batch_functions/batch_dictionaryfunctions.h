/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch dictionary functions implementation
 */
#ifndef OMNI_RUNTIME_BATCH_DICTIONARYFUNCTIONS_H
#define OMNI_RUNTIME_BATCH_DICTIONARYFUNCTIONS_H

#include <cstdint>
#include "type/decimal128.h"
using namespace omniruntime::type;

namespace omniruntime::codegen::function {
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT void BatchGetIntFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t *rowIdxArray,
    int32_t rowCnt, int32_t *output);

extern "C" DLLEXPORT void BatchGetLongFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t *rowIdxArray,
    int32_t rowCnt, int64_t *output);

extern "C" DLLEXPORT void BatchGetDoubleFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t *rowIdxArray,
    int32_t rowCnt, double *output);

extern "C" DLLEXPORT void BatchGetBooleanFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t *rowIdxArray,
    int32_t rowCnt, bool *output);

extern "C" DLLEXPORT void BatchGetVarcharFromDictionaryVector(int64_t contextPtr, int64_t dictionaryVectorAddr,
    int32_t *rowIdxArray, int32_t rowCnt, uint8_t **str, int32_t *length);

extern "C" DLLEXPORT void BatchGetDecimalFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t *rowIdxArray,
    int32_t rowCnt, Decimal128 *output);

extern "C" DLLEXPORT void BatchGetIntFromVector(int32_t *vector, int32_t *rowIdxArray, int32_t rowCnt, int32_t *output);

extern "C" DLLEXPORT void BatchGetLongFromVector(int64_t *vector, int32_t *rowIdxArray, int32_t rowCnt,
    int64_t *output);

extern "C" DLLEXPORT void BatchGetDoubleFromVector(double *vector, int32_t *rowIdxArray, int32_t rowCnt,
    double *output);

extern "C" DLLEXPORT void BatchGetBooleanFromVector(bool *vector, int32_t *rowIdxArray, int32_t rowCnt, bool *output);

extern "C" DLLEXPORT void BatchGetVarcharFromVector(int64_t contextPtr, int32_t *offsetArray, const char *vector,
    int32_t *rowIdxArray, int32_t rowCnt, uint8_t **str, int32_t *length);

extern "C" DLLEXPORT void BatchGetDecimalFromVector(Decimal128 *vector, int32_t *rowIdxArray, int32_t rowCnt,
    Decimal128 *output);
}

#endif // OMNI_RUNTIME_BATCH_DICTIONARYFUNCTIONS_H
