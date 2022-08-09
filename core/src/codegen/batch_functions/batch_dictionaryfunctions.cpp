/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: batch dictionary functions implementation
 */

#include "batch_dictionaryfunctions.h"
#include "vector/dictionary_vector.h"
#include "../functions/context_helper.h"

using namespace omniruntime::vec;

namespace omniruntime {
namespace codegen {
extern DLLEXPORT void BatchGetIntFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t *rowIdxArray,
    int32_t rowCnt, int32_t *output)
{
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector *>(dictionaryVectorAddr);
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = dictionaryVectorPtr->GetInt(rowIdxArray[i]);
    }
}

extern DLLEXPORT void BatchGetLongFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t *rowIdxArray,
    int32_t rowCnt, int64_t *output)
{
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector *>(dictionaryVectorAddr);
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = dictionaryVectorPtr->GetLong(rowIdxArray[i]);
    }
}

extern DLLEXPORT void BatchGetDoubleFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t *rowIdxArray,
    int32_t rowCnt, double *output)
{
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector *>(dictionaryVectorAddr);
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = dictionaryVectorPtr->GetDouble(rowIdxArray[i]);
    }
}

extern DLLEXPORT void BatchGetBooleanFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t *rowIdxArray,
    int32_t rowCnt, bool *output)
{
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector *>(dictionaryVectorAddr);
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = dictionaryVectorPtr->GetBoolean(rowIdxArray[i]);
    }
}

extern DLLEXPORT void BatchGetVarcharFromDictionaryVector(int64_t contextPtr, int64_t dictionaryVectorAddr,
    int32_t *rowIdxArray, int32_t rowCnt, uint8_t **str, int32_t *length)
{
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector *>(dictionaryVectorAddr);
    uint8_t *result = nullptr;
    errno_t err;
    char *ret;
    for (int i = 0; i < rowCnt; ++i) {
        length[i] = dictionaryVectorPtr->GetVarchar(rowIdxArray[i], &result);
        ret = ArenaAllocatorMalloc(contextPtr, length[i]);
        err = memcpy_s(ret, length[i], result, length[i]);
        str[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchGetDecimalFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t *rowIdxArray,
    int32_t rowCnt, Decimal128 *output)
{
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector *>(dictionaryVectorAddr);
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = dictionaryVectorPtr->GetDecimal128(rowIdxArray[i]);
    }
}

extern DLLEXPORT void BatchGetIntFromVector(int32_t *vector, int32_t *rowIdxArray, int32_t rowCnt, int32_t *output)
{
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = vector[rowIdxArray[i]];
    }
}

extern DLLEXPORT void BatchGetLongFromVector(int64_t *vector, int32_t *rowIdxArray, int32_t rowCnt, int64_t *output)
{
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = vector[rowIdxArray[i]];
    }
}

extern DLLEXPORT void BatchGetDoubleFromVector(double *vector, int32_t *rowIdxArray, int32_t rowCnt, double *output)
{
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = vector[rowIdxArray[i]];
    }
}

extern DLLEXPORT void BatchGetBooleanFromVector(bool *vector, int32_t *rowIdxArray, int32_t rowCnt, bool *output)
{
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = vector[rowIdxArray[i]];
    }
}

extern DLLEXPORT void BatchGetVarcharFromVector(int64_t contextPtr, int32_t *offsetArray, const char *vector,
    int32_t *rowIdxArray, int32_t rowCnt, uint8_t **str, int32_t *length)
{
    errno_t err;
    char *ret;
    for (int i = 0; i < rowCnt; ++i) {
        length[i] = offsetArray[rowIdxArray[i] + 1] - offsetArray[rowIdxArray[i]];
        ret = ArenaAllocatorMalloc(contextPtr, length[i]);
        err = memcpy_s(ret, length[i], vector + offsetArray[rowIdxArray[i]], length[i]);
        str[i] = reinterpret_cast<uint8_t *>(ret);
    }
}

extern DLLEXPORT void BatchGetDecimalFromVector(Decimal128 *vector, int32_t *rowIdxArray, int32_t rowCnt,
    Decimal128 *output)
{
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = vector[rowIdxArray[i]];
    }
}
}
}