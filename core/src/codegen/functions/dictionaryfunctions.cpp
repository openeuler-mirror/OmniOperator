/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry dictionary functions
 */

#include "dictionaryfunctions.h"
#include "../../vector/dictionary_vector.h"
#include "context_helper.h"

using namespace omniruntime::vec;
using namespace std;

__attribute__((always_inline))
extern "C" DLLEXPORT int32_t GetIntFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index) {
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector*>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetInt(index);
}

__attribute__((always_inline))
extern "C" DLLEXPORT int64_t GetLongFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index) {
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector*>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetLong(index);
}

__attribute__((always_inline))
extern "C" DLLEXPORT double GetDoubleFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index) {
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector*>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetDouble(index);
}

__attribute__((always_inline))
extern "C" DLLEXPORT bool GetBooleanFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index) {
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector*>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetBoolean(index);
}

__attribute__((always_inline))
extern "C" DLLEXPORT uint8_t *GetVarcharFromDictionaryVector(
    int64_t dictionaryVectorAddr, int32_t index, int32_t *lengthPtr) {
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector*>(dictionaryVectorAddr);
    uint8_t *result = nullptr;
    int32_t length = dictionaryVectorPtr->GetVarchar(index, &result);
    *lengthPtr = length;
    return result;
}

__attribute__((always_inline))
extern "C" DLLEXPORT int64_t GetDecimalFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index,
                                                            int64_t contextPtr) {
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector*>(dictionaryVectorAddr);
    Decimal128 value = dictionaryVectorPtr->GetDecimal128(index);
    auto result = reinterpret_cast<int64_t*>(ArenaAllocatorMalloc(contextPtr, sizeof (long) * 2));
    result[0] = value.LowBits();
    result[1] = value.HighBits();
    return reinterpret_cast<int64_t>(result);
}