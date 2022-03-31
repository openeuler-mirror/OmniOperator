/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry dictionary functions
 */

#include "vector/dictionary_vector.h"
#include "common.h"

using namespace omniruntime::vec;
using namespace std;

extern "C" {
INLINE int32_t GetIntFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index) {
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector*>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetInt(index);
}

INLINE int64_t GetLongFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index) {
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector*>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetLong(index);
}

INLINE double GetDoubleFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index) {
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector*>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetDouble(index);
}

INLINE bool GetBooleanFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index) {
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector*>(dictionaryVectorAddr);
    return dictionaryVectorPtr->GetBoolean(index);
}

INLINE uint8_t *GetVarcharFromDictionaryVector(
    int64_t dictionaryVectorAddr, int32_t index, int32_t *lengthPtr) {
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector*>(dictionaryVectorAddr);
    uint8_t *result = nullptr;
    int32_t length = dictionaryVectorPtr->GetVarchar(index, &result);
    *lengthPtr = length;
    return result;
}

INLINE void GetDecimalFromDictionaryVector(int64_t dictionaryVectorAddr,
    int32_t index, int64_t *outHighPtr, uint64_t *outLowPtr) {
    auto dictionaryVectorPtr = reinterpret_cast<DictionaryVector*>(dictionaryVectorAddr);
    Decimal128 value = dictionaryVectorPtr->GetDecimal128(index);
    *outLowPtr = value.LowBits();
    *outHighPtr = value.HighBits();
}
}