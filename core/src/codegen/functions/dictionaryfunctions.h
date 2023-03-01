/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry dictionary functions
 */

#ifndef OMNI_RUNTIME_DICTIONARYFUNCTIONS_H
#define OMNI_RUNTIME_DICTIONARYFUNCTIONS_H

#include <cstdint>

namespace omniruntime::codegen::function {
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern DLLEXPORT int32_t GetIntFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index);

extern DLLEXPORT int64_t GetLongFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index);

extern DLLEXPORT double GetDoubleFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index);

extern DLLEXPORT bool GetBooleanFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index);

extern DLLEXPORT uint8_t *GetVarcharFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index,
    int32_t *lengthPtr);

extern DLLEXPORT void GetDecimalFromDictionaryVector(int64_t dictionaryVectorAddr, int32_t index, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);
extern "C" DLLEXPORT uint8_t *GetStringViewValueAndLength(int64_t stringValueAddr, int32_t index, int32_t *length);
}
#endif // OMNI_RUNTIME_DICTIONARYFUNCTIONS_H
