/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#ifndef OMNI_RUNTIME_DECIMALFUNCTIONS_H
#define OMNI_RUNTIME_DECIMALFUNCTIONS_H


#include <iostream>
#include "../../vector/type/decimal128.h"
#include <vector>

// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT int32_t Decimal128Compare(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow);

extern "C" DLLEXPORT void AddDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow,
                                       int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow,
                                       int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow,
                                       int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow,
                                       int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void AbsDecimal128(int64_t xHigh, uint64_t xLow, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void CastInt64ToDecimal128(int64_t x, int64_t *outHighPtr, uint64_t *outLowPtr);

#endif // OMNI_RUNTIME_DECIMALFUNCTIONS_H
