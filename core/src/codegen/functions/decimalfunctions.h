/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#ifndef OMNI_RUNTIME_DECIMALFUNCTIONS_H
#define OMNI_RUNTIME_DECIMALFUNCTIONS_H

#include <iostream>
#include <vector>
#include "type/decimal128.h"
#include "type/decimal_operations.h"

namespace omniruntime {
namespace codegen {
// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT int32_t Decimal128Compare(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow);

extern "C" DLLEXPORT void AddDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void AbsDecimal128(int64_t xHigh, uint64_t xLow, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void CastInt64ToDecimal128(int64_t x, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int64_t CastDoubleToDecimal64(double x, int32_t precision, int32_t scale);

extern "C" DLLEXPORT int64_t MakeDecimal64(int64_t x, int32_t precision, int32_t scale, int32_t newPrecision,
    int32_t newScale);

extern "C" DLLEXPORT void MakeDecimal128(int64_t xHigh, uint64_t xLow, int32_t precision, int32_t scale,
    int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void MakeDecimal64To128(int64_t x, int32_t precision, int32_t scale, int32_t newPrecision,
    int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int64_t MakeDecimalLongTo64(int64_t x, int32_t precision, int32_t scale);

extern "C" DLLEXPORT int64_t DivDec64(int64_t contextPtr, int64_t x, int64_t y);

extern "C" DLLEXPORT int64_t DownScaleDec64(int64_t x, int32_t y);

extern "C" DLLEXPORT int64_t UnscaledValue64(int64_t x, int32_t precision, int32_t scale);

extern "C" DLLEXPORT bool IsOverflowDecimal64(int64_t x, int32_t precision, int32_t scale, int32_t checkPrecision,
    int32_t checkScale);

extern "C" DLLEXPORT bool IsOverflowDecimal128(int64_t xHigh, uint64_t xLow, int32_t precision, int32_t scale,
    int32_t checkPrecision, int32_t checkScale);
}
}
#endif // OMNI_RUNTIME_DECIMALFUNCTIONS_H
