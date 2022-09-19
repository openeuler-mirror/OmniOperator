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

extern "C" DLLEXPORT int32_t Decimal128Compare(int64_t xHigh, uint64_t xLow, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, bool isNull);

extern "C" DLLEXPORT void AbsDecimal128(int64_t xHigh, uint64_t xLow, int32_t xPrecision, int32_t xScale, bool isNull,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int32_t Decimal64Compare(int64_t x, int32_t xPrecision, int32_t xScale, int64_t y,
    int32_t yPrecision, int32_t yScale, bool isNull);

extern "C" DLLEXPORT int64_t AbsDecimal64(int64_t x, int32_t xPrecision, int32_t xScale, bool isNull,
    int32_t outPrecision,
    int32_t outScale);

// Decimal AddOperator
extern "C" DLLEXPORT int64_t AddDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void AddDec64Dec64Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void AddDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void AddDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void AddDec128Dec64Dec128(int64_t contextPtr, int64_t yHigh, uint64_t yLow, int32_t yPrecision,
    int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal SubOperation
extern "C" DLLEXPORT int64_t SubDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void SubDec64Dec64Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec128Dec64Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal MulOperation
extern "C" DLLEXPORT int64_t MulDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void MulDec64Dec64Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec128Dec64Dec128(int64_t contextPtr, int64_t yHigh, uint64_t yLow, int32_t yPrecision,
    int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal DivOperation
extern "C" DLLEXPORT int64_t DivDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t DivDec64Dec128Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t DivDec128Dec64Dec64(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void DivDec64Dec64Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec128Dec64Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal ModOperation
extern "C" DLLEXPORT int64_t ModDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t ModDec64Dec128Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t ModDec128Dec64Dec64(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void ModDec128Dec64Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void ModDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int64_t ModDec128Dec128Dec64(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT void ModDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

// Cast
extern "C" DLLEXPORT int64_t CastDecimal64To64(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale,
    bool isNull,
    int32_t newPrecision, int32_t newScale);

extern "C" DLLEXPORT void CastDecimal128To128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, bool isNull, int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void CastDecimal64To128(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale,
    bool isNull,
    int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int64_t CastDecimal128To64(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, bool isNull, int32_t newPrecision, int32_t newScale);

extern "C" DLLEXPORT int64_t CastIntToDecimal64(int64_t contextPtr, int32_t x, bool isNull, int32_t precision,
    int32_t scale);

extern "C" DLLEXPORT int64_t CastLongToDecimal64(int64_t contextPtr, int64_t x, bool isNull, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT int64_t CastDoubleToDecimal64(int64_t contextPtr, double x, bool isNull, int32_t precision,
    int32_t scale);

extern "C" DLLEXPORT void CastIntToDecimal128(int64_t contextPtr, int32_t x, bool isNull, int32_t outPrecision,
    int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void CastLongToDecimal128(int64_t contextPtr, int64_t x, bool isNull, int32_t outPrecision,
    int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void CastDoubleToDecimal128(int64_t contextPtr, double x, bool isNull, int32_t outPrecision,
    int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int32_t CastDecimal64ToInt(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale,
    bool isNull);

extern "C" DLLEXPORT int64_t CastDecimal64ToLong(int64_t x, int32_t precision, int32_t scale, bool isNull);

extern "C" DLLEXPORT double CastDecimal64ToDouble(int64_t x, int32_t precision, int32_t scale, bool isNull);

extern "C" DLLEXPORT int32_t CastDecimal128ToInt(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, bool isNull);

extern "C" DLLEXPORT int64_t CastDecimal128ToLong(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, bool isNull);

extern "C" DLLEXPORT double CastDecimal128ToDouble(int64_t xHigh, uint64_t xLow, int32_t precision, int32_t scale,
    bool isNull);

// Return Null
extern "C" DLLEXPORT int64_t AddDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void AddDec64Dec64Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void AddDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void AddDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void AddDec128Dec64Dec128RetNull(bool *isNull, int64_t yHigh, uint64_t yLow, int32_t yPrecision,
    int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal SubOperator
extern "C" DLLEXPORT int64_t SubDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void SubDec64Dec64Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec128Dec64Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal MulOperator
extern "C" DLLEXPORT int64_t MulDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void MulDec64Dec64Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec128Dec64Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal DivOperation
extern "C" DLLEXPORT int64_t DivDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t DivDec64Dec128Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t DivDec128Dec64Dec64RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void DivDec64Dec64Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec128Dec64Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal ModOperation
extern "C" DLLEXPORT int64_t ModDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t ModDec64Dec128Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t ModDec128Dec64Dec64RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void ModDec128Dec64Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void ModDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int64_t ModDec128Dec128Dec64RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT void ModDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

// Cast
extern "C" DLLEXPORT int64_t CastDecimal64To64RetNull(bool *isNull, int64_t x, int32_t precision, int32_t scale,
    int32_t newPrecision, int32_t newScale);

extern "C" DLLEXPORT void CastDecimal128To128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void CastDecimal64To128RetNull(bool *isNull, int64_t x, int32_t precision, int32_t scale,
    int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int64_t CastDecimal128To64RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, int32_t newPrecision, int32_t newScale);

extern "C" DLLEXPORT int64_t CastIntToDecimal64RetNull(bool *isNull, int32_t x, int32_t precision, int32_t scale);

extern "C" DLLEXPORT int64_t CastLongToDecimal64RetNull(bool *isNull, int64_t x, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT int64_t CastDoubleToDecimal64RetNull(bool *isNull, double x, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT void CastIntToDecimal128RetNull(bool *isNull, int32_t x, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void CastLongToDecimal128RetNull(bool *isNull, int64_t x, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void CastDoubleToDecimal128RetNull(bool *isNull, double x, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int32_t CastDecimal64ToIntRetNull(bool *isNull, int64_t x, int32_t precision, int32_t scale);

extern "C" DLLEXPORT int64_t CastDecimal64ToLongRetNull(bool *isNull, int64_t x, int32_t precision, int32_t scale);

extern "C" DLLEXPORT double CastDecimal64ToDoubleRetNull(bool *isNull, int64_t x, int32_t precision, int32_t scale);

extern "C" DLLEXPORT int32_t CastDecimal128ToIntRetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale);

extern "C" DLLEXPORT int64_t CastDecimal128ToLongRetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale);

extern "C" DLLEXPORT double CastDecimal128ToDoubleRetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale);

extern "C" DLLEXPORT int64_t UnscaledValue64(int64_t x, int32_t precision, int32_t scale);

extern "C" DLLEXPORT int64_t MakeDecimal64(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale);

extern "C" DLLEXPORT int64_t MakeDecimal64RetNull(bool *isNull, int64_t x, int32_t precision, int32_t scale);
}
}
#endif // OMNI_RUNTIME_DECIMALFUNCTIONS_H
