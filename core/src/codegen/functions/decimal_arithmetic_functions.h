/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#ifndef OMNI_RUNTIME_DECIMAL_ARITHMETIC_FUNCTIONS_H
#define OMNI_RUNTIME_DECIMAL_ARITHMETIC_FUNCTIONS_H

#include <iostream>
#include <vector>
#include <cmath>
#include <iomanip>
#include "type/decimal128.h"
#include "codegen/context_helper.h"
#include "type/decimal_operations.h"
#include "util/config_util.h"
#include "type/data_type.h"

namespace omniruntime::codegen::function {
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
    int32_t outPrecision, int32_t outScale);

// Decimal AddOperator ReScale
extern "C" DLLEXPORT int64_t AddDec64Dec64Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void AddDec64Dec64Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void AddDec128Dec128Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void AddDec64Dec128Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void AddDec128Dec64Dec128ReScale(int64_t contextPtr, int64_t yHigh, uint64_t yLow,
    int32_t yPrecision, int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal SubOperation ReScale
extern "C" DLLEXPORT int64_t SubDec64Dec64Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void SubDec64Dec64Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec128Dec128Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec64Dec128Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec128Dec64Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal MulOperation ReScale
extern "C" DLLEXPORT int64_t MulDec64Dec64Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void MulDec64Dec64Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec128Dec128Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec64Dec128Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec128Dec64Dec128ReScale(int64_t contextPtr, int64_t yHigh, uint64_t yLow,
    int32_t yPrecision, int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal DivOperation ReScale
extern "C" DLLEXPORT int64_t DivDec64Dec64Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t DivDec64Dec128Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT int64_t DivDec128Dec64Dec64ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT void DivDec64Dec64Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec128Dec128Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec64Dec128Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec128Dec64Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal ModOperation ReScale
extern "C" DLLEXPORT int64_t ModDec64Dec64Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t ModDec64Dec128Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT int64_t ModDec128Dec64Dec64ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT void ModDec128Dec64Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void ModDec128Dec128Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int64_t ModDec128Dec128Dec64ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void ModDec64Dec128Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal AddOperator NotReScale
extern "C" DLLEXPORT int64_t AddDec64Dec64Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void AddDec64Dec64Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void AddDec128Dec128Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void AddDec64Dec128Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void AddDec128Dec64Dec128NotReScale(int64_t contextPtr, int64_t yHigh, uint64_t yLow,
    int32_t yPrecision, int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal SubOperation NotReScale
extern "C" DLLEXPORT int64_t SubDec64Dec64Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void SubDec64Dec64Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec128Dec128Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec64Dec128Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void SubDec128Dec64Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal MulOperation NotReScale
extern "C" DLLEXPORT int64_t MulDec64Dec64Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void MulDec64Dec64Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec128Dec128Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec64Dec128Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void MulDec128Dec64Dec128NotReScale(int64_t contextPtr, int64_t yHigh, uint64_t yLow,
    int32_t yPrecision, int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal DivOperation NotReScale
extern "C" DLLEXPORT int64_t DivDec64Dec64Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t DivDec64Dec128Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT int64_t DivDec128Dec64Dec64NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT void DivDec64Dec64Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec128Dec128Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec64Dec128Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void DivDec128Dec64Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

// Decimal ModOperation NotReScale
extern "C" DLLEXPORT int64_t ModDec64Dec64Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t ModDec64Dec128Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT int64_t ModDec128Dec64Dec64NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT void ModDec128Dec64Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void ModDec128Dec128Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int64_t ModDec128Dec128Dec64NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void ModDec64Dec128Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

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

extern "C" DLLEXPORT int64_t UnscaledValue64(int64_t x, int32_t precision, int32_t scale, bool isNull);

extern "C" DLLEXPORT int64_t MakeDecimal64(int64_t contextPtr, int64_t x, bool isNull, int32_t precision,
    int32_t scale);

extern "C" DLLEXPORT int64_t MakeDecimal64RetNull(bool *isNull, int64_t x, int32_t precision, int32_t scale);

extern "C" DLLEXPORT void RoundDecimal128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int32_t round, bool isNull, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT void RoundDecimal128WithoutRound(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, bool isNull, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr);

extern "C" DLLEXPORT int64_t RoundDecimal64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int32_t round, bool isNull, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t RoundDecimal64WithoutRound(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, bool isNull, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT void RoundDecimal128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int32_t round, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int64_t RoundDecimal64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int32_t round, int32_t outPrecision, int32_t outScale);

extern "C" DLLEXPORT int64_t GreatestDecimal64(int64_t contextPtr, int64_t xValue, int32_t xPrecision, int32_t xScale,
    bool xIsNull, int64_t yValue, int32_t yPrecision, int32_t yScale, bool yIsNull, bool *retIsNull,
    int32_t newPrecision, int32_t newScale);

extern "C" DLLEXPORT void GreatestDecimal128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, bool xIsNull, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, bool yIsNull,
    bool *retIsNull, int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int64_t GreatestDecimal64RetNull(bool *isNull, int64_t xValue, int32_t xPrecision, int32_t xScale,
    bool xIsNull, int64_t yValue, int32_t yPrecision, int32_t yScale, bool yIsNull, bool *retIsNull,
    int32_t newPrecision, int32_t newScale);

extern "C" DLLEXPORT void GreatestDecimal128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, bool xIsNull, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, bool yIsNull,
    bool *retIsNull, int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr);
}

#endif // OMNI_RUNTIME_DECIMAL_ARITHMETIC_FUNCTIONS_H
