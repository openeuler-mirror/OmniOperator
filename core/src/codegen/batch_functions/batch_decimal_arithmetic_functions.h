/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch decimal functions implementation
 */

#ifndef OMNI_RUNTIME_BATCH_DECIMAL_ARITHMETIC_FUNCTIONS_H
#define OMNI_RUNTIME_BATCH_DECIMAL_ARITHMETIC_FUNCTIONS_H

#include <iostream>
#include <vector>
#include "type/decimal128.h"
#include "type/decimal_operations.h"

namespace omniruntime::codegen::function {
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

// type::Decimal128 compare
extern "C" DLLEXPORT void BatchDecimal128Compare(type::Decimal128 *x, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLessThanDecimal128(type::Decimal128 *left, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLessThanEqualDecimal128(type::Decimal128 *left, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreaterThanDecimal128(type::Decimal128 *left, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreaterThanEqualDecimal128(type::Decimal128 *left, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchEqualDecimal128(type::Decimal128 *left, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchNotEqualDecimal128(type::Decimal128 *left, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAbsDecimal128(type::Decimal128 *x, int32_t xPrecision, int32_t xScale, bool *isNull,
    type::Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchRoundDecimal128(int64_t contextPtr, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int32_t *round, bool *isNull, type::Decimal128 *output, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchRoundDecimal64(int64_t contextPtr, int64_t *x, int32_t xPrecision, int32_t xScale,
    int32_t *round, bool *isNull, int64_t *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchRoundDecimal128WithoutRound(int64_t contextPtr, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, bool *isNull, type::Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchRoundDecimal64WithoutRound(int64_t contextPtr, int64_t *x, int32_t xPrecision,
    int32_t xScale, bool *isNull, int64_t *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

// decimal64 compare
extern "C" DLLEXPORT void BatchDecimal64Compare(int64_t *x, int32_t xPrecision, int32_t xScale, int64_t *y,
    int32_t yPrecision, int32_t yScale, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAbsDecimal64(int64_t *x, int32_t xPrecision, int32_t xScale, bool *isNull,
    int64_t *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLessThanDecimal64(int64_t *left, int32_t xPrecision, int32_t xScale, int64_t *right,
    int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLessThanEqualDecimal64(int64_t *left, int32_t xPrecision, int32_t xScale, int64_t *right,
    int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreaterThanDecimal64(int64_t *left, int32_t xPrecision, int32_t xScale, int64_t *right,
    int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreaterThanEqualDecimal64(int64_t *left, int32_t xPrecision, int32_t xScale,
    int64_t *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchEqualDecimal64(int64_t *left, int32_t xPrecision, int32_t xScale, int64_t *right,
    int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchNotEqualDecimal64(int64_t *left, int32_t xPrecision, int32_t xScale, int64_t *right,
    int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchUnscaledValue64(int64_t *x, int32_t precision, int32_t scale, bool *isAnyNull,
    int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMakeDecimal64(int64_t contextPtr, int64_t *x, bool *isAnyNull, int64_t *output,
    int32_t precision, int32_t scale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMakeDecimal64RetNull(bool *isNull, int64_t *x, int64_t *output, int32_t precision,
    int32_t scale, int32_t rowCnt);

// Decimal Add Operator ReScale
extern "C" DLLEXPORT void BatchAddDec64Dec64Dec64ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAddDec64Dec64Dec128ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, type::Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAddDec128Dec128Dec128ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAddDec64Dec128Dec128ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAddDec128Dec64Dec128ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *y,
    int32_t yPrecision, int32_t yScale, int64_t *x, int32_t xPrecision, int32_t xScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

// Decimal Sub Operation ReScale
extern "C" DLLEXPORT void BatchSubDec64Dec64Dec64ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubDec64Dec64Dec128ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, type::Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubDec128Dec128Dec128ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubDec64Dec128Dec128ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubDec128Dec64Dec128ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

// Decimal Mul Operation ReScale
extern "C" DLLEXPORT void BatchMulDec64Dec64Dec64ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMulDec64Dec64Dec128ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, type::Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMulDec128Dec128Dec128ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMulDec64Dec128Dec128ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMulDec128Dec64Dec128ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *y,
    int32_t yPrecision, int32_t yScale, int64_t *x, int32_t xPrecision, int32_t xScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

// Decimal Div Operation ReScale
extern "C" DLLEXPORT void BatchDivDec64Dec64Dec64ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec64Dec128Dec64ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec128Dec64Dec64ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec64Dec64Dec128ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, type::Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec128Dec128Dec128ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec64Dec128Dec128ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec128Dec64Dec128ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

// Decimal Mod Operation ReScale
extern "C" DLLEXPORT void BatchModDec64Dec64Dec64ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec64Dec128Dec64ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec128Dec64Dec64ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec128Dec64Dec128ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec128Dec128Dec128ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec128Dec128Dec64ReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec64Dec128Dec128ReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

// Decimal Add Operator NotReScale
extern "C" DLLEXPORT void BatchAddDec64Dec64Dec64NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAddDec64Dec64Dec128NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, type::Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAddDec128Dec128Dec128NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAddDec64Dec128Dec128NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAddDec128Dec64Dec128NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *y,
    int32_t yPrecision, int32_t yScale, int64_t *x, int32_t xPrecision, int32_t xScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

// Decimal Sub Operation NotReScale
extern "C" DLLEXPORT void BatchSubDec64Dec64Dec64NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubDec64Dec64Dec128NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, type::Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubDec128Dec128Dec128NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubDec64Dec128Dec128NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubDec128Dec64Dec128NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

// Decimal Mul Operation NotReScale
extern "C" DLLEXPORT void BatchMulDec64Dec64Dec64NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMulDec64Dec64Dec128NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, type::Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMulDec128Dec128Dec128NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMulDec64Dec128Dec128NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMulDec128Dec64Dec128NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *y,
    int32_t yPrecision, int32_t yScale, int64_t *x, int32_t xPrecision, int32_t xScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

// Decimal Div Operation NotReScale
extern "C" DLLEXPORT void BatchDivDec64Dec64Dec64NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec64Dec128Dec64NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec128Dec64Dec64NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec64Dec64Dec128NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, type::Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec128Dec128Dec128NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec64Dec128Dec128NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec128Dec64Dec128NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

// Decimal Mod Operation NotReScale
extern "C" DLLEXPORT void BatchModDec64Dec64Dec64NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec64Dec128Dec64NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec128Dec64Dec64NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec128Dec64Dec128NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec128Dec128Dec128NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec128Dec128Dec64NotReScale(int64_t contextPtr, bool *isNull, type::Decimal128 *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec64Dec128Dec128NotReScale(int64_t contextPtr, bool *isNull, int64_t *x,
    int32_t xPrecision, int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

// add ret null
extern "C" DLLEXPORT void BatchAddDec64Dec64Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAddDec64Dec64Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, type::Decimal128 *output, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchAddDec128Dec128Dec128RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchAddDec64Dec128Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAddDec128Dec64Dec128RetNull(bool *isNull, type::Decimal128 *y, int32_t yPrecision,
    int32_t yScale, int64_t *x, int32_t xPrecision, int32_t xScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

// sub ret null
extern "C" DLLEXPORT void BatchSubDec64Dec64Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubDec64Dec64Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, type::Decimal128 *output, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubDec128Dec128Dec128RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubDec64Dec128Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubDec128Dec64Dec128RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

// mul ret null
extern "C" DLLEXPORT void BatchMulDec64Dec64Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMulDec64Dec64Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, type::Decimal128 *output, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchMulDec128Dec128Dec128RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchMulDec64Dec128Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMulDec128Dec64Dec128RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

// div ret null
extern "C" DLLEXPORT void BatchDivDec64Dec64Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec64Dec128Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec128Dec64Dec64RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec64Dec64Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, type::Decimal128 *output, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec128Dec128Dec128RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec64Dec128Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivDec128Dec64Dec128RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

// mod ret null
extern "C" DLLEXPORT void BatchModDec64Dec64Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec64Dec128Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec128Dec64Dec64RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec128Dec64Dec128RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec128Dec128Dec128RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec128Dec128Dec64RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int64_t *output, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModDec64Dec128Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    type::Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchRoundDecimal128RetNull(bool *isNull, type::Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int32_t *round, type::Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchRoundDecimal64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int32_t *round, int64_t *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreatestDecimal64(int64_t contextPtr, int64_t *xValue, int32_t xPrecision,
    int32_t xScale, bool *xIsNull, int64_t *yValue, int32_t yPrecision, int32_t yScale, bool *yIsNull, bool *retIsNull,
    int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreatestDecimal128(int64_t contextPtr, type::Decimal128 *xValue, int32_t xPrecision,
    int32_t xScale, bool *xIsNull, type::Decimal128 *yValue, int32_t yPrecision, int32_t yScale, bool *yIsNull,
    bool *retIsNull, type::Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreatestDecimal64RetNull(bool *isNull, int64_t *xValue, int32_t xPrecision,
    int32_t xScale, bool *xIsNull, int64_t *yValue, int32_t yPrecision, int32_t yScale, bool *yIsNull, bool *retIsNull,
    int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreatestDecimal128RetNull(bool *isNull, type::Decimal128 *xValue, int32_t xPrecision,
    int32_t xScale, bool *xIsNull, type::Decimal128 *yValue, int32_t yPrecision, int32_t yScale, bool *yIsNull,
    bool *retIsNull, type::Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);
}

#endif // OMNI_RUNTIME_BATCH_DECIMAL_ARITHMETIC_FUNCTIONS_H