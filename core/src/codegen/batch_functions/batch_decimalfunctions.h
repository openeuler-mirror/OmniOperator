/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch decimal functions implementation
 */

#ifndef OMNI_RUNTIME_BATCH_DECIMALFUNCTIONS_H
#define OMNI_RUNTIME_BATCH_DECIMALFUNCTIONS_H

#include <iostream>
#include <vector>
#include "type/decimal128.h"
#include "type/decimal_operations.h"

using namespace omniruntime::type;

namespace omniruntime {
namespace codegen {
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

// decimal128 compare
extern "C" DLLEXPORT void BatchDecimal128Compare(Decimal128 *x, int32_t xPrecision, int32_t xScale, Decimal128 *y,
    int32_t yPrecision, int32_t yScale, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLessThanDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale,
    Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLessThanEqualDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale,
    Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreaterThanDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale,
    Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreaterThanEqualDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale,
    Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchEqualDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale, Decimal128 *right,
    int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchNotEqualDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale,
    Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchAbsDecimal128(Decimal128 *x, int32_t xPrecision, int32_t xScale, bool *isNull,
    Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

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

// Cast Function
extern "C" DLLEXPORT void BatchCastDecimal64To64(int64_t contextPtr, int64_t *x, int32_t precision, int32_t scale,
    bool *isAnyNull, int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128To128(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    bool *isAnyNull, Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64To128(int64_t contextPtr, int64_t *x, int32_t precision, int32_t scale,
    bool *isAnyNull, Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128To64(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    bool *isAnyNull, int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastIntToDecimal64(int64_t contextPtr, int32_t *x, bool *isAnyNull, int64_t *output,
    int32_t precision, int32_t scale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastLongToDecimal64(int64_t contextPtr, int64_t *x, bool *isAnyNull, int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDoubleToDecimal64(int64_t contextPtr, double *x, bool *isAnyNull, int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastIntToDecimal128(int64_t contextPtr, int32_t *x, bool *isAnyNull, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastLongToDecimal128(int64_t contextPtr, int64_t *x, bool *isAnyNull, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDoubleToDecimal128(int64_t contextPtr, double *x, bool *isAnyNull,
    Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64ToInt(int64_t contextPtr, int64_t *x, int32_t precision, int32_t scale,
    bool *isAnyNull, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64ToLong(int64_t *x, int32_t precision, int32_t scale, bool *isAnyNull,
    int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64ToDouble(const int64_t *x, int32_t precision, int32_t scale,
    bool *isAnyNull, double *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128ToInt(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    bool *isAnyNull, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128ToLong(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    bool *isAnyNull, int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128ToDouble(Decimal128 *x, int32_t precision, int32_t scale, bool *isAnyNull,
    double *output, int32_t rowCnt);

// Cast Function Return Null
extern "C" DLLEXPORT void BatchCastDecimal64To64RetNull(bool *isNull, int64_t *x, int32_t precision, int32_t scale,
    int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128To128RetNull(bool *isNull, Decimal128 *x, int32_t precision, int32_t scale,
    Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64To128RetNull(bool *isNull, int64_t *x, int32_t precision, int32_t scale,
    Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128To64RetNull(bool *isNull, Decimal128 *x, int32_t precision, int32_t scale,
    int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastIntToDecimal64RetNull(bool *isNull, int32_t *x, int64_t *output, int32_t precision,
    int32_t scale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastLongToDecimal64RetNull(bool *isNull, int64_t *x, int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDoubleToDecimal64RetNull(bool *isNull, double *x, int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastIntToDecimal128RetNull(bool *isNull, int32_t *x, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);
extern "C" DLLEXPORT void BatchCastLongToDecimal128RetNull(bool *isNull, int64_t *x, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDoubleToDecimal128RetNull(bool *isNull, double *x, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64ToIntRetNull(bool *isNull, int64_t *x, int32_t precision, int32_t scale,
    int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64ToLongRetNull(bool *isNull, int64_t *x, int32_t precision, int32_t scale,
    int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64ToDoubleRetNull(bool *isNull, const int64_t *x, int32_t precision,
    int32_t scale, double *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128ToIntRetNull(bool *isNull, Decimal128 *x, int32_t precision, int32_t scale,
    int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128ToLongRetNull(bool *isNull, Decimal128 *x, int32_t precision,
    int32_t scale, int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128ToDoubleRetNull(bool *isNull, Decimal128 *x, int32_t precision,
    int32_t scale, double *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchUnscaledValue64(int64_t *x, int32_t precision, int32_t scale, bool *isAnyNull,
    int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMakeDecimal64(int64_t contextPtr, int64_t *x, bool *isAnyNull, int64_t *output,
    int32_t precision, int32_t scale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMakeDecimal64RetNull(bool *isNull, int64_t *x, int64_t *output, int32_t precision,
    int32_t scale, int32_t rowCnt);
}
}

#endif // OMNI_RUNTIME_BATCH_DECIMALFUNCTIONS_H