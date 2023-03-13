/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch decimal functions implementation
 */

#ifndef OMNI_RUNTIME_BATCH_DECIMAL_CAST_FUNCTIONS_H
#define OMNI_RUNTIME_BATCH_DECIMAL_CAST_FUNCTIONS_H

#include <iostream>
#include <vector>
#include "type/decimal128.h"
#include "type/decimal_operations.h"
#include "type/data_type.h"

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

namespace omniruntime::codegen::function {
using namespace omniruntime::type;

// Cast Function
extern "C" DLLEXPORT void BatchCastDecimal64To64(int64_t contextPtr, int64_t *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128To128(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64To128(int64_t contextPtr, int64_t *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128To64(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastIntToDecimal64(int64_t contextPtr, int32_t *x, const bool *isAnyNull,
    int64_t *output, int32_t precision, int32_t scale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastLongToDecimal64(int64_t contextPtr, int64_t *x, const bool *isAnyNull,
    int64_t *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDoubleToDecimal64(int64_t contextPtr, double *x, const bool *isAnyNull,
    int64_t *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastIntToDecimal128(int64_t contextPtr, int32_t *x, const bool *isAnyNull,
    Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastLongToDecimal128(int64_t contextPtr, int64_t *x, const bool *isAnyNull,
    Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDoubleToDecimal128(int64_t contextPtr, double *x, bool *isAnyNull,
    Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64ToIntDown(int64_t contextPtr, int64_t *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64ToLongDown(int64_t *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64ToDoubleDown(const int64_t *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, double *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64ToIntHalfUp(int64_t contextPtr, int64_t *x, int32_t precision,
    int32_t scale, const bool *isAnyNull, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64ToLongHalfUp(int64_t *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal64ToDoubleHalfUp(const int64_t *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, double *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128ToInt(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128ToLong(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128ToDoubleDown(Decimal128 *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, double *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDecimal128ToDoubleHalfUp(Decimal128 *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, double *output, int32_t rowCnt);

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
}

#endif // OMNI_RUNTIME_BATCH_DECIMAL_CAST_FUNCTIONS_H
