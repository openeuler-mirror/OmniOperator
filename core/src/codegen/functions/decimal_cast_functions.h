/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch decimal functions implementation
 */

#ifndef OMNI_RUNTIME_DECIMAL_CAST_FUNCTIONS_H
#define OMNI_RUNTIME_DECIMAL_CAST_FUNCTIONS_H

#include <iostream>
#include <vector>
#include <cmath>
#include <iomanip>
#include "type/decimal128.h"
#include "codegen/context_helper.h"
#include "type/decimal_operations.h"
#include "util/config_util.h"
#include "type/data_type.h"

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

namespace omniruntime::codegen::function {
using namespace omniruntime::type;

// Cast
extern "C" DLLEXPORT int64_t CastDecimal64To64(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale,
    bool isNull, int32_t newPrecision, int32_t newScale);

extern "C" DLLEXPORT void CastDecimal128To128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, bool isNull, int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void CastDecimal64To128(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale,
    bool isNull, int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int64_t CastDecimal128To64(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, bool isNull, int32_t newPrecision, int32_t newScale);

extern "C" DLLEXPORT int64_t CastIntToDecimal64(int64_t contextPtr, int32_t x, bool isNull, int32_t precision,
    int32_t scale);

extern "C" DLLEXPORT int64_t CastLongToDecimal64(int64_t contextPtr, int64_t x, bool isNull, int32_t outPrecision,
    int32_t outScale);

extern "C" DLLEXPORT int64_t CastDoubleToDecimal64(int64_t contextPtr, double x, bool isNull, int32_t precision,
    int32_t scale);

extern "C" DLLEXPORT void CastIntToDecimal128(int64_t contextPtr, int32_t x, bool isNull, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void CastLongToDecimal128(int64_t contextPtr, int64_t x, bool isNull, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT void CastDoubleToDecimal128(int64_t contextPtr, double x, bool isNull, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr);

extern "C" DLLEXPORT int32_t CastDecimal64ToIntHalfUp(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale,
    bool isNull);

extern "C" DLLEXPORT int64_t CastDecimal64ToLongHalfUp(int64_t x, int32_t precision, int32_t scale, bool isNull);

extern "C" DLLEXPORT double CastDecimal64ToDoubleHalfUp(int64_t x, int32_t precision, int32_t scale, bool isNull);

extern "C" DLLEXPORT int32_t CastDecimal128ToIntHalfUp(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t precision, int32_t scale, bool isNull);

extern "C" DLLEXPORT int64_t CastDecimal128ToLongHalfUp(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t precision, int32_t scale, bool isNull);

extern "C" DLLEXPORT double CastDecimal128ToDoubleHalfUp(int64_t xHigh, uint64_t xLow, int32_t precision, int32_t scale,
    bool isNull);

extern "C" DLLEXPORT int32_t CastDecimal64ToIntDown(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale,
    bool isNull);

extern "C" DLLEXPORT int64_t CastDecimal64ToLongDown(int64_t x, int32_t precision, int32_t scale, bool isNull);

extern "C" DLLEXPORT double CastDecimal64ToDoubleDown(int64_t x, int32_t precision, int32_t scale, bool isNull);

extern "C" DLLEXPORT int32_t CastDecimal128ToIntDown(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t precision, int32_t scale, bool isNull);

extern "C" DLLEXPORT int64_t CastDecimal128ToLongDown(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t precision, int32_t scale, bool isNull);

extern "C" DLLEXPORT double CastDecimal128ToDoubleDown(int64_t xHigh, uint64_t xLow, int32_t precision, int32_t scale,
    bool isNull);

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
}


#endif // OMNI_RUNTIME_DECIMAL_CAST_FUNCTIONS_H
