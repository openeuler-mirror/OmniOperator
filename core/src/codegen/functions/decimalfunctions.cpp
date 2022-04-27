/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#include "decimalfunctions.h"
#include <cmath>
#include "type/decimal_operations.h"

using namespace omniruntime::type;
using namespace std;

namespace omniruntime {
namespace codegen {
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT int32_t Decimal128Compare(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow)
{
    Decimal128 left(xHigh, xLow);
    Decimal128 right(yHigh, yLow);

    if (left < right) {
        return -1;
    }
    if (left > right) {
        return 1;
    }
    return 0;
}

extern "C" DLLEXPORT void AddDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue(yHigh, yLow);
    Decimal128 result(0, 0);
    DecimalOperations::AddWithOverflow(lValue, rValue, result);

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue(yHigh, yLow);
    Decimal128 result(0, 0);
    DecimalOperations::Subtract(lValue, rValue, result);

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue(yHigh, yLow);
    Decimal128 result = DecimalOperations::DivideRoundUp(lValue, rValue, 0, 0);

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue(yHigh, yLow);
    Decimal128 result(0, 0);
    DecimalOperations::Multiply(lValue, rValue, result);

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AbsDecimal128(int64_t xHigh, uint64_t xLow, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 value(xHigh, xLow);

    auto result = DecimalOperations::AbsExact(value);

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void CastInt64ToDecimal128(int64_t x, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (x >= 0) {
        *outHighPtr = 0;
        *outLowPtr = x;
    } else {
        *outHighPtr = 1L << 63;
        *outLowPtr = -x;
    }
}

extern "C" DLLEXPORT int64_t MakeDecimal64(int64_t x, int32_t precision, int32_t scale, int32_t newPrecision,
    int32_t newScale)
{
    return int64_t(x * pow(10, newScale - scale));
}

extern "C" DLLEXPORT void MakeDecimal128(int64_t xHigh, uint64_t xLow, int32_t precision, int32_t scale,
    int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    int32_t scaleDelta = newScale - scale;
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue;
    rValue.SetValue(0, int64_t(pow(10, abs(scaleDelta))));
    Decimal128 result(0, 0);
    if (scaleDelta >= 0) {
        DecimalOperations::Multiply(lValue, rValue, result);
    } else {
        result = DecimalOperations::DivideRoundUp(lValue, rValue, 0, 0);
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MakeDecimal64To128(int64_t x, int32_t precision, int32_t scale, int32_t newPrecision,
    int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    int32_t scaleDelta = newScale - scale;
    Decimal128 lValue;
    Decimal128 rValue;
    rValue.SetValue(0, int64_t(pow(10, abs(scaleDelta))));
    if (x >= 0) {
        lValue.SetValue(0, x);
    } else {
        lValue.SetValue(1L << 63, -x);
    }
    Decimal128 result(0, 0);
    if (scaleDelta >= 0) {
        DecimalOperations::Multiply(lValue, rValue, result);
    } else {
        result = DecimalOperations::DivideRoundUp(lValue, rValue, 0, 0);
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int64_t DivDec64(int64_t x, int64_t y)
{
    return (int64_t) round(double(x) / y);
}

extern "C" DLLEXPORT int64_t DownScaleDec64(int64_t x, int32_t y)
{
    return (int64_t) round(x / pow(10, y));
}
}
}