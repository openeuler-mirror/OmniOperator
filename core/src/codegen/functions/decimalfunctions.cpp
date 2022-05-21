/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#include "decimalfunctions.h"
#include <cmath>
#include "context_helper.h"
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

extern "C" DLLEXPORT void DivDec128(int64_t contextPtr,
    int64_t xHigh, uint64_t xLow, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    int32_t scaleFactor = outScale - xScale + yScale;
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue(yHigh, yLow);
    if (yHigh == 0 && yLow == 0) {
        *outHighPtr = 0;
        *outLowPtr = 0;
        char message[] = "Division by zero error!";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }
    Decimal128 result = DecimalOperations::DivideRoundUp(lValue, rValue, scaleFactor, 0);

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

extern "C" DLLEXPORT int64_t CastDoubleToDecimal64(double x, int32_t precision, int32_t scale)
{
    return (int64_t)(x * pow(10, scale));
}
extern "C" DLLEXPORT int64_t MakeDecimal64(int64_t x, int32_t precision, int32_t scale, int32_t newPrecision,
    int32_t newScale)
{
    return DecimalOperations::Rescale64(x, newScale - scale);
}

extern "C" DLLEXPORT void MakeDecimal128(int64_t xHigh, uint64_t xLow, int32_t precision, int32_t scale,
    int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    int32_t scaleDelta = newScale - scale;
    Decimal128 lValue(xHigh, xLow);
    Decimal128 result;
    DecimalOperations::Rescale128(lValue, scaleDelta, result);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MakeDecimal64To128(int64_t x, int32_t precision, int32_t scale, int32_t newPrecision,
    int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    int32_t scaleDelta = newScale - scale;
    Decimal128 result;
    DecimalOperations::Rescale64To128(x, scaleDelta, result);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int64_t DivDec64(int64_t contextPtr, int64_t x, int64_t y)
{
    if (y == 0) {
        char message[] = "Divided by zero error!";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    Decimal128 dividend = DecimalOperations::UnscaledDecimal(x);
    Decimal128 divisor = DecimalOperations::UnscaledDecimal(y);
    auto result = DecimalOperations::DivideRoundUp(dividend, divisor, 0, 0);
    int64_t low = result.LowBits();
    int64_t shortResult = DecimalOperations::IsNegative(result) ? -low : low;
    return shortResult;
}

extern "C" DLLEXPORT int64_t DownScaleDec64(int64_t x, int32_t y)
{
    return (int64_t)round(x / pow(10, y));
}

extern "C" DLLEXPORT int64_t MakeDecimalLongTo64(int64_t x, int32_t precision, int32_t scale)
{
    return (int64_t)(x);
}

extern "C" DLLEXPORT int64_t UnscaledValue64(int64_t x, int32_t precision, int32_t scale)
{
    return (int64_t)(x);
}

extern "C" DLLEXPORT bool IsOverflowDecimal64(int64_t x, int32_t precision, int32_t scale, int32_t checkPrecision,
    int32_t checkScale)
{
    int wholeNumerSize = precision - scale;
    int checkWholeNumerSize = checkPrecision - checkScale;
    if (checkWholeNumerSize >= wholeNumerSize) {
        return false;
    }
    int left = scale + checkWholeNumerSize;
    int64_t numverValue = abs(x);
    while (left > 0) {
        numverValue = numverValue / 10;
        left--;
    }
    return numverValue > 0;
}

extern "C" DLLEXPORT bool IsOverflowDecimal128(int64_t xHigh, uint64_t xLow, int32_t precision, int32_t scale,
    int32_t checkPrecision, int32_t checkScale)
{
    return false;
}
}
}