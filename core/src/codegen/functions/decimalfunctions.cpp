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

// decimal128 arithmetical functions
extern "C" DLLEXPORT int32_t Decimal128Compare(int64_t xHigh, uint64_t xLow, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale)
{
    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);
    Decimal128 x(xHigh, xLow);
    Decimal128 y(yHigh, yLow);
    Decimal128 xRescaled = DecimalOperations::UnscaledDecimal(0);
    Decimal128 yRescaled = DecimalOperations::UnscaledDecimal(0);
    if (xRescaleFactor > 0) {
        DecimalOperations::Rescale128(x, xRescaleFactor, xRescaled);
        yRescaled = y;
    } else {
        DecimalOperations::Rescale128(y, yRescaleFactor, yRescaled);
        xRescaled = x;
    }
    if (xRescaled < yRescaled) {
        return -1;
    }
    if (xRescaled > yRescaled) {
        return 1;
    }
    return 0;
}

extern "C" DLLEXPORT void AddDec128(int64_t xHigh, uint64_t xLow, int32_t xPrecision, int32_t xScale, int64_t yHigh,
    uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 x(xHigh, xLow);
    Decimal128 y(yHigh, yLow);
    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);

    Decimal128 left = DecimalOperations::UnscaledDecimal(0);
    Decimal128 right;
    if (xRescaleFactor > 0) {
        DecimalOperations::Rescale128(x, xRescaleFactor, left);
        right = y;
    } else {
        DecimalOperations::Rescale128(y, yRescaleFactor, left);
        right = x;
    }

    DecimalOperations::AddWithOverflow(left, right, left);

    *outHighPtr = left.HighBits();
    *outLowPtr = left.LowBits();
}

extern "C" DLLEXPORT void SubDec128(int64_t xHigh, uint64_t xLow, int32_t xPrecision, int32_t xScale, int64_t yHigh,
    uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 x(xHigh, xLow);
    Decimal128 y(yHigh, yLow);
    Decimal128 tmp = DecimalOperations::UnscaledDecimal(0);

    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);

    if (xRescaleFactor > 0) {
        DecimalOperations::Rescale128(x, xRescaleFactor, tmp);
        DecimalOperations::Subtract(tmp, y, tmp);
    } else {
        DecimalOperations::Rescale128(y, yRescaleFactor, tmp);
        DecimalOperations::Subtract(x, tmp, tmp);
    }
    *outHighPtr = tmp.HighBits();
    *outLowPtr = tmp.LowBits();
}

extern "C" DLLEXPORT void MulDec128(int64_t xHigh, uint64_t xLow, int32_t xPrecision, int32_t xScale, int64_t yHigh,
    uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue(yHigh, yLow);
    Decimal128 result(0, 0);
    DecimalOperations::Multiply(lValue, rValue, result);

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (yHigh == 0 && yLow == 0) {
        *outHighPtr = 0;
        *outLowPtr = 0;
        char message[] = "Division by zero error!";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }

    int32_t scaleFactor = DecimalOperations::DivideRescaleFactor(xScale, yScale, outScale);
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue(yHigh, yLow);
    Decimal128 result = DecimalOperations::DivideRoundUp(lValue, rValue, scaleFactor, 0);

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void ModDec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (yHigh == 0 && yLow == 0) {
        *outHighPtr = 0;
        *outLowPtr = 0;
        char message[] = "Division by zero error!";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }

    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue(yHigh, yLow);
    Decimal128 res;
    if (xRescaleFactor > 0) {
        res = DecimalOperations::Remainder(lValue, xRescaleFactor, rValue, 0);
    } else {
        res = DecimalOperations::Remainder(lValue, 0, rValue, yRescaleFactor);
    };

    *outHighPtr = res.HighBits();
    *outLowPtr = res.LowBits();
}

extern "C" DLLEXPORT void AbsDecimal128(int64_t xHigh, uint64_t xLow, int32_t xPrecision, int32_t xScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 value(xHigh, xLow);

    auto result = DecimalOperations::AbsExact(value);

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// decimal64 arithmetical functions
extern "C" DLLEXPORT int32_t Decimal64Compare(int64_t x, int32_t xPrecision, int32_t xScale, int64_t y,
    int32_t yPrecision, int32_t yScale)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(x);
    Decimal128 right = DecimalOperations::UnscaledDecimal(y);
    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);
    if (xRescaleFactor > 0) {
        DecimalOperations::Rescale128(left, xRescaleFactor, left);
    } else {
        DecimalOperations::Rescale128(right, yRescaleFactor, right);
    }

    if (left < right) {
        return -1;
    }
    if (left > right) {
        return 1;
    }
    return 0;
}

extern "C" DLLEXPORT int64_t AddDec64Ret64(int64_t x, int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision,
    int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(x);
    Decimal128 right = DecimalOperations::UnscaledDecimal(y);
    Decimal128 res = DecimalOperations::UnscaledDecimal(0);

    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);

    if (xRescaleFactor > 0) {
        DecimalOperations::Rescale128(left, xRescaleFactor, res);
        DecimalOperations::AddWithOverflow(res, right, res);
    } else {
        DecimalOperations::Rescale128(right, yRescaleFactor, res);
        DecimalOperations::AddWithOverflow(left, res, res);
    }

    return res.HighBits() < 0 ? -res.LowBits() : res.LowBits();
}

extern "C" DLLEXPORT void AddDec64Ret128(int64_t x, int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision,
    int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(x);
    Decimal128 right = DecimalOperations::UnscaledDecimal(y);
    Decimal128 res = DecimalOperations::UnscaledDecimal(0);

    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);

    if (xRescaleFactor > 0) {
        DecimalOperations::Rescale128(left, xRescaleFactor, res);
        DecimalOperations::AddWithOverflow(res, right, res);
    } else {
        DecimalOperations::Rescale128(right, yRescaleFactor, res);
        DecimalOperations::AddWithOverflow(left, res, res);
    }

    *outHighPtr = res.HighBits();
    *outLowPtr = res.LowBits();
}

extern "C" DLLEXPORT int64_t SubDec64Ret64(int64_t x, int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision,
    int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(x);
    Decimal128 right = DecimalOperations::UnscaledDecimal(y);
    Decimal128 res = DecimalOperations::UnscaledDecimal(0);

    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);

    if (xRescaleFactor > 0) {
        DecimalOperations::Rescale128(left, xRescaleFactor, res);
        DecimalOperations::Subtract(res, right, res);
    } else {
        DecimalOperations::Rescale128(right, yRescaleFactor, res);
        DecimalOperations::Subtract(left, res, res);
    }

    return res.HighBits() < 0 ? -res.LowBits() : res.LowBits();
}

extern "C" DLLEXPORT void SubDec64Ret128(int64_t x, int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision,
    int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(x);
    Decimal128 right = DecimalOperations::UnscaledDecimal(y);
    Decimal128 res = DecimalOperations::UnscaledDecimal(0);

    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);

    if (xRescaleFactor > 0) {
        DecimalOperations::Rescale128(left, xRescaleFactor, res);
        DecimalOperations::Subtract(res, right, res);
    } else {
        DecimalOperations::Rescale128(right, yRescaleFactor, res);
        DecimalOperations::Subtract(left, res, res);
    }

    *outHighPtr = res.HighBits();
    *outLowPtr = res.LowBits();
}

extern "C" DLLEXPORT int64_t MulDec64Ret64(int64_t x, int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision,
    int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(x);
    Decimal128 right = DecimalOperations::UnscaledDecimal(y);
    Decimal128 res = DecimalOperations::UnscaledDecimal(0);
    DecimalOperations::Multiply(left, right, res);
    int32_t deltaScale = xScale + yScale - outScale;
    if (deltaScale != 0) {
        Decimal128 temp(0, (uint64_t)pow(10, deltaScale));
        res = DecimalOperations::DivideRoundUp(res, temp, 0, 0);
    }

    return res.HighBits() < 0 ? -res.LowBits() : res.LowBits();
}

extern "C" DLLEXPORT void MulDec64Ret128(int64_t x, int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision,
    int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(x);
    Decimal128 right = DecimalOperations::UnscaledDecimal(y);
    Decimal128 res = DecimalOperations::UnscaledDecimal(0);
    DecimalOperations::Multiply(left, right, res);
    int32_t deltaScale = xScale + yScale - outScale;
    if (deltaScale != 0) {
        Decimal128 temp(0, (uint64_t)pow(10, deltaScale));
        res = DecimalOperations::DivideRoundUp(res, temp, 0, 0);
    }

    *outHighPtr = res.HighBits();
    *outLowPtr = res.LowBits();
}

extern "C" DLLEXPORT int64_t DivDec64Ret64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale, int64_t y,
    int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (y == 0) {
        char message[] = "Divided by zero error!";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    int32_t scaleFactor = DecimalOperations::DivideRescaleFactor(xScale, yScale, outScale);
    Decimal128 dividend = DecimalOperations::UnscaledDecimal(x);
    Decimal128 divisor = DecimalOperations::UnscaledDecimal(y);
    auto res = DecimalOperations::DivideRoundUp(dividend, divisor, scaleFactor, 0);

    return res.HighBits() < 0 ? -res.LowBits() : res.LowBits();
}

extern "C" DLLEXPORT void DivDec64Ret128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale, int64_t y,
    int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    if (y == 0) {
        char message[] = "Divided by zero error!";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }
    int32_t scaleFactor = DecimalOperations::DivideRescaleFactor(xScale, yScale, outScale);
    Decimal128 dividend = DecimalOperations::UnscaledDecimal(x);
    Decimal128 divisor = DecimalOperations::UnscaledDecimal(y);
    auto res = DecimalOperations::DivideRoundUp(dividend, divisor, scaleFactor, 0);

    *outHighPtr = res.HighBits();
    *outLowPtr = res.LowBits();
}

extern "C" DLLEXPORT int64_t ModDec64Ret64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale, int64_t y,
    int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (y == 0) {
        char message[] = "Divided by zero error!";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);
    Decimal128 dividend = DecimalOperations::UnscaledDecimal(x);
    Decimal128 divisor = DecimalOperations::UnscaledDecimal(y);
    Decimal128 res;
    if (xRescaleFactor > 0) {
        res = DecimalOperations::Remainder(dividend, xRescaleFactor, divisor, 0);
    } else {
        res = DecimalOperations::Remainder(dividend, 0, divisor, yRescaleFactor);
    };

    int64_t low = res.LowBits();
    int64_t shortResult = DecimalOperations::IsNegative(res) ? -low : low;
    return shortResult;
}

extern "C" DLLEXPORT void ModDec64Ret128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale, int64_t y,
    int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    if (y == 0) {
        char message[] = "Divided by zero error!";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }
    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);
    Decimal128 dividend = DecimalOperations::UnscaledDecimal(x);
    Decimal128 divisor = DecimalOperations::UnscaledDecimal(y);
    Decimal128 res;
    if (xRescaleFactor > 0) {
        res = DecimalOperations::Remainder(dividend, xRescaleFactor, divisor, 0);
    } else {
        res = DecimalOperations::Remainder(dividend, 0, divisor, yRescaleFactor);
    };

    *outHighPtr = res.HighBits();
    *outLowPtr = res.LowBits();
}

extern "C" DLLEXPORT int64_t AbsDecimal64(int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision,
    int32_t outScale)
{
    return std::abs(x);
}

extern "C" DLLEXPORT void CastInt64ToDecimal128(int64_t x, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
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