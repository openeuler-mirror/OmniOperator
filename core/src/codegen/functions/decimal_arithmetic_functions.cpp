/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#include "decimal_arithmetic_functions.h"

namespace omniruntime::codegen {

static constexpr int DOUBLE_MAX_PRECISION = std::numeric_limits<double>::max_digits10;
const std::string DECIMAL_OVERFLOW { "Decimal overflow" }; /* NOLINT */
const std::string DIVIDE_ZERO { "Division by zero" }; /* NOLINT */

// decimal128 arithmetical functions
extern "C" DLLEXPORT int32_t Decimal128Compare(int64_t xHigh, uint64_t xLow, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, bool isNull)
{
    Decimal128Wrapper x(xHigh, xLow);
    Decimal128Wrapper y(yHigh, yLow);
    return x.SetScale(xScale).Compare(y.SetScale(yScale));
}

extern "C" DLLEXPORT void AbsDecimal128(int64_t xHigh, uint64_t xLow, int32_t xPrecision, int32_t xScale, bool isNull,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result(xHigh, xLow);
    result.Abs();
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// decimal64 arithmetical functions
extern "C" DLLEXPORT int32_t Decimal64Compare(int64_t x, int32_t xPrecision, int32_t xScale, int64_t y,
    int32_t yPrecision, int32_t yScale, bool isNull)
{
    Decimal64 left(x);
    Decimal64 right(y);
    return left.SetScale(xScale).Compare(right.SetScale(yScale));
}

extern "C" DLLEXPORT int64_t AbsDecimal64(int64_t x, int32_t xPrecision, int32_t xScale, bool isNull,
    int32_t outPrecision, int32_t outScale)
{
    return std::abs(x);
}

// Decimal AddOperator
extern "C" DLLEXPORT int64_t AddDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalAdd(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision)
    return result.GetValue();
}

extern "C" DLLEXPORT void AddDec64Dec64Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec128Dec64Dec128(int64_t contextPtr, int64_t yHigh, uint64_t yLow, int32_t yPrecision,
    int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}


// Decimal SubOperator
extern "C" DLLEXPORT int64_t SubDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalSubtract(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision)
    return result.GetValue();
}

extern "C" DLLEXPORT void SubDec64Dec64Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128Dec64Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal MulOperator
extern "C" DLLEXPORT int64_t MulDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalMultiply(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision)
    return result.GetValue();
}

extern "C" DLLEXPORT void MulDec64Dec64Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    if (ConfigUtil::GetCheckReScaleRule() == CheckReScaleRule::NOT_CHECK_RESCALE) {
        DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        CHECK_OVERFLOW(result, outPrecision)
        *outHighPtr = result.HighBits();
        *outLowPtr = result.LowBits();
    } else {
        result =
            Decimal128Wrapper(xHigh, xLow).MultiplyRoundUp(Decimal128Wrapper(yHigh, yLow), xScale + yScale - outScale);
        CHECK_OVERFLOW(result, outPrecision)
        *outHighPtr = result.HighBits();
        *outLowPtr = result.LowBits();
    }
}

extern "C" DLLEXPORT void MulDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec64Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(xHigh, xLow), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal DivOperation
extern "C" DLLEXPORT int64_t DivDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalDivide(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision)
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t DivDec64Dec128Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor)
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision)
    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT int64_t DivDec128Dec64Dec64(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor)
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision)
    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT void DivDec64Dec64Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO(divisor)
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO(divisor)
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO(divisor)
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec128Dec64Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO(divisor)
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal ModOperation
extern "C" DLLEXPORT int64_t ModDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision)
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t ModDec64Dec128Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor)
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision)
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t ModDec128Dec64Dec64(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor)
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision)
    return result.GetValue();
}

extern "C" DLLEXPORT void ModDec128Dec64Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO(divisor)
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void ModDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO(divisor)
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int64_t ModDec128Dec128Dec64(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor)
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision)

    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT void ModDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO(divisor)
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale),
        yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision)
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// return null
extern "C" DLLEXPORT int64_t AddDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalAdd(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result.GetValue();
}

extern "C" DLLEXPORT void AddDec64Dec64Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec128Dec64Dec128RetNull(bool *isNull, int64_t yHigh, uint64_t yLow, int32_t yPrecision,
    int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}


// Decimal SubOperator
extern "C" DLLEXPORT int64_t SubDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalSubtract(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result.GetValue();
}

extern "C" DLLEXPORT void SubDec64Dec64Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128Dec64Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal MulOperator
extern "C" DLLEXPORT int64_t MulDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalMultiply(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result.GetValue();
}

extern "C" DLLEXPORT void MulDec64Dec64Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result = Decimal128Wrapper(xHigh, xLow).MultiplyRoundUp(Decimal128Wrapper(yHigh, yLow),
        xScale + yScale - outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec64Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal DivOperation
extern "C" DLLEXPORT int64_t DivDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalDivide(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t DivDec64Dec128Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t DivDec128Dec64Dec64RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result.GetValue();
}

extern "C" DLLEXPORT void DivDec64Dec64Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec128Dec64Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal ModOperation
extern "C" DLLEXPORT int64_t ModDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t ModDec64Dec128Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t ModDec128Dec64Dec64RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result.GetValue();
}

extern "C" DLLEXPORT void ModDec128Dec64Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void ModDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int64_t ModDec128Dec128Dec64RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result.GetValue();
}

extern "C" DLLEXPORT void ModDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int64_t UnscaledValue64(int64_t x, int32_t precision, int32_t scale)
{
    return x;
}

extern "C" DLLEXPORT int64_t MakeDecimal64(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale)
{
    if (DecimalOperations::IsUnscaledLongOverflow(x, precision, scale)) {
        ReSetErrorMessage();
        errorMessage << "Unscaled value " << x << " out of Decimal(" << precision << ", " << scale << ") range";
        SetError(contextPtr, errorMessage.str());
        return 0;
    }
    return x;
}

extern "C" DLLEXPORT int64_t MakeDecimal64RetNull(bool *isNull, int64_t x, int32_t precision, int32_t scale)
{
    if (DecimalOperations::IsUnscaledLongOverflow(x, precision, scale)) {
        *isNull = true;
        return 0;
    }
    *isNull = false;
    return x;
}

extern "C" DLLEXPORT void RoundDecimal128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int32_t round, bool isNull, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper input(xHigh, xLow);
    input.SetScale(xScale);
    DecimalOperations::Round(input, outScale, round);
    if (input.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        SetError(contextPtr, DECIMAL_OVERFLOW);
        return;
    }
    *outHighPtr = input.HighBits();
    *outLowPtr = input.LowBits();
}

extern "C" DLLEXPORT void RoundDecimal128WithoutRound(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, bool isNull, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper input(xHigh, xLow);
    input.SetScale(xScale);
    DecimalOperations::Round(input, outScale, 0);
    if (input.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        SetError(contextPtr, DECIMAL_OVERFLOW);
        return;
    }
    *outHighPtr = input.HighBits();
    *outLowPtr = input.LowBits();
}

extern "C" DLLEXPORT int64_t RoundDecimal64(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int32_t round, bool isNull, int32_t outPrecision, int32_t outScale)
{
    Decimal64 input(x);
    input.SetScale(xScale);
    DecimalOperations::Round(input, outScale, round);
    if (input.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        SetError(contextPtr, DECIMAL_OVERFLOW);
        return 0;
    }
    return input.GetValue();
}

extern "C" DLLEXPORT int64_t RoundDecimal64WithoutRound(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, bool isNull, int32_t outPrecision, int32_t outScale)
{
    Decimal64 input(x);
    input.SetScale(xScale);
    DecimalOperations::Round(input, outScale, 0);
    if (input.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        SetError(contextPtr, DECIMAL_OVERFLOW);
        return 0;
    }
    return input.GetValue();
}

extern "C" DLLEXPORT void RoundDecimal128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int32_t round, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper input(xHigh, xLow);
    input.SetScale(xScale);
    DecimalOperations::Round(input, outScale, 0);
    if (input.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull= true;
        return;
    }
    *outHighPtr = input.HighBits();
    *outLowPtr = input.LowBits();
}

extern "C" DLLEXPORT int64_t RoundDecimal64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int32_t round, int32_t outPrecision, int32_t outScale)
{
    Decimal64 input(x);
    input.SetScale(xScale);
    DecimalOperations::Round(input, outScale, round);
    if (input.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
        *isNull= true;
        return 0;
    }
    return input.GetValue();
}
}