/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#include "decimal_arithmetic_functions.h"

using namespace omniruntime::type;

namespace omniruntime::codegen::function {
static constexpr int DOUBLE_MAX_PRECISION = std::numeric_limits<double>::max_digits10;
const std::string DECIMAL_OVERFLOW { "Decimal overflow" }; /* NOLINT */
const std::string DIVIDE_ZERO { "Division by zero" };      /* NOLINT */

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

// Decimal AddOperator ReScale
extern "C" DLLEXPORT int64_t AddDec64Dec64Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalAdd(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT void AddDec64Dec64Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec128Dec128Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec64Dec128Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec128Dec64Dec128ReScale(int64_t contextPtr, int64_t yHigh, uint64_t yLow,
    int32_t yPrecision, int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}


// Decimal SubOperator  ReScale
extern "C" DLLEXPORT int64_t SubDec64Dec64Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalSubtract(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT void SubDec64Dec64Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128Dec128Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec64Dec128Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128Dec64Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal MulOperator ReScale
extern "C" DLLEXPORT int64_t MulDec64Dec64Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalMultiply(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT void MulDec64Dec64Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec128Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result =
        Decimal128Wrapper(xHigh, xLow).MultiplyRoundUp(Decimal128Wrapper(yHigh, yLow), xScale + yScale - outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec64Dec128Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec64Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(xHigh, xLow), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal DivOperation ReScale
extern "C" DLLEXPORT int64_t DivDec64Dec64Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    CHECK_DIVIDE_BY_ZERO_RETURN(y);
    Decimal64 result;
    DecimalOperations::InternalDecimalDivide(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t DivDec64Dec128Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT int64_t DivDec128Dec64Dec64ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT void DivDec64Dec64Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec128Dec128Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec64Dec128Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec128Dec64Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal ModOperation ReScale
extern "C" DLLEXPORT int64_t ModDec64Dec64Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    CHECK_DIVIDE_BY_ZERO_RETURN(y);
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t ModDec64Dec128Dec64ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor);
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t ModDec128Dec64Dec64ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor);
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT void ModDec128Dec64Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void ModDec128Dec128Dec128ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int64_t ModDec128Dec128Dec64ReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);

    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT void ModDec64Dec128Dec128ReScale(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    result.ReScale(outScale);
    CHECK_OVERFLOW(result, outPrecision);
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
    CHECK_OVERFLOW_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}


// Decimal AddOperator NotReScale
extern "C" DLLEXPORT int64_t AddDec64Dec64Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalAdd(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT void AddDec64Dec64Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec128Dec128Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec64Dec128Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec128Dec64Dec128NotReScale(int64_t contextPtr, int64_t yHigh, uint64_t yLow,
    int32_t yPrecision, int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}


// Decimal SubOperator  NotReScale
extern "C" DLLEXPORT int64_t SubDec64Dec64Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalSubtract(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT void SubDec64Dec64Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128Dec128Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec64Dec128Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128Dec64Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal MulOperator NotReScale
extern "C" DLLEXPORT int64_t MulDec64Dec64Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalMultiply(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT void MulDec64Dec64Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec128Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec64Dec128Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec64Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(xHigh, xLow), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal DivOperation NotReScale
extern "C" DLLEXPORT int64_t DivDec64Dec64Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    CHECK_DIVIDE_BY_ZERO_RETURN(y);
    Decimal64 result;
    DecimalOperations::InternalDecimalDivide(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result, outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t DivDec64Dec128Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT int64_t DivDec128Dec64Dec64NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT void DivDec64Dec64Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec128Dec128Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec64Dec128Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec128Dec64Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result, outScale);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal ModOperation NotReScale
extern "C" DLLEXPORT int64_t ModDec64Dec64Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    CHECK_DIVIDE_BY_ZERO_RETURN(y);
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal64(x).SetScale(xScale), xScale, xPrecision,
        Decimal64(y).SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t ModDec64Dec128Dec64NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor);
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t ModDec128Dec64Dec64NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor);
    Decimal64 result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW_RETURN(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT void ModDec128Dec64Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(y);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void ModDec128Dec128Dec128NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int64_t ModDec128Dec128Dec64NotReScale(int64_t contextPtr, int64_t xHigh, uint64_t xLow,
    int32_t xPrecision, int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale,
    int32_t outPrecision, int32_t outScale)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO_RETURN(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW_RETURN(result, outPrecision);

    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT void ModDec64Dec128Dec128NotReScale(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper divisor(yHigh, yLow);
    CHECK_DIVIDE_BY_ZERO(divisor);
    Decimal128Wrapper result;
    DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        divisor.SetScale(yScale), yScale, yPrecision, result);
    CHECK_OVERFLOW(result, outPrecision);
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
    CHECK_OVERFLOW_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper result =
        Decimal128Wrapper(xHigh, xLow).MultiplyRoundUp(Decimal128Wrapper(yHigh, yLow), xScale + yScale - outScale);
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_RETURN_NULL(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t DivDec64Dec128Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(yHigh, yLow).SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN_NULL(result, outPrecision);
    return result.GetValue();
}

extern "C" DLLEXPORT int64_t DivDec128Dec64Dec64RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal64 result;
    DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(xHigh, xLow).SetScale(xScale), xScale, xPrecision,
        Decimal128Wrapper(y).SetScale(yScale), yScale, yPrecision, result, outScale);
    result.ReScale(outScale);
    CHECK_OVERFLOW_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_RETURN_NULL(result, outPrecision);
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
    CHECK_OVERFLOW_VOID_RETURN_NULL(result, outPrecision);
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int64_t UnscaledValue64(int64_t x, int32_t precision, int32_t scale, bool isNull)
{
    return x;
}

extern "C" DLLEXPORT int64_t MakeDecimal64(int64_t contextPtr, int64_t x, bool isNull, int32_t precision, int32_t scale)
{
    if (DecimalOperations::IsUnscaledLongOverflow(x, precision, scale)) {
        std::ostringstream errorMessage;
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
    CHECK_OVERFLOW(input, outPrecision);
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
    CHECK_OVERFLOW(input, outPrecision);
    *outHighPtr = input.HighBits();
    *outLowPtr = input.LowBits();
}

extern "C" DLLEXPORT int64_t RoundDecimal64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int32_t round, bool isNull, int32_t outPrecision, int32_t outScale)
{
    Decimal64 input(x);
    input.SetScale(xScale);
    DecimalOperations::Round(input, outScale, round);
    CHECK_OVERFLOW_RETURN(input, outPrecision);
    return input.GetValue();
}

extern "C" DLLEXPORT int64_t RoundDecimal64WithoutRound(int64_t contextPtr, int64_t x, int32_t xPrecision,
    int32_t xScale, bool isNull, int32_t outPrecision, int32_t outScale)
{
    Decimal64 input(x);
    input.SetScale(xScale);
    DecimalOperations::Round(input, outScale, 0);
    CHECK_OVERFLOW_RETURN(input, outPrecision);
    return input.GetValue();
}

extern "C" DLLEXPORT void RoundDecimal128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int32_t round, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128Wrapper input(xHigh, xLow);
    input.SetScale(xScale);
    DecimalOperations::Round(input, outScale, 0);
    CHECK_OVERFLOW_VOID_RETURN_NULL(input, outPrecision);
    *outHighPtr = input.HighBits();
    *outLowPtr = input.LowBits();
}

extern "C" DLLEXPORT int64_t RoundDecimal64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int32_t round, int32_t outPrecision, int32_t outScale)
{
    Decimal64 input(x);
    input.SetScale(xScale);
    DecimalOperations::Round(input, outScale, round);
    CHECK_OVERFLOW_RETURN_NULL(input, outPrecision);
    return input.GetValue();
}
}