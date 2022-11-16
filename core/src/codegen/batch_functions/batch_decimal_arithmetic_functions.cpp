/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: batch decimal functions implementation
 */
#include "batch_decimal_arithmetic_functions.h"
#include <cmath>
#include <iomanip>
#include "codegen/context_helper.h"
#include "type/decimal_operations.h"

using namespace omniruntime::type;

namespace omniruntime::codegen {

const std::string DECIMAL_OVERFLOW { "Decimal overflow" };
const std::string DIVIDE_ZERO { "Division by zero" };
static constexpr int DOUBLE_MAX_PRECISION = std::numeric_limits<double>::max_digits10;

extern "C" DLLEXPORT void BatchDecimal128Compare(Decimal128 *x, int32_t xPrecision, int32_t xScale, Decimal128 *y,
    int32_t yPrecision, int32_t yScale, int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = Decimal128Wrapper(x[i]).SetScale(xScale).Compare(Decimal128Wrapper(y[i]).SetScale(yScale));
    }
}

extern "C" DLLEXPORT void BatchLessThanDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale,
    Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt)
{
    auto tmp = new int32_t[rowCnt];
    BatchDecimal128Compare(left, xPrecision, xScale, right, yPrecision, yScale, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (tmp[i] < 0);
    }
    delete[] tmp;
}

extern "C" DLLEXPORT void BatchLessThanEqualDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale,
    Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt)
{
    auto tmp = new int32_t[rowCnt];
    BatchDecimal128Compare(left, xPrecision, xScale, right, yPrecision, yScale, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (tmp[i] <= 0);
    }
    delete[] tmp;
}

extern "C" DLLEXPORT void BatchGreaterThanDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale,
    Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt)
{
    auto tmp = new int32_t[rowCnt];
    BatchDecimal128Compare(left, xPrecision, xScale, right, yPrecision, yScale, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (tmp[i] > 0);
    }
    delete[] tmp;
}

extern "C" DLLEXPORT void BatchGreaterThanEqualDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale,
    Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt)
{
    auto tmp = new int32_t[rowCnt];
    BatchDecimal128Compare(left, xPrecision, xScale, right, yPrecision, yScale, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (tmp[i] >= 0);
    }
    delete[] tmp;
}

extern "C" DLLEXPORT void BatchEqualDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale,
    Decimal128 *right,
    int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt)
{
    auto tmp = new int32_t[rowCnt];
    BatchDecimal128Compare(left, xPrecision, xScale, right, yPrecision, yScale, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (tmp[i] == 0);
    }
    delete[] tmp;
}

extern "C" DLLEXPORT void BatchNotEqualDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale,
    Decimal128 *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt)
{
    auto tmp = new int32_t[rowCnt];
    BatchDecimal128Compare(left, xPrecision, xScale, right, yPrecision, yScale, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (tmp[i] != 0);
    }
    delete[] tmp;
}

extern "C" DLLEXPORT void BatchAbsDecimal128(Decimal128 *x, int32_t xPrecision, int32_t xScale, bool *isNull,
    Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = Decimal128Wrapper(x[i]).Abs().ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchRoundDecimal128(int64_t contextPtr, Decimal128 *x, int32_t xPrecision, int32_t xScale,
    int32_t *round, bool *isNull, Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        Decimal128Wrapper input(x[i]);
        input.SetScale(xScale);
        DecimalOperations::Round(input, outScale, round[i]);
        if (input.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        output[i] = input.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchRoundDecimal64(int64_t contextPtr, int64_t *x, int32_t xPrecision, int32_t xScale,
    int32_t *round, bool *isNull, int64_t *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        Decimal64 input(x[i]);
        input.SetScale(xScale);
        DecimalOperations::Round(input, outScale, round[i]);
        if (input.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        output[i] = input.GetValue();
    }

}

extern "C" DLLEXPORT void BatchRoundDecimal128WithoutRound(int64_t contextPtr, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, bool *isNull, Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        Decimal128Wrapper input(x[i]);
        input.SetScale(xScale);
        DecimalOperations::Round(input, outScale, 0);
        if (input.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        output[i] = input.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchRoundDecimal64WithoutRound(int64_t contextPtr, int64_t *x, int32_t xPrecision,
    int32_t xScale, bool *isNull, int64_t *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        Decimal64 input(x[i]);
        input.SetScale(xScale);
        DecimalOperations::Round(input, outScale, 0);
        if (input.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        output[i] = input.GetValue();
    }
}

// decimal64 arith functions
extern "C" DLLEXPORT void BatchDecimal64Compare(int64_t *x, int32_t xPrecision, int32_t xScale, int64_t *y,
    int32_t yPrecision, int32_t yScale, int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = Decimal64(x[i]).SetScale(xScale).Compare(Decimal64(y[i]).SetScale(yScale));
    }
}

extern "C" DLLEXPORT void BatchAbsDecimal64(int64_t *x, int32_t xPrecision, int32_t xScale, bool *isNull,
    int64_t *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = std::abs(x[i]);
    }
}

extern "C" DLLEXPORT void BatchLessThanDecimal64(int64_t *left, int32_t xPrecision, int32_t xScale, int64_t *right,
    int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt)
{
    auto tmp = new int32_t[rowCnt];
    BatchDecimal64Compare(left, xPrecision, xScale, right, yPrecision, yScale, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (tmp[i] < 0);
    }
    delete[] tmp;
}

extern "C" DLLEXPORT void BatchLessThanEqualDecimal64(int64_t *left, int32_t xPrecision, int32_t xScale, int64_t *right,
    int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt)
{
    auto tmp = new int32_t[rowCnt];
    BatchDecimal64Compare(left, xPrecision, xScale, right, yPrecision, yScale, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (tmp[i] <= 0);
    }
    delete[] tmp;
}

extern "C" DLLEXPORT void BatchGreaterThanDecimal64(int64_t *left, int32_t xPrecision, int32_t xScale, int64_t *right,
    int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt)
{
    auto tmp = new int32_t[rowCnt];
    BatchDecimal64Compare(left, xPrecision, xScale, right, yPrecision, yScale, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (tmp[i] > 0);
    }
    delete[] tmp;
}

extern "C" DLLEXPORT void BatchGreaterThanEqualDecimal64(int64_t *left, int32_t xPrecision, int32_t xScale,
    int64_t *right, int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt)
{
    auto tmp = new int32_t[rowCnt];
    BatchDecimal64Compare(left, xPrecision, xScale, right, yPrecision, yScale, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (tmp[i] >= 0);
    }
    delete[] tmp;
}

extern "C" DLLEXPORT void BatchEqualDecimal64(int64_t *left, int32_t xPrecision, int32_t xScale, int64_t *right,
    int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt)
{
    auto tmp = new int32_t[rowCnt];
    BatchDecimal64Compare(left, xPrecision, xScale, right, yPrecision, yScale, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (tmp[i] == 0);
    }
    delete[] tmp;
}

extern "C" DLLEXPORT void BatchNotEqualDecimal64(int64_t *left, int32_t xPrecision, int32_t xScale, int64_t *right,
    int32_t yPrecision, int32_t yScale, bool *output, int32_t rowCnt)
{
    auto tmp = new int32_t[rowCnt];
    BatchDecimal64Compare(left, xPrecision, xScale, right, yPrecision, yScale, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (tmp[i] != 0);
    }
    delete[] tmp;
}

extern "C" DLLEXPORT void BatchUnscaledValue64(int64_t *x, int32_t precision, int32_t scale, bool *isAnyNull,
    int64_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = x[i];
    }
}

extern "C" DLLEXPORT void BatchMakeDecimal64(int64_t contextPtr, int64_t *x, bool *isAnyNull, int64_t *output,
    int32_t precision, int32_t scale, int32_t rowCnt)
{
    std::ostringstream errorMessage;
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        if (DecimalOperations::IsUnscaledLongOverflow(x[i], precision, scale)) {
            errorMessage << "Unscaled value " << x << " out of Decimal(" << precision << ", " << scale << ") range";
            SetError(contextPtr, errorMessage.str());
            output[i] = 1;
            continue;
        }
        output[i] = x[i];
    }
}

extern "C" DLLEXPORT void BatchMakeDecimal64RetNull(bool *isNull, int64_t *x, int64_t *output, int32_t precision,
    int32_t scale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (DecimalOperations::IsUnscaledLongOverflow(x[i], precision, scale)) {
            isNull[i] = true;
            output[i] = 1;
            continue;
        }
        output[i] = x[i];
    }
}

// Decimal Add Operator
extern "C" DLLEXPORT void BatchAddDec64Dec64Dec64(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalAdd(Decimal64(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal64(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.GetValue();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchAddDec64Dec64Dec128(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, Decimal128 *output, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        output[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchAddDec128Dec128Dec128(int64_t contextPtr, bool *isNull, Decimal128 *x,
    int32_t xPrecision, int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchAddDec64Dec128Dec128(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        y[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchAddDec128Dec64Dec128(int64_t contextPtr, bool *isNull, Decimal128 *y,
    int32_t yPrecision,
    int32_t yScale, int64_t *x, int32_t xPrecision, int32_t xScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        y[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

// Decimal SubOperator
extern "C" DLLEXPORT void BatchSubDec64Dec64Dec64(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalSubtract(Decimal64(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal64(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.GetValue();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchSubDec64Dec64Dec128(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, Decimal128 *output, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        output[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchSubDec128Dec128Dec128(int64_t contextPtr, bool *isNull, Decimal128 *x,
    int32_t xPrecision, int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchSubDec64Dec128Dec128(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        y[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchSubDec128Dec64Dec128(int64_t contextPtr, bool *isNull, Decimal128 *x,
    int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

// Decimal MulOperator
extern "C" DLLEXPORT void BatchMulDec64Dec64Dec64(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            continue;
        }
        DecimalOperations::InternalDecimalMultiply(Decimal64(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal64(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.GetValue();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchMulDec64Dec64Dec128(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, Decimal128 *output, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            continue;
        }
        DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        output[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchMulDec128Dec128Dec128(int64_t contextPtr, bool *isNull, Decimal128 *x,
    int32_t xPrecision, int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            continue;
        }
        DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchMulDec64Dec128Dec128(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            continue;
        }
        DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        y[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchMulDec128Dec64Dec128(int64_t contextPtr, bool *isNull, Decimal128 *x,
    int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            continue;
        }
        DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
    }
}

// Decimal DivOperation
extern "C" DLLEXPORT void BatchDivDec64Dec64Dec64(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalDivide(Decimal64(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal64(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        x[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchDivDec64Dec128Dec64(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        x[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchDivDec128Dec64Dec64(int64_t contextPtr, bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        y[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchDivDec64Dec64Dec128(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, Decimal128 *output, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        output[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchDivDec128Dec128Dec128(int64_t contextPtr, bool *isNull, Decimal128 *x,
    int32_t xPrecision, int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        x[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchDivDec64Dec128Dec128(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        y[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchDivDec128Dec64Dec128(int64_t contextPtr, bool *isNull, Decimal128 *x,
    int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        x[i] = result.ToDecimal128();
    }
}

// Decimal Mod Operation
extern "C" DLLEXPORT void BatchModDec64Dec64Dec64(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalMod(Decimal64(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal64(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        x[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchModDec64Dec128Dec64(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        x[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchModDec128Dec64Dec64(int64_t contextPtr, bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        y[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchModDec128Dec64Dec128(int64_t contextPtr, bool *isNull, Decimal128 *x,
    int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        x[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchModDec128Dec128Dec128(int64_t contextPtr, bool *isNull, Decimal128 *x,
    int32_t xPrecision, int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        x[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchModDec128Dec128Dec64(int64_t contextPtr, bool *isNull, Decimal128 *x,
    int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int64_t *output, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        output[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchModDec64Dec128Dec128(int64_t contextPtr, bool *isNull, int64_t *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            x[i] = 1;
            continue;
        }
        DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            SetError(contextPtr, DECIMAL_OVERFLOW);
            continue;
        }
        y[i] = result.ToDecimal128();
    }
}


// add return null
extern "C" DLLEXPORT void BatchAddDec64Dec64Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalAdd(Decimal64(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal64(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.GetValue();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchAddDec64Dec64Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, Decimal128 *output, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        output[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchAddDec128Dec128Dec128RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchAddDec64Dec128Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        y[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchAddDec128Dec64Dec128RetNull(bool *isNull, Decimal128 *y, int32_t yPrecision,
    int32_t yScale, int64_t *x, int32_t xPrecision, int32_t xScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalAdd(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        y[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

// sub ret null
extern "C" DLLEXPORT void BatchSubDec64Dec64Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalSubtract(Decimal64(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal64(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.GetValue();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchSubDec64Dec64Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, Decimal128 *output, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        output[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchSubDec128Dec128Dec128RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchSubDec64Dec128Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        y[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchSubDec128Dec64Dec128RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalSubtract(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

// mul ret null
extern "C" DLLEXPORT void BatchMulDec64Dec64Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            continue;
        }
        DecimalOperations::InternalDecimalMultiply(Decimal64(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal64(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.GetValue();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchMulDec64Dec64Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, Decimal128 *output, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            continue;
        }
        DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        output[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchMulDec128Dec128Dec128RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            continue;
        }
        DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchMulDec64Dec128Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            continue;
        }
        DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        y[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchMulDec128Dec64Dec128RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        if (isNull[i]) {
            continue;
        }
        DecimalOperations::InternalDecimalMultiply(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        x[i] = result.ToDecimal128();
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
    }
}

// div ret null
extern "C" DLLEXPORT void BatchDivDec64Dec64Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalDivide(Decimal64(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal64(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        x[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchDivDec64Dec128Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        x[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchDivDec128Dec64Dec64RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        y[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchDivDec64Dec64Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, Decimal128 *output, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        output[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchDivDec128Dec128Dec128RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        x[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchDivDec64Dec128Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        y[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchDivDec128Dec64Dec128RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalDivide(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result, outScale);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        x[i] = result.ToDecimal128();
    }
}

// mod ret null
extern "C" DLLEXPORT void BatchModDec64Dec64Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalMod(Decimal64(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal64(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        x[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchModDec64Dec128Dec64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        x[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchModDec128Dec64Dec64RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        y[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchModDec128Dec64Dec128RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, int64_t *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        x[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchModDec128Dec128Dec128RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        x[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchModDec128Dec128Dec64RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision,
    int32_t xScale, Decimal128 *y, int32_t yPrecision, int32_t yScale, int64_t *output, int32_t outPrecision,
    int32_t outScale, int32_t rowCnt)
{
    Decimal64 result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        output[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchModDec64Dec128Dec128RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    Decimal128 *y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal128Wrapper result;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::InternalDecimalMod(Decimal128Wrapper(x[i]).SetScale(xScale), xScale, xPrecision,
            Decimal128Wrapper(y[i]).SetScale(yScale), yScale, yPrecision, result);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        y[i] = result.ToDecimal128();
    }
}


extern "C" DLLEXPORT void BatchRoundDecimal128RetNull(bool *isNull, Decimal128 *x, int32_t xPrecision, int32_t xScale,
    int32_t *round, Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        Decimal128Wrapper input(x[i]);
        input.SetScale(xScale);
        DecimalOperations::Round(input, outScale, round[i]);
        if (input.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        output[i] = input.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchRoundDecimal64RetNull(bool *isNull, int64_t *x, int32_t xPrecision, int32_t xScale,
    int32_t *round, int64_t *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        Decimal64 input(x[i]);
        input.SetScale(xScale);
        DecimalOperations::Round(input, outScale, round[i]);
        if (input.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        output[i] = input.GetValue();
    }
}
}