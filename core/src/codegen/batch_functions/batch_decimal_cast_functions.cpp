/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch decimal functions implementation
 */

#include "batch_decimal_cast_functions.h"
#include <cmath>
#include <iomanip>
#include "codegen/context_helper.h"
#include "type/decimal_operations.h"

namespace omniruntime::codegen::function {

using namespace omniruntime::type;

static constexpr int DOUBLE_MAX_PRECISION = std::numeric_limits<double>::max_digits10;

// Cast Function Return Null
extern "C" DLLEXPORT void BatchCastDecimal64To64RetNull(bool *isNull, int64_t *x, int32_t precision, int32_t scale,
    int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        Decimal64 result(x[i]);
        result.SetScale(scale).ReScale(newScale);
        if (result.IsOverflow(newPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            output[i] = 1;
            continue;
        }
        output[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128To128RetNull(bool *isNull, Decimal128 *x, int32_t precision, int32_t scale,
    Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        auto result = Decimal128Wrapper(x[i]).SetScale(scale).ReScale(newScale);
        if (result.IsOverflow(newPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        output[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64To128RetNull(bool *isNull, int64_t *x, int32_t precision, int32_t scale,
    Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        Decimal128Wrapper result(x[i]);
        result.SetScale(scale).ReScale(newScale);
        if (result.IsOverflow(newPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        output[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128To64RetNull(bool *isNull, Decimal128 *x, int32_t precision, int32_t scale,
    int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        Decimal64 result(Decimal128Wrapper(x[i]).SetScale(scale).ReScale(newScale));
        if (result.IsOverflow(newPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        output[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchCastIntToDecimal64RetNull(bool *isNull, int32_t *x, int64_t *output, int32_t precision,
    int32_t scale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        Decimal64 result(x[i]);
        result.ReScale(scale);
        if (result.IsOverflow(precision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        output[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchCastLongToDecimal64RetNull(bool *isNull, int64_t *x, int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        Decimal64 result(x[i]);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        output[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchCastDoubleToDecimal64RetNull(bool *isNull, double *x, int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        Decimal64 result(x[i]);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            output[i] = 1;
            continue;
        }
        output[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchCastIntToDecimal128RetNull(bool *isNull, int32_t *x, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        Decimal128Wrapper result(x[i]);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        output[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchCastLongToDecimal128RetNull(bool *isNull, int64_t *x, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        Decimal128Wrapper result(x[i]);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        output[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchCastDoubleToDecimal128RetNull(bool *isNull, double *x, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        Decimal128Wrapper result(x[i]);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            isNull[i] = true;
            continue;
        }
        output[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64ToIntRetNull(bool *isNull, int64_t *x, int32_t precision, int32_t scale,
    int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = static_cast<int32_t>(Decimal64(x[i]).ReScale(-scale, RoundingMode::ROUND_FLOOR).GetValue());
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64ToLongRetNull(bool *isNull, int64_t *x, int32_t precision, int32_t scale,
    int64_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = static_cast<int64_t>(Decimal64(x[i]).ReScale(-scale, RoundingMode::ROUND_FLOOR).GetValue());
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64ToDoubleRetNull(bool *isNull, const int64_t *x, int32_t precision,
    int32_t scale, double *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        std::string doubleString = Decimal64(x[i]).SetScale(scale).ToString();
        output[i] = stod(doubleString);
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128ToIntRetNull(bool *isNull, Decimal128 *x, int32_t precision, int32_t scale,
    int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        int128_t result =
            Decimal128Wrapper(x[i]).ReScale(-scale, RoundingMode::ROUND_FLOOR).ToInt128().convert_to<__int128>();
        if (result > INT32_MAX || result < INT32_MIN) {
            isNull[i] = true;
        }
        output[i] = static_cast<int32_t>(result);
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128ToLongRetNull(bool *isNull, Decimal128 *x, int32_t precision,
    int32_t scale, int64_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        int128_t result =
            Decimal128Wrapper(x[i]).ReScale(-scale, RoundingMode::ROUND_FLOOR).ToInt128().convert_to<__int128>();
        if (result > INT64_MAX || result < INT64_MIN) {
            isNull[i] = true;
        }
        output[i] = static_cast<int64_t>(result);
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128ToDoubleRetNull(bool *isNull, Decimal128 *x, int32_t precision,
    int32_t scale, double *output, int32_t rowCnt)
{
    Decimal128Wrapper input;
    std::string doubleString;
    for (int i = 0; i < rowCnt; ++i) {
        doubleString = Decimal128Wrapper(x[i]).SetScale(scale).ToString();
        output[i] = stod(doubleString);
    }
}

// Cast Function
extern "C" DLLEXPORT void BatchCastDecimal64To64(int64_t contextPtr, int64_t *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        Decimal64 result(x[i]);
        result.SetScale(scale).ReScale(newScale);
        if (result.IsOverflow(newPrecision) != OpStatus::SUCCESS) {
            ReSetErrorMessage();
            errorMessage << "Cannot cast DECIMAL(" << precision << ", " << scale << ") '" <<
                Decimal64(x[i]).SetScale(scale).ToString() << "' to DECIMAL(" << newPrecision <<
                ", " << newScale << ")";
            SetError(contextPtr, errorMessage.str());
            output[i] = 1;
            continue;
        }
        output[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128To128(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            continue;
        }
        Decimal128Wrapper result = Decimal128Wrapper(x[i]).SetScale(scale).ReScale(newScale);
        if (result.IsOverflow(newPrecision) != OpStatus::SUCCESS) {
            ReSetErrorMessage();
            errorMessage << "Cannot cast DECIMAL(" << precision << ", " << scale << ") '" <<
                Decimal128Wrapper(x[i]).SetScale(scale).ToString() << "' to DECIMAL(" << newPrecision
                << ", " <<
                newScale << ")";
            SetError(contextPtr, errorMessage.str());
            continue;
        }
        output[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64To128(int64_t contextPtr, int64_t *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            continue;
        }
        Decimal128Wrapper result(x[i]);
        result.SetScale(scale).ReScale(newScale);
        if (result.IsOverflow(newPrecision) != OpStatus::SUCCESS) {
            ReSetErrorMessage();
            errorMessage << "Cannot cast  DECIMAL(" << precision << ", " << scale << ") '" <<
                Decimal64(x[i]).SetScale(scale).ToString() << "' to DECIMAL(" << newPrecision <<
                "," << newScale << ")";
            SetError(contextPtr, errorMessage.str());
            continue;
        }
        output[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128To64(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        Decimal64 result(Decimal128Wrapper(x[i]).SetScale(scale).ReScale(newScale));
        if (result.IsOverflow(newPrecision) != OpStatus::SUCCESS) {
            ReSetErrorMessage();
            errorMessage << "Cannot cast  DECIMAL(" << precision << ", " << scale << ") '"
                << Decimal128Wrapper(x[i]).SetScale(scale).ToString() << "' to DECIMAL(" << newPrecision << ","
                << newScale << ")";
            SetError(contextPtr, errorMessage.str());
            output[i] = 1;
            continue;
        }
        output[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchCastIntToDecimal64(int64_t contextPtr, int32_t *x, const bool *isAnyNull,
    int64_t *output,
    int32_t precision, int32_t scale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        Decimal64 result(x[i]);
        result.ReScale(scale);
        if (result.IsOverflow(precision) != OpStatus::SUCCESS) {
            ReSetErrorMessage();
            errorMessage << "Cannot cast INTEGER '" << x[i] << "' to DECIMAL(" << precision << "," << scale << ")";
            SetError(contextPtr, errorMessage.str());
            output[i] = 1;
            continue;
        }
        output[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchCastLongToDecimal64(int64_t contextPtr, int64_t *x, const bool *isAnyNull,
    int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        Decimal64 result(x[i]);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            ReSetErrorMessage();
            errorMessage << "Cannot cast BIGINT '" << x[i] << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
            SetError(contextPtr, errorMessage.str());
            output[i] = 1;
            continue;
        }
        output[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchCastDoubleToDecimal64(int64_t contextPtr, double *x, const bool *isAnyNull,
    int64_t *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        Decimal64 result(x[i]);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            ReSetErrorMessage();
            errorMessage.precision(DOUBLE_MAX_PRECISION);
            errorMessage << "Cannot cast DOUBLE '" << x[i] << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
            SetError(contextPtr, errorMessage.str());
            output[i] = 1;
            continue;
        }
        output[i] = result.GetValue();
    }
}

extern "C" DLLEXPORT void BatchCastIntToDecimal128(int64_t contextPtr, int32_t *x, const bool *isAnyNull,
    Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            continue;
        }
        Decimal128Wrapper result(x[i]);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            ReSetErrorMessage();
            errorMessage << "Cannot cast INTEGER '" << x[i] << "' to DECIMAL(" << outPrecision << "," << outScale <<
                ")";
            SetError(contextPtr, errorMessage.str());
            continue;
        }
        output[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchCastLongToDecimal128(int64_t contextPtr, int64_t *x, const bool *isAnyNull,
    Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            continue;
        }
        Decimal128Wrapper result(x[i]);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            ReSetErrorMessage();
            errorMessage << "Cannot cast BIGINT '" << x[i] << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
            SetError(contextPtr, errorMessage.str());
            continue;
        }
        output[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchCastDoubleToDecimal128(int64_t contextPtr, double *x, bool *isAnyNull,
    Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        Decimal128Wrapper result(x[i]);
        result.ReScale(outScale);
        if (result.IsOverflow(outPrecision) != OpStatus::SUCCESS) {
            ReSetErrorMessage();
            errorMessage.precision(DOUBLE_MAX_PRECISION);
            errorMessage << "Cannot cast DOUBLE '" << x[i] << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
            SetError(contextPtr, errorMessage.str());
            continue;
        }
        output[i] = result.ToDecimal128();
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64ToInt(int64_t contextPtr, int64_t *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, int32_t *output, int32_t rowCnt)
{
    int32_t result;
    if (ConfigUtil::GetRoundingRule() == RoundingRule::DOWN) {
        int64_t scaledValue;
        for (int i = 0; i < rowCnt; ++i) {
            if (isAnyNull[i]) {
                output[i] = 1;
                continue;
            }
            scaledValue = Decimal64(x[i]).SetScale(scale).ReScale(0, RoundingMode::ROUND_FLOOR).GetValue();
            if (scaledValue < INT_MIN || scaledValue > INT_MAX) {
                ReSetErrorMessage();
                errorMessage << "Cannot cast " << Decimal64(x[i]).SetScale(scale).ToString() <<
                    " to INTEGER";
                SetError(contextPtr, errorMessage.str());
                output[i] = 1;
                continue;
            }
            output[i] = static_cast<int32_t>(scaledValue);
        }
        return;
    }

    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        try {
            result = static_cast<int32_t>(Decimal64(x[i]).SetScale(scale));
        } catch (std::overflow_error &e) {
            ReSetErrorMessage();
            errorMessage << "Cannot cast '" << Decimal64(x[i]).SetScale(scale).ToString() <<
                "' to  INTEGER";
            SetError(contextPtr, errorMessage.str());
            output[i] = 1;
            continue;
        }
        output[i] = result;
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64ToLong(int64_t *x, int32_t precision, int32_t scale, const bool *isAnyNull,
    int64_t *output, int32_t rowCnt)
{
    if (ConfigUtil::GetRoundingRule() == RoundingRule::DOWN) {
        for (int i = 0; i < rowCnt; ++i) {
            if (isAnyNull[i]) {
                output[i] = 1;
                continue;
            }
            output[i] = Decimal64(x[i]).SetScale(scale).ReScale(0, RoundingMode::ROUND_FLOOR).GetValue();
        }
        return;
    }
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        output[i] = static_cast<int64_t>(Decimal64(x[i]).SetScale(scale));
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64ToDouble(const int64_t *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, double *output, int32_t rowCnt)
{
    if (ConfigUtil::GetRoundingRule() == RoundingRule::DOWN) {
        for (int i = 0; i < rowCnt; ++i) {
            if (isAnyNull[i]) {
                output[i] = 1.0;
                continue;
            }
            std::string doubleString = Decimal64(x[i]).SetScale(scale).ToString();
            output[i] = stod(doubleString);
        }
        return;
    }
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1.0;
            continue;
        }
        output[i] = static_cast<double>(Decimal64(x[i]).SetScale(scale));
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128ToInt(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        int32_t result;
        try {
            result = static_cast<int32_t>(Decimal128Wrapper(x[i]).SetScale(scale));
        } catch (std::overflow_error &e) {
            ReSetErrorMessage();
            errorMessage << "Cannot cast '" << Decimal128Wrapper(x[i]).SetScale(scale).ToString() <<
                "' to  INTEGER";
            SetError(contextPtr, errorMessage.str());
            return;
        }
        output[i] = result;
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128ToLong(int64_t contextPtr, Decimal128 *x, int32_t precision,
    int32_t scale,
    const bool *isAnyNull, int64_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        int64_t result;
        try {
            result = static_cast<int64_t>(Decimal128Wrapper(x[i]).SetScale(scale));
        } catch (std::overflow_error &e) {
            ReSetErrorMessage();
            errorMessage << "Cannot cast '" << Decimal128Wrapper(x[i]).SetScale(scale).ToString() <<
                "' to  BIGINTEGER";
            SetError(contextPtr, errorMessage.str());
            return;
        }
        output[i] = result;
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128ToDouble(Decimal128 *x, int32_t precision, int32_t scale,
    const bool *isAnyNull, double *output, int32_t rowCnt)
{
    if (ConfigUtil::GetRoundingRule() == RoundingRule::DOWN) {
        for (int i = 0; i < rowCnt; ++i) {
            if (isAnyNull[i]) {
                output[i] = 1.0;
                continue;
            }
            output[i] = static_cast<double>(Decimal128Wrapper(x[i])) / DOUBLE_10_POW[scale];
        }
        return;
    }
    std::string doubleString;
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1.0;
            continue;
        }
        doubleString = Decimal64(Decimal128Wrapper(x[i])).SetScale(scale).ToString();
        output[i] = stod(doubleString);
    }
}
}