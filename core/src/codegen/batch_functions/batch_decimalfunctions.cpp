/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch decimal functions implementation
 */
#include "batch_decimalfunctions.h"
#include <cmath>
#include <iomanip>
#include "codegen/functions/context_helper.h"
#include "util/engine.h"
#include "type/decimal_operations.h"

using namespace omniruntime::type;

namespace omniruntime {
namespace codegen {
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

static constexpr int DOUBLE_MAX_PRECISION = std::numeric_limits<double>::max_digits10;

extern "C" DLLEXPORT void BatchDecimal128Compare(Decimal128 *x, int32_t xPrecision, int32_t xScale, Decimal128 *y,
    int32_t yPrecision, int32_t yScale, int32_t *output, int32_t rowCnt)
{
    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);
    Decimal128 xRescaled = DecimalOperations::UnscaledDecimal(0);
    Decimal128 yRescaled = DecimalOperations::UnscaledDecimal(0);

    if (xRescaleFactor > 0) {
        for (int i = 0; i < rowCnt; i++) {
            DecimalOperations::Rescale128(x[i], xRescaleFactor, xRescaled);
            if (xRescaled < y[i]) {
                output[i] = -1;
            } else if (xRescaled > y[i]) {
                output[i] = 1;
            } else {
                output[i] = 0;
            }
        }
    } else {
        for (int i = 0; i < rowCnt; i++) {
            DecimalOperations::Rescale128(y[i], yRescaleFactor, yRescaled);
            if (x[i] < yRescaled) {
                output[i] = -1;
            } else if (x[i] > yRescaled) {
                output[i] = 1;
            } else {
                output[i] = 0;
            }
        }
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

extern "C" DLLEXPORT void BatchEqualDecimal128(Decimal128 *left, int32_t xPrecision, int32_t xScale, Decimal128 *right,
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
        output[i] = DecimalOperations::AbsExact(x[i]);
    }
}

// decimal64 arith functions
extern "C" DLLEXPORT void BatchDecimal64Compare(int64_t *x, int32_t xPrecision, int32_t xScale, int64_t *y,
    int32_t yPrecision, int32_t yScale, int32_t *output, int32_t rowCnt)
{
    Decimal128 xRescaled, yRescaled;
    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);
    if (xRescaleFactor > 0) {
        for (int i = 0; i < rowCnt; i++) {
            xRescaled = DecimalOperations::UnscaledDecimal(x[i]);
            yRescaled = DecimalOperations::UnscaledDecimal(y[i]);
            DecimalOperations::Rescale128(xRescaled, xRescaleFactor, xRescaled);
            if (xRescaled < yRescaled) {
                output[i] = -1;
            } else if (xRescaled > yRescaled) {
                output[i] = 1;
            } else {
                output[i] = 0;
            }
        }
    } else {
        for (int i = 0; i < rowCnt; i++) {
            xRescaled = DecimalOperations::UnscaledDecimal(x[i]);
            yRescaled = DecimalOperations::UnscaledDecimal(y[i]);
            DecimalOperations::Rescale128(yRescaled, yRescaleFactor, yRescaled);
            if (xRescaled < yRescaled) {
                output[i] = -1;
            } else if (xRescaled > yRescaled) {
                output[i] = 1;
            } else {
                output[i] = 0;
            }
        }
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

// Cast Function
extern "C" DLLEXPORT void BatchCastDecimal64To64(int64_t contextPtr, int64_t *x, int32_t precision, int32_t scale,
    bool *isAnyNull, int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    OpStatus status;
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        status = DecimalOperations::Rescale64(x[i], newScale - scale, output[i]);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], newPrecision);
        }
        if (status != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage << "Cannot cast DECIMAL(" << precision << ", " << scale << ") '" <<
                DecimalOperations::ScaleOfDecimal(std::to_string(x[i]), scale) << "' to DECIMAL(" << newPrecision <<
                ", " << newScale << ")";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i] = 1;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128To128(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    bool *isAnyNull, Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    OpStatus status;
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i].SetValue(0, 1);
            continue;
        }
        status = DecimalOperations::Rescale128(x[i], newScale - scale, output[i]);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], newPrecision);
        }
        if (status != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage << "Cannot cast DECIMAL(" << precision << ", " << scale << ") '" <<
                DecimalOperations::ScaleOfDecimal(x[i].ToString(), scale) << "' to DECIMAL(" << newPrecision << ", " <<
                newScale << ")";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i].SetValue(0, 1);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64To128(int64_t contextPtr, int64_t *x, int32_t precision, int32_t scale,
    bool *isAnyNull, Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    int32_t scaleDelta = newScale - scale;
    OpStatus status;
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i].SetValue(0, 1);
            continue;
        }
        status = DecimalOperations::Rescale64To128(x[i], scaleDelta, output[i]);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], newPrecision);
        }
        if (status != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage << "Cannot cast  DECIMAL(" << precision << ", " << scale << ") '" <<
                DecimalOperations::ScaleOfDecimal(std::to_string(x[i]), scale) << "' to DECIMAL(" << newPrecision <<
                "," << newScale << ")";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i].SetValue(0, 1);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128To64(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    bool *isAnyNull, int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    int32_t scaleDelta = newScale - scale;
    OpStatus status;
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        status = DecimalOperations::Rescale128To64(x[i], scaleDelta, output[i]);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], newPrecision);
        }
        if (status != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage << "Cannot cast  DECIMAL(" << precision << ", " << scale << ") '" <<
                DecimalOperations::ScaleOfDecimal(x[i].ToString(), scale) << "' to DECIMAL(" << newPrecision << "," <<
                newScale << ")";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i] = 1;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastIntToDecimal64(int64_t contextPtr, int32_t *x, bool *isAnyNull, int64_t *output,
    int32_t precision, int32_t scale, int32_t rowCnt)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    OpStatus status = OP_OVERFLOW;
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        if (!__builtin_smull_overflow(x[i], tenToScale, &output[i])) {
            status = DecimalOperations::IsOverflows(output[i], precision);
        }
        if (status != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage << "Cannot cast INTEGER '" << x[i] << "' to DECIMAL(" << precision << "," << scale << ")";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i] = 1;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastLongToDecimal64(int64_t contextPtr, int64_t *x, bool *isAnyNull, int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(outScale).LowBits());
    OpStatus status = OP_OVERFLOW;
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        if (!__builtin_smull_overflow(x[i], tenToScale, &output[i])) {
            status = DecimalOperations::IsOverflows(output[i], outPrecision);
        }
        if (status != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage << "Cannot cast BIGINT '" << x[i] << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i] = 1;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDoubleToDecimal64(int64_t contextPtr, double *x, bool *isAnyNull, int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    std::stringstream ss;
    int32_t precision = 0;
    int32_t scale = 0;
    OpStatus status;
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        ss.clear();
        ss.str("");
        ss << std::setprecision(DOUBLE_MAX_PRECISION) << x[i];
        std::string s;
        ss >> s;

        int64_t result = 0;
        status = DecimalOperations::StringToDecimal64(s, result, scale, precision);
        if (status == SUCCESS) {
            status = DecimalOperations::Rescale64(result, outScale - scale, result);
        }
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
        if (status != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage.precision(DOUBLE_MAX_PRECISION);
            errorMessage << "Cannot cast DOUBLE '" << x << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i] = 1;
            continue;
        }
        output[i] = result;
    }
}

extern "C" DLLEXPORT void BatchCastIntToDecimal128(int64_t contextPtr, int32_t *x, bool *isAnyNull, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    OpStatus status;
    Decimal128 left = DecimalOperations::TenToScale(outScale);
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i].SetValue(0, 1);
            continue;
        }
        Decimal128 right = DecimalOperations::UnscaledDecimal(x[i]);
        status = DecimalOperations::Multiply(left, right, output[i]);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], outPrecision);
        }
        if (status != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage << "Cannot cast INTEGER '" << x[i] << "' to DECIMAL(" << outPrecision << "," << outScale <<
                ")";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i].SetValue(0, 1);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastLongToDecimal128(int64_t contextPtr, int64_t *x, bool *isAnyNull, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    OpStatus status;
    Decimal128 left = DecimalOperations::TenToScale(outScale);
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i].SetValue(0, 1);
            continue;
        }
        Decimal128 right = DecimalOperations::UnscaledDecimal(x[i]);

        status = DecimalOperations::Multiply(left, right, output[i]);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], outPrecision);
        }
        if (status != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage << "Cannot cast BIGINT '" << x[i] << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i].SetValue(0, 1);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDoubleToDecimal128(int64_t contextPtr, double *x, bool *isAnyNull,
    Decimal128 *output, int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    int32_t precision = 0;
    int32_t scale = 0;
    OpStatus status;
    std::stringstream ss;
    std::string s;
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i].SetValue(0, 1);
            continue;
        }
        ss.clear();
        ss << std::setprecision(DOUBLE_MAX_PRECISION) << x[i];
        s = ss.str();

        status = DecimalOperations::StringToDecimal128(s, output[i], scale, precision);
        if (status == SUCCESS) {
            status = DecimalOperations::Rescale128(output[i], outScale - scale, output[i]);
        }
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], outPrecision);
        }
        if (status != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage.precision(DOUBLE_MAX_PRECISION);
            errorMessage << "Cannot cast DOUBLE '" << x << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i].SetValue(0, 1);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64ToInt(int64_t contextPtr, int64_t *x, int32_t precision, int32_t scale,
    bool *isAnyNull, int32_t *output, int32_t rowCnt)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    int32_t result;
    OpStatus status;
    if (EngineUtil::GetInstance().GetEngineType() == EngineType::Spark) {
        int64_t scaledValue = 0;
        for (int i = 0; i < rowCnt; ++i) {
            if (isAnyNull[i]) {
                output[i] = 1;
                continue;
            }
            status = DecimalOperations::Rescale64RoundToZero(x[i], -scale, scaledValue);
            if (status == type::OP_OVERFLOW || scaledValue < INT_MIN || scaledValue > INT_MAX) {
                std::ostringstream errorMessage;
                errorMessage << "Cannot cast " << DecimalOperations::ScaleOfDecimal(std::to_string(x[i]), scale) <<
                    " to INTEGER";
                int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
                SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
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

        long longResult;
        if (x[i] < 0) {
            longResult = -((-x[i] + tenToScale / 2) / tenToScale);
        } else {
            longResult = (x[i] + tenToScale / 2) / tenToScale;
        }

        status = DecimalOperations::ToIntExact(longResult, result);
        if (status != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage << "Cannot cast '" << DecimalOperations::ScaleOfDecimal(std::to_string(x[i]), scale) <<
                "' to  INTEGER";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i] = 1;
            continue;
        }
        output[i] = result;
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64ToLong(int64_t *x, int32_t precision, int32_t scale, bool *isAnyNull,
    int64_t *output, int32_t rowCnt)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    if (EngineUtil::GetInstance().GetEngineType() == EngineType::Spark) {
        int64_t scaledValue = 0;
        for (int i = 0; i < rowCnt; ++i) {
            if (isAnyNull[i]) {
                output[i] = 1;
                continue;
            }
            DecimalOperations::Rescale64RoundToZero(x[i], -scale, scaledValue);
            output[i] = static_cast<int32_t>(scaledValue);
        }
        return;
    }
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        if (x[i] >= 0) {
            output[i] = (x[i] + tenToScale / 2) / tenToScale;
        } else {
            output[i] = -((-x[i] + tenToScale / 2) / tenToScale);
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64ToDouble(const int64_t *x, int32_t precision, int32_t scale,
    bool *isAnyNull, double *output, int32_t rowCnt)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1.0;
            continue;
        }
        output[i] = (static_cast<double>(x[i])) / static_cast<double>(tenToScale);
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128ToInt(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    bool *isAnyNull, int32_t *output, int32_t rowCnt)
{
    Decimal128 outDecimal(0, 0);
    int64_t longValue;
    int32_t result;
    OpStatus statusDecimal;
    OpStatus statusInt;
    if (EngineUtil::GetInstance().GetEngineType() == EngineType::Spark) {
        for (int i = 0; i < rowCnt; ++i) {
            if (isAnyNull[i]) {
                output[i] = 1;
                continue;
            }
            DecimalOperations::Rescale128RoundToZero(x[i], -scale, outDecimal);
            statusDecimal = DecimalOperations::UnscaledDecimal128ToLong(outDecimal, longValue);
            statusInt = DecimalOperations::ToIntExact(longValue, result);
            if (statusDecimal != SUCCESS || statusInt != SUCCESS) {
                std::ostringstream errorMessage;
                errorMessage << "Cannot cast '" << DecimalOperations::ScaleOfDecimal(x[i].ToString(), scale) <<
                    "' to  INTEGER";
                int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
                codegen::SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
                output[i] = 1;
                continue;
            }
            output[i] = result;
        }
        return;
    }

    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        DecimalOperations::Rescale128(x[i], -scale, outDecimal);
        statusDecimal = DecimalOperations::UnscaledDecimal128ToLong(outDecimal, longValue);
        statusInt = DecimalOperations::ToIntExact(longValue, result);
        if (statusDecimal != SUCCESS || statusInt != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage << "Cannot cast '" << DecimalOperations::ScaleOfDecimal(x[i].ToString(), scale) <<
                "' to  INTEGER";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            codegen::SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i] = 1;
            continue;
        }
        output[i] = result;
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128ToLong(int64_t contextPtr, Decimal128 *x, int32_t precision, int32_t scale,
    bool *isAnyNull, int64_t *output, int32_t rowCnt)
{
    Decimal128 outDecimal(0, 0);
    OpStatus status;
    if (EngineUtil::GetInstance().GetEngineType() == EngineType::Spark) {
        for (int i = 0; i < rowCnt; ++i) {
            if (isAnyNull[i]) {
                output[i] = 1;
                continue;
            }
            DecimalOperations::Rescale128RoundToZero(x[i], -scale, outDecimal);
            status = DecimalOperations::UnscaledDecimal128ToLong(outDecimal, output[i]);
            if (status != SUCCESS) {
                std::ostringstream errorMessage;
                errorMessage << "Cannot cast '" << DecimalOperations::ScaleOfDecimal(x[i].ToString(), scale) <<
                    "' to  BIGINT";
                int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
                SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
                output[i] = 1;
                continue;
            }
        }
        return;
    }

    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1;
            continue;
        }
        DecimalOperations::Rescale128(x[i], -scale, outDecimal);
        status = DecimalOperations::UnscaledDecimal128ToLong(outDecimal, output[i]);
        if (status != SUCCESS) {
            std::ostringstream errorMessage;
            errorMessage << "Cannot cast '" << DecimalOperations::ScaleOfDecimal(x[i].ToString(), scale) <<
                "' to  BIGINT";
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
            output[i] = 1;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128ToDouble(Decimal128 *x, int32_t precision, int32_t scale, bool *isAnyNull,
    double *output, int32_t rowCnt)
{
    std::string doubleString;
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 1.0;
            continue;
        }
        doubleString = DecimalOperations::ScaleOfDecimal(x[i].ToString(), scale);
        output[i] = stod(doubleString);
    }
}

// Cast Function Return Null
extern "C" DLLEXPORT void BatchCastDecimal64To64RetNull(bool *isNull, int64_t *x, int32_t precision, int32_t scale,
    int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    OpStatus status;
    for (int i = 0; i < rowCnt; ++i) {
        status = DecimalOperations::Rescale64(x[i], newScale - scale, output[i]);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], newPrecision);
        }
        if (status != SUCCESS) {
            isNull[i] = true;
            output[i] = 1;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128To128RetNull(bool *isNull, Decimal128 *x, int32_t precision, int32_t scale,
    Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    OpStatus status;
    for (int i = 0; i < rowCnt; ++i) {
        status = DecimalOperations::Rescale128(x[i], newScale - scale, output[i]);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], newPrecision);
        }
        if (status != SUCCESS) {
            isNull[i] = true;
            output[i].SetValue(0, 1);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64To128RetNull(bool *isNull, int64_t *x, int32_t precision, int32_t scale,
    Decimal128 *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    int32_t scaleDelta = newScale - scale;
    OpStatus status;
    for (int i = 0; i < rowCnt; ++i) {
        status = DecimalOperations::Rescale64To128(x[i], scaleDelta, output[i]);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], newPrecision);
        }
        if (status != SUCCESS) {
            isNull[i] = true;
            output[i].SetValue(0, 1);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128To64RetNull(bool *isNull, Decimal128 *x, int32_t precision, int32_t scale,
    int64_t *output, int32_t newPrecision, int32_t newScale, int32_t rowCnt)
{
    int32_t scaleDelta = newScale - scale;
    OpStatus status;
    for (int i = 0; i < rowCnt; ++i) {
        status = DecimalOperations::Rescale128To64(x[i], scaleDelta, output[i]);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], newPrecision);
        }
        if (status != SUCCESS) {
            isNull[i] = true;
            output[i] = 1;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastIntToDecimal64RetNull(bool *isNull, int32_t *x, int64_t *output, int32_t precision,
    int32_t scale, int32_t rowCnt)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    OpStatus status = OP_OVERFLOW;
    for (int i = 0; i < rowCnt; ++i) {
        if (!__builtin_smull_overflow(x[i], tenToScale, &output[i])) {
            status = DecimalOperations::IsOverflows(output[i], precision);
        }
        if (status != SUCCESS) {
            isNull[i] = true;
            output[i] = 1;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastLongToDecimal64RetNull(bool *isNull, int64_t *x, int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(outScale).LowBits());
    OpStatus status = OP_OVERFLOW;
    for (int i = 0; i < rowCnt; ++i) {
        if (!__builtin_smull_overflow(x[i], tenToScale, &output[i])) {
            status = DecimalOperations::IsOverflows(output[i], outPrecision);
        }
        if (status != SUCCESS) {
            isNull[i] = true;
            output[i] = 1;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDoubleToDecimal64RetNull(bool *isNull, double *x, int64_t *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    std::stringstream ss;
    int32_t precision = 0;
    int32_t scale = 0;
    OpStatus status;
    for (int i = 0; i < rowCnt; ++i) {
        ss.clear();
        ss.str("");
        ss << std::setprecision(DOUBLE_MAX_PRECISION) << x[i];
        std::string s = ss.str();

        int64_t result = 0;
        status = DecimalOperations::StringToDecimal64(s, result, scale, precision);
        if (status == SUCCESS) {
            status = DecimalOperations::Rescale64(result, outScale - scale, result);
        }
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
        if (status != SUCCESS) {
            isNull[i] = true;
            output[i] = 1;
            continue;
        }
        output[i] = result;
    }
}

extern "C" DLLEXPORT void BatchCastIntToDecimal128RetNull(bool *isNull, int32_t *x, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    Decimal128 result;
    OpStatus status;
    Decimal128 left = DecimalOperations::TenToScale(outScale);
    for (int i = 0; i < rowCnt; ++i) {
        Decimal128 right = DecimalOperations::UnscaledDecimal(x[i]);
        status = DecimalOperations::Multiply(left, right, output[i]);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], outPrecision);
        }
        if (status != SUCCESS) {
            isNull[i] = true;
            output[i].SetValue(0, 1);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastLongToDecimal128RetNull(bool *isNull, int64_t *x, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    OpStatus status;
    Decimal128 left = DecimalOperations::TenToScale(outScale);
    for (int i = 0; i < rowCnt; ++i) {
        Decimal128 right = DecimalOperations::UnscaledDecimal(x[i]);
        status = DecimalOperations::Multiply(left, right, output[i]);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], outPrecision);
        }
        if (status != SUCCESS) {
            isNull[i] = true;
            output[i].SetValue(0, 1);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDoubleToDecimal128RetNull(bool *isNull, double *x, Decimal128 *output,
    int32_t outPrecision, int32_t outScale, int32_t rowCnt)
{
    std::stringstream ss;
    int32_t precision = 0;
    int32_t scale = 0;
    OpStatus status;
    for (int i = 0; i < rowCnt; ++i) {
        ss.clear();
        ss.str("");
        ss << std::setprecision(DOUBLE_MAX_PRECISION) << x[i];
        std::string s;
        ss >> s;

        status = DecimalOperations::StringToDecimal128(s, output[i], scale, precision);
        if (status == SUCCESS) {
            status = DecimalOperations::Rescale128(output[i], outScale - scale, output[i]);
        }
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(output[i], outPrecision);
        }
        if (status != SUCCESS) {
            isNull[i] = true;
            output[i].SetValue(0, 1);
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64ToIntRetNull(bool *isNull, int64_t *x, int32_t precision, int32_t scale,
    int32_t *output, int32_t rowCnt)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    OpStatus status;

    if (EngineUtil::GetInstance().GetEngineType() == EngineType::Spark) {
        int64_t scaledValue = 0;
        for (int i = 0; i < rowCnt; ++i) {
            DecimalOperations::Rescale64RoundToZero(x[i], -scale, scaledValue);
            output[i] = static_cast<int32_t>(scaledValue);
        }
        return;
    }

    for (int i = 0; i < rowCnt; ++i) {
        long longResult;
        if (x[i] < 0) {
            longResult = -((-x[i] + tenToScale / 2) / tenToScale);
        } else {
            longResult = (x[i] + tenToScale / 2) / tenToScale;
        }
        status = DecimalOperations::ToIntExact(longResult, output[i]);
        if (status != SUCCESS) {
            isNull[i] = true;
            output[i] = 1;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64ToLongRetNull(bool *isNull, int64_t *x, int32_t precision, int32_t scale,
    int64_t *output, int32_t rowCnt)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    if (EngineUtil::GetInstance().GetEngineType() == EngineType::Spark) {
        int64_t scaledValue = 0;
        for (int i = 0; i < rowCnt; ++i) {
            DecimalOperations::Rescale64RoundToZero(x[i], -scale, scaledValue);
            output[i] = static_cast<int32_t>(scaledValue);
        }
        return;
    }

    for (int i = 0; i < rowCnt; ++i) {
        if (x[i] >= 0) {
            output[i] = (x[i] + tenToScale / 2) / tenToScale;
        } else {
            output[i] = -((-x[i] + tenToScale / 2) / tenToScale);
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal64ToDoubleRetNull(bool *isNull, const int64_t *x, int32_t precision,
    int32_t scale, double *output, int32_t rowCnt)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = (static_cast<double>(x[i])) / static_cast<double>(tenToScale);
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128ToIntRetNull(bool *isNull, Decimal128 *x, int32_t precision, int32_t scale,
    int32_t *output, int32_t rowCnt)
{
    Decimal128 outDecimal(0, 0);
    if (EngineUtil::GetInstance().GetEngineType() == EngineType::Spark) {
        int32_t result;
        for (int i = 0; i < rowCnt; ++i) {
            DecimalOperations::Rescale128RoundToZero(x[i], -scale, outDecimal);
            result = static_cast<int32_t>(outDecimal.LowBits());
            output[i] = outDecimal.HighBits() < 0 ? -result : result;
        }
        return;
    }

    int64_t longValue;
    OpStatus statusDecimal;
    OpStatus statusInt;
    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::Rescale128(x[i], -scale, outDecimal);
        statusDecimal = DecimalOperations::UnscaledDecimal128ToLong(outDecimal, longValue);
        statusInt = DecimalOperations::ToIntExact(longValue, output[i]);
        if (statusDecimal != SUCCESS || statusInt != SUCCESS) {
            isNull[i] = true;
            output[i] = 1;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128ToLongRetNull(bool *isNull, Decimal128 *x, int32_t precision,
    int32_t scale, int64_t *output, int32_t rowCnt)
{
    Decimal128 outDecimal(0, 0);
    int64_t result;
    OpStatus status;
    if (EngineUtil::GetInstance().GetEngineType() == EngineType::Spark) {
        for (int i = 0; i < rowCnt; ++i) {
            DecimalOperations::Rescale128RoundToZero(x[i], -scale, outDecimal);
            result = static_cast<int64_t>(outDecimal.LowBits());
            output[i] = outDecimal.HighBits() < 0 ? -result : result;
        }
        return;
    }

    for (int i = 0; i < rowCnt; ++i) {
        DecimalOperations::Rescale128(x[i], -scale, outDecimal);
        status = DecimalOperations::UnscaledDecimal128ToLong(outDecimal, output[i]);
        if (status != SUCCESS) {
            isNull[i] = true;
            output[i] = 1;
            continue;
        }
    }
}

extern "C" DLLEXPORT void BatchCastDecimal128ToDoubleRetNull(bool *isNull, Decimal128 *x, int32_t precision,
    int32_t scale, double *output, int32_t rowCnt)
{
    Decimal128 input;
    std::string doubleString;
    for (int i = 0; i < rowCnt; ++i) {
        doubleString = DecimalOperations::ScaleOfDecimal(x[i].ToString(), scale);
        output[i] = stod(doubleString);
    }
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
            int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
            SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
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
}
}