/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#include <cmath>
#include <iomanip>
#include "decimalfunctions.h"
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
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, bool isNull)
{
    int32_t xRescaleFactor = DecimalOperations::RescaleFactor(xScale, yScale);
    int32_t yRescaleFactor = DecimalOperations::RescaleFactor(yScale, xScale);
    Decimal128 x(xHigh, xLow);
    Decimal128 y(yHigh, yLow);
    Decimal128 xRescaled;
    Decimal128 yRescaled;
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

extern "C" DLLEXPORT void AbsDecimal128(int64_t xHigh, uint64_t xLow, int32_t xPrecision, int32_t xScale, bool isNull,
    int32_t outPrecision, int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 value(xHigh, xLow);

    auto result = DecimalOperations::AbsExact(value);

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// decimal64 arithmetical functions
extern "C" DLLEXPORT int32_t Decimal64Compare(int64_t x, int32_t xPrecision, int32_t xScale, int64_t y,
    int32_t yPrecision, int32_t yScale, bool isNull)
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

extern "C" DLLEXPORT int64_t AbsDecimal64(int64_t x, int32_t xPrecision, int32_t xScale, bool isNull, int32_t outPrecision,
    int32_t outScale)
{
    return std::abs(x);
}

extern "C" DLLEXPORT int64_t UnscaledValue64(int64_t x, int32_t precision, int32_t scale, bool isNull)
{
    return (int64_t)(x);
}

extern "C" DLLEXPORT bool IsOverflowDecimal64(int64_t x, int32_t precision, int32_t scale, int32_t checkPrecision,
    int32_t checkScale, bool isNull)
{
    int32_t wholeNumerSize = precision - scale;
    int32_t checkWholeNumerSize = checkPrecision - checkScale;
    if (checkWholeNumerSize >= wholeNumerSize) {
        return false;
    }
    int32_t left = scale + checkWholeNumerSize;
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

// Decimal AddOperator
extern "C" DLLEXPORT int64_t AddDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal128 xRescaleFactor = DecimalOperations::TenToScale(DecimalOperations::RescaleFactor(xScale, yScale));
    Decimal128 yRescaleFactor = DecimalOperations::TenToScale(DecimalOperations::RescaleFactor(yScale, xScale));
    int64_t result =
        x * static_cast<int64_t>(xRescaleFactor.LowBits()) + y * static_cast<int64_t>(yRescaleFactor.LowBits());

    OpStatus status = OP_OVERFLOW;
    if (xRescaleFactor > 1) {
        if (DecimalOperations::Rescale64(result, outScale - yScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    } else {
        if (DecimalOperations::Rescale64(result, outScale - xScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }

    return result;
}

extern "C" DLLEXPORT void AddDec64Dec64Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status =
        DecimalOperations::InternalAddDec128(Decimal128(x), xScale, Decimal128(y), yScale, resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        if (DecimalOperations::Rescale128(result, outScale - resultScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalAddDec128(Decimal128(xHigh, xLow), xScale, Decimal128(yHigh, yLow),
        yScale, resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalAddDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale,
        resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec128Dec64Dec128(int64_t contextPtr, int64_t yHigh, uint64_t yLow, int32_t yPrecision,
    int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalAddDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale,
        resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}


// Decimal SubOperator
extern "C" DLLEXPORT int64_t SubDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal128 xRescaleFactor = DecimalOperations::TenToScale(DecimalOperations::RescaleFactor(xScale, yScale));
    Decimal128 yRescaleFactor = DecimalOperations::TenToScale(DecimalOperations::RescaleFactor(yScale, xScale));
    int64_t result =
        x * static_cast<int64_t>(xRescaleFactor.LowBits()) - y * static_cast<int64_t>(yRescaleFactor.LowBits());

    OpStatus status = OP_OVERFLOW;
    if (xRescaleFactor > 1) {
        if (DecimalOperations::Rescale64(result, outScale - yScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    } else {
        if (DecimalOperations::Rescale64(result, outScale - xScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }

    return result;
}

extern "C" DLLEXPORT void SubDec64Dec64Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status =
        DecimalOperations::InternalSubDec128(Decimal128(x), xScale, Decimal128(y), yScale, resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalSubDec128(Decimal128(xHigh, xLow), xScale, Decimal128(yHigh, yLow),
        yScale, resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalSubDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale,
        resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128Dec64Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalSubDec128(Decimal128(xHigh, xLow), xScale, Decimal128(y), yScale,
        resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal MulOperator
extern "C" DLLEXPORT int64_t MulDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    int64_t result = x * y;
    int32_t reScale = xScale + yScale;
    OpStatus status = DecimalOperations::Rescale64(result, outScale - reScale, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }

    return result;
}

extern "C" DLLEXPORT void MulDec64Dec64Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 result;
    Decimal128 left(x);
    Decimal128 right(y);
    OpStatus status = DecimalOperations::Multiply(left, right, result);
    int32_t reScale = xScale + yScale;
    if (status == SUCCESS && reScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - reScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    Decimal128 left(xHigh, xLow);
    Decimal128 right(yHigh, yLow);
    OpStatus status = DecimalOperations::Multiply(left, right, result);
    int32_t reScale = xScale + yScale;
    if (status == SUCCESS && reScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - reScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    Decimal128 left(x);
    Decimal128 right(yHigh, yLow);
    OpStatus status = DecimalOperations::Multiply(left, right, result);
    int32_t reScale = xScale + yScale;
    if (status == SUCCESS && reScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - reScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec64Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    Decimal128 left(xHigh, xLow);
    Decimal128 right(y);
    OpStatus status = DecimalOperations::Multiply(left, right, result);
    int32_t reScale = xScale + yScale;
    if (status == SUCCESS && reScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - reScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status != SUCCESS) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal DivOperation
extern "C" DLLEXPORT int64_t DivDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (y == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    Decimal128 result;
    OpStatus status =
        DecimalOperations::InternalDivDec128(Decimal128(x), xScale, Decimal128(y), yScale, result, outScale);
    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT int64_t DivDec64Dec128Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (yHigh == 0 && yLow == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    Decimal128 result;
    OpStatus status =
        DecimalOperations::InternalDivDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale, result, outScale);
    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT int64_t DivDec128Dec64Dec64(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (y == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    Decimal128 result;
    OpStatus status =
        DecimalOperations::InternalDivDec128(Decimal128(xHigh, xLow), xScale, Decimal128(y), yScale, result, outScale);
    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT void DivDec64Dec64Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    if (y == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }
    Decimal128 result;
    OpStatus status =
        DecimalOperations::InternalDivDec128(Decimal128(x), xScale, Decimal128(y), yScale, result, outScale);
    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (yHigh == 0 && yLow == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }
    Decimal128 result;
    OpStatus status = DecimalOperations::InternalDivDec128(Decimal128(xHigh, xLow), xScale, Decimal128(yHigh, yLow),
        yScale, result, outScale);
    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (yHigh == 0 && yLow == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }
    Decimal128 result;
    OpStatus status =
        DecimalOperations::InternalDivDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale, result, outScale);
    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void DivDec128Dec64Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (y == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }

    Decimal128 result;
    OpStatus status =
        DecimalOperations::InternalDivDec128(Decimal128(xHigh, xLow), xScale, Decimal128(y), yScale, result, outScale);
    if (status == SUCCESS && outScale != xScale) {
        if (DecimalOperations::Rescale128(result, outScale - xScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal ModOperation
extern "C" DLLEXPORT int64_t ModDec64Dec64Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (y == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }

    Decimal128 resultDecimal;
    int64_t result;
    int32_t resultScale;
    OpStatus status =
        DecimalOperations::InternalModDec128(Decimal128(x), xScale, Decimal128(y), yScale, resultScale, resultDecimal);
    result = static_cast<int64_t>(resultDecimal.LowBits());
    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale64(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t ModDec64Dec128Dec64(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (yHigh == 0 && yLow == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }

    Decimal128 resultDecimal;
    int64_t result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalModDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale,
        resultScale, resultDecimal);
    result = static_cast<int64_t>(resultDecimal.LowBits());
    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale64(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t ModDec128Dec64Dec64(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (y == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    Decimal128 resultDecimal;
    int64_t result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalModDec128(Decimal128(xHigh, xLow), xScale, Decimal128(y), yScale,
        resultScale, resultDecimal);
    result = static_cast<int64_t>(resultDecimal.LowBits());

    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale64(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT void ModDec128Dec64Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (y == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }

    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalModDec128(Decimal128(xHigh, xLow), xScale, Decimal128(y), yScale,
        resultScale, result);
    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void ModDec128Dec128Dec128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (yHigh == 0 && yLow == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }

    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalModDec128(Decimal128(xHigh, xLow), xScale, Decimal128(yHigh, yLow),
        yScale, resultScale, result);
    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int64_t ModDec128Dec128Dec64(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale)
{
    if (yHigh == 0 && yLow == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }

    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalModDec128(Decimal128(xHigh, xLow), xScale, Decimal128(yHigh, yLow),
        yScale, resultScale, result);
    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }

    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT void ModDec64Dec128Dec128(int64_t contextPtr, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (yHigh == 0 && yLow == 0) {
        char message[] = "Division by zero";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }

    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalModDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale,
        resultScale, result);
    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        char message[] = "Decimal overflow";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Cast Function
extern "C" DLLEXPORT int64_t CastDecimal64To64(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale, bool isNull,
    int32_t newPrecision, int32_t newScale)
{
    int64_t result;
    OpStatus status = DecimalOperations::Rescale64(x, newScale - scale, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, newPrecision);
    }
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast DECIMAL(" << precision << ", " << scale << ") '"
            << DecimalOperations::ScaleOfDecimal(to_string(x), scale) << "' to DECIMAL(" << newPrecision << ", "
            << newScale << ")";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT void CastDecimal128To128(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, bool isNull, int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 lValue(xHigh, xLow);
    Decimal128 result;
    OpStatus status = DecimalOperations::Rescale128(lValue, newScale - scale, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, newPrecision);
    }
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast DECIMAL(" << precision << ", " << scale << ") '" <<
            DecimalOperations::ScaleOfDecimal(Decimal128(xHigh, xLow).ToString(), scale) << "' to DECIMAL(" <<
            newPrecision << ", " << newScale << ")";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void CastDecimal64To128(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale, bool isNull,
    int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    int32_t scaleDelta = newScale - scale;
    Decimal128 result;
    OpStatus status = DecimalOperations::Rescale64To128(x, scaleDelta, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, newPrecision);
    }
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast  DECIMAL(" << precision << ", " << scale << ") '"
            << DecimalOperations::ScaleOfDecimal(to_string(x), scale) << "' to DECIMAL(" << newPrecision << ","
            << newScale << ")";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        *outHighPtr = 0;
        *outLowPtr = 0;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int64_t CastDecimal128To64(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, bool isNull, int32_t newPrecision, int32_t newScale)
{
    int32_t scaleDelta = newScale - scale;
    int64_t result = 0;
    Decimal128 input(xHigh, xLow);
    OpStatus status = DecimalOperations::Rescale128To64(input, scaleDelta, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, newPrecision);
    }
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast  DECIMAL(" << precision << ", " << scale << ") '" <<
            DecimalOperations::ScaleOfDecimal(input.ToString(), scale) << "' to DECIMAL(" << newPrecision << "," <<
            newScale << ")";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t CastIntToDecimal64(int64_t contextPtr, int32_t x, bool isNull, int32_t precision, int32_t scale)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    int64_t result = 0;
    OpStatus status = OP_OVERFLOW;
    if (!__builtin_smull_overflow(x, tenToScale, &result)) {
        status = DecimalOperations::IsOverflows(result, precision);
    }
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast INTEGER '" << x << "' to DECIMAL(" << precision << "," << scale << ")";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t CastLongToDecimal64(int64_t contextPtr, int64_t x, bool isNull, int32_t outPrecision, int32_t outScale)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(outScale).LowBits());
    int64_t result = 0;
    OpStatus status = OP_OVERFLOW;
    if (!__builtin_smull_overflow(x, tenToScale, &result)) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast BIGINT '" << x << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t CastDoubleToDecimal64(int64_t contextPtr, double x, bool isNull, int32_t outPrecision, int32_t outScale)
{
    std::stringstream ss;
    ss << std::setprecision(15) << x;
    string s = ss.str();

    int32_t precision = 0;
    int32_t scale = 0;
    int64_t result = 0;
    OpStatus status = DecimalOperations::StringToDecimal64(s, result, scale, precision);
    if (status == SUCCESS) {
        status = DecimalOperations::Rescale64(result, outScale - scale, result);
    }
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage.precision(15);
        errorMessage << "Cannot cast DOUBLE '" << x << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT void CastIntToDecimal128(int64_t contextPtr, int32_t x, bool isNull, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    Decimal128 right = DecimalOperations::UnscaledDecimal(x);
    Decimal128 left = DecimalOperations::TenToScale(outScale);
    OpStatus status = DecimalOperations::Multiply(left, right, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast INTEGER '" << x << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void CastLongToDecimal128(int64_t contextPtr, int64_t x, bool isNull, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    Decimal128 right = DecimalOperations::UnscaledDecimal(x);
    Decimal128 left = DecimalOperations::TenToScale(outScale);
    OpStatus status = DecimalOperations::Multiply(left, right, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast BIGINT '" << x << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void CastDoubleToDecimal128(int64_t contextPtr, double x, bool isNull, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    std::stringstream ss;
    ss << std::setprecision(15) << x;
    string s = ss.str();

    int32_t precision = 0;
    int32_t scale = 0;
    Decimal128 result = 0;
    OpStatus status = DecimalOperations::StringToDecimal128(s, result, scale, precision);
    if (status == SUCCESS) {
        status = DecimalOperations::Rescale128(result, outScale - scale, result);
    }
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage.precision(15);
        errorMessage << "Cannot cast DOUBLE '" << x << "' to DECIMAL(" << outPrecision << "," << outScale << ")";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        *outHighPtr = 0;
        *outLowPtr = 0;
        return;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int32_t CastDecimal64ToInt(int64_t contextPtr, int64_t x, int32_t precision, int32_t scale, bool isNull)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    // this rounds the decimal value to the nearest integral value
    long longResult = (x + tenToScale / 2) / tenToScale;
    if (x < 0) {
        longResult = -((-x + tenToScale / 2) / tenToScale);
    }
    int32_t result;
    OpStatus status = DecimalOperations::ToIntExact(longResult, result);
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast '" << DecimalOperations::ScaleOfDecimal(to_string(x), scale) << "' to  BIGINT";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t CastDecimal64ToLong(int64_t x, int32_t precision, int32_t scale, bool isNull)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    if (x >= 0) {
        return (x + tenToScale / 2) / tenToScale;
    }
    return -((-x + tenToScale / 2) / tenToScale);
}

extern "C" DLLEXPORT double CastDecimal64ToDouble(int64_t x, int32_t precision, int32_t scale, bool isNull)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    return (static_cast<double>(x)) / static_cast<double>(tenToScale);
}

extern "C" DLLEXPORT int32_t CastDecimal128ToInt(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, bool isNull)
{
    Decimal128 inputDecimal(xHigh, xLow);
    Decimal128 outDecimal(0, 0);
    DecimalOperations::Rescale128(inputDecimal, -scale, outDecimal);
    int64_t longValue;
    OpStatus statusDecimal = DecimalOperations::UnscaledDecimal128ToLong(outDecimal, longValue);
    int32_t result;
    OpStatus statusInt = DecimalOperations::ToIntExact(longValue, result);
    if (statusDecimal != SUCCESS || statusInt != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast '" << DecimalOperations::ScaleOfDecimal(inputDecimal.ToString(), scale) <<
            "' to  INTEGER";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t CastDecimal128ToLong(int64_t contextPtr, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, bool isNull)
{
    Decimal128 inputDecimal(xHigh, xLow);
    Decimal128 outDecimal(0, 0);
    DecimalOperations::Rescale128(inputDecimal, -scale, outDecimal);
    int64_t result;
    OpStatus status = DecimalOperations::UnscaledDecimal128ToLong(outDecimal, result);
    if (status != SUCCESS) {
        ostringstream errorMessage;
        errorMessage << "Cannot cast '" << DecimalOperations::ScaleOfDecimal(inputDecimal.ToString(), scale) <<
            "' to  BIGINT";
        int32_t len = static_cast<int>(errorMessage.str().length()) + 1;
        SetError(contextPtr, const_cast<char *>(errorMessage.str().c_str()), len);
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT double CastDecimal128ToDouble(int64_t high, uint64_t low, int32_t precision, int32_t scale, bool isNull)
{
    Decimal128 input(high, low);
    string doubleString = DecimalOperations::ScaleOfDecimal(input.ToString(), scale);
    return stod(doubleString);
}

// return null
extern "C" DLLEXPORT int64_t AddDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y,
    int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal128 xRescaleFactor = DecimalOperations::TenToScale(DecimalOperations::RescaleFactor(xScale, yScale));
    Decimal128 yRescaleFactor = DecimalOperations::TenToScale(DecimalOperations::RescaleFactor(yScale, xScale));
    int64_t result =
        x * static_cast<int64_t>(xRescaleFactor.LowBits()) + y * static_cast<int64_t>(yRescaleFactor.LowBits());

    OpStatus status = OP_OVERFLOW;
    if (xRescaleFactor > 1) {
        if (DecimalOperations::Rescale64(result, outScale - yScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    } else {
        if (DecimalOperations::Rescale64(result, outScale - xScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
        return 0;
    }

    *isNull = false;
    return result;
}

extern "C" DLLEXPORT void AddDec64Dec64Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status =
        DecimalOperations::InternalAddDec128(Decimal128(x), xScale, Decimal128(y), yScale, resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        if (DecimalOperations::Rescale128(result, outScale - resultScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalAddDec128(Decimal128(xHigh, xLow), xScale, Decimal128(yHigh, yLow),
        yScale, resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalAddDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale,
        resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void AddDec128Dec64Dec128RetNull(bool *isNull, int64_t yHigh, uint64_t yLow, int32_t yPrecision,
    int32_t yScale, int64_t x, int32_t xPrecision, int32_t xScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalAddDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale,
        resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}


// Decimal SubOperator
extern "C" DLLEXPORT int64_t SubDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    Decimal128 xRescaleFactor = DecimalOperations::TenToScale(DecimalOperations::RescaleFactor(xScale, yScale));
    Decimal128 yRescaleFactor = DecimalOperations::TenToScale(DecimalOperations::RescaleFactor(yScale, xScale));
    int64_t result =
        x * static_cast<int64_t>(xRescaleFactor.LowBits()) - y * static_cast<int64_t>(yRescaleFactor.LowBits());

    OpStatus status = OP_OVERFLOW;
    if (xRescaleFactor > 1) {
        if (DecimalOperations::Rescale64(result, outScale - yScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    } else {
        if (DecimalOperations::Rescale64(result, outScale - xScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
        return 0;
    }

    return result;
}

extern "C" DLLEXPORT void SubDec64Dec64Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status =
        DecimalOperations::InternalSubDec128(Decimal128(x), xScale, Decimal128(y), yScale, resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalSubDec128(Decimal128(xHigh, xLow), xScale, Decimal128(yHigh, yLow),
        yScale, resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalSubDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale,
        resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void SubDec128Dec64Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalSubDec128(Decimal128(xHigh, xLow), xScale, Decimal128(y), yScale,
        resultScale, result);
    if (status == SUCCESS && outScale != resultScale) {
        status = DecimalOperations::Rescale128(result, outScale - resultScale, result);
        if (status == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal MulOperator
extern "C" DLLEXPORT int64_t MulDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    int64_t result = x * y;
    int32_t reScale = xScale + yScale;
    OpStatus status = DecimalOperations::Rescale64(result, outScale - reScale, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }

    if (status != SUCCESS) {
        *isNull = true;
    }

    return result;
}

extern "C" DLLEXPORT void MulDec64Dec64Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    Decimal128 result;
    Decimal128 left(x);
    Decimal128 right(y);
    OpStatus status = DecimalOperations::Multiply(left, right, result);
    int32_t reScale = xScale + yScale;
    if (status == SUCCESS && reScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - reScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec128Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision,
    int32_t outScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    Decimal128 left(xHigh, xLow);
    Decimal128 right(yHigh, yLow);
    OpStatus status = DecimalOperations::Multiply(left, right, result);
    int32_t reScale = xScale + yScale;
    if (status == SUCCESS && reScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - reScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    Decimal128 left(x);
    Decimal128 right(yHigh, yLow);
    OpStatus status = DecimalOperations::Multiply(left, right, result);
    int32_t reScale = xScale + yScale;
    if (status == SUCCESS && reScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - reScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128Dec64Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    Decimal128 left(xHigh, xLow);
    Decimal128 right(y);
    OpStatus status = DecimalOperations::Multiply(left, right, result);
    int32_t reScale = xScale + yScale;
    if (status == SUCCESS && reScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - reScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status != SUCCESS) {
        *isNull = true;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Decimal DivOperation
extern "C" DLLEXPORT int64_t DivDec64Dec64Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (y == 0) {
        *isNull = true;
        return 0;
    }
    Decimal128 result;
    OpStatus status =
        DecimalOperations::InternalDivDec128(Decimal128(x), xScale, Decimal128(y), yScale, result, outScale);
    if (status == OP_OVERFLOW) {
        *isNull = true;
        return 0;
    }
    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT int64_t DivDec64Dec128Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (yHigh == 0 && yLow == 0) {
        *isNull = true;
        return 0;
    }
    Decimal128 result;
    OpStatus status =
        DecimalOperations::InternalDivDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale, result, outScale);
    if (status == OP_OVERFLOW) {
        *isNull = true;
        return 0;
    }
    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT int64_t DivDec128Dec64Dec64RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (y == 0) {
        *isNull = true;
        return 0;
    }
    Decimal128 result;
    OpStatus status =
        DecimalOperations::InternalDivDec128(Decimal128(xHigh, xLow), xScale, Decimal128(y), yScale, result, outScale);
    if (status == OP_OVERFLOW) {
        *isNull = true;
        return 0;
    }
    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT void DivDec64Dec64Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale, int64_t *outHighPtr,
    uint64_t *outLowPtr)
{
    if (y == 0) {
        *isNull = true;
        return;
    }
    Decimal128 result;
    OpStatus status =
        DecimalOperations::InternalDivDec128(Decimal128(x), xScale, Decimal128(y), yScale, result, outScale);
    if (status == OP_OVERFLOW) {
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
    if (yHigh == 0 && yLow == 0) {
        *isNull = true;
        return;
    }
    Decimal128 result;
    OpStatus status = DecimalOperations::InternalDivDec128(Decimal128(xHigh, xLow), xScale, Decimal128(yHigh, yLow),
        yScale, result, outScale);
    if (status == OP_OVERFLOW) {
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
    if (yHigh == 0 && yLow == 0) {
        *isNull = true;
        return;
    }
    Decimal128 result;
    OpStatus status =
        DecimalOperations::InternalDivDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale, result, outScale);
    if (status == OP_OVERFLOW) {
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
    if (y == 0) {
        *isNull = true;
        return;
    }

    Decimal128 result;
    OpStatus status =
        DecimalOperations::InternalDivDec128(Decimal128(xHigh, xLow), xScale, Decimal128(y), yScale, result, outScale);
    if (status == SUCCESS && outScale != xScale) {
        if (DecimalOperations::Rescale128(result, outScale - xScale, result) == SUCCESS) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
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
    if (y == 0) {
        *isNull = true;
        return 0;
    }

    Decimal128 resultDecimal;
    int64_t result;
    int32_t resultScale;
    OpStatus status =
        DecimalOperations::InternalModDec128(Decimal128(x), xScale, Decimal128(y), yScale, resultScale, resultDecimal);
    result = static_cast<int64_t>(resultDecimal.LowBits());
    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale64(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        *isNull = true;
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t ModDec64Dec128Dec64RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, int64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (yHigh == 0 && yLow == 0) {
        *isNull = true;
        return 0;
    }

    Decimal128 resultDecimal;
    int64_t result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalModDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale,
        resultScale, resultDecimal);
    result = static_cast<int64_t>(resultDecimal.LowBits());
    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale64(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        *isNull = true;
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t ModDec128Dec64Dec64RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale)
{
    if (y == 0) {
        *isNull = true;
        return 0;
    }
    Decimal128 resultDecimal;
    int64_t result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalModDec128(Decimal128(xHigh, xLow), xScale, Decimal128(y), yScale,
        resultScale, resultDecimal);
    result = static_cast<int64_t>(resultDecimal.LowBits());

    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale64(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        *isNull = true;
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT void ModDec128Dec64Dec128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t xPrecision,
    int32_t xScale, int64_t y, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (y == 0) {
        *isNull = true;
        return;
    }

    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalModDec128(Decimal128(xHigh, xLow), xScale, Decimal128(y), yScale,
        resultScale, result);
    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
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
    if (yHigh == 0 && yLow == 0) {
        *isNull = true;
        return;
    }

    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalModDec128(Decimal128(xHigh, xLow), xScale, Decimal128(yHigh, yLow),
        yScale, resultScale, result);
    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
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
    if (yHigh == 0 && yLow == 0) {
        *isNull = true;
        return 0;
    }

    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalModDec128(Decimal128(xHigh, xLow), xScale, Decimal128(yHigh, yLow),
        yScale, resultScale, result);
    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        *isNull = true;
        return 0;
    }

    return result.HighBits() < 0 ? -static_cast<int64_t>(result.LowBits()) : static_cast<int64_t>(result.LowBits());
}

extern "C" DLLEXPORT void ModDec64Dec128Dec128RetNull(bool *isNull, int64_t x, int32_t xPrecision, int32_t xScale,
    int64_t yHigh, uint64_t yLow, int32_t yPrecision, int32_t yScale, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    if (yHigh == 0 && yLow == 0) {
        *isNull = true;
        return;
    }

    Decimal128 result;
    int32_t resultScale;
    OpStatus status = DecimalOperations::InternalModDec128(Decimal128(x), xScale, Decimal128(yHigh, yLow), yScale,
        resultScale, result);
    if (status == SUCCESS && resultScale != outScale) {
        if (DecimalOperations::Rescale128(result, outScale - resultScale, result)) {
            status = DecimalOperations::IsOverflows(result, outPrecision);
        } else {
            status = OP_OVERFLOW;
        }
    }

    if (status == OP_OVERFLOW) {
        *isNull = true;
        return;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

// Cast Function
extern "C" DLLEXPORT int64_t CastDecimal64To64RetNull(bool *isNull, int64_t x, int32_t precision, int32_t scale,
    int32_t newPrecision, int32_t newScale)
{
    int64_t result;
    OpStatus status = DecimalOperations::Rescale64(x, newScale - scale, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, newPrecision);
    }
    if (status != SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT void CastDecimal128To128RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 lValue(xHigh, xLow);
    Decimal128 result;
    OpStatus status = DecimalOperations::Rescale128(lValue, newScale - scale, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, newPrecision);
    }
    if (status != SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void CastDecimal64To128RetNull(bool *isNull, int64_t x, int32_t precision, int32_t scale,
    int32_t newPrecision, int32_t newScale, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    int32_t scaleDelta = newScale - scale;
    Decimal128 result;
    OpStatus status = DecimalOperations::Rescale64To128(x, scaleDelta, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, newPrecision);
    }
    if (status != SUCCESS) {
        *isNull = true;
        *outHighPtr = 0;
        *outLowPtr = 0;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int64_t CastDecimal128To64RetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale, int32_t newPrecision, int32_t newScale)
{
    int32_t scaleDelta = newScale - scale;
    int64_t result = 0;
    Decimal128 input(xHigh, xLow);
    OpStatus status = DecimalOperations::Rescale128To64(input, scaleDelta, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, newPrecision);
    }
    if (status != SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t CastIntToDecimal64RetNull(bool *isNull, int32_t x, int32_t precision, int32_t scale)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    int64_t result = 0;
    OpStatus status = OP_OVERFLOW;
    if (!__builtin_smull_overflow(x, tenToScale, &result)) {
        status = DecimalOperations::IsOverflows(result, precision);
    }
    if (status != SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t CastLongToDecimal64RetNull(bool *isNull, int64_t x, int32_t outPrecision, int32_t outScale)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(outScale).LowBits());
    int64_t result = 0;
    OpStatus status = OP_OVERFLOW;
    if (!__builtin_smull_overflow(x, tenToScale, &result)) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t CastDoubleToDecimal64RetNull(bool *isNull, double x, int32_t outPrecision,
    int32_t outScale)
{
    std::stringstream ss;
    ss << std::setprecision(15) << x;
    string s = ss.str();

    int32_t precision = 0;
    int32_t scale = 0;
    int64_t result = 0;
    OpStatus status = DecimalOperations::StringToDecimal64(s, result, scale, precision);
    if (status == SUCCESS) {
        status = DecimalOperations::Rescale64(result, outScale - scale, result);
    }
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT void CastIntToDecimal128RetNull(bool *isNull, int32_t x, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    Decimal128 right = DecimalOperations::UnscaledDecimal(x);
    Decimal128 left = DecimalOperations::TenToScale(outScale);
    OpStatus status = DecimalOperations::Multiply(left, right, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void CastLongToDecimal128RetNull(bool *isNull, int64_t x, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 result;
    Decimal128 right = DecimalOperations::UnscaledDecimal(x);
    Decimal128 left = DecimalOperations::TenToScale(outScale);
    OpStatus status = DecimalOperations::Multiply(left, right, result);
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        *isNull = true;
        return;
    }
    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void CastDoubleToDecimal128RetNull(bool *isNull, double x, int32_t outPrecision, int32_t outScale,
    int64_t *outHighPtr, uint64_t *outLowPtr)
{
    std::stringstream ss;
    ss << std::setprecision(15) << x;
    string s = ss.str();

    int32_t precision = 0;
    int32_t scale = 0;
    Decimal128 result = 0;
    OpStatus status = DecimalOperations::StringToDecimal128(s, result, scale, precision);
    if (status == SUCCESS) {
        status = DecimalOperations::Rescale128(result, outScale - scale, result);
    }
    if (status == SUCCESS) {
        status = DecimalOperations::IsOverflows(result, outPrecision);
    }
    if (status != SUCCESS) {
        *isNull = true;
        *outHighPtr = 0;
        *outLowPtr = 0;
        return;
    }

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT int32_t CastDecimal64ToIntRetNull(bool *isNull, int64_t x, int32_t precision, int32_t scale)
{
    int64_t tenToScale = static_cast<int64_t>(DecimalOperations::TenToScale(scale).LowBits());
    // this rounds the decimal value to the nearest integral value
    long longResult = (x + tenToScale / 2) / tenToScale;
    if (x < 0) {
        longResult = -((-x + tenToScale / 2) / tenToScale);
    }
    int32_t result;
    OpStatus status = DecimalOperations::ToIntExact(longResult, result);
    if (status != SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int32_t CastDecimal128ToIntRetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale)
{
    Decimal128 inputDecimal(xHigh, xLow);
    Decimal128 outDecimal(0, 0);
    DecimalOperations::Rescale128(inputDecimal, -scale, outDecimal);
    int64_t longValue;
    OpStatus statusDecimal = DecimalOperations::UnscaledDecimal128ToLong(outDecimal, longValue);
    int32_t result;
    OpStatus statusInt = DecimalOperations::ToIntExact(longValue, result);
    if (statusDecimal != SUCCESS || statusInt != SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int64_t CastDecimal128ToLongRetNull(bool *isNull, int64_t xHigh, uint64_t xLow, int32_t precision,
    int32_t scale)
{
    Decimal128 inputDecimal(xHigh, xLow);
    Decimal128 outDecimal(0, 0);
    DecimalOperations::Rescale128(inputDecimal, -scale, outDecimal);
    int64_t result;
    OpStatus status = DecimalOperations::UnscaledDecimal128ToLong(outDecimal, result);
    if (status != SUCCESS) {
        *isNull = true;
        return 0;
    }
    return result;
}
}
}