/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */


#include "context_helper.h"
#include "decimalfunctions.h"

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif
using namespace omniruntime::vec;
using namespace std;

extern "C" DLLEXPORT int32_t Decimal128Compare(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow)
{
    Decimal128 left (xHigh, xLow);
    Decimal128 right (yHigh, yLow);

    if (left < right) {
        return -1;
    }
    if (left > right) {
        return 1;
    }
    return 0;
}
extern "C" DLLEXPORT void AddDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow,
                                       int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue(yHigh, yLow);

    lValue += rValue;

    *outHighPtr = lValue.HighBits();
    *outLowPtr = lValue.LowBits();
}

extern "C" DLLEXPORT void SubDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow,
                                       int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue(yHigh, yLow);

    lValue -= rValue;

    *outHighPtr = lValue.HighBits();
    *outLowPtr = lValue.LowBits();
}

extern "C" DLLEXPORT void DivDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow,
                                       int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue(yHigh, yLow);
    Decimal128 remainder;
    Decimal128 result;
    lValue.Divide(rValue, result, remainder);
    DecimalOperations::RoundUp(lValue, rValue, result, remainder);

    *outHighPtr = result.HighBits();
    *outLowPtr = result.LowBits();
}

extern "C" DLLEXPORT void MulDec128(int64_t xHigh, uint64_t xLow, int64_t yHigh, uint64_t yLow,
                                       int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 lValue(xHigh, xLow);
    Decimal128 rValue(yHigh, yLow);

    lValue *= rValue;

    *outHighPtr = lValue.HighBits();
    *outLowPtr = lValue.LowBits();
}

extern "C" DLLEXPORT void AbsDecimal128(int64_t xHigh, uint64_t xLow, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    Decimal128 value(xHigh, xLow);

    value.Abs();

    *outHighPtr = value.HighBits();
    *outLowPtr = value.LowBits();
}

extern "C" DLLEXPORT void CastInt64ToDecimal128(int64_t x, int64_t *outHighPtr, uint64_t *outLowPtr)
{
    *outHighPtr = 0;
    *outLowPtr = x;
}