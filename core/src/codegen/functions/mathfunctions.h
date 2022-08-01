/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#ifndef __MATHFUNCTIONS_H__
#define __MATHFUNCTIONS_H__

#include <iostream>
#include <cmath>

// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

// Absolute value
template <typename T> extern DLLEXPORT T Abs(T x)
{
    return std::abs(x);
}

extern "C" DLLEXPORT double CastInt32ToDouble(int32_t x);

extern "C" DLLEXPORT double CastInt64ToDouble(int64_t x);

extern "C" DLLEXPORT int64_t CastInt32ToInt64(int32_t x);

extern "C" DLLEXPORT int32_t CastInt64ToInt32(int64_t x);

extern "C" DLLEXPORT int32_t CastDoubleToInt32(double x);

extern "C" DLLEXPORT int64_t CastDoubleToInt64(double x);

// double binary operations
extern "C" DLLEXPORT double AddDouble(double left, double right);

extern "C" DLLEXPORT double SubtractDouble(double left, double right);

extern "C" DLLEXPORT double MultiplyDouble(double left, double right);

extern "C" DLLEXPORT double DivideDouble(double divident, double divisor);

extern "C" DLLEXPORT double ModulusDouble(double divident, double divisor);

extern "C" DLLEXPORT bool LessThanDouble(double left, double right);

extern "C" DLLEXPORT bool LessThanEqualDouble(double left, double right);

extern "C" DLLEXPORT bool GreaterThanDouble(double left, double right);

extern "C" DLLEXPORT bool GreaterThanEqualDouble(double left, double right);

extern "C" DLLEXPORT bool EqualDouble(double left, double right);

extern "C" DLLEXPORT bool NotEqualDouble(double left, double right);

// long binary operations

extern "C" DLLEXPORT int64_t AddInt64(int64_t left, int64_t right);

extern "C" DLLEXPORT int64_t SubtractInt64(int64_t left, int64_t right);

extern "C" DLLEXPORT int64_t MultiplyInt64(int64_t left, int64_t right);

extern "C" DLLEXPORT int64_t DivideInt64(int64_t contextPtr, int64_t divident, int64_t divisor);

extern "C" DLLEXPORT int64_t ModulusInt64(int64_t contextPtr, int64_t divident, int64_t divisor);

extern "C" DLLEXPORT bool LessThanInt64(int64_t left, int64_t right);

extern "C" DLLEXPORT bool LessThanEqualInt64(int64_t left, int64_t right);

extern "C" DLLEXPORT bool GreaterThanInt64(int64_t left, int64_t right);

extern "C" DLLEXPORT bool GreaterThanEqualInt64(int64_t left, int64_t right);

extern "C" DLLEXPORT bool EqualInt64(int64_t left, int64_t right);

extern "C" DLLEXPORT bool NotEqualInt64(int64_t left, int64_t right);

extern "C" DLLEXPORT int32_t AddInt32(int32_t left, int32_t right);

extern "C" DLLEXPORT int32_t SubtractInt32(int32_t left, int32_t right);

extern "C" DLLEXPORT int32_t MultiplyInt32(int32_t left, int32_t right);

extern "C" DLLEXPORT int32_t DivideInt32(int64_t contextPtr, int32_t divident, int32_t divisor);

extern "C" DLLEXPORT int32_t ModulusInt32(int64_t contextPtr, int32_t divident, int32_t divisor);

extern "C" DLLEXPORT bool LessThanInt32(int32_t left, int32_t right);

extern "C" DLLEXPORT bool LessThanEqualInt32(int32_t left, int32_t right);

extern "C" DLLEXPORT bool GreaterThanInt32(int32_t left, int32_t right);

extern "C" DLLEXPORT bool GreaterThanEqualInt32(int32_t left, int32_t right);

extern "C" DLLEXPORT bool EqualInt32(int32_t left, int32_t right);

extern "C" DLLEXPORT bool NotEqualInt32(int32_t left, int32_t right);

extern "C" DLLEXPORT int32_t Pmod(int32_t x, int32_t y);

template <typename T> extern DLLEXPORT T Round(T num, int32_t decimals)
{
    if (std::isnan(num) || std::isinf(num)) {
        return num;
    }
    int32_t tenthPower = 10;
    double factor = std::pow(tenthPower, decimals);
    if (num < 0) {
        return -(std::round(-num * factor) / factor);
    }

    return std::round(num * factor) / factor;
}

extern "C" DLLEXPORT double OverflowThrowException(int64_t contextPtr, int x);

extern "C" DLLEXPORT double OverflowReturnNull(int x, bool *isNull);

#endif