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

extern "C" DLLEXPORT double DivideDouble(double divident, double divisor);

extern "C" DLLEXPORT double ModulusDouble(double divident, double divisor);

extern "C" DLLEXPORT int64_t CombineHash(int64_t prevHashVal, int64_t val);

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

#endif