/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#include "mathfunctions.h"
#include <iostream>


#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif
namespace {
    const int COMBINE_HASH_VALUE = 31;
}

// Absolute value
extern "C" DLLEXPORT int32_t AbsInt32(int32_t x)
{
    return std::abs(x);
}

extern "C" DLLEXPORT int64_t AbsInt64(int64_t x)
{
    return std::abs(x);
}

extern "C" DLLEXPORT double AbsDouble(double x)
{
    return std::abs(x);
}

extern "C" DLLEXPORT long CastInt32ToInt64(int32_t x)
{
    return static_cast<long>(x);
}


extern "C" DLLEXPORT int CastInt64ToInt32(int64_t x)
{
    return static_cast<int>(x);
}

extern "C" DLLEXPORT double CastInt32ToDouble(int32_t x)
{
    return static_cast<double>(x);
}

extern "C" DLLEXPORT double CastInt64ToDouble(int64_t x)
{
    return static_cast<double>(x);
}

extern "C" DLLEXPORT int64_t CombineHash(int64_t prevHashVal, int64_t val)
{
    return COMBINE_HASH_VALUE * prevHashVal + val;
}

extern "C" DLLEXPORT int32_t Pmod(int32_t x, int32_t y)
{
    if (y == 0) {
        return 0;
    }
    int32_t r = x % y;
    if (r < 0) {
        return (r + y) % y;
    } else {
        return r;
    }
}