/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#include "mathfunctions.h"
#include <iostream>
#include "context_helper.h"


#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif
namespace {
const int COMBINE_HASH_VALUE = 31;
}

extern "C" DLLEXPORT int64_t CastInt32ToInt64(int32_t x)
{
    return static_cast<int64_t>(x);
}

extern "C" DLLEXPORT int32_t CastInt64ToInt32(int64_t x)
{
    return static_cast<int32_t>(x);
}

extern "C" DLLEXPORT double CastInt32ToDouble(int32_t x)
{
    return static_cast<double>(x);
}

extern "C" DLLEXPORT double CastInt64ToDouble(int64_t x)
{
    return static_cast<double>(x);
}

extern "C" DLLEXPORT int32_t CastDoubleToInt32(double x)
{
    return static_cast<int32_t>(Round(x, 0));
}

extern "C" DLLEXPORT int64_t CastDoubleToInt64(double x)
{
    return static_cast<int64_t>(Round(x, 0));
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