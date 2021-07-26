/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2027. All rights reserved.
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

extern "C" DLLEXPORT double CastInt32(int32_t x)
{
    return (double)(x);
}

extern "C" DLLEXPORT double CastInt64(int64_t x)
{
    return (double)(x);
}


extern "C" DLLEXPORT int64_t CombineHash(int64_t prevHashVal, int64_t val)
{
    return COMBINE_HASH_VALUE * prevHashVal + val;
}