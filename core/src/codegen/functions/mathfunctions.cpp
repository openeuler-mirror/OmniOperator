/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#include "mathfunctions.h"
#include <iostream>
#include "context_helper.h"
#include "util/engine.h"


#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

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
    EngineType engineType = EngineUtil::GetInstance().GetEngineType();
    if (engineType == EngineType::Spark) {
        return static_cast<int32_t>(x);
    } else {
        return static_cast<int32_t>(Round(x, 0));
    }
}

extern "C" DLLEXPORT int64_t CastDoubleToInt64(double x)
{
    EngineType engineType = EngineUtil::GetInstance().GetEngineType();
    if (engineType == EngineType::Spark) {
        return static_cast<int64_t>(x);
    } else {
        return static_cast<int64_t>(Round(x, 0));
    }
}


extern "C" DLLEXPORT double DivideDouble(double divident, double divisor)
{
    return divident / divisor;
}

extern "C" DLLEXPORT double ModulusDouble(double divident, double divisor)
{
    return std::fmod(divident, divisor);
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