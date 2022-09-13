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

using namespace omniruntime::codegen;

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

// double functions

extern "C" DLLEXPORT double AddDouble(double left, double right)
{
    return left + right;
}

extern "C" DLLEXPORT double SubtractDouble(double left, double right)
{
    return left - right;
}

extern "C" DLLEXPORT double MultiplyDouble(double left, double right)
{
    return left * right;
}

extern "C" DLLEXPORT double DivideDouble(double divident, double divisor)
{
    return divident / divisor;
}

extern "C" DLLEXPORT double ModulusDouble(double divident, double divisor)
{
    return std::fmod(divident, divisor);
}

extern "C" DLLEXPORT bool LessThanDouble(double left, double right)
{
    return left < right;
}

extern "C" DLLEXPORT bool LessThanEqualDouble(double left, double right)
{
    return left <= right;
}

extern "C" DLLEXPORT bool GreaterThanDouble(double left, double right)
{
    return left > right;
}

extern "C" DLLEXPORT bool GreaterThanEqualDouble(double left, double right)
{
    return left >= right;
}

extern "C" DLLEXPORT bool EqualDouble(double left, double right)
{
    return left == right;
}

extern "C" DLLEXPORT bool NotEqualDouble(double left, double right)
{
    return left != right;
}

// long functions

extern "C" DLLEXPORT int64_t AddInt64(int64_t left, int64_t right)
{
    return left + right;
}

extern "C" DLLEXPORT int64_t SubtractInt64(int64_t left, int64_t right)
{
    return left - right;
}

extern "C" DLLEXPORT int64_t MultiplyInt64(int64_t left, int64_t right)
{
    return left * right;
}

extern "C" DLLEXPORT int64_t DivideInt64(int64_t contextPtr, int64_t divident, int64_t divisor)
{
    if (divisor == 0) {
        char message[] = "Divided by zero error!";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    return divident / divisor;
}

extern "C" DLLEXPORT int64_t ModulusInt64(int64_t contextPtr, int64_t divident, int64_t divisor)
{
    if (divisor == 0) {
        char message[] = "Divided by zero error!";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    return std::fmod(divident, divisor);
}

extern "C" DLLEXPORT bool LessThanInt64(int64_t left, int64_t right)
{
    return left < right;
}

extern "C" DLLEXPORT bool LessThanEqualInt64(int64_t left, int64_t right)
{
    return left <= right;
}

extern "C" DLLEXPORT bool GreaterThanInt64(int64_t left, int64_t right)
{
    return left > right;
}

extern "C" DLLEXPORT bool GreaterThanEqualInt64(int64_t left, int64_t right)
{
    return left >= right;
}

extern "C" DLLEXPORT bool EqualInt64(int64_t left, int64_t right)
{
    return left == right;
}

extern "C" DLLEXPORT bool NotEqualInt64(int64_t left, int64_t right)
{
    return left != right;
}

// int functions

extern "C" DLLEXPORT int32_t AddInt32(int32_t left, int32_t right)
{
    return left + right;
}

extern "C" DLLEXPORT int32_t SubtractInt32(int32_t left, int32_t right)
{
    return left - right;
}

extern "C" DLLEXPORT int32_t MultiplyInt32(int32_t left, int32_t right)
{
    return left * right;
}

extern "C" DLLEXPORT int32_t DivideInt32(int64_t contextPtr, int32_t divident, int32_t divisor)
{
    if (divisor == 0) {
        char message[] = "Divided by zero error!";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    return divident / divisor;
}

extern "C" DLLEXPORT int32_t ModulusInt32(int64_t contextPtr, int32_t divident, int32_t divisor)
{
    if (divisor == 0) {
        char message[] = "Divided by zero error!";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return 0;
    }
    return std::fmod(divident, divisor);
}

extern "C" DLLEXPORT bool LessThanInt32(int32_t left, int32_t right)
{
    return left < right;
}

extern "C" DLLEXPORT bool LessThanEqualInt32(int32_t left, int32_t right)
{
    return left <= right;
}

extern "C" DLLEXPORT bool GreaterThanInt32(int32_t left, int32_t right)
{
    return left > right;
}

extern "C" DLLEXPORT bool GreaterThanEqualInt32(int32_t left, int32_t right)
{
    return left >= right;
}

extern "C" DLLEXPORT bool EqualInt32(int32_t left, int32_t right)
{
    return left == right;
}

extern "C" DLLEXPORT bool NotEqualInt32(int32_t left, int32_t right)
{
    return left != right;
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