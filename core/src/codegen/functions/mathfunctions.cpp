/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#include "mathfunctions.h"
#include <iostream>
#include <cfloat>
#include "codegen/context_helper.h"
#include "codegen/common_util.h"


#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

const double DOUBLE_NAN = (0.0 / 0.0);
const uint64_t DOUBLE_BIT_MASK = ((static_cast<uint64_t>(1) << (sizeof(double) * 8 - 1)) - 1);

namespace omniruntime::codegen::function {
static constexpr char DIVIDE_ZERO_EROR[] = "Divided by zero error!";

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

extern "C" DLLEXPORT int32_t CastDoubleToInt32Down(double x)
{
    return static_cast<int32_t>(x);
}

extern "C" DLLEXPORT int64_t CastDoubleToInt64Down(double x)
{
    return static_cast<int64_t>(x);
}

extern "C" DLLEXPORT int32_t CastDoubleToInt32HalfUp(double x)
{
    return static_cast<int32_t>(Round(x, 0));
}

extern "C" DLLEXPORT int64_t CastDoubleToInt64HalfUp(double x)
{
    return static_cast<int64_t>(Round(x, 0));
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

extern "C" DLLEXPORT double DivideDouble(bool *isNull, double divident, double divisor)
{
    if (divisor == 0) {
        *isNull = true;
        return 0;
    }
    return divident / divisor;
}

extern "C" DLLEXPORT double ModulusDouble(bool *isNull, double divident, double divisor)
{
    if (divisor == 0) {
        *isNull = true;
        return 0;
    }
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
    return std::fabs(left - right) < DBL_EPSILON;
}

extern "C" DLLEXPORT bool NotEqualDouble(double left, double right)
{
    return std::fabs(left - right) >= DBL_EPSILON;
}

extern "C" DLLEXPORT double NormalizeNaNAndZero(double value)
{
    if (std::isnan(value)) {
        return DOUBLE_NAN;
    }
    union {
        uint64_t l;
        double d;
    } u;
    u.d = value;
    if (u.l & DOUBLE_BIT_MASK) {
        return value;
    }
    return 0.0;
}

extern "C" DLLEXPORT double PowerDouble(double base, double exponent)
{
    return pow(base, exponent);
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

extern "C" DLLEXPORT int64_t DivideInt64(bool *isNull, int64_t divident, int64_t divisor)
{
    if (divisor == 0) {
        *isNull = true;
        return 0;
    }
    return divident / divisor;
}

extern "C" DLLEXPORT int64_t ModulusInt64(bool *isNull, int64_t divident, int64_t divisor)
{
    if (divisor == 0) {
        *isNull = true;
        return 0;
    }
    return divident % divisor;
}

extern "C" DLLEXPORT int64_t AddInt64RetNull(bool *isNull, int64_t left, int64_t right)
{
    int64_t result;
    *isNull = __builtin_add_overflow(left, right, &result);
    return result;
}

extern "C" DLLEXPORT int64_t SubtractInt64RetNull(bool *isNull, int64_t left, int64_t right)
{
    int64_t result;
    *isNull = __builtin_sub_overflow(left, right, &result);
    return result;
}

extern "C" DLLEXPORT int64_t MultiplyInt64RetNull(bool *isNull, int64_t left, int64_t right)
{
    int64_t result;
    *isNull = __builtin_mul_overflow(left, right, &result);
    return result;
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

extern "C" DLLEXPORT int32_t DivideInt32(bool *isNull, int32_t divident, int32_t divisor)
{
    if (divisor == 0) {
        *isNull = true;
        return 0;
    }
    return divident / divisor;
}

extern "C" DLLEXPORT int32_t ModulusInt32(bool *isNull, int32_t divident, int32_t divisor)
{
    if (divisor == 0) {
        *isNull = true;
        return 0;
    }
    return divident % divisor;
}

extern "C" DLLEXPORT int32_t AddInt32RetNull(bool *isNull, int32_t left, int32_t right)
{
    int32_t result;
    *isNull = __builtin_add_overflow(left, right, &result);
    return result;
}

extern "C" DLLEXPORT int32_t SubtractInt32RetNull(bool *isNull, int32_t left, int32_t right)
{
    int32_t result;
    *isNull = __builtin_sub_overflow(left, right, &result);
    return result;
}

extern "C" DLLEXPORT int32_t MultiplyInt32RetNull(bool *isNull, int32_t left, int32_t right)
{
    int32_t result;
    *isNull = __builtin_mul_overflow(left, right, &result);
    return result;
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

extern "C" DLLEXPORT int64_t RoundLong(int64_t num, int32_t decimals)
{
    return RoundOperator(num, decimals);
}
}