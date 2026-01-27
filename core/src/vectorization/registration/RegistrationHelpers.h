/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include <vector>
#include <string>
#include "Register.h"
#include "SimpleFunctionRegistry.h"

namespace omniruntime::vectorization {
template <template <class> typename T>
void RegisterBinaryIntegral(const std::string &aliases)
{
    RegisterFunction<T, int8_t, int8_t, int8_t>(aliases, {OMNI_BYTE, OMNI_BYTE}, OMNI_BYTE);
    RegisterFunction<T, int16_t, int16_t, int16_t>(aliases, {OMNI_SHORT, OMNI_SHORT}, OMNI_SHORT);
    RegisterFunction<T, int32_t, int32_t, int32_t>(aliases, {OMNI_INT, OMNI_INT}, OMNI_INT);
    RegisterFunction<T, int64_t, int64_t, int64_t>(aliases, {OMNI_LONG, OMNI_LONG}, OMNI_LONG);
}

template <template <class> typename T>
void RegisterBinaryFloatingPoint(const std::string &aliases)
{
    RegisterFunction<T, double, double, double>(aliases, {OMNI_DOUBLE, OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<T, float, float, float>(aliases, {OMNI_FLOAT, OMNI_FLOAT}, OMNI_FLOAT);
}

template <template <class> typename T>
void RegisterBinaryNumeric(const std::string &aliases)
{
    RegisterBinaryIntegral<T>(aliases);
    RegisterBinaryFloatingPoint<T>(aliases);
}

template <template <class> typename T>
void RegisterCompareIntegral(const std::string &aliases)
{
    RegisterFunction<T, bool, int8_t, int8_t>(aliases, {OMNI_BYTE, OMNI_BYTE}, OMNI_BOOLEAN);
    RegisterFunction<T, bool, int16_t, int16_t>(aliases, {OMNI_SHORT, OMNI_SHORT}, OMNI_BOOLEAN);
    RegisterFunction<T, bool, int32_t, int32_t>(aliases, {OMNI_INT, OMNI_INT}, OMNI_BOOLEAN);
    RegisterFunction<T, bool, int64_t, int64_t>(aliases, {OMNI_LONG, OMNI_LONG}, OMNI_BOOLEAN);
}

template <template <class> typename T>
void RegisterUnaryIntegral(const std::string &aliases)
{
    RegisterFunction<T, bool, bool>(aliases, {OMNI_BOOLEAN}, OMNI_BOOLEAN);
}

template <template <class> typename T>
void RegisterUnaryIntegralNumeric(const std::string &aliases)
{
    RegisterFunction<T, int8_t, int8_t>(aliases, {OMNI_BYTE}, OMNI_BYTE);
    RegisterFunction<T, int16_t, int16_t>(aliases, {OMNI_SHORT}, OMNI_SHORT);
    RegisterFunction<T, int32_t, int32_t>(aliases, {OMNI_INT}, OMNI_INT);
    RegisterFunction<T, int64_t, int64_t>(aliases, {OMNI_LONG}, OMNI_LONG);
}

template <template <class> typename T>
void RegisterUnaryFloatingPoint(const std::string &aliases)
{
    RegisterFunction<T, double, double>(aliases, {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<T, float, float>(aliases, {OMNI_FLOAT}, OMNI_FLOAT);
}

template <template <class> typename T>
void RegisterUnaryDecimal(const std::string &aliases)
{
    RegisterFunction<T, int64_t, int64_t>(aliases, {OMNI_DECIMAL64}, OMNI_DECIMAL64);
    RegisterFunction<T, Decimal128, Decimal128>(aliases, {OMNI_DECIMAL128}, OMNI_DECIMAL128);
}

template <template <class> typename T>
void RegisterUnaryNumeric(const std::string &aliases)
{
    RegisterUnaryIntegralNumeric<T>(aliases);
    RegisterUnaryFloatingPoint<T>(aliases);
    RegisterUnaryDecimal<T>(aliases);
}

template <template <class> typename T>
void RegisterBinaryLogical(const std::string &aliases)
{
    RegisterFunction<T, bool, bool, bool>(aliases, {OMNI_BOOLEAN, OMNI_BOOLEAN}, OMNI_BOOLEAN);
}

template <template <class> typename T>
void RegisterBinaryCompare(const std::string &aliases)
{
    RegisterCompareIntegral<T>(aliases);
}

template <template <class> typename T>
void RegisterString(const std::string &aliases)
{
    RegisterFunction<T, bool, std::string_view, std::string_view>(aliases, {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_BOOLEAN);
}
}
