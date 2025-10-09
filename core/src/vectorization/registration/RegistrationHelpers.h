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
void registerBinaryIntegral(const std::string &aliases)
{
    registerFunction<T, int8_t, int8_t, int8_t>(aliases, {OMNI_BYTE, OMNI_BYTE}, OMNI_BYTE);
    registerFunction<T, int16_t, int16_t, int16_t>(aliases, {OMNI_SHORT, OMNI_SHORT}, OMNI_SHORT);
    registerFunction<T, int32_t, int32_t, int32_t>(aliases, {OMNI_INT, OMNI_INT}, OMNI_INT);
    registerFunction<T, int64_t, int64_t, int64_t>(aliases, {OMNI_LONG, OMNI_LONG}, OMNI_LONG);
}

template <template <class> typename T>
void registerBinaryFloatingPoint(const std::string &aliases)
{
    registerFunction<T, double, double, double>(aliases, {OMNI_DOUBLE, OMNI_DOUBLE}, OMNI_DOUBLE);
    registerFunction<T, float, float, float>(aliases, {OMNI_FLOAT, OMNI_FLOAT}, OMNI_FLOAT);
}

template <template <class> typename T>
void registerBinaryNumeric(const std::string &aliases)
{
    registerBinaryIntegral<T>(aliases);
    registerBinaryFloatingPoint<T>(aliases);
}

template <template <class> typename T>
void registerString(const std::string &aliases)
{
    registerFunction<T, bool, std::string_view, std::string_view>(aliases, {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_BOOLEAN);
}
}
