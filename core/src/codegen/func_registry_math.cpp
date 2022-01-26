/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Math Functions Registry
 */
#include "func_registry_math.h"
#include "functions/mathfunctions.h"
using namespace omniruntime;
using namespace omniruntime::expressions;

std::vector<Function> GetMathFunctionRegistry()
{
    std::string absFnStr = "abs";
    std::string castFnStr = "CAST";
    static std::vector<Function> mathFnRegistry = {
        // insert native functions for each absolute math function
        Function(reinterpret_cast<void*>(Abs<int32_t>), absFnStr, {}, {INT32D}, INT32D, true),
        Function(reinterpret_cast<void*>(Abs<int64_t>), absFnStr, {}, {INT64D}, INT64D, true),
        Function(reinterpret_cast<void*>(Abs<double>), absFnStr, {}, {DOUBLED}, DOUBLED, true),

        // insert native functions for each cast math function
        Function(reinterpret_cast<void*>(CastInt32ToDouble), castFnStr, {}, {INT32D}, DOUBLED, true),
        Function(reinterpret_cast<void*>(CastInt64ToDouble), castFnStr, {}, {INT64D}, DOUBLED, true),
        Function(reinterpret_cast<void*>(CastInt32ToInt64), castFnStr, {}, {INT32D}, INT64D, true),
        Function(reinterpret_cast<void*>(CastInt64ToInt32), castFnStr, {}, {INT64D}, INT32D, true),

        // insert native function for combine hash math function
        Function(reinterpret_cast<void*>(CombineHash), "combine_hash", {}, {INT64D, INT64D}, INT64D, true),

        // insert pmod function for project operator support
        Function(reinterpret_cast<void*>(Pmod), "pmod", {}, {INT32D, INT32D}, INT32D, true)

    };
    return mathFnRegistry;
}