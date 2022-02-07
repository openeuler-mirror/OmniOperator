/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Math Functions Registry
 */
#include "func_registry_math.h"
#include "functions/mathfunctions.h"
using namespace omniruntime;
using namespace omniruntime::vec;

std::vector<Function> GetMathFunctionRegistry()
{
    std::string absFnStr = "abs";
    std::string castFnStr = "CAST";
    static std::vector<Function> mathFnRegistry = {
        // insert native functions for each absolute math function
        Function(reinterpret_cast<void*>(Abs<int32_t>), absFnStr, {}, {OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_INT),
        Function(reinterpret_cast<void*>(Abs<int64_t>), absFnStr, {}, {OMNI_VEC_TYPE_LONG}, OMNI_VEC_TYPE_LONG),
        Function(reinterpret_cast<void*>(Abs<double>), absFnStr, {}, {OMNI_VEC_TYPE_DOUBLE}, OMNI_VEC_TYPE_DOUBLE),

        // insert native functions for each cast math function
        Function(reinterpret_cast<void*>(CastInt32ToDouble), castFnStr, {}, {OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_DOUBLE),
        Function(reinterpret_cast<void*>(CastInt64ToDouble), castFnStr, {}, {OMNI_VEC_TYPE_LONG}, OMNI_VEC_TYPE_DOUBLE),
        Function(reinterpret_cast<void*>(CastInt32ToInt64), castFnStr, {}, {OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_LONG),
        Function(reinterpret_cast<void*>(CastInt64ToInt32), castFnStr, {}, {OMNI_VEC_TYPE_LONG}, OMNI_VEC_TYPE_INT),
        Function(reinterpret_cast<void*>(CastInt32ToInt64), castFnStr, {}, {OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_LONG),
        Function(reinterpret_cast<void*>(CastInt64ToInt32), castFnStr, {}, {OMNI_VEC_TYPE_LONG}, OMNI_VEC_TYPE_INT),

        // insert native function for combine hash math function
        Function(reinterpret_cast<void*>(CombineHash), "combine_hash", {}, {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG}, OMNI_VEC_TYPE_LONG),

        // insert pmod function for project operator support
        Function(reinterpret_cast<void*>(Pmod), "pmod", {}, {OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_INT)
    };
    return mathFnRegistry;
}