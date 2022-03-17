/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Math Functions Registry
 */
#include "func_registry_math.h"
#include "functions/mathfunctions.h"
using namespace omniruntime;
using namespace omniruntime::type;

std::vector<Function> MathFunctionRegistry::GetFunctions()
{
    std::string absFnStr = "abs";
    std::string castFnStr = "CAST";
    std::string roundFnStr = "round";
    std::vector<Function> mathFnRegistry = { // insert native functions for each absolute math function
        Function(reinterpret_cast<void *>(Abs<int32_t>), absFnStr, {}, {OMNI_INT }, OMNI_INT),
        Function(reinterpret_cast<void *>(Abs<int64_t>), absFnStr, {}, {OMNI_LONG }, OMNI_LONG),
        Function(reinterpret_cast<void *>(Abs<double>), absFnStr, {}, {OMNI_DOUBLE }, OMNI_DOUBLE),

        // insert native functions for each cast math function
        Function(reinterpret_cast<void *>(CastInt32ToDouble), castFnStr, {}, {OMNI_INT },
                 OMNI_DOUBLE),
        Function(reinterpret_cast<void *>(CastInt64ToDouble), castFnStr, {}, {OMNI_LONG },
                 OMNI_DOUBLE),
        Function(reinterpret_cast<void *>(CastInt32ToInt64), castFnStr, {}, {OMNI_INT }, OMNI_LONG),
        Function(reinterpret_cast<void *>(CastInt64ToInt32), castFnStr, {}, {OMNI_LONG }, OMNI_INT),
        Function(reinterpret_cast<void *>(CastDoubleToInt64), castFnStr, {}, {OMNI_DOUBLE },
                 OMNI_LONG),
        Function(reinterpret_cast<void *>(CastDoubleToInt32), castFnStr, {}, {OMNI_DOUBLE },
                 OMNI_INT),

        // insert native function for combine hash math function
        Function(reinterpret_cast<void *>(CombineHash), "combine_hash", {}, {OMNI_LONG, OMNI_LONG },
                 OMNI_LONG),

        // insert pmod function for project operator support
        Function(reinterpret_cast<void *>(Pmod), "pmod", {}, {OMNI_INT, OMNI_INT },
                 OMNI_INT),

        // insert native functions for each round math function
        Function(reinterpret_cast<void *>(Round<int32_t>), roundFnStr, {}, {OMNI_INT, OMNI_INT },
                 OMNI_INT),
        Function(reinterpret_cast<void *>(Round<int64_t>), roundFnStr, {}, {OMNI_LONG, OMNI_INT },
                 OMNI_LONG),
        Function(reinterpret_cast<void *>(Round<double>), roundFnStr, {}, {OMNI_DOUBLE, OMNI_INT },
                 OMNI_DOUBLE)
    };
    return mathFnRegistry;
}