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
    std::string divideFnStr = "divide";
    std::string modulusFnStr = "modulus";
    std::vector<Function> mathFnRegistry = { // insert native functions for each absolute math function
        Function(reinterpret_cast<void *>(Abs<int32_t>), absFnStr, {},
                 { OMNI_INT }, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(Abs<int64_t>), absFnStr, {},
                 { OMNI_LONG }, OMNI_LONG, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(Abs<double>), absFnStr, {},
                 { OMNI_DOUBLE }, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(Abs<int64_t>), absFnStr, {},
                 { OMNI_DECIMAL64 }, OMNI_DECIMAL64, NULL_RESULT_IF_ANY_NULL_ARG),

        // insert native functions for each cast math function
        Function(reinterpret_cast<void *>(CastInt32ToDouble), castFnStr, {},
                 { OMNI_INT }, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(CastInt64ToDouble), castFnStr, {},
                 { OMNI_LONG }, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(CastInt32ToInt64), castFnStr, {},
                 { OMNI_INT }, OMNI_LONG, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(CastInt64ToInt32), castFnStr, {},
                 { OMNI_LONG }, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(CastDoubleToInt64), castFnStr, {},
                 { OMNI_DOUBLE }, OMNI_LONG, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(CastDoubleToInt32), castFnStr, {},
                 { OMNI_DOUBLE }, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG),

        // insert native function for each divide operations
        Function(reinterpret_cast<void *>(DivideDouble), divideFnStr, {},
                 { OMNI_DOUBLE, OMNI_DOUBLE }, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG),

        Function(reinterpret_cast<void *>(ModulusDouble), modulusFnStr, {},
                 { OMNI_DOUBLE, OMNI_DOUBLE }, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG),

        // insert pmod function for project operator support
        Function(reinterpret_cast<void *>(Pmod), "pmod", {},
                 { OMNI_INT, OMNI_INT }, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG),

        // insert native functions for each round math function
        Function(reinterpret_cast<void *>(Round<int32_t>), roundFnStr, {},
                 { OMNI_INT, OMNI_INT }, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(Round<int64_t>), roundFnStr, {},
                 { OMNI_LONG, OMNI_INT }, OMNI_LONG, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(Round<double>), roundFnStr, {},
                 { OMNI_DOUBLE, OMNI_INT }, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG)
    };
    return mathFnRegistry;
}