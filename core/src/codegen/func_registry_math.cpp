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
    std::string addFnStr = "add";
    std::string subtractFnStr = "subtract";
    std::string multiplyFnStr = "multiply";
    std::string divideFnStr = "divide";
    std::string modulusFnStr = "modulus";
    std::string lessThanFnStr = "lessThan";
    std::string lessThanEqualFnStr = "lessThanEqual";
    std::string greaterThanFnStr = "greaterThan";
    std::string greaterThanEqualFnStr = "greaterThanEqual";
    std::string equalFnStr = "equal";
    std::string notEqualFnStr = "notEqual";

    const std::vector<omniruntime::type::DataTypeId> doubleParams = { OMNI_DOUBLE, OMNI_DOUBLE };
    const std::vector<omniruntime::type::DataTypeId> longParams = { OMNI_LONG, OMNI_LONG };
    const std::vector<omniruntime::type::DataTypeId> intParams = { OMNI_INT, OMNI_INT };

    std::vector<Function> mathFnRegistry = { // insert native functions for each absolute math function
        Function(reinterpret_cast<void *>(Abs<int32_t>), absFnStr, {},
                 { OMNI_INT }, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(Abs<int64_t>), absFnStr, {},
                 { OMNI_LONG }, OMNI_LONG, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(Abs<double>), absFnStr, {},
                 { OMNI_DOUBLE }, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG),

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

        // insert native function for each double operations
        Function(reinterpret_cast<void *>(AddDouble), addFnStr, {},
                 doubleParams, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(SubtractDouble), subtractFnStr, {},
                 doubleParams, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(MultiplyDouble), multiplyFnStr, {},
                 doubleParams, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(DivideDouble), divideFnStr, {},
                 doubleParams, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(ModulusDouble), modulusFnStr, {},
                 doubleParams, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(LessThanDouble), lessThanFnStr, {},
                 doubleParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(LessThanEqualDouble), lessThanEqualFnStr, {},
                 doubleParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(GreaterThanDouble), greaterThanFnStr, {},
                 doubleParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(GreaterThanEqualDouble), greaterThanEqualFnStr, {},
                 doubleParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(EqualDouble), equalFnStr, {},
                 doubleParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(NotEqualDouble), notEqualFnStr, {},
                 doubleParams, OMNI_BOOLEAN, NOT_NULL_RESULT),

        // insert native function for each long operations
        Function(reinterpret_cast<void *>(AddInt64), addFnStr, {},
                 longParams, OMNI_LONG, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(SubtractInt64), subtractFnStr, {},
                 longParams, OMNI_LONG, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(MultiplyInt64), multiplyFnStr, {},
                 longParams, OMNI_LONG, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(DivideInt64), divideFnStr, {},
                 longParams, OMNI_LONG, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(ModulusInt64), modulusFnStr, {},
                 longParams, OMNI_LONG, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(LessThanInt64), lessThanFnStr, {},
                 longParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(LessThanEqualInt64), lessThanEqualFnStr, {},
                 longParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(GreaterThanInt64), greaterThanFnStr, {},
                 longParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(GreaterThanEqualInt64), greaterThanEqualFnStr, {},
                 longParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(EqualInt64), equalFnStr, {},
                 longParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(NotEqualInt64), notEqualFnStr, {},
                 longParams, OMNI_BOOLEAN, NOT_NULL_RESULT),

        // insert native function for each int operations
        Function(reinterpret_cast<void *>(AddInt32), addFnStr, {},
                 intParams, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(SubtractInt32), subtractFnStr, {},
                 intParams, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(MultiplyInt32), multiplyFnStr, {},
                 intParams, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(DivideInt32), divideFnStr, {},
                 intParams, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(ModulusInt32), modulusFnStr, {},
                 intParams, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(LessThanInt32), lessThanFnStr, {},
                 intParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(LessThanEqualInt32), lessThanEqualFnStr, {},
                 intParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(GreaterThanInt32), greaterThanFnStr, {},
                 intParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(GreaterThanEqualInt32), greaterThanEqualFnStr, {},
                 intParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(EqualInt32), equalFnStr, {},
                 intParams, OMNI_BOOLEAN, NOT_NULL_RESULT),
        Function(reinterpret_cast<void *>(NotEqualInt32), notEqualFnStr, {},
                 intParams, OMNI_BOOLEAN, NOT_NULL_RESULT),

        // insert pmod function for project operator support
        Function(reinterpret_cast<void *>(Pmod), "pmod", {},
                 intParams, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG),
        // insert native functions for each round math function
        Function(reinterpret_cast<void *>(Round<int32_t>), roundFnStr, {},
                 intParams, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(Round<int64_t>), roundFnStr, {},
                 { OMNI_LONG, OMNI_INT }, OMNI_LONG, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(Round<double>), roundFnStr, {},
                 { OMNI_DOUBLE, OMNI_INT }, OMNI_DOUBLE, NULL_RESULT_IF_ANY_NULL_ARG)
    };
    return mathFnRegistry;
}