/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Math Functions Registry
 */
#include "func_registry_math.h"
#include "functions/mathfunctions.h"
namespace omniruntime::codegen {
using namespace omniruntime::type;
using namespace omniruntime::codegen::function;

const std::string AbsFnStr()
{
    const std::string absFnStr = "abs";
    return absFnStr;
}

const std::string RoundFnStr()
{
    const std::string roundFnStr = "round";
    return roundFnStr;
}

const std::string AddFnStr()
{
    const std::string addFnStr = "add";
    return addFnStr;
}

const std::string SubtractFnStr()
{
    const std::string subtractFnStr = "subtract";
    return subtractFnStr;
}

const std::string MultiplyFnStr()
{
    const std::string multiplyFnStr = "multiply";
    return multiplyFnStr;
}

const std::string DivideFnStr()
{
    const std::string divideFnStr = "divide";
    return divideFnStr;
}

const std::string ModulusFnStr()
{
    const std::string modulusFnStr = "modulus";
    return modulusFnStr;
}

const std::string LessThanFnStr()
{
    const std::string lessThanFnStr = "lessThan";
    return lessThanFnStr;
}

const std::string LessThanEqualFnStr()
{
    const std::string lessThanEqualFnStr = "lessThanEqual";
    return lessThanEqualFnStr;
}

const std::string GreaterThanFnStr()
{
    const std::string greaterThanFnStr = "greaterThan";
    return greaterThanFnStr;
}

const std::string GreaterThanEqualFnStr()
{
    const std::string greaterThanEqualFnStr = "greaterThanEqual";
    return greaterThanEqualFnStr;
}

const std::string EqualFnStr()
{
    const std::string equalFnStr = "equal";
    return equalFnStr;
}

const std::string NotEqualFnStr()
{
    const std::string notEqualFnStr = "notEqual";
    return notEqualFnStr;
}

const std::string MathCastFnStr()
{
    const std::string mathCastFnStr = "CAST";
    return mathCastFnStr;
}

const std::string PmodFnStr()
{
    const std::string pmodFnStr = "pmod";
    return pmodFnStr;
}

std::vector<Function> MathFunctionRegistry::GetFunctions()
{
    const std::vector<omniruntime::type::DataTypeId> doubleParams = { OMNI_DOUBLE, OMNI_DOUBLE };
    const std::vector<omniruntime::type::DataTypeId> longParams = { OMNI_LONG, OMNI_LONG };
    const std::vector<omniruntime::type::DataTypeId> intParams = { OMNI_INT, OMNI_INT };

    std::vector<Function> mathFnRegistry = {
        // insert native functions for each absolute math function
        Function(reinterpret_cast<void *>(Abs<int32_t>), AbsFnStr(), {}, { OMNI_INT }, OMNI_INT, INPUT_DATA),
        Function(reinterpret_cast<void *>(Abs<int64_t>), AbsFnStr(), {}, { OMNI_LONG }, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(Abs<double>), AbsFnStr(), {}, { OMNI_DOUBLE }, OMNI_DOUBLE, INPUT_DATA),

        // insert native functions for each cast math function
        Function(reinterpret_cast<void *>(CastInt32ToDouble), MathCastFnStr(), {}, { OMNI_INT }, OMNI_DOUBLE,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(CastInt64ToDouble), MathCastFnStr(), {}, { OMNI_LONG }, OMNI_DOUBLE,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(CastInt32ToInt64), MathCastFnStr(), {}, { OMNI_INT }, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(CastInt64ToInt32), MathCastFnStr(), {}, { OMNI_LONG }, OMNI_INT, INPUT_DATA),

        // insert native function for each double operations
        Function(reinterpret_cast<void *>(AddDouble), AddFnStr(), {}, doubleParams, OMNI_DOUBLE, INPUT_DATA),
        Function(reinterpret_cast<void *>(SubtractDouble), SubtractFnStr(), {}, doubleParams, OMNI_DOUBLE, INPUT_DATA),
        Function(reinterpret_cast<void *>(MultiplyDouble), MultiplyFnStr(), {}, doubleParams, OMNI_DOUBLE, INPUT_DATA),
        Function(reinterpret_cast<void *>(DivideDouble), DivideFnStr(), {}, doubleParams, OMNI_DOUBLE, INPUT_DATA),
        Function(reinterpret_cast<void *>(ModulusDouble), ModulusFnStr(), {}, doubleParams, OMNI_DOUBLE, INPUT_DATA),
        Function(reinterpret_cast<void *>(LessThanDouble), LessThanFnStr(), {}, doubleParams, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(LessThanEqualDouble), LessThanEqualFnStr(), {}, doubleParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(GreaterThanDouble), GreaterThanFnStr(), {}, doubleParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(GreaterThanEqualDouble), GreaterThanEqualFnStr(), {}, doubleParams,
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(EqualDouble), EqualFnStr(), {}, doubleParams, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(NotEqualDouble), NotEqualFnStr(), {}, doubleParams, OMNI_BOOLEAN, INPUT_DATA),

        // insert native function for each long operations
        Function(reinterpret_cast<void *>(AddInt64), AddFnStr(), {}, longParams, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(SubtractInt64), SubtractFnStr(), {}, longParams, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(MultiplyInt64), MultiplyFnStr(), {}, longParams, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(DivideInt64), DivideFnStr(), {}, longParams, OMNI_LONG, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ModulusInt64), ModulusFnStr(), {}, longParams, OMNI_LONG, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(LessThanInt64), LessThanFnStr(), {}, longParams, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(LessThanEqualInt64), LessThanEqualFnStr(), {}, longParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(GreaterThanInt64), GreaterThanFnStr(), {}, longParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(GreaterThanEqualInt64), GreaterThanEqualFnStr(), {}, longParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(EqualInt64), EqualFnStr(), {}, longParams, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(NotEqualInt64), NotEqualFnStr(), {}, longParams, OMNI_BOOLEAN, INPUT_DATA),

        // insert native function for each int operations
        Function(reinterpret_cast<void *>(AddInt32), AddFnStr(), {}, intParams, OMNI_INT, INPUT_DATA),
        Function(reinterpret_cast<void *>(SubtractInt32), SubtractFnStr(), {}, intParams, OMNI_INT, INPUT_DATA),
        Function(reinterpret_cast<void *>(MultiplyInt32), MultiplyFnStr(), {}, intParams, OMNI_INT, INPUT_DATA),
        Function(reinterpret_cast<void *>(DivideInt32), DivideFnStr(), {}, intParams, OMNI_INT, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ModulusInt32), ModulusFnStr(), {}, intParams, OMNI_INT, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(LessThanInt32), LessThanFnStr(), {}, intParams, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(LessThanEqualInt32), LessThanEqualFnStr(), {}, intParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(GreaterThanInt32), GreaterThanFnStr(), {}, intParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(GreaterThanEqualInt32), GreaterThanEqualFnStr(), {}, intParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(EqualInt32), EqualFnStr(), {}, intParams, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(NotEqualInt32), NotEqualFnStr(), {}, intParams, OMNI_BOOLEAN, INPUT_DATA),

        // insert pmod function for project operator support
        Function(reinterpret_cast<void *>(Pmod), PmodFnStr(), {}, intParams, OMNI_INT, INPUT_DATA),
        // insert native functions for each round math function
        Function(reinterpret_cast<void *>(Round<int32_t>), RoundFnStr(), {}, intParams, OMNI_INT, INPUT_DATA),
        Function(reinterpret_cast<void *>(Round<int64_t>), RoundFnStr(), {}, { OMNI_LONG, OMNI_INT }, OMNI_LONG,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(Round<double>), RoundFnStr(), {}, { OMNI_DOUBLE, OMNI_INT }, OMNI_DOUBLE,
            INPUT_DATA),
    };

    return mathFnRegistry;
}

std::vector<Function> MathFunctionRegistryHalfUp::GetFunctions()
{
    std::vector<Function> mathFnRegistry = {
        // insert native functions for each absolute math function
        Function(reinterpret_cast<void *>(CastDoubleToInt64HalfUp), MathCastFnStr(), {}, { OMNI_DOUBLE }, OMNI_LONG,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(CastDoubleToInt32HalfUp), MathCastFnStr(), {}, { OMNI_DOUBLE }, OMNI_INT,
            INPUT_DATA),
    };

    return mathFnRegistry;
}

std::vector<Function> MathFunctionRegistryDown::GetFunctions()
{
    std::vector<Function> mathFnRegistry = {
        // insert native functions for each absolute math function
        Function(reinterpret_cast<void *>(CastDoubleToInt64Down), MathCastFnStr(), {}, { OMNI_DOUBLE }, OMNI_LONG,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(CastDoubleToInt32Down), MathCastFnStr(), {}, { OMNI_DOUBLE }, OMNI_INT,
            INPUT_DATA),
    };

    return mathFnRegistry;
}
}