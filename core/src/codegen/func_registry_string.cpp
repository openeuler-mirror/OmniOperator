/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: String Function Registry
 */
#include "func_registry_string.h"
#include "functions/stringfunctions.h"

namespace omniruntime::codegen {
using namespace omniruntime::type;
using namespace codegen::function;

const std::string CountCharFnStr()
{
    const std::string countCharFnStr = "CountChar";
    return countCharFnStr;
}

const std::string SplitIndexFnStr()
{
    const std::string splitIndexFnStr = "SplitIndex";
    return splitIndexFnStr;
}

const std::string RegexpExtractFnStr()
{
    const std::string regexpExtractFnStr = "regex_extract_null";
    return regexpExtractFnStr;
}

const std::string StrEqualFnStr()
{
    const std::string compareFnStr = "strequal";
    return compareFnStr;
}

const std::string ConcatFnStr()
{
    const std::string concatFnStr = "concat";
    return concatFnStr;
}

const std::string ConcatWsFnStr()
{
    const std::string concatWsFnStr = "concat_ws";
    return concatWsFnStr;
}

const std::string LikeFnStr()
{
    const std::string likeFnStr = "LIKE";
    return likeFnStr;
}

const std::string CastFnStr()
{
    const std::string castFnStr = "CAST";
    return castFnStr;
}

const std::string LowerFnStr()
{
    const std::string lowerFnStr = "lower";
    return lowerFnStr;
}

const std::string UpperFnStr()
{
    const std::string upperFnStr = "upper";
    return upperFnStr;
}

const std::string CompareFnStr()
{
    const std::string compareFnStr = "compare";
    return compareFnStr;
}

const std::string LengthFnStr()
{
    const std::string lengthFnStr = "length";
    return lengthFnStr;
}

const std::string CastNullFnStr()
{
    const std::string castNullFnStr = "CAST_null";
    return castNullFnStr;
}

const std::string ConcatNullFnStr()
{
    const std::string concatNullFnStr = "concat_null";
    return concatNullFnStr;
}

const std::string ReplaceFnStr()
{
    const std::string replaceFnStr = "replace";
    return replaceFnStr;
}

const std::string EmptyToNullStr()
{
    const std::string empty2nullFnStr = "empty2null";
    return empty2nullFnStr;
}

const std::string SubstrFnStr()
{
    const std::string substrFnStr = "substr";
    return substrFnStr;
}

const std::string InStrFnStr()
{
    const std::string instrFnStr = "instr";
    return instrFnStr;
}

const std::string StartsWithFnStr()
{
    const std::string startsWithFnStr = "StartsWith";
    return startsWithFnStr;
}

const std::string EndsWithFnStr()
{
    const std::string endsWithFnStr = "EndsWith";
    return endsWithFnStr;
}

const std::string RLikeFnStr()
{
    const std::string rLikeFnStr = "RLike";
    return rLikeFnStr;
}

const std::string Md5FnStr()
{
    const std::string md5FnStr = "Md5";
    return md5FnStr;
}

const std::string ContainsFnStr()
{
    const std::string containsFnStr = "Contains";
    return containsFnStr;
}

const std::string GreatestStrFnStr()
{
    const std::string greatestStrFnStr = "Greatest";
    return greatestStrFnStr;
}

const std::string StaticInvokeVarcharTypeWriteSideCheckFnStr()
{
    const std::string staticInvokeVarcharTypeWriteSideCheckFnStr = "StaticInvokeVarcharTypeWriteSideCheck";
    return staticInvokeVarcharTypeWriteSideCheckFnStr;
}

const std::string StaticInvokeCharReadPaddingFnStr()
{
    const std::string staticInvokeCharReadPaddingFnStr = "StaticInvokeCharReadPadding";
    return staticInvokeCharReadPaddingFnStr;
}

std::vector<Function> StringFunctionRegistry::GetFunctions()
{
    std::vector<Function> stringFnRegistry = { // concat functions
        Function(reinterpret_cast<void *>(SplitIndexRetNull), SplitIndexFnStr(), {}, { OMNI_VARCHAR, OMNI_CHAR, OMNI_INT },
                 OMNI_VARCHAR, INPUT_DATA_AND_NULL_AND_RETURN_NULL),
        Function(reinterpret_cast<void *>(ConcatStrStr), ConcatFnStr(), {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ConcatCharChar), ConcatFnStr(), {}, { OMNI_CHAR, OMNI_CHAR }, OMNI_CHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ConcatCharStr), ConcatFnStr(), {}, { OMNI_CHAR, OMNI_VARCHAR }, OMNI_CHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ConcatStrChar), ConcatFnStr(), {}, { OMNI_VARCHAR, OMNI_CHAR }, OMNI_CHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ConcatWsStr), ConcatWsFnStr(), {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_VARCHAR, INPUT_DATA, true),

        Function(reinterpret_cast<void *>(LikeStr), LikeFnStr(), {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(LikeChar), LikeFnStr(), {}, { OMNI_CHAR, OMNI_VARCHAR }, OMNI_BOOLEAN,
            INPUT_DATA),

        Function(reinterpret_cast<void *>(ToUpperStr), UpperFnStr(), {}, { OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(ToUpperChar), UpperFnStr(), {}, { OMNI_CHAR }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ToLowerStr), LowerFnStr(), {}, { OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(ToLowerChar), LowerFnStr(), {}, { OMNI_CHAR }, OMNI_CHAR, INPUT_DATA, true),

        Function(reinterpret_cast<void *>(StrCompare), CompareFnStr(), {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_INT),

        Function(reinterpret_cast<void *>(StrEquals), StrEqualFnStr(), {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_BOOLEAN),

        Function(reinterpret_cast<void *>(CastIntToString), CastFnStr(), {}, { OMNI_INT }, OMNI_VARCHAR, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(CastLongToString), CastFnStr(), {}, { OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(CastDoubleToString), CastFnStr(), {}, { OMNI_DOUBLE }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDecimal64ToString), CastFnStr(), {}, { OMNI_DECIMAL64 }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDecimal128ToString), CastFnStr(), {}, { OMNI_DECIMAL128 }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDateToString), CastFnStr(), {}, { OMNI_DATE32 }, OMNI_VARCHAR, INPUT_DATA,
            true),

        Function(reinterpret_cast<void *>(CastStringToInt), CastFnStr(), {}, { OMNI_VARCHAR }, OMNI_INT, INPUT_DATA,
            true),

        Function(reinterpret_cast<void *>(CastStringToLong), CastFnStr(), {}, { OMNI_VARCHAR }, OMNI_LONG, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(CastStringToDouble), CastFnStr(), {}, { OMNI_VARCHAR }, OMNI_DOUBLE,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastStrWithDiffWidths), CastFnStr(), {}, { OMNI_VARCHAR }, OMNI_VARCHAR,
            INPUT_DATA, true),

        // length functions
        Function(reinterpret_cast<void *>(CountChar), CountCharFnStr(), {}, { OMNI_VARCHAR, OMNI_CHAR }, OMNI_LONG),
        Function(reinterpret_cast<void *>(LengthChar), LengthFnStr(), {}, { OMNI_CHAR }, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(LengthStr), LengthFnStr(), {}, { OMNI_VARCHAR }, OMNI_LONG, INPUT_DATA),

        // replace functions
        Function(reinterpret_cast<void *>(LengthCharReturnInt32), LengthFnStr(), {}, { OMNI_CHAR }, OMNI_INT,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(LengthStrReturnInt32), LengthFnStr(), {}, { OMNI_VARCHAR }, OMNI_INT,
            INPUT_DATA),

        Function(reinterpret_cast<void *>(ConcatStrStrRetNull), ConcatNullFnStr(), {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(RegexpExtractRetNull), RegexpExtractFnStr(), {}, { OMNI_VARCHAR, OMNI_CHAR, OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA_AND_NULL_AND_RETURN_NULL, true),
        Function(reinterpret_cast<void *>(ConcatCharCharRetNull), ConcatNullFnStr(), {}, { OMNI_CHAR, OMNI_CHAR },
            OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(ConcatCharStrRetNull), ConcatNullFnStr(), {}, { OMNI_CHAR, OMNI_VARCHAR },
            OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(ConcatStrCharRetNull), ConcatNullFnStr(), {}, { OMNI_VARCHAR, OMNI_CHAR },
            OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),

        Function(reinterpret_cast<void *>(CastIntToStringRetNull), CastNullFnStr(), {}, { OMNI_INT }, OMNI_VARCHAR,
            INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(CastLongToStringRetNull), CastNullFnStr(), {}, { OMNI_LONG }, OMNI_VARCHAR,
            INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(CastDoubleToStringRetNull), CastNullFnStr(), {}, { OMNI_DOUBLE },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(CastDecimal64ToStringRetNull), CastNullFnStr(), {}, { OMNI_DECIMAL64 },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(CastDecimal128ToStringRetNull), CastNullFnStr(), {}, { OMNI_DECIMAL128 },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(CastDateToStringRetNull), CastNullFnStr(), {}, { OMNI_DATE32 }, OMNI_VARCHAR,
            INPUT_DATA_AND_OVERFLOW_NULL, true),

        Function(reinterpret_cast<void *>(CastStringToIntRetNull), CastNullFnStr(), {}, { OMNI_VARCHAR }, OMNI_INT,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastStringToLongRetNull), CastNullFnStr(), {}, { OMNI_VARCHAR }, OMNI_LONG,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastStringToDoubleRetNull), CastNullFnStr(), {}, { OMNI_VARCHAR },
            OMNI_DOUBLE, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastStrWithDiffWidthsRetNull), CastNullFnStr(), {}, { OMNI_VARCHAR },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),

        Function(reinterpret_cast<void *>(InStr), InStrFnStr(), {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_INT,
            INPUT_DATA),

        // like functions
        Function(reinterpret_cast<void *>(StartsWithStr), StartsWithFnStr(), {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(EndsWithStr), EndsWithFnStr(), {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(RegexMatch), RLikeFnStr(), {}, {OMNI_VARCHAR, OMNI_VARCHAR},
            OMNI_BOOLEAN, INPUT_DATA),

        Function(reinterpret_cast<void *>(Md5Str), Md5FnStr(), {}, { OMNI_VARCHAR }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ContainsStr), ContainsFnStr(), {}, {OMNI_VARCHAR, OMNI_VARCHAR},
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(GreatestStr), GreatestStrFnStr(), {}, {OMNI_VARCHAR, OMNI_VARCHAR},
            OMNI_VARCHAR, INPUT_DATA_AND_NULL_AND_RETURN_NULL),
        Function(reinterpret_cast<void *>(EmptyToNull), EmptyToNullStr(), {}, { OMNI_VARCHAR }, OMNI_VARCHAR,
            INPUT_DATA, false),
        Function(reinterpret_cast<void *>(StaticInvokeVarcharTypeWriteSideCheck),
            StaticInvokeVarcharTypeWriteSideCheckFnStr(), {}, { OMNI_VARCHAR, OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(StaticInvokeCharReadPadding), StaticInvokeCharReadPaddingFnStr(), {},
            {OMNI_VARCHAR, OMNI_INT}, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstringIndex), "substring_index", {},
            {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_INT}, OMNI_VARCHAR, INPUT_DATA, true)
    };

    return stringFnRegistry;
}

std::vector<Function> StringFunctionRegistryNotAllowReducePrecison::GetFunctions()
{
    std::vector<Function> stringFnRegistry = {
        Function(reinterpret_cast<void *>(CastStringToDateNotAllowReducePrecison), CastFnStr(), {}, { OMNI_VARCHAR },
            OMNI_DATE32, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastStringToDateRetNullNotAllowReducePrecison), CastNullFnStr(), {},
            { OMNI_VARCHAR }, OMNI_DATE32, INPUT_DATA_AND_OVERFLOW_NULL),
    };

    return stringFnRegistry;
}

std::vector<Function> StringFunctionRegistryAllowReducePrecison::GetFunctions()
{
    std::vector<Function> stringFnRegistry = {
        Function(reinterpret_cast<void *>(CastStringToDateAllowReducePrecison), CastFnStr(), {}, { OMNI_VARCHAR },
            OMNI_DATE32, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastStringToDateRetNullAllowReducePrecison), CastNullFnStr(), {},
            { OMNI_VARCHAR }, OMNI_DATE32, INPUT_DATA_AND_OVERFLOW_NULL),
    };

    return stringFnRegistry;
}

std::vector<Function> StringFunctionRegistryNotReplace::GetFunctions()
{
    std::vector<Function> stringFnRegistry = {
        Function(reinterpret_cast<void *>(ReplaceStrStrStrWithRepNotReplace), ReplaceFnStr(), {},
            { OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ReplaceStrStrWithoutRepNotReplace), ReplaceFnStr(), {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
    };

    return stringFnRegistry;
}

std::vector<Function> StringFunctionRegistryReplace::GetFunctions()
{
    std::vector<Function> stringFnRegistry = {
        Function(reinterpret_cast<void *>(ReplaceStrStrStrWithRepReplace), ReplaceFnStr(), {},
            { OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ReplaceStrStrWithoutRepReplace), ReplaceFnStr(), {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
    };

    return stringFnRegistry;
}

std::vector<Function> StringFunctionRegistrySupportNegativeAndZeroIndex::GetFunctions()
{
    std::vector<Function> stringFnRegistry = {
        // substr functions
        Function(reinterpret_cast<void *>(SubstrVarchar<int32_t, true, true>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_INT, OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrChar<int32_t, true, true>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_INT, OMNI_INT }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrVarchar<int64_t, true, true>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrChar<int64_t, true, true>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_LONG, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(SubstrVarcharWithStart<int32_t, true, true>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int32_t, true, true>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_INT }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrVarcharWithStart<int64_t, true, true>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int64_t, true, true>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),
    };

    return stringFnRegistry;
}

std::vector<Function> StringFunctionRegistrySupportNotNegativeAndZeroIndex::GetFunctions()
{
    std::vector<Function> stringFnRegistry = {
        // substr functions
        Function(reinterpret_cast<void *>(SubstrVarchar<int32_t, false, true>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_INT, OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrChar<int32_t, false, true>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_INT, OMNI_INT }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrVarchar<int64_t, false, true>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrChar<int64_t, false, true>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_LONG, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(SubstrVarcharWithStart<int32_t, false, true>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int32_t, false, true>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_INT }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrVarcharWithStart<int64_t, false, true>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int64_t, false, true>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),
    };

    return stringFnRegistry;
}

std::vector<Function> StringFunctionRegistrySupportNegativeAndNotZeroIndex::GetFunctions()
{
    std::vector<Function> stringFnRegistry = {
        // substr functions
        Function(reinterpret_cast<void *>(SubstrVarchar<int32_t, true, false>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_INT, OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrChar<int32_t, true, false>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_INT, OMNI_INT }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrVarchar<int64_t, true, false>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrChar<int64_t, true, false>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_LONG, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(SubstrVarcharWithStart<int32_t, true, false>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int32_t, true, false>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_INT }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrVarcharWithStart<int64_t, true, false>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int64_t, true, false>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),
    };

    return stringFnRegistry;
}

std::vector<Function> StringFunctionRegistrySupportNotNegativeAndNotZeroIndex::GetFunctions()
{
    std::vector<Function> stringFnRegistry = {
        // substr functions
        Function(reinterpret_cast<void *>(SubstrVarchar<int32_t, false, false>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_INT, OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrChar<int32_t, false, false>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_INT, OMNI_INT }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrVarchar<int64_t, false, false>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrChar<int64_t, false, false>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_LONG, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(SubstrVarcharWithStart<int32_t, false, false>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int32_t, false, false>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_INT }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrVarcharWithStart<int64_t, false, false>), SubstrFnStr(), {},
            { OMNI_VARCHAR, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int64_t, false, false>), SubstrFnStr(), {},
            { OMNI_CHAR, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),
    };

    return stringFnRegistry;
}

std::vector<Function> StringToDecimalFunctionRegistryAllowRoundUp::GetFunctions()
{
    std::vector<Function> stringFnRegistry = {
        Function(reinterpret_cast<void *>(CastStringToDecimal64RoundUpRetNull), CastNullFnStr(), {}, {OMNI_VARCHAR},
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastStringToDecimal128RoundUpRetNull), CastNullFnStr(), {}, {OMNI_VARCHAR},
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastStringToDecimal64RoundUp), CastFnStr(), {}, {OMNI_VARCHAR},
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastStringToDecimal128RoundUp), CastFnStr(), {}, {OMNI_VARCHAR},
            OMNI_DECIMAL128,
            INPUT_DATA, true)
    };
    return stringFnRegistry;
}

std::vector<Function> StringToDecimalFunctionRegistry::GetFunctions()
{
    std::vector<Function> stringFnRegistry = {
        Function(reinterpret_cast<void *>(CastStringToDecimal64RetNull), CastNullFnStr(), {}, {OMNI_VARCHAR},
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastStringToDecimal128RetNull), CastNullFnStr(), {}, {OMNI_VARCHAR},
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastStringToDecimal64), CastFnStr(), {}, {OMNI_VARCHAR}, OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastStringToDecimal128), CastFnStr(), {}, {OMNI_VARCHAR}, OMNI_DECIMAL128,
            INPUT_DATA, true)
    };
    return stringFnRegistry;
}
}
