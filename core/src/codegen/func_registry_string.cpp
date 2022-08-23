/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: String Function Registry
 */
#include "func_registry_string.h"
#include "functions/stringfunctions.h"
#include "../type/data_type.h"
using namespace omniruntime;
using namespace omniruntime::type;

std::vector<Function> StringFunctionRegistry::GetFunctions()
{
    std::string substrStr = "substr";
    std::vector<Function> stringFnRegistry = { // substr functions
        Function(reinterpret_cast<void *>(Substr<int32_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_INT, OMNI_INT },
                 OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrChar<int32_t>), substrStr, {}, { OMNI_CHAR, OMNI_INT, OMNI_INT },
                 OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(Substr<int64_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG },
                 OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrChar<int64_t>), substrStr, {}, { OMNI_CHAR, OMNI_LONG, OMNI_LONG },
                 OMNI_CHAR, INPUT_DATA, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(SubstrWithStart<int32_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_INT },
                 OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int32_t>), substrStr, {}, { OMNI_CHAR, OMNI_INT },
                 OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrWithStart<int64_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_LONG },
                 OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int64_t>), substrStr, {}, { OMNI_CHAR, OMNI_LONG },
                 OMNI_CHAR, INPUT_DATA, true),

        // concat functions
        Function(reinterpret_cast<void *>(ConcatStrStr), "concat", {},
                 { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ConcatCharChar), "concat", {},
                 { OMNI_CHAR, OMNI_CHAR }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ConcatCharStr), "concat", {},
                 { OMNI_CHAR, OMNI_VARCHAR }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ConcatStrChar), "concat", {},
                 { OMNI_VARCHAR, OMNI_CHAR }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ConcatStrStrRetNull), "concat", {},
                 { OMNI_VARCHAR, OMNI_CHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(ConcatCharCharRetNull), "concat", {},
                 { OMNI_CHAR, OMNI_CHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(ConcatCharStrRetNull), "concat", {},
                 { OMNI_CHAR, OMNI_VARCHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(ConcatStrCharRetNull), "concat", {},
                 { OMNI_VARCHAR, OMNI_CHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),

        Function(reinterpret_cast<void *>(LikeStr), "LIKE", {},
                 { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(LikeChar), "LIKE", {},
              { OMNI_CHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, INPUT_DATA),

        Function(reinterpret_cast<void *>(ToUpper), "upper", {},
                 { OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ToUpperChar), "upper", {},
                 { OMNI_CHAR }, OMNI_CHAR, INPUT_DATA, true),

        Function(reinterpret_cast<void *>(StrCompare), "compare", {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_INT),

        Function(reinterpret_cast<void *>(CastIntToString), "CAST", {},
                 { OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastIntToStringRetNull), "CAST", {},
                 { OMNI_INT }, OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(CastLongToString), "CAST", {},
                 { OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastLongToStringRetNull), "CAST", {},
                 { OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(CastDoubleToString), "CAST", {},
                 { OMNI_DOUBLE }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDoubleToStringRetNull), "CAST", {},
                 { OMNI_DOUBLE }, OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(CastDecimal64ToString), "CAST", {},
                 { OMNI_DECIMAL64 }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDecimal64ToStringRetNull), "CAST", {},
                 { OMNI_DECIMAL64 }, OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(CastDecimal128ToString), "CAST", {},
                 { OMNI_DECIMAL128 }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDecimal128ToStringRetNull), "CAST", {},
                 { OMNI_DECIMAL128 }, OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),

        Function(reinterpret_cast<void *>(CastStringToInt), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_INT, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastStringToLong), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_LONG, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastStringToDouble), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_DOUBLE, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastStringToDecimal64), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_DECIMAL64, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastStringToDecimal128), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastStringToDate), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_DATE32, INPUT_DATA, true),

        Function(reinterpret_cast<void *>(CastStringToIntRetNull), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_INT, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastStringToLongRetNull), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_LONG, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastStringToDoubleRetNull), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_DOUBLE, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastStringToDecimal64RetNull), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastStringToDecimal128RetNull), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastStringToDateRetNull), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_DATE32, INPUT_DATA_AND_OVERFLOW_NULL)

    };
    return stringFnRegistry;
}
