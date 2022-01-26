/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: String Function Registry
 */
#include "func_registry_string.h"
#include "functions/stringfunctions.h"
using namespace omniruntime;
using namespace omniruntime::expressions;

std::vector<Function> GetStringFunctionRegistry()
{
    std::string substrStr = "substr_";
    std::string substrWithStartStr = substrStr + "start_";
    static std::vector<Function> stringFnRegistry = {
        // substr functions
        Function(reinterpret_cast<void*>(SubstrExt), substrStr + DataTypeString(INT32D),
                 {}, {VARCHARD, INT32D, INT32D, INT32PTRD}, VARCHARD, false, true),
        Function(reinterpret_cast<void*>(SubstrExt64), substrStr + DataTypeString(INT64D),
                 {}, {VARCHARD, INT64D, INT64D, INT32PTRD}, VARCHARD, false, true),

        // substr with start index functions
        Function(reinterpret_cast<void*>(SubstrWithStartExt), substrWithStartStr + DataTypeString(INT32D),
                 {}, {VARCHARD, INT32D, INT32PTRD}, VARCHARD, false, true),
        Function(reinterpret_cast<void*>(SubstrWithStartExt64), substrWithStartStr + DataTypeString(INT64D),
                 {}, {VARCHARD, INT64D, INT32PTRD}, VARCHARD, false, true),

        // concat functions
        Function(reinterpret_cast<void*>(ConcatStrExt), "concat_string",
                 {}, {VARCHARD, VARCHARD, INT32PTRD}, VARCHARD, false, true),
        Function(reinterpret_cast<void*>(ConcatCharExt), "concat_char",
                 {}, {CHARD, VARCHARD, INT32PTRD}, VARCHARD, false, true),

        Function(reinterpret_cast<void*>(LikeExt), "LIKE",
                 {}, {VARCHARD, VARCHARD}, BOOLD, false),
        Function(reinterpret_cast<void*>(CastString), "CAST",
                 {}, {VARCHARD}, INT32D)
    };
    return stringFnRegistry;
}

std::vector<Function> GetStringCmpFn()
{
    static std::vector<Function> stringFnRegistry = {
        Function(reinterpret_cast<void*>(StrCompareExt), "StrCompareExt", {},
            {VARCHARD, VARCHARD}, INT32D, false)
    };
    return stringFnRegistry;
}
