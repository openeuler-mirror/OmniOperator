/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: String Function Registry
 */
#include "func_registry_string.h"

using namespace omniruntime;
using namespace omniruntime::type;

std::vector<Function> StringFunctionRegistry::GetFunctions()
{
    std::vector<Function> stringFnRegistry = { // substr functions
        Function("Substr_int32", "substr", {}, { OMNI_VARCHAR, OMNI_INT, OMNI_INT }, OMNI_VARCHAR, true),
        Function("Substr_char_int32", "substr", {}, { OMNI_CHAR, OMNI_INT, OMNI_INT }, OMNI_CHAR, true),
        Function("Substr_int64", "substr", {}, { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG }, OMNI_VARCHAR, true),
        Function("Substr_char_int64", "substr", {}, { OMNI_CHAR, OMNI_LONG, OMNI_LONG }, OMNI_CHAR, true),

        // substr with start index functions
        Function("SubstrWithStart_int32", "substr", {}, { OMNI_VARCHAR, OMNI_INT }, OMNI_VARCHAR, true),
        Function("SubstrWithStart_char_int32", "substr", {}, { OMNI_CHAR, OMNI_INT }, OMNI_CHAR, true),
        Function("SubstrWithStart_int64", "substr", {}, { OMNI_VARCHAR, OMNI_LONG }, OMNI_VARCHAR, true),
        Function("SubstrWithStart_char_int64", "substr", {}, { OMNI_CHAR, OMNI_LONG }, OMNI_CHAR, true),

        // concat functions
        Function("ConcatStrStr", "concat", {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, true),
        Function("ConcatCharChar", "concat", {}, { OMNI_CHAR, OMNI_CHAR }, OMNI_CHAR, true),
        Function("ConcatCharStr", "concat", {}, { OMNI_CHAR, OMNI_VARCHAR }, OMNI_CHAR, true),
        Function("ConcatStrChar", "concat", {}, { OMNI_VARCHAR, OMNI_CHAR }, OMNI_CHAR, true),

        Function("Like", "LIKE", {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN),
        Function("CastString", "CAST", {}, { OMNI_VARCHAR }, OMNI_INT),

        Function("ToUpper", "upper", {}, { OMNI_VARCHAR }, OMNI_VARCHAR, true),
        Function("ToUpperChar", "upper", {}, { OMNI_CHAR }, OMNI_CHAR, true),

        Function("StrCompare", "compare", {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_INT)
    };
    return stringFnRegistry;
}
