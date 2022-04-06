/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Math Functions Registry
 */
#include "func_registry_math.h"

using namespace omniruntime;
using namespace omniruntime::type;

namespace omniruntime {
std::vector<Function> MathFunctionRegistry::GetFunctions()
{
    std::vector<Function> mathFnRegistry = { // insert native functions for each absolute math function
        Function("Abs_int32", "abs", {}, { OMNI_INT }, OMNI_INT),
        Function("Abs_int64", "abs", {}, { OMNI_LONG }, OMNI_LONG),
        Function("Abs_double", "abs", {}, { OMNI_DOUBLE }, OMNI_DOUBLE),

        // insert native functions for each cast math function
        Function("Cast_int32_to_double", "CAST", {}, { OMNI_INT }, OMNI_DOUBLE),
        Function("Cast_int64_to_double", "CAST", {}, { OMNI_LONG }, OMNI_DOUBLE),
        Function("Cast_int32_to_int64", "CAST", {}, { OMNI_INT }, OMNI_LONG),
        Function("Cast_int64_to_int32", "CAST", {}, { OMNI_LONG }, OMNI_INT),
        Function("Cast_double_to_int64", "CAST", {}, { OMNI_DOUBLE }, OMNI_LONG),
        Function("Cast_double_to_int32", "CAST", {}, { OMNI_DOUBLE }, OMNI_INT),

        // insert native function for combine hash math function
        Function("CombineHash", "combine_hash", {}, { OMNI_LONG, OMNI_LONG }, OMNI_LONG),

        // insert pmod function for project operator support
        Function("Pmod", "pmod", {}, { OMNI_INT, OMNI_INT }, OMNI_INT),

        // insert native functions for each round math function
        Function("Round_int32", "round", {}, { OMNI_INT, OMNI_INT }, OMNI_INT),
        Function("Round_int64", "round", {}, { OMNI_LONG, OMNI_INT }, OMNI_LONG),
        Function("Round_double", "round", {}, { OMNI_DOUBLE, OMNI_INT }, OMNI_DOUBLE)
    };
    return mathFnRegistry;
}
}
