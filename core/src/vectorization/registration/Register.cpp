/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "Register.h"
#include "SimpleFunctionRegistry.h"

namespace omniruntime::vectorization {
extern void RegisterArrayFunctions(const std::string &prefix);

extern void RegisterBinaryFunctions(const std::string &prefix);

extern void RegisterBitwiseFunctions(const std::string &prefix);

extern void RegisterCompareFunctions(const std::string &prefix);

extern void RegisterConditionalFunctions(const std::string &prefix);

extern void RegisterConversionFunctions(const std::string &prefix);

extern void RegisterDatetimeFunctions(const std::string &prefix);

extern void RegisterJsonFunctions(const std::string &prefix);

extern void RegisterMapFunctions(const std::string &prefix);

extern void RegisterMathFunctions(const std::string &prefix);

extern void RegisterMiscFunctions(const std::string &prefix);

extern void RegisterRegexpFunctions(const std::string &prefix);

extern void RegisterSpecialFormGeneralFunctions(const std::string &prefix);

extern void RegisterStringFunctions(const std::string &prefix);

extern void RegisterUrlFunctions(const std::string &prefix);

extern void RegisterLambdaFunctions(const std::string &prefix);

int RegisterFunctions::Register()
{
    RegisterAllFunctions();
    return 1;
}

void RegisterFunctions::RegisterAllFunctions(const std::string &prefix)
{
    RegisterArrayFunctions(prefix);
    RegisterCompareFunctions(prefix);
    RegisterConditionalFunctions(prefix);
    RegisterConversionFunctions(prefix);
    RegisterMapFunctions(prefix);
    RegisterMathFunctions(prefix);
    RegisterStringFunctions(prefix);
    RegisterBitwiseFunctions(prefix);
    RegisterLambdaFunctions(prefix);
    RegisterRegexpFunctions(prefix);
}

void link_register_functions() {}
}
