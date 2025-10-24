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

extern void RegisterDatetimeFunctions(const std::string &prefix);

extern void RegisterJsonFunctions(const std::string &prefix);

extern void RegisterMapFunctions(const std::string &prefix);

extern void RegisterMathFunctions(const std::string &prefix);

extern void RegisterMiscFunctions(const std::string &prefix);

extern void RegisterRegexpFunctions(const std::string &prefix);

extern void RegisterSpecialFormGeneralFunctions(const std::string &prefix);

extern void RegisterStringFunctions(const std::string &prefix);

extern void RegisterUrlFunctions(const std::string &prefix);

int RegisterFunctions::Register()
{
    if (!VectorFunction::functionMap_) {
        VectorFunction::functionMap_ = std::make_unique<std::unordered_map<FunctionSignaturePtr, std::shared_ptr<
            VectorFunction>, Hash, Equals>>();
    }
    if (!VectorFunction::functionFactoryMap_) {
        VectorFunction::functionFactoryMap_ = std::make_unique<std::unordered_map<FunctionSignaturePtr,
            VectorFunctionFactory, Hash, Equals>>();
    }
    if (!SimpleFunctionRegistry::functionMap_) {
        SimpleFunctionRegistry::functionMap_ = std::make_unique<std::unordered_map<FunctionSignaturePtr, std::shared_ptr
            <VectorFunction>, Hash, Equals>>();
    }
    RegisterAllFunctions();
    return 1;
}

void RegisterFunctions::RegisterAllFunctions(const std::string &prefix)
{
    RegisterArrayFunctions(prefix);
    RegisterCompareFunctions(prefix);
    RegisterMapFunctions(prefix);
    RegisterMathFunctions(prefix);
    RegisterStringFunctions(prefix);
}

void link_register_functions() {}
}
