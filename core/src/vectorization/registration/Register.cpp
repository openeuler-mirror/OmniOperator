/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "Register.h"
#include "SimpleFunctionRegistry.h"

namespace omniruntime::vectorization {
extern void registerArrayFunctions(const std::string &prefix);

extern void registerBinaryFunctions(const std::string &prefix);

extern void registerBitwiseFunctions(const std::string &prefix);

extern void registerCompareFunctions(const std::string &prefix);

extern void registerDatetimeFunctions(const std::string &prefix);

extern void registerJsonFunctions(const std::string &prefix);

extern void registerMapFunctions(const std::string &prefix);

extern void registerMathFunctions(const std::string &prefix);

extern void registerMiscFunctions(const std::string &prefix);

extern void registerRegexpFunctions(const std::string &prefix);

extern void registerSpecialFormGeneralFunctions(const std::string &prefix);

extern void registerStringFunctions(const std::string &prefix);

extern void registerUrlFunctions(const std::string &prefix);

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
    registerFunctions();
    return 1;
}

void RegisterFunctions::registerFunctions(const std::string &prefix)
{
    registerArrayFunctions(prefix);
    // registerBinaryFunctions(prefix);
    // registerBitwiseFunctions(prefix);
    registerCompareFunctions(prefix);
    // registerDatetimeFunctions(prefix);
    // registerJsonFunctions(prefix);
    registerMapFunctions(prefix);
    registerMathFunctions(prefix);
    // registerMiscFunctions(prefix);
    // registerRegexpFunctions(prefix);
    // registerSpecialFormGeneralFunctions(prefix);
    registerStringFunctions(prefix);
    // registerUrlFunctions(prefix);
}

void link_register_functions() {}
}
