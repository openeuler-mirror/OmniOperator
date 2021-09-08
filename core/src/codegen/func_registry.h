/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function
 */
#ifndef __FUNC_REGISTRY_H__
#define __FUNC_REGISTRY_H__
#include <vector>
#include <map>
#include <set>
#include <string>

#include "llvm/ExecutionEngine/Orc/LLJIT.h"

#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"


#include "./func_signature.h"

#include "./functions/mathfunctions.h"
#include "./functions/stringfunctions.h"
#include "./functions/external_func_registry.h"

class FunctionRegistry {
public:
    FunctionRegistry
    (std::unique_ptr<llvm::orc::LLJIT> &j, std::unique_ptr<llvm::LLVMContext> &c, std::unique_ptr<llvm::Module> &m);
    ~FunctionRegistry();
    void RegisterFunctionFromSignature(const FunctionSignature& funcSignature) const;

    // Function to initialize necessary internal functions and helpers
    void RegisterAbsFunctions(const std::string& fnName);
    void RegisterCastFunctions(const std::string& fnName);
    void RegisterStringFunctions(const std::string& fnName);
    void RegisterNecessaryFuncs(const std::set<std::string>& requiredFuncs);

    llvm::orc::LLJIT* jit;
    llvm::LLVMContext* frContext;
    llvm::Module* module;

    ExternalFuncRegistry efr;

    std::map<std::string, FunctionSignature> funcNameToSignatureMap;

    // List of functions
    const std::string strCompareExtStr = "StrCompareExt";
    const std::string likeExtStr = "LikeExt";
    const std::string absInt32Str = "Abs_int32";
    const std::string absInt64Str = "Abs_int64";
    const std::string absDoubleStr = "Abs_double";
    const std::string substrExtStr = "SubstrExt";
    const std::string substrWithStartExtStr = "SubstrWithStartExt";
    const std::string concatStrExtStr = "ConcatStrExt";
    const std::string castInt32ToDoubleStr = "Cast_int32_double";
    const std::string castInt64ToDoubleStr = "Cast_int64_double";
    const std::string castInt64ToInt32Str = "Cast_int32_int64";
    const std::string castStringStr = "Cast_string_int32";
    const std::string combineHashStr = "CombineHash";
};


#endif