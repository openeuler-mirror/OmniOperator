/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function
 */
#ifndef __FUNC_REGISTERY_H__
#define __FUNC_REGISTERY_H__
#include <vector>
#include <map>
#include <set>
#include <string>
#include <cstring>

#include "llvm/ExecutionEngine/Orc/LLJIT.h"

#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"


#include "../common/expressions.h"
#include "./func_signature.h"

#include "./functions/mathfunctions.h"
#include "./functions/stringfunctions.h"
#include "./functions/external_func_registry.h"

using namespace llvm;
class FunctionRegistry {
public:
    FunctionRegistry(std::unique_ptr<llvm::orc::LLJIT> &J, std::unique_ptr<LLVMContext> &C, std::unique_ptr<Module> &M);
    ~FunctionRegistry();
    void RegisterFunctionFromSignature(FunctionSignature funcSignature);

    void InitNecessary(std::set<std::string> requiredFuncs);
    void InitAll();

    llvm::orc::LLJIT* JIT;
    llvm::LLVMContext* frContext;
    llvm::Module* module;

    std::map<std::string, FunctionSignature*> funcNameToSignatureMap;
};



// List of functions
const std::string strCompareExt_str = "strCompareExt";
const std::string likeExt_str = "likeExt";
const std::string abs_int32_str = "abs_int32";
const std::string abs_int64_str = "abs_int64";
const std::string abs_double_str = "abs_double";
const std::string substrExt_str = "substrExt";
const std::string substrWithStartExt_str = "substrWithStartExt";
const std::string concatStrExt_str = "concatStrExt";
const std::string cast_int32_str = "cast_int32";
const std::string cast_int64_str = "cast_int64";
const std::string cast_string_str = "cast_string";
const std::string combine_hash_str = "combine_hash";


#endif