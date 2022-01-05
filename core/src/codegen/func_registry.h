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
            (std::unique_ptr<llvm::orc::LLJIT> &j, std::unique_ptr<llvm::LLVMContext> &c,
             std::unique_ptr<llvm::Module> &m);

    ~FunctionRegistry();

    void RegisterFunctionFromSignature(const FunctionSignature &funcSignature) const;

    // Function to initialize necessary internal functions and helpers
    void RegisterAbsFunctions(const std::string& fnName);
    void RegisterCastFunctions(const std::string& fnName);
    void RegisterStringFunctions(const std::string& fnName);
    void RegisterMm3HashFunctions(const std::string& fnName);
    void RegisterPmodFunctions();
    void RegisterCombineHashFunctions();
    void RegisterNecessaryFuncs(const std::set<std::string>& requiredFuncs);
    void RegisterDecimalFuncs();
    void RegisterDictionaryFuncs();
    void ContextHelperFuncs();
    llvm::orc::LLJIT *jit;
    llvm::LLVMContext *frContext;
    llvm::Module *module;

    ExternalFuncRegistry efr;

    std::map<std::string, FunctionSignature> funcNameToSignatureMap;

    // List of functions
    const std::string int32TypeStr = "int32";
    const std::string int64TypeStr = "int64";
    const std::string doubleTypeStr = "double";
    const std::string decimal128Str = "decimal128";
    const std::string mm3hashStr = "mm3hash";
    const std::string absPrefixStr = "abs_";
    const std::string castPrefixStr = "CAST_";
    const std::string strCompareExtStr = "StrCompareExt";
    const std::string likeExtStr = "LIKE_";
    const std::string concatStrExtStr = "concat_";
    const std::string castInt32ToDoubleStr = "CAST_int32_double";
    const std::string castInt64ToDoubleStr = "CAST_int64_double";
    const std::string castInt32ToInt64Str = "CAST_int32_int64";
    const std::string castInt64ToInt32Str = "CAST_int64_int32";
    const std::string castInt64ToDecimal128Str = "CAST_int64_decimal128";
    const std::string castStringStr = "CAST_string_int32";
    const std::string combineHashInt32Str = "combine_hash_int64_int64_int32";
    const std::string combineHashInt64Str = "combine_hash_int64_int64_int64";
    const std::string pmodInt32Str = "pmod_int32_int32_int32";
    const std::string mm3hashInt32Str = "mm3hash_int32";
    const std::string mm3hashInt64Str = "mm3hash_int64";
    const std::string mm3hashStringStr = "mm3hash_string";
    const std::string mm3hashDoubleStr = "mm3hash_double";
    const std::string decimal128CompareExtStr = "Decimal128CompareExt";
    const std::string addDec128Str = "Add_decimal128";
    const std::string subDec128Str = "Sub_decimal128";
    const std::string mulDec128Str = "Mul_decimal128";
    const std::string divDec128Str = "Div_decimal128";
    const std::string contextMalloc = "ArenaAllocatorMalloc";
    const std::string contextReset = "ArenaAllocatorReset";

    const std::string dictionaryGetIntStr = "DictionaryGetInt";
    const std::string dictionaryGetLongStr = "DictionaryGetLong";
    const std::string dictionaryGetDoubleStr = "DictionaryGetDouble";
    const std::string dictionaryGetBooleanStr = "DictionaryGetBoolean";
    const std::string dictionaryGetVarcharStr = "DictionaryGetVarchar";
    const std::string dictionaryGetDecimalStr = "DictionaryGetDecimal";
};
#endif