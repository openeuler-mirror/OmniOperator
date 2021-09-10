/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function
 */
#include <vector>
#include <map>
#include "../common/expressions.h"
#include "./functions/mathfunctions.h"
#include "./functions/stringfunctions.h"
#include "func_registry.h"

using namespace std;
using namespace omniruntime::expressions;
using namespace llvm;


// Helper function to find the corresponding llvm type of a DataType
Type* ToLlvmType(DataType t, LLVMContext* context)
{
    switch (t) {
        case DataType::INT32D:
            return Type::getInt32Ty(*context);
        case DataType::INT64D:
            return Type::getInt64Ty(*context);
        case DataType::DOUBLED:
            return Type::getDoubleTy(*context);
        case DataType::BOOLD:
            return Type::getInt1Ty(*context);
        case DataType::STRINGD:
            return Type::getInt64Ty(*context);
        default:
            std::cout << "Error: Unknown argument datatype " << t << endl;
            return nullptr;
    }
}


FunctionRegistry::FunctionRegistry(unique_ptr<llvm::orc::LLJIT> &j, unique_ptr<LLVMContext> &c, unique_ptr<Module> &m)
{
    jit = j.get();
    frContext = c.get();
    module = m.get();
}

FunctionRegistry::~FunctionRegistry() {
}


// From codegen-refactor-func-reg branch
// Registers one function given the function signature
void FunctionRegistry::RegisterFunctionFromSignature(const FunctionSignature& funcSignature) const
{
    // Register function in JIT
    auto &jd = jit->getMainJITDylib();
    auto &dl = jit->getDataLayout();
    llvm::orc::MangleAndInterner mangle(jit->getExecutionSession(), dl);
    vector<Type*> args;
    std::vector<DataType> params = funcSignature.GetParams();
    args.reserve(params.size());
    for (auto type : params) {
        args.push_back(ToLlvmType(type, frContext));
    }

    // register a function
    auto s =  llvm::orc::absoluteSymbols(
        {
            {
                mangle(funcSignature.GetName()),
                JITEvaluatedSymbol(pointerToJITTargetAddress(funcSignature.GetFunctionAddress()),
                                   JITSymbolFlags::Exported)
            }
        }
        );
    auto ign = jd.define(s);
    if (ign) {
        cerr << "Error while defining absolute symbol in jd" << endl;
    }
    llvm::FunctionType* ft = llvm::FunctionType::get(ToLlvmType(funcSignature.GetReturnType(), frContext),
                                                     args, false);
    Function* fn = llvm::Function::Create(ft, Function::ExternalLinkage, funcSignature.GetName(), module);
    FunctionCallee callee = module->getOrInsertFunction(funcSignature.GetName(), ft);
}


void FunctionRegistry::RegisterAbsFunctions(const std::string& fn)
{
    // Math functions
    if (fn == "abs_int32") {
        vector<DataType> absInt32Types {DataType::INT32D};
        FunctionSignature absInt32Sig (absInt32Str, absInt32Types, DataType::INT32D,
                                       reinterpret_cast<void *>(AbsInt32));
        this->RegisterFunctionFromSignature(absInt32Sig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(absInt32Str, absInt32Sig));
    }
    if (fn == "abs_int64") {
        vector<DataType> absInt64Types {DataType::INT64D};
        FunctionSignature absInt64Sig (absInt64Str, absInt64Types, DataType::INT64D,
                                       reinterpret_cast<void *>(AbsInt64));
        this->RegisterFunctionFromSignature(absInt64Sig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(absInt64Str, absInt64Sig));
    }
    if (fn == "abs_double") {
        vector<DataType> absDoubleTypes {DataType::DOUBLED};
        FunctionSignature absDoubleSig (absDoubleStr, absDoubleTypes, DataType::DOUBLED,
                                        reinterpret_cast<void *>(AbsDouble));
        this->RegisterFunctionFromSignature(absDoubleSig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(absDoubleStr, absDoubleSig));
    }
}

void FunctionRegistry::RegisterCastFunctions(const std::string& fn)
{
    if (fn == "CAST_int32_double") {
        vector<DataType> castInt32Types {DataType::INT32D};
        FunctionSignature signature (castInt32ToDoubleStr, castInt32Types, DataType::DOUBLED,
                                     reinterpret_cast<void *>(CastInt32ToDouble));
        this->RegisterFunctionFromSignature(signature);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(castInt32ToDoubleStr, signature));
    }
    if (fn == "CAST_int64_double") {
        vector<DataType> castInt64Types {DataType::INT64D};
        FunctionSignature signature (castInt64ToDoubleStr, castInt64Types, DataType::DOUBLED,
                                     reinterpret_cast<void *>(CastInt64ToDouble));
        this->RegisterFunctionFromSignature(signature);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(castInt64ToDoubleStr, signature));
    }
    if (fn == "CAST_int32_int64") {
        vector<DataType> castInt32Types {DataType::INT32D};
        FunctionSignature signature (castInt64ToInt32Str, castInt32Types, DataType::INT64D,
                                     reinterpret_cast<void *>(CastInt32ToInt64));
        this->RegisterFunctionFromSignature(signature);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(castInt64ToDoubleStr, signature));
    }
    if (fn == "CAST_string_int32") {
        vector<DataType> castStringTypes {DataType::INT64D};
        FunctionSignature signature (castStringStr, castStringTypes, DataType::INT32D,
                                     reinterpret_cast<void *>(CastString));
        this->RegisterFunctionFromSignature(signature);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(castStringStr, signature));
    }
}

void FunctionRegistry::RegisterStringFunctions(const std::string& fn)
{
    if (fn == "substrExt") {
        vector<DataType> substrExtTypes {DataType::INT64D, DataType::INT32D, DataType::INT32D};
        FunctionSignature substrExtSig (substrExtStr, substrExtTypes, DataType::INT64D,
                                        reinterpret_cast<void *>(SubstrExt));
        this->RegisterFunctionFromSignature(substrExtSig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(substrExtStr, substrExtSig));
    }
    if (fn == "substrWithStartExt") {
        vector <DataType> substrWithStartExtTypes{DataType::INT64D, DataType::INT32D};
        FunctionSignature substrWithStartExtSig
                (substrWithStartExtStr, substrWithStartExtTypes,
                 DataType::INT64D, reinterpret_cast<void *>(SubstrWithStartExt));
        this->RegisterFunctionFromSignature(substrWithStartExtSig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(substrWithStartExtStr,
                                                                      substrWithStartExtSig));
    }
    if (fn == "concat") {
        vector<DataType> concatStrExtTypes {DataType::INT64D, DataType::INT64D};
        FunctionSignature concatStrExtSig (concatStrExtStr, concatStrExtTypes,
                                           DataType::INT64D, reinterpret_cast<void *>(ConcatStrExt));
        this->RegisterFunctionFromSignature(concatStrExtSig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(concatStrExtStr, concatStrExtSig));
    }


    if (fn == "LIKE") {
        vector<DataType> likeExtTypes {DataType::INT64D, DataType::INT64D};
        FunctionSignature likeExtSig (likeExtStr, likeExtTypes, DataType::BOOLD,
                                      reinterpret_cast<void *>(LikeExt));
        this->RegisterFunctionFromSignature(likeExtSig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(likeExtStr, likeExtSig));
    }
}

bool isMathFunction(const string& fn)
{
    return fn == "abs_int32" || fn == "abs_int64" || fn == "abs_double";
}

bool isStringFunction(const string& fn)
{
    return fn == "substrExt" || fn == "substrWithStartExt" || fn == "concat" || fn == "LIKE";
}

bool isCastFunction(const string& fn)
{
    return fn.size() > 5 && fn.substr(0, 5) == "CAST_";
}

bool isHashFunction(const string& fn)
{
    return fn == "combine_hash";
}

// Only registers necessary functions
void FunctionRegistry::RegisterNecessaryFuncs(const std::set<string>& requiredFuncs)
{
    // TODO: remove hard-coded strings

    // Always register string comparison
    vector<DataType> strCompareExtTypes {DataType::INT64D, DataType::INT64D};
    FunctionSignature strCompareExtSig(strCompareExtStr, strCompareExtTypes, DataType::INT32D,
                                       reinterpret_cast<void*>(StrCompareExt));
    this->RegisterFunctionFromSignature(strCompareExtSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(strCompareExtStr, strCompareExtSig));

    set<string> externalFuncNames = efr.GetAllExternalFunctionNames();
    for (const auto& fn : requiredFuncs) {
        if (isMathFunction(fn)) {
            this->RegisterAbsFunctions(fn);
        }

        if (isStringFunction(fn)) {
            this->RegisterStringFunctions(fn);
        }

        if (isCastFunction(fn)) {
            this->RegisterCastFunctions(fn);
        }

        if (isHashFunction(fn)) {
            vector<DataType> combineHashTypes {DataType::INT64D, DataType::INT64D};
            FunctionSignature combineHashSig (combineHashStr, combineHashTypes,
                                              DataType::INT64D, reinterpret_cast<void *>(CombineHash));
            this->RegisterFunctionFromSignature(combineHashSig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature>(combineHashStr, combineHashSig));
        }

        // External functions
        if (externalFuncNames.find(fn) != externalFuncNames.end()) {
            FunctionSignature externalSig = efr.GetExternalSignature(fn);
            this->RegisterFunctionFromSignature(externalSig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, externalSig));
        }
    }
}

