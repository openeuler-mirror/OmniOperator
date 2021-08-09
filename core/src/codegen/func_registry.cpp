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
Type* ToLLVMType(DataType t, LLVMContext* context)
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


FunctionRegistry::FunctionRegistry(unique_ptr<llvm::orc::LLJIT> &J, unique_ptr<LLVMContext> &C, unique_ptr<Module> &M)
{
    JIT = J.get();
    frContext = C.get();
    module = M.get();
}

FunctionRegistry::~FunctionRegistry()
{
    for (pair<const string, FunctionSignature*>& p : funcNameToSignatureMap) {
        delete p.second;
    }
}


// From codegen-refactor-func-reg branch
// Registers one function given the function signature
void FunctionRegistry::RegisterFunctionFromSignature(FunctionSignature funcSignature)
{
    // Register function in JIT
    auto &jd = JIT->getMainJITDylib();
    auto &dl = JIT->getDataLayout();
    llvm::orc::MangleAndInterner Mangle(JIT->getExecutionSession(), dl);
    vector<Type*> args;
    std::vector<DataType> params = funcSignature.GetParams();
    args.reserve(params.size());
    for (int32_t i = 0; i < params.size(); i++) {
        DataType type = params.at(i);
        args.push_back(ToLLVMType(type, frContext));
    }

    // register a function
    auto s =  llvm::orc::absoluteSymbols({{Mangle(funcSignature.GetName()), JITEvaluatedSymbol(pointerToJITTargetAddress(funcSignature.GetFunctionAddress()), JITSymbolFlags::Exported)}});
    auto ign = jd.define(s);
    if (ign) cerr << "Error while defining absolute symbol in jd" << endl;
    llvm::FunctionType* ft = llvm::FunctionType::get(ToLLVMType(funcSignature.GetReturnType(), frContext), args, false);
    Function* fn = llvm::Function::Create(ft, Function::ExternalLinkage, funcSignature.GetName(), module);
    FunctionCallee callee = module->getOrInsertFunction(funcSignature.GetName(), ft);
}


// Only registers necessary functions
void FunctionRegistry::InitNecessary(std::set<string> requiredFuncs)
{
    // TODO: remove hard-coded strings

    // Always register string comparison
    vector<DataType> strCompareExt_types {DataType::INT64D, DataType::INT64D};
    auto strCompareExtSig = make_unique<FunctionSignature>(strCompareExt_str, strCompareExt_types, DataType::INT32D, (void*)(StrCompareExt)).release();
    this->RegisterFunctionFromSignature(*strCompareExtSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(strCompareExt_str, strCompareExtSig));

    set<string> externalFuncNames = GetAllExternalFunctionNames();
    for (auto fn : requiredFuncs) {

        // Math functions
        if (fn == "abs_int32") {
            vector<DataType> abs_int32_types {DataType::INT32D};
            auto abs_int32_sig = make_unique<FunctionSignature>(abs_int32_str, abs_int32_types, DataType::INT32D, (void*)(AbsInt32)).release();
            this->RegisterFunctionFromSignature(*abs_int32_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(abs_int32_str, abs_int32_sig));
        }
        if (fn == "abs_int64") {
            vector<DataType> abs_int64_types {DataType::INT64D};
            auto abs_int64_sig =  make_unique<FunctionSignature>(abs_int64_str, abs_int64_types, DataType::INT64D, (void*)(AbsInt64)).release();
            this->RegisterFunctionFromSignature(*abs_int64_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(abs_int64_str, abs_int64_sig));
        }
        if (fn == "abs_double") {
            vector<DataType> abs_double_types {DataType::DOUBLED};
            auto abs_double_sig =  make_unique<FunctionSignature>(abs_double_str, abs_double_types, DataType::DOUBLED, (void*)(AbsDouble)).release();
            this->RegisterFunctionFromSignature(*abs_double_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(abs_double_str, abs_double_sig));
        }

        // String functions
        if (fn == "substrExt") {
            vector<DataType> substrExt_types {DataType::INT64D, DataType::INT32D, DataType::INT32D};
            auto substrExt_sig =  make_unique<FunctionSignature>(substrExt_str, substrExt_types, DataType::INT64D, (void*)(SubstrExt)).release();
            this->RegisterFunctionFromSignature(*substrExt_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(substrExt_str, substrExt_sig));
        }
        if (fn == "substrWithStartExt") {
            vector <DataType> substrWithStartExt_types{DataType::INT64D, DataType::INT32D};
            auto substrWithStartExt_sig =  make_unique<FunctionSignature>(substrWithStartExt_str, substrWithStartExt_types, DataType::INT64D, (void *) (SubstrWithStartExt)).release();
            this->RegisterFunctionFromSignature(*substrWithStartExt_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(substrWithStartExt_str, substrWithStartExt_sig));
        }
        if (fn == "concat") {
            vector<DataType> concatStrExt_types {DataType::INT64D, DataType::INT64D};
            auto concatStrExt_sig =  make_unique<FunctionSignature>(concatStrExt_str, concatStrExt_types, DataType::INT64D, (void*)(ConcatStrExt)).release();
            this->RegisterFunctionFromSignature(*concatStrExt_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(concatStrExt_str, concatStrExt_sig));
        }


        if (fn == "LIKE") {
            vector<DataType> likeExt_types {DataType::INT64D, DataType::INT64D};
            auto likeExt_sig =  make_unique<FunctionSignature>(likeExt_str, likeExt_types, DataType::BOOLD, (void*)(LikeExt)).release();
            this->RegisterFunctionFromSignature(*likeExt_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(likeExt_str, likeExt_sig));
        }

        // Cast functions
        if (fn == "CAST_int32") {
            vector<DataType> cast_int32_types {DataType::INT32D};
            auto cast_int32_sig =  make_unique<FunctionSignature>(cast_int32_str, cast_int32_types, DataType::DOUBLED, (void*)(CastInt32)).release();
            this->RegisterFunctionFromSignature(*cast_int32_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(cast_int32_str, cast_int32_sig));
        }
        if (fn == "CAST_int64") {
            vector<DataType> cast_int64_types {DataType::INT64D};
            auto cast_int64_sig =  make_unique<FunctionSignature>(cast_int64_str, cast_int64_types, DataType::DOUBLED, (void*)(CastInt64)).release();
            this->RegisterFunctionFromSignature(*cast_int64_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(cast_int64_str, cast_int64_sig));
        }
        if (fn == "CAST_string") {
            vector<DataType> cast_string_types {DataType::INT64D};
            auto cast_string_sig =  make_unique<FunctionSignature>(cast_string_str, cast_string_types, DataType::INT32D, (void*)(CastString)).release();
            this->RegisterFunctionFromSignature(*cast_string_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(cast_string_str, cast_string_sig));
        }

        // Hash-related functions
        if (fn == "combine_hash") {
            vector<DataType> combine_hash_types {DataType::INT64D, DataType::INT64D};
            auto combine_hash_sig =  make_unique<FunctionSignature>(combine_hash_str, combine_hash_types, DataType::INT64D, (void*)(CombineHash)).release();
            this->RegisterFunctionFromSignature(*combine_hash_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(combine_hash_str, combine_hash_sig));
        }

        // External developer functions
        if (externalFuncNames.find(fn) != externalFuncNames.end()) {
            FunctionSignature* external_sig = GetExternalSignature(fn);
            this->RegisterFunctionFromSignature(*external_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(fn, external_sig));
        }
    }
}

