/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function
 */
#include <vector>
#include <map>
#include "./functions/mathfunctions.h"
#include "./functions/stringfunctions.h"
#include "./functions/murmur3_hash.h"
#include "./functions/decimalfunctions.h"
#include "./functions/context_helper.h"
#include "./functions/dictionaryfunctions.h"
#include "func_registry.h"

using namespace std;
using namespace omniruntime::expressions;
using namespace llvm;
namespace {
    const int STARTEXT_VALUE = 2;
    const int SUBSTREXT_VALUE = 3;
}

// Helper function to find the corresponding llvm type of a DataType
Type* ToLlvmType(DataType t, LLVMContext* context)
{
    switch (t) {
        case DataType::VOIDD:
            return Type::getVoidTy(*context);
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
        case DataType::DECIMAL128D:
            return Type::getInt64Ty(*context);
        case DataType::INT32PTRD:
            return Type::getInt32PtrTy(*context);
        case DataType::INT8PTRD:
            return Type::getInt8PtrTy(*context);
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

std::string GetAbsType(const std::string& str)
{
    int pos = 0;
    int prev = 0;
    while ((pos = str.find('_', prev)) != std::string::npos) {
        prev = pos + 1;
    }
    return str.substr(prev);
}

void FunctionRegistry::RegisterAbsFunctions(const std::string& fn)
{
    std::string type = GetAbsType(fn);
    // Math functions
    if (type == int32TypeStr) {
        vector<DataType> absInt32Types {DataType::INT32D};
        FunctionSignature absInt32Sig (fn, absInt32Types, DataType::INT32D,
                                       reinterpret_cast<void *>(AbsInt32));
        this->RegisterFunctionFromSignature(absInt32Sig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, absInt32Sig));
    }
    if (type == int64TypeStr) {
        vector<DataType> absInt64Types {DataType::INT64D};
        FunctionSignature absInt64Sig (fn, absInt64Types, DataType::INT64D,
                                       reinterpret_cast<void *>(AbsInt64));
        this->RegisterFunctionFromSignature(absInt64Sig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, absInt64Sig));
    }
    if (type == doubleTypeStr) {
        vector<DataType> absDoubleTypes {DataType::DOUBLED};
        FunctionSignature absDoubleSig (fn, absDoubleTypes, DataType::DOUBLED,
                                        reinterpret_cast<void *>(AbsDouble));
        this->RegisterFunctionFromSignature(absDoubleSig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, absDoubleSig));
    }
    if (type == decimal128Str) {
        vector<DataType> absDecimal128Types {DataType::INT64D};
        FunctionSignature absDecimal128Sig (fn, absDecimal128Types, DataType::INT64D,
                                       reinterpret_cast<void *>(AbsDecimal128));
        this->RegisterFunctionFromSignature(absDecimal128Sig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, absDecimal128Sig));
    }
}

void FunctionRegistry::RegisterCastFunctions(const std::string& fn)
{
    if (fn == castInt32ToDoubleStr) {
        vector<DataType> castInt32Types {DataType::INT32D};
        FunctionSignature signature (castInt32ToDoubleStr, castInt32Types, DataType::DOUBLED,
                                     reinterpret_cast<void *>(CastInt32ToDouble));
        this->RegisterFunctionFromSignature(signature);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(castInt32ToDoubleStr, signature));
    }
    if (fn == castInt64ToDoubleStr) {
        vector<DataType> castInt64Types {DataType::INT64D};
        FunctionSignature signature (castInt64ToDoubleStr, castInt64Types, DataType::DOUBLED,
                                     reinterpret_cast<void *>(CastInt64ToDouble));
        this->RegisterFunctionFromSignature(signature);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(castInt64ToDoubleStr, signature));
    }
    if (fn == castInt32ToInt64Str) {
        vector<DataType> castInt32Types {DataType::INT32D};
        FunctionSignature signature (castInt32ToInt64Str, castInt32Types, DataType::INT64D,
                                     reinterpret_cast<void *>(CastInt32ToInt64));
        this->RegisterFunctionFromSignature(signature);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(castInt32ToInt64Str, signature));
    }
    if (fn == castStringStr) {
        vector<DataType> castStringTypes {DataType::INT8PTRD, DataType::INT32D};
        FunctionSignature signature (castStringStr, castStringTypes, DataType::INT32D,
                                     reinterpret_cast<void *>(CastString));
        this->RegisterFunctionFromSignature(signature);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(castStringStr, signature));
    }
}

bool IsSubstrFunc(const std::string& fn)
{
    return (fn.size() > 7 && fn.substr(0,7) == "substr_");
}

void FunctionRegistry::RegisterStringFunctions(const std::string& fn)
{
    if (IsSubstrFunc(fn)) {
        int numArgs = -1;
        for (int i = 0; i < fn.size(); i++) {
            if (fn[i] == '_') {
                numArgs++;
            }
        }
        if (numArgs == SUBSTREXT_VALUE) {
            vector<DataType> substrExtTypes {
                    DataType::INT8PTRD, DataType::INT32D, DataType::INT32D, DataType::INT32D,
                    DataType::INT32PTRD, DataType::INT64D
            };
            FunctionSignature substrExtSig(fn, substrExtTypes, DataType::INT8PTRD,
                                           reinterpret_cast<void *>(SubstrExt));
            this->RegisterFunctionFromSignature(substrExtSig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, substrExtSig));
        }
        if (numArgs == STARTEXT_VALUE) {
            vector<DataType> substrWithStartExtTypes {
                    DataType::INT8PTRD, DataType::INT32D, DataType::INT32D,
                    DataType::INT32PTRD, DataType::INT64D
            };
            FunctionSignature substrWithStartExtSig
                    (fn, substrWithStartExtTypes,
                     DataType::INT8PTRD, reinterpret_cast<void *>(SubstrWithStartExt));
            this->RegisterFunctionFromSignature(substrWithStartExtSig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn,
                                                                          substrWithStartExtSig));
        }
    }

    if (fn.find(concatStrExtStr) != std::string::npos) {
        vector<DataType> concatStrExtTypes {DataType::INT8PTRD, DataType::INT32D, DataType::INT8PTRD, DataType::INT32D,
                                            DataType::INT32PTRD, DataType::INT64D};
        FunctionSignature concatStrExtSig (fn, concatStrExtTypes,
                                           DataType::INT8PTRD, reinterpret_cast<void *>(ConcatStrExt));
        this->RegisterFunctionFromSignature(concatStrExtSig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, concatStrExtSig));
    }

    if (fn.find(likeExtStr) != std::string::npos) {
        vector<DataType> likeExtTypes {DataType::INT8PTRD, DataType::INT32D, DataType::INT8PTRD, DataType::INT32D};
        FunctionSignature likeExtSig (fn, likeExtTypes, DataType::BOOLD,
                                      reinterpret_cast<void *>(LikeExt));
        this->RegisterFunctionFromSignature(likeExtSig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, likeExtSig));
    }
}

void FunctionRegistry::RegisterDecimalFuncs()
{
    // Decimal comparison operators
    vector<DataType> decimalExtTypes {DataType::INT64D, DataType::INT64D};
    FunctionSignature decimalCompareExtSig(decimal128CompareExtStr, decimalExtTypes, DataType::INT32D,
                                           reinterpret_cast<void *>(Decimal128CompareExt));
    this->RegisterFunctionFromSignature(decimalCompareExtSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(decimal128CompareExtStr, decimalCompareExtSig));

    // Decimal Add
    FunctionSignature decimalAddExtSig(addDec128Str, decimalExtTypes, DataType::INT64D,
                                       reinterpret_cast<void *>(AddDec128));
    this->RegisterFunctionFromSignature(decimalAddExtSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(addDec128Str, decimalAddExtSig));

    // Decimal Subtract
    FunctionSignature decimalSubExtSig(subDec128Str, decimalExtTypes, DataType::INT64D,
                                       reinterpret_cast<void *>(SubDec128));
    this->RegisterFunctionFromSignature(decimalSubExtSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(subDec128Str, decimalSubExtSig));

    // Decimal Multiplication
    FunctionSignature decimalMulExtSig(mulDec128Str, decimalExtTypes, DataType::INT64D,
                                       reinterpret_cast<void *>(MulDec128));
    this->RegisterFunctionFromSignature(decimalMulExtSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(mulDec128Str, decimalMulExtSig));

    // Decimal Division
    FunctionSignature decimalDivExtSig(divDec128Str, decimalExtTypes, DataType::INT64D,
                                       reinterpret_cast<void *>(DivDec128));
    this->RegisterFunctionFromSignature(decimalDivExtSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(divDec128Str, decimalDivExtSig));
}

void FunctionRegistry::RegisterMm3HashFunctions(const std::string& fn)
{
    // Mm3Hash functions
    if (fn.find(mm3hashInt32Str) != std::string::npos) {
        std::cout << fn << std::endl;
        vector<DataType> mm3Int32Types {DataType::INT32D, DataType::INT32D};
        FunctionSignature mm3Int32Sig (fn, mm3Int32Types, DataType::INT32D,
                                       reinterpret_cast<void *>(Mm3Int32));
        this->RegisterFunctionFromSignature(mm3Int32Sig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, mm3Int32Sig));
    }
    if (fn.find(mm3hashInt64Str) != std::string::npos) {
        vector<DataType> mm3Int64Types {DataType::INT64D, DataType::INT32D};
        FunctionSignature mm3Int64Sig (fn, mm3Int64Types, DataType::INT32D,
                                       reinterpret_cast<void *>(Mm3Int64));
        this->RegisterFunctionFromSignature(mm3Int64Sig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, mm3Int64Sig));
    }
    if (fn.find(mm3hashDoubleStr) != std::string::npos) {
        vector<DataType> mm3DoubleTypes {DataType::DOUBLED, DataType::INT32D};
        FunctionSignature mm3DoubleSig (fn, mm3DoubleTypes, DataType::INT32D,
                                        reinterpret_cast<void *>(Mm3Double));
        this->RegisterFunctionFromSignature(mm3DoubleSig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, mm3DoubleSig));
    }
    if (fn.find(mm3hashStringStr) != std::string::npos) {
        vector<DataType> mm3StringTypes {DataType::INT64D, DataType::INT32D};
        FunctionSignature mm3StringSig (fn, mm3StringTypes, DataType::INT32D,
                                        reinterpret_cast<void *>(Mm3String));
        this->RegisterFunctionFromSignature(mm3StringSig);
        funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, mm3StringSig));
    }
}

void FunctionRegistry::RegisterDictionaryFuncs()
{
    vector<DataType> params {DataType::INT64D, DataType::INT32D};
    FunctionSignature dictionaryGetIntSig(dictionaryGetIntStr, params, DataType::INT32D,
                                           reinterpret_cast<void *>(GetIntFromDictionaryVector));
    this->RegisterFunctionFromSignature(dictionaryGetIntSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(dictionaryGetIntStr, dictionaryGetIntSig));

    FunctionSignature dictionaryGetLongSig(dictionaryGetLongStr, params, DataType::INT64D,
                                       reinterpret_cast<void *>(GetLongFromDictionaryVector));
    this->RegisterFunctionFromSignature(dictionaryGetLongSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(dictionaryGetLongStr, dictionaryGetLongSig));

    FunctionSignature dictionaryGetDoubleSig(dictionaryGetDoubleStr, params, DataType::DOUBLED,
                                       reinterpret_cast<void *>(GetDoubleFromDictionaryVector));
    this->RegisterFunctionFromSignature(dictionaryGetDoubleSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(dictionaryGetDoubleStr, dictionaryGetDoubleSig));

    FunctionSignature dictionaryGetBoolSig(dictionaryGetBooleanStr, params, DataType::BOOLD,
                                       reinterpret_cast<void *>(GetBooleanFromDictionaryVector));
    this->RegisterFunctionFromSignature(dictionaryGetBoolSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(dictionaryGetBooleanStr, dictionaryGetBoolSig));

    params.push_back(DataType::INT32PTRD);
    FunctionSignature dictionaryGetVarcharSig(dictionaryGetVarcharStr, params, DataType::INT8PTRD,
                                       reinterpret_cast<void *>(GetVarcharFromDictionaryVector));
    this->RegisterFunctionFromSignature(dictionaryGetVarcharSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(dictionaryGetVarcharStr, dictionaryGetVarcharSig));
}

void FunctionRegistry::RegisterCombineHashFunctions()
{
    vector<DataType> combineHashTypes {DataType::INT64D, DataType::INT64D};
    FunctionSignature combineHashSig (combineHashInt64Str, combineHashTypes,
                                      DataType::INT64D, reinterpret_cast<void *>(CombineHash));
    this->RegisterFunctionFromSignature(combineHashSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(combineHashInt64Str, combineHashSig));
}

void FunctionRegistry::ContextHelperFuncs()
{
    vector<DataType> contextAllocatorTypes {DataType::INT64D, DataType::INT32D};
    FunctionSignature contextAllocatorSig (contextMalloc, contextAllocatorTypes, DataType::INT8PTRD,
                                       reinterpret_cast<void*>(ArenaAllocatorMalloc));
    this->RegisterFunctionFromSignature(contextAllocatorSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(contextMalloc, contextAllocatorSig));

    vector<DataType> contextResetTypes {DataType::INT64D};
    FunctionSignature contextResetSig (contextReset, contextResetTypes, DataType::VOIDD,
                                           reinterpret_cast<void*>(ArenaAllocatorReset));
    this->RegisterFunctionFromSignature(contextResetSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(contextReset, contextResetSig));
}

// Only registers necessary functions
void FunctionRegistry::RegisterNecessaryFuncs(const std::set<string>& requiredFuncs)
{
    // TODO: remove hard-coded strings

    // Always register string comparison
    vector<DataType> strCompareExtTypes {DataType::INT8PTRD, DataType::INT32D, DataType::INT8PTRD, DataType::INT32D};
    FunctionSignature strCompareExtSig(strCompareExtStr, strCompareExtTypes, DataType::INT32D,
                                       reinterpret_cast<void*>(StrCompareExt));
    this->RegisterFunctionFromSignature(strCompareExtSig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature>(strCompareExtStr, strCompareExtSig));

    // Always register Decimal Binary and Arithmetic Functions
    this->RegisterDecimalFuncs();
    this->ContextHelperFuncs();
    this->RegisterDictionaryFuncs();
    set<string> externalFuncNames = efr.GetAllExternalFunctionNames();
    for (const auto& fn : requiredFuncs) {
        if (fn.size() > absPrefixStr.length() && fn.substr(0, absPrefixStr.length()) == absPrefixStr) {
            this->RegisterAbsFunctions(fn);
        }

        if (IsSubstrFunc(fn) || (fn.find(concatStrExtStr) != std::string::npos) ||
            (fn.find(likeExtStr) != std::string::npos)) {
            this->RegisterStringFunctions(fn);
        }

        if (fn.size() > castPrefixStr.length() && fn.substr(0, castPrefixStr.length()) == castPrefixStr) {
            this->RegisterCastFunctions(fn);
        }

        if (fn == combineHashInt32Str || fn == combineHashInt64Str) {
            this->RegisterCombineHashFunctions();
        }

        if (fn.size() > mm3hashStr.length() && fn.substr(0, mm3hashStr.length()) == mm3hashStr) {
            this->RegisterMm3HashFunctions(fn);
        }

        // External functions
        if (externalFuncNames.find(fn) != externalFuncNames.end()) {
            FunctionSignature externalSig = efr.GetExternalSignature(fn);
            this->RegisterFunctionFromSignature(externalSig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature>(fn, externalSig));
        }
    }
}
