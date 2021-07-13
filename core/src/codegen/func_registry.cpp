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
Type* toLLVMType(DataType t, LLVMContext* context) {
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


FunctionRegistry::FunctionRegistry(unique_ptr<LLJIT> &J, unique_ptr<LLVMContext> &C, unique_ptr<Module> &M) {
    JIT = J.get();
    FRContext = C.get();
    _module = M.get();
}

FunctionRegistry::~FunctionRegistry() {
    for (pair<const string, FunctionSignature*>& p : funcNameToSignatureMap) {
        delete p.second;
    }
}


// From codegen-refactor-func-reg branch
// Registers one function given the function signature
void FunctionRegistry::registerFunctionFromSignature(FunctionSignature func_signature) {
    // Register function in JIT
    auto &jd = JIT->getMainJITDylib();
    auto &dl = JIT->getDataLayout();
    MangleAndInterner Mangle(JIT->getExecutionSession(), dl);
    vector<Type*> args;
    std::vector<DataType> params = func_signature.getParams();
    args.reserve(params.size());
    for (int32_t i = 0; i < params.size(); i++) {
        DataType type = params.at(i);
        args.push_back(toLLVMType(type, FRContext));
    }

    // register a function
    auto s = absoluteSymbols({{Mangle(func_signature.getName()), JITEvaluatedSymbol(pointerToJITTargetAddress(func_signature.getFunctionAddress()), JITSymbolFlags::Exported)}});
    auto ign = jd.define(s);
    if (ign) cerr << "Error while defining absolute symbol in jd" << endl;
    llvm::FunctionType* ft = llvm::FunctionType::get(toLLVMType(func_signature.getReturnType(), FRContext), args, false);
    Function* fn = llvm::Function::Create(ft, Function::ExternalLinkage, func_signature.getName(), _module);
    FunctionCallee callee = _module->getOrInsertFunction(func_signature.getName(), ft);

    // cout << "Registered function " << func_signature.getName() << endl;
}


// Only registers necessary functions
void FunctionRegistry::initNecessary(std::set<string> requiredFuncs)
{
    // TODO: remove hard-coded strings

    // Always register string comparison
    vector<DataType> strCompareExt_types {DataType::INT64D, DataType::INT64D};
    FunctionSignature* strCompareExt_sig = new FunctionSignature(strCompareExt_str, strCompareExt_types, DataType::INT32D, (void*)(strCompareExt));
    this->registerFunctionFromSignature(*strCompareExt_sig);
    funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(strCompareExt_str, strCompareExt_sig));
    

    set<string> externalFuncNames = getAllExternalFunctionNames();
    for (auto fn : requiredFuncs) {

        // Math functions
        if (fn == "abs_int32") {
            vector<DataType> abs_int32_types {DataType::INT32D};
            FunctionSignature* abs_int32_sig = new FunctionSignature(abs_int32_str, abs_int32_types, DataType::INT32D, (void*)(abs_int32));
            this->registerFunctionFromSignature(*abs_int32_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(abs_int32_str, abs_int32_sig));
        }
        if (fn == "abs_int64") {
            vector<DataType> abs_int64_types {DataType::INT64D};
            FunctionSignature* abs_int64_sig = new FunctionSignature(abs_int64_str, abs_int64_types, DataType::INT64D, (void*)(abs_int64));
            this->registerFunctionFromSignature(*abs_int64_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(abs_int64_str, abs_int64_sig));
        }
        if (fn == "abs_double") {
            vector<DataType> abs_double_types {DataType::DOUBLED};
            FunctionSignature* abs_double_sig = new FunctionSignature(abs_double_str, abs_double_types, DataType::DOUBLED, (void*)(abs_double));
            this->registerFunctionFromSignature(*abs_double_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(abs_double_str, abs_double_sig));
        }

        // String functions
        if (fn == "substrExt") {
            vector<DataType> substrExt_types {DataType::INT64D, DataType::INT32D, DataType::INT32D};
            FunctionSignature* substrExt_sig = new FunctionSignature(substrExt_str, substrExt_types, DataType::INT64D, (void*)(substrExt));
            this->registerFunctionFromSignature(*substrExt_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(substrExt_str, substrExt_sig));
        }
        if (fn == "substrWithStartExt") {
            vector <DataType> substrWithStartExt_types{DataType::INT64D, DataType::INT32D};
            FunctionSignature* substrWithStartExt_sig = new FunctionSignature(substrWithStartExt_str, substrWithStartExt_types, DataType::INT64D, (void *) (substrWithStartExt));
            this->registerFunctionFromSignature(*substrWithStartExt_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(substrWithStartExt_str, substrWithStartExt_sig));
        }
        if (fn == "concat") {
            vector<DataType> concatStrExt_types {DataType::INT64D, DataType::INT64D};
            FunctionSignature* concatStrExt_sig = new FunctionSignature(concatStrExt_str, concatStrExt_types, DataType::INT64D, (void*)(concatStrExt));
            this->registerFunctionFromSignature(*concatStrExt_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(concatStrExt_str, concatStrExt_sig));
        }


        if (fn == "LIKE") {
            vector<DataType> likeExt_types {DataType::INT64D, DataType::INT64D};
            FunctionSignature* likeExt_sig = new FunctionSignature(likeExt_str, likeExt_types, DataType::BOOLD, (void*)(likeExt));
            this->registerFunctionFromSignature(*likeExt_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(likeExt_str, likeExt_sig));
        }

        // Cast functions
        if (fn == "CAST_int32") {
            vector<DataType> cast_int32_types {DataType::INT32D};
            FunctionSignature* cast_int32_sig = new FunctionSignature(cast_int32_str, cast_int32_types, DataType::DOUBLED, (void*)(cast_int32));
            this->registerFunctionFromSignature(*cast_int32_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(cast_int32_str, cast_int32_sig));
        }
        if (fn == "CAST_int64") {
            vector<DataType> cast_int64_types {DataType::INT64D};
            FunctionSignature* cast_int64_sig = new FunctionSignature(cast_int64_str, cast_int64_types, DataType::DOUBLED, (void*)(cast_int64));
            this->registerFunctionFromSignature(*cast_int64_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(cast_int64_str, cast_int64_sig));
        }
        if (fn == "CAST_string") {
            vector<DataType> cast_string_types {DataType::INT64D};
            FunctionSignature* cast_string_sig = new FunctionSignature(cast_string_str, cast_string_types, DataType::INT32D, (void*)(cast_string));
            this->registerFunctionFromSignature(*cast_string_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(cast_string_str, cast_string_sig));
        }

        // Hash-related functions
        if (fn == "combine_hash") {
            vector<DataType> combine_hash_types {DataType::INT64D, DataType::INT64D};
            FunctionSignature* combine_hash_sig = new FunctionSignature(combine_hash_str, combine_hash_types, DataType::INT64D, (void*)(combine_hash));
            this->registerFunctionFromSignature(*combine_hash_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(combine_hash_str, combine_hash_sig));
        }

        // External developer functions
        if (externalFuncNames.find(fn) != externalFuncNames.end()) {
            FunctionSignature* external_sig = getExternalSignature(fn);
            this->registerFunctionFromSignature(*external_sig);
            funcNameToSignatureMap.insert(pair<string, FunctionSignature*>(fn, external_sig));
        }


    }
}

