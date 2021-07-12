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

using namespace std;
using namespace omniruntime::expressions;
using namespace llvm;
using namespace llvm::orc;



class FunctionRegistry {
public:
    FunctionRegistry(unique_ptr<LLJIT> &J, unique_ptr<LLVMContext> &C, unique_ptr<Module> &M);
    ~FunctionRegistry();
    void registerFunctionFromSignature(FunctionSignature func_signature);
    void initNecessary(std::set<string> requiredFuncs);
    void initAll();

    LLJIT* JIT;
    LLVMContext* FRContext;
    Module* _module;

    map<string, FunctionSignature*> funcNameToSignatureMap;
};



// List of functions
const string strCompareExt_str = "strCompareExt";
const string likeExt_str = "likeExt";
const string abs_int32_str = "abs_int32";
const string abs_int64_str = "abs_int64";
const string abs_double_str = "abs_double";
const string substrExt_str = "substrExt";
const string substrWithStartExt_str = "substrWithStartExt";
const string concatStrExt_str = "concatStrExt";
const string cast_int32_str = "cast_int32";
const string cast_int64_str = "cast_int64";
const string cast_string_str = "cast_string";
const string combine_hash_str = "combine_hash";


#endif