#ifndef __LLVM_CODEGEN_H__
#define __LLVM_CODEGEN_H__

#include "../common/expressions.h"
#include "../common/parser/parser.h"
#include "./functions/mathfunctions.h"
#include "./functions/stringfunctions.h"

#include <iostream>
#include <string>
#include <cstring>
#include <memory>
#include <vector>
#include <cassert>
#include <algorithm>

#include "llvm/ADT/APFloat.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/ADT/APInt.h"
#include "llvm/IR/Instructions.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Target/TargetMachine.h"

using namespace llvm;
using namespace orc;
using namespace std;
using namespace expressions;

// Given an expression generates the function for it.
class LLVMCodeGen
{

public:
    LLVMCodeGen(std::string name, Expr *expr, vector<DataType>* datatypes);
    ~LLVMCodeGen();

    // compile the generated code
    void compile();
    // todo: make this take a 2d array and return array of selected rows
    int32_t execute(int64_t* data, int32_t nRows, int32_t* selected);
    std::string dumpCode();


private:
    void registerFunc(void* funcAddr, string funcName, llvm::Type* retType, vector<Type*> paramTypes);
    void registerFunctions();

    Value* parseExpr(Expr* root, map<string, Value*>& args);
    // Generate the functions
    Function* generateFunc();
    int64_t createWrapper(Function* filterFunc);

    Value* createConstantBool(bool v);
    Value* createConstantInt(int32_t n);
    Value* createConstantLong(int64_t n);
    Value* createConstantDouble(double n);
    Type* toLLVMType(DataType t);

    Value* stringEq(Value* LHS, Value* RHS);
    Value* stringCmp(Value *LHS, Value *RHS);
    Function* createConditional(DataType retType, Expr* cond, Expr* ifTrue, Expr* ifFalse);
    Value* funcCodegen(FuncExpr* fExpr, map<string, Value*>& args);

    std::string _func_name;
    Expr* _expr = nullptr;
    vector<DataType>* datatypes;

    int32_t (*_filter)(int64_t*, int32_t, int32_t*);
    unique_ptr<LLVMContext> context;
    unique_ptr<IRBuilder<>> builder;
    unique_ptr<Module> _module;
    ExitOnError EOE;
    unique_ptr<LLJIT> JIT;
    ResourceTrackerSP rt;

};

#endif