#ifndef __LLVM_CODEGEN_H__
#define __LLVM_CODEGEN_H__

#include "../common/expressions.h"
#include "../common/parser/parser.h"
#include "./functions/mathfunctions.h"
#include "./functions/stringfunctions.h"
#include "./func_registry.h"

#include <iostream>
#include <string>
#include <cstring>
#include <memory>
#include <vector>
#include <cassert>
#include <algorithm>

#include "llvm/ADT/APInt.h"
#include "llvm/ADT/APFloat.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/Support/Error.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/SourceMgr.h"
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/ADT/APInt.h"
#include "llvm/IR/Instructions.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Target/TargetMachine.h"

using namespace llvm;
using namespace orc;
using namespace std;
using namespace omniruntime::expressions;

// Given an expression generates the function for it.
class LLVMCodeGen
{

public:
    LLVMCodeGen(string name, Expr *expr, vector<DataType>* datatypes);
    ~LLVMCodeGen();

    std::string dumpCode();
    virtual int64_t getFunction() = 0;

// TODO: Figure out which of these can be private
protected:
    // Parse a generic expression by calling the helper functions below
    Value* parseExpr(Expr* root, map<string, Value*>& args);

    Value* createConstantBool(bool n);
    Value* createConstantInt(int32_t n);
    Value* createConstantLong(int64_t n);
    Value* createConstantDouble(double n);
    Type* toLLVMType(DataType t);
    
    Function* createFunction();
    
    // Parsing different kinds of expressions
    Value* parseDataExpr(DataExpr* dExpr, map<string, Value*>& args);
    Value* parseBinaryExpr(BinaryExpr* bExpr, map<string, Value*>& args);
    Value* parseUnaryExpr(UnaryExpr* uExpr, map<string, Value*>& args);
    Value* parseIfExpr(IfExpr* ifExpr, map<string, Value*>& args);
    Value* parseInExpr(InExpr* inExpr, map<string, Value*>& args);
    Value* parseBetweenExpr(BetweenExpr* btExpr, map<string, Value*>& args);
    Value* parseCoalesceExpr(CoalesceExpr* cExpr, map<string, Value*>& args);
    Value* parseFuncExpr(FuncExpr* fExpr, map<string, Value*>& args);

    // Helper functions for generating IR for operators and special forms
    Value* stringCmp(Value *LHS, Value *RHS);
    Function* createConditional(DataType retType, Expr* cond, Expr* ifTrue, Expr* ifFalse);
    Function* createCoalesceFunc(DataType retType, DataExpr* dExpr1, Expr* value2Expr);
    
    std::string _func_name;
    Expr* _expr = nullptr;
    vector<DataType>* datatypes;


    // Returns a set of all the required functions for a given row expression
    // Currently a separate function
    // Can be integrated with parseRowExpression, but then the method declaration would need refactoring
    set<string> requiredFunctions(Expr* expr);
    void requiredFunctionsHelper(Expr* expr, set<string>& s);
    map<string, FunctionSignature*> funcNameToSignature;

    unique_ptr<LLVMContext> context;
    unique_ptr<IRBuilder<>> builder;
    unique_ptr<Module> _module;
    ExitOnError EOE; 
    unique_ptr<LLJIT> JIT;
    ResourceTrackerSP rt;
    FunctionRegistry *FR;

};

#endif