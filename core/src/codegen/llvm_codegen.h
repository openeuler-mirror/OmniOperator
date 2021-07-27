/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2027. All rights reserved.
 * Description: code generation methods
 */
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

// Given an expression generates the function for it.
class LLVMCodeGen {

public:
    LLVMCodeGen(std::string name, Expr *expr, std::vector<DataType>* datatypes);
    ~LLVMCodeGen();

    std::string dumpCode();
    virtual int64_t GetFunction() = 0;

// TODO: Figure out which of these can be private
protected:
    // Parse a generic expression by calling the helper functions below
    Value* parseExpr(Expr* root, std::map<std::string, Value*>& args);

    Value* createConstantBool(bool n);
    Value* createConstantInt(int32_t n);
    Value* createConstantLong(int64_t n);
    Value* createConstantDouble(double n);
    Type* toLLVMType(DataType t);

    Function* createFunction();

    // Parsing different kinds of expressions
    Value* parseDataExpr(DataExpr* dExpr, std::map<std::string, Value*>& args);
    Value* parseBinaryExpr(BinaryExpr* bExpr, std::map<std::string, Value*>& args);
    Value* parseUnaryExpr(UnaryExpr* uExpr, std::map<std::string, Value*>& args);
    Value* parseIfExpr(IfExpr* ifExpr, std::map<std::string, Value*>& args);
    Value* parseInExpr(InExpr* inExpr, std::map<std::string, Value*>& args);
    Value* parseBetweenExpr(BetweenExpr* btExpr, std::map<std::string, Value*>& args);
    Value* parseCoalesceExpr(CoalesceExpr* cExpr, std::map<std::string, Value*>& args);
    Value* parseFuncExpr(FuncExpr* fExpr, std::map<std::string, Value*>& args);

    // Helper functions for generating IR for operators and special forms
    Value* stringCmp(Value *LHS, Value *RHS);
    Function* createConditional(DataType retType, Expr* cond, Expr* ifTrue, Expr* ifFalse);
    Function* createCoalesceFunc(DataType retType, DataExpr* dExpr1, Expr* value2Expr);

    std::string funcName;
    Expr* expr = nullptr;
    std::vector<DataType>* datatypes;


    // Returns a set of all the required functions for a given row expression
    // Currently a separate function
    // Can be integrated with ParseRowExpression, but then the method declaration would need refactoring
    std::set<std::string> requiredFunctions(Expr* cpExpr);
    void requiredFunctionsHelper(Expr* cpExpr, std::set<std::string>& s);
    std::map<std::string, FunctionSignature*> funcNameToSignature;

    std::unique_ptr<LLVMContext> context;
    std::unique_ptr<IRBuilder<>> builder;
    std::unique_ptr<Module> module;
    ExitOnError EOE;
    std::unique_ptr<llvm::orc::LLJIT> JIT;
    orc::ResourceTrackerSP rt;
    FunctionRegistry *fr;
};

#endif