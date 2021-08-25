/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
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
#include <memory>
#include <vector>
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
#include "llvm/Target/TargetMachine.h"

// Given an expression generates the function for it.
class LLVMCodeGen {

public:
    LLVMCodeGen(std::string name, omniruntime::expressions::Expr &expr,
                std::vector<omniruntime::expressions::DataType> &datatypes);
    ~LLVMCodeGen();

    std::string DumpCode();
    virtual int64_t GetFunction() = 0;

// TODO: Figure out which of these can be private
protected:
    // Parse a generic expression by calling the helper functions below
    llvm::Value* ParseExpr(omniruntime::expressions::Expr &root, std::map<std::string, llvm::Value*>& args);

    llvm::Value* CreateConstantBool(bool n);
    llvm::Value* CreateConstantInt(int32_t n);
    llvm::Value* CreateConstantLong(int64_t n);
    llvm::Value* CreateConstantDouble(double n);
    llvm::Type* ToLlvmType(omniruntime::expressions::DataType t);

    llvm::Function* CreateFunction();

    // Parsing different kinds of expressions
    llvm::Value* ParseDataExpr(omniruntime::expressions::DataExpr &dExpr, std::map<std::string, llvm::Value*>& args);

    // Helper functions and main function for parsing binary expressions
    llvm::Value* ParseBinaryExpr(omniruntime::expressions::BinaryExpr &bExpr, std::map<std::string,
        llvm::Value*>& args);
    llvm::Value *ParseBinaryExprInt(omniruntime::expressions::Operator op, llvm::Value &left, llvm::Value &right);
    llvm::Value *ParseBinaryExprDouble(omniruntime::expressions::Operator op, llvm::Value &left, llvm::Value &right);
    llvm::Value *ParseBinaryExprString(omniruntime::expressions::Operator op, llvm::Value &left, llvm::Value &right);

    llvm::Value* ParseUnaryExpr(omniruntime::expressions::UnaryExpr &uExpr, std::map<std::string, llvm::Value*>& args);
    llvm::Value* ParseIfExpr(omniruntime::expressions::IfExpr &ifExpr, std::map<std::string, llvm::Value*>& args);
    llvm::Value* ParseInExpr(omniruntime::expressions::InExpr &inExpr, std::map<std::string, llvm::Value*>& args);
    llvm::Value* ParseBetweenExpr(omniruntime::expressions::BetweenExpr &btExpr, std::map<std::string,
        llvm::Value*>& args);
    llvm::Value* ParseCoalesceExpr(omniruntime::expressions::CoalesceExpr &cExpr, std::map<std::string,
        llvm::Value*>& args);

    // Helper functions and main function for parsing function expressions
    llvm::Value* ParseFuncExpr(omniruntime::expressions::FuncExpr &fExpr, std::map<std::string, llvm::Value*>& args);
    llvm::Value* ParseFuncExprAbs(omniruntime::expressions::FuncExpr &fExpr, std::map<std::string, llvm::Value*>& args);
    llvm::Value* ParseFuncExprSubstr(omniruntime::expressions::FuncExpr &fExpr, std::map<std::string,
                                     llvm::Value*>& args);
    llvm::Value* ParseFuncExprCast(omniruntime::expressions::FuncExpr &fExpr, std::map<std::string,
                                   llvm::Value*>& args);
    llvm::Value* ParseFuncExprExt(omniruntime::expressions::FuncExpr &fExpr, std::map<std::string, llvm::Value*>& args);

    // Helper functions for generating IR for operators and special forms
    llvm::Value* StringCmp(llvm::Value *lhs, llvm::Value *rhs);
    llvm::Function* CreateConditional(omniruntime::expressions::DataType retType, omniruntime::expressions::Expr &cond,
        omniruntime::expressions::Expr &ifTrue, omniruntime::expressions::Expr &ifFalse);
    llvm::Function* CreateCoalesceFunc(omniruntime::expressions::DataType retType,
        omniruntime::expressions::DataExpr &dExpr1, omniruntime::expressions::Expr &value2Expr);
    llvm::Function *CreateCoalesceFuncHelper(omniruntime::expressions::DataExpr &dExpr1,
                                          omniruntime::expressions::Expr &value2Expr, std::map<std::string,
                                          llvm::Value *> fArgs, llvm::Function &func);

    std::string funcName;
    omniruntime::expressions::Expr *expr = nullptr;
    std::vector<omniruntime::expressions::DataType> &datatypes;


    // Returns a set of all the required functions for a given row expression
    // Currently a separate function
    // Can be integrated with ParseRowExpression, but then the method declaration would need refactoring
    std::set<std::string> RequiredFunctions(omniruntime::expressions::Expr &cpExpr);
    void RequiredFunctionsHelper2(omniruntime::expressions::Expr &funcExpr, std::set<std::string> &s);
    void RequiredFunctionsHelper(omniruntime::expressions::Expr &cpExpr, std::set<std::string> &s);
    std::map<std::string, FunctionSignature> funcNameToSignature;

    std::unique_ptr<llvm::LLVMContext> context;
    std::unique_ptr<llvm::IRBuilder<>> builder;
    std::unique_ptr<llvm::Module> module;
    llvm::ExitOnError eoe;
    std::unique_ptr<llvm::orc::LLJIT> jit;
    llvm::orc::ResourceTrackerSP rt;
    FunctionRegistry *fr;
};

#endif