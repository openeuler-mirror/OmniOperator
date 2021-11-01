/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#ifndef __EXPRESSION_CODEGEN_H__
#define __EXPRESSION_CODEGEN_H__

#include "./codegen_value.h"
#include "../common/expressions.h"
#include "../common/parser/parser.h"
#include "../common/expr_printer.h"
#include "./functions/mathfunctions.h"
#include "./functions/stringfunctions.h"
#include "./functions/murmur3_hash.h"
#include "./functions/decimalfunctions.h"
#include "./func_registry.h"
#include "../util/debug.h"

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
#include "llvm/IR/Instructions.h"
#include "llvm/Target/TargetMachine.h"

using CodeGenValuePtr = std::shared_ptr<CodeGenValue>;

class CodegenContext {
public:
    explicit CodegenContext() : data(nullptr), nullBitmap(nullptr), offsets(nullptr),
                                rowIdx(nullptr), isResultNull(nullptr), print(nullptr) {}

    explicit CodegenContext(llvm::Value *data, llvm::Value *nullBitmap, llvm::Value *offsets, llvm::Value *rowIdx,
                            llvm::Value *isResultNull, llvm::Value *executionContext, llvm::Value *dictionaryVectors) : data(data),
                            nullBitmap(nullBitmap), offsets(offsets), rowIdx(rowIdx), isResultNull(isResultNull),
                            executionContext(executionContext), dictionaryVectors(dictionaryVectors), print(nullptr) {}

    ~CodegenContext() {}

    friend class ExpressionCodeGen;

private:
    llvm::Value *data;
    llvm::Value *nullBitmap;
    llvm::Value *offsets;
    llvm::Value *rowIdx;
    // Boolean flag which contains 'OR' of nullBitmap[#xxx] utilized in expression evaluation
    // If true, it means that at least one column_value is null when processing the row.
    llvm::Value *isResultNull;
    llvm::Value *executionContext;
    llvm::Value *dictionaryVectors;
    llvm::FunctionCallee print;
};

// Given an expression generates the function for it.
class ExpressionCodeGen : public ExprVisitor {

public:
    ExpressionCodeGen(std::string name, omniruntime::expressions::Expr &expr,
                      std::vector<omniruntime::expressions::DataType> &datatypes);
    ~ExpressionCodeGen() override;

    std::string DumpCode();
    virtual int64_t GetFunction() = 0;

    // visitor methods
    void Visit(omniruntime::expressions::DataExpr &e) override;
    void Visit(omniruntime::expressions::UnaryExpr &e) override;
    void Visit(omniruntime::expressions::BinaryExpr &e) override;
    void Visit(omniruntime::expressions::InExpr &e) override;
    void Visit(omniruntime::expressions::BetweenExpr &e) override;
    void Visit(omniruntime::expressions::IfExpr &e) override;
    void Visit(omniruntime::expressions::CoalesceExpr &e) override;
    void Visit(omniruntime::expressions::IsNullExpr &e) override;
    void Visit(omniruntime::expressions::FuncExpr &e) override;

    // returns llvm value ptr of codegen functions
    CodeGenValuePtr VisitExpr(omniruntime::expressions::Expr &e);

// TODO: Figure out which of these can be private
protected:
    // Parse a generic expression by calling the helper functions below
    llvm::Value* CreateConstantBool(bool n);
    llvm::Value* CreateConstantInt(int32_t n);
    llvm::Value* CreateConstantLong(int64_t n);
    llvm::Value* CreateConstantDouble(double n);
    llvm::Value* GetIntToPtr(omniruntime::expressions::DataExpr &dExpr, llvm::Value *elementAddr);
    llvm::Type* ToLlvmType(omniruntime::expressions::DataType t);
    llvm::Type* GetFunctionReturnType(omniruntime::expressions::DataType t);
    llvm::Type* ToPointerType(omniruntime::expressions::DataType type);
    void PrintValues(std::string format, const std::vector<llvm::Value *>& values);
    llvm::Function* CreateFunction();

    // Helper functions for generating IR for operators and special forms
    llvm::Value* StringCmp(llvm::Value *lhs, llvm::Value *lLen, llvm::Value *rhs, llvm::Value *rLen);
    llvm::Value* Decimal128Cmp(const llvm::Value &lhs, const llvm::Value &rhs);

    // Helper functions and main function for parsing binary expressions
    llvm::Value *BinaryExprIntHelper(omniruntime::expressions::Operator op, llvm::Value *left, llvm::Value *right);
    llvm::Value *BinaryExprDoubleHelper(omniruntime::expressions::Operator op, llvm::Value *left, llvm::Value *right);
    llvm::Value *BinaryExprStringHelper(omniruntime::expressions::Operator op, llvm::Value *leftVal,
                                        llvm::Value *leftLen, llvm::Value *rightVal, llvm::Value *rightLen);
    llvm::Value *BinaryExprDecimalHelper(omniruntime::expressions::Operator op, llvm::Value *left, llvm::Value *right);

    // Helper functions and main function for parsing function expressions
    void FuncExprAbsHelper(
        omniruntime::expressions::FuncExpr &fExpr);
    void FuncExprSubstrHelper(omniruntime::expressions::FuncExpr &fExpr);
    void FuncExprCastHelper(omniruntime::expressions::FuncExpr &fExpr);
    void FuncExprConcatHelper(omniruntime::expressions::FuncExpr &fExpr);
    void FuncExprLikeHelper(omniruntime::expressions::FuncExpr &fExpr);
    void FuncExprMm3HashHelper(omniruntime::expressions::FuncExpr &fExpr);
    void FuncExprExtHelper(omniruntime::expressions::FuncExpr &fExpr);
    void FuncExprCombineHashHelper(omniruntime::expressions::FuncExpr &fExpr);

    // Helper functions and main function for parsing constant data expressions
    CodeGenValue *DataExprConstantHelper(omniruntime::expressions::DataExpr &dExpr);

    // Helper functions and main function for parsing if expressions
    llvm::Function* ConditionalHelper(omniruntime::expressions::DataType retType, omniruntime::expressions::Expr &cond,
        omniruntime::expressions::Expr &ifTrue, omniruntime::expressions::Expr &ifFalse);

    // Helper functions and main function for parsing coalesce expressions
    llvm::Function* CreateCoalesceFuncHelper(omniruntime::expressions::DataType retType,
        omniruntime::expressions::DataExpr &dExpr1, omniruntime::expressions::Expr &value2Expr);
    llvm::Function *CreateCoalesceFuncHelper2(omniruntime::expressions::DataExpr &dExpr1,
        omniruntime::expressions::Expr &value2Expr, llvm::Function &func);
    bool InitializeCodegenContext(llvm::iterator_range<llvm::Function::arg_iterator> args);
    CodeGenValuePtr value = nullptr;
    std::unique_ptr<CodegenContext> codegenContext;
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
    llvm::Function *func = nullptr;
    int numGlobalValues = 0;
};

#endif