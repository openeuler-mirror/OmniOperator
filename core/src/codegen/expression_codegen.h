/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#ifndef __EXPRESSION_CODEGEN_H__
#define __EXPRESSION_CODEGEN_H__

#include "./codegen_value.h"

#include <iostream>
#include <string>
#include <memory>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>

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
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/SourceMgr.h"
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include "llvm/IR/Instructions.h"
#include "llvm/Target/TargetMachine.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"

#include "./codegen_context.h"
#include "../common/expressions.h"
#include "../common/parser/parser.h"
#include "../common/expr_printer.h"
#include "../util/debug.h"
#include "llvm_types.h"

using CodeGenValuePtr = std::shared_ptr<CodeGenValue>;

// Given an expression generates the function for it.
class ExpressionCodeGen : public ExprVisitor {
public:
    ExpressionCodeGen(std::string name, const omniruntime::expressions::Expr &cpExpr);
    ~ExpressionCodeGen() override;

    void Initialize();
    std::string DumpCode();
    virtual int64_t GetFunction() = 0;

    // visitor methods
    void Visit(const omniruntime::expressions::DataExpr &e) override;
    void Visit(const omniruntime::expressions::UnaryExpr &e) override;
    void Visit(const omniruntime::expressions::BinaryExpr &e) override;
    void Visit(const omniruntime::expressions::InExpr &e) override;
    void Visit(const omniruntime::expressions::BetweenExpr &e) override;
    void Visit(const omniruntime::expressions::IfExpr &e) override;
    void Visit(const omniruntime::expressions::CoalesceExpr &e) override;
    void Visit(const omniruntime::expressions::IsNullExpr &e) override;
    void Visit(const omniruntime::expressions::FuncExpr &e) override;

    // returns llvm value ptr of codegen functions
    CodeGenValuePtr VisitExpr(const omniruntime::expressions::Expr &e);
    std::set<int32_t> vectorIndexes;

    // TODO: Figure out which of these can be private
protected:
    // Util functions
    llvm::Value* GetIntToPtr(const omniruntime::expressions::DataExpr &dExpr, llvm::Value *elementAddr);
    std::vector<llvm::Type*> GetFunctionArgTypeVector(std::vector<VecTypeId> &params, VecTypeId &retTypeId,
                                                bool needsContext);
    void PrintValues(std::string format, const std::vector<llvm::Value *>& values);
    // Helper functions for generating IR for operators and special forms
    llvm::Value *StringCmp(llvm::Value *lhs, llvm::Value *lLen, llvm::Value *rhs, llvm::Value *rLen);
    llvm::Value *Decimal128Cmp(const llvm::Value &lhs, const llvm::Value &rhs);

    // Helper functions and main function for parsing binary expressions
    llvm::Value *BinaryExprIntHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    llvm::Value *BinaryExprDoubleHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    llvm::Value *BinaryExprStringHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *leftVal,
        llvm::Value *leftLen, llvm::Value *rightVal, llvm::Value *rightLen, llvm::Value *leftIsNull,
        llvm::Value *rightIsNull);
    llvm::Value *BinaryExprDecimalHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    void BinaryExprNullHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull, llvm::PHINode **leftPhi,
        llvm::PHINode **rightPhi, llvm::Value **isNeitherNull);
    void DivExprNullHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
                              llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull,
                              llvm::PHINode **leftPhi, llvm::PHINode **rightPhi);
    // Helper functions and main function for parsing constant data expressions
    CodeGenValue *DataExprConstantHelper(const omniruntime::expressions::DataExpr &dExpr);

    virtual llvm::Function *CreateFunction();
    void OptimizeFunctionsAndModule();

    const omniruntime::expressions::Expr *expr;
    std::unique_ptr<llvm::LLVMContext> context;
    std::unique_ptr<llvm::IRBuilder<>> builder;
    std::unique_ptr<llvm::Module> module;
    llvm::ExitOnError eoe;
    std::unique_ptr<llvm::legacy::FunctionPassManager> fpm = nullptr;
    llvm::legacy::PassManager mpm;
    std::unique_ptr<llvm::orc::LLJIT> jit;
    llvm::orc::ResourceTrackerSP rt;
    llvm::Function *func = nullptr;
    CodeGenValuePtr value = nullptr;
    std::unique_ptr<CodegenContext> codegenContext;
    int numGlobalValues = 0;
    std::unique_ptr<LLVMTypes> llvmTypes;

private:
    std::string funcName;
    std::map<std::string, FunctionSignature> funcNameToSignature;
    static void InitializeCodegenTargets();
    void RegisterFunctions(std::vector<omniruntime::Function> func);
    void RegisterFunctionsHelper(omniruntime::Function &func, std::set<std::string> jitRegisteredFuncs);
    bool InitializeCodegenContext(llvm::iterator_range<llvm::Function::arg_iterator> args);
    llvm::Value *GetDictionaryVectorValue(omniruntime::vec::VecType vectorType, llvm::Value *rowIdx,
        llvm::Value *dictionaryVectorPtr, llvm::AllocaInst *&lengthAllocaInst);
    void CreateOrExprHelper(llvm::Value *leftValue, llvm::Value *leftNull, llvm::Value *rightValue,
        llvm::Value *rightNull);
};

#endif