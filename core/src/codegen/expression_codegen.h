/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#ifndef __EXPRESSION_CODEGEN_H__
#define __EXPRESSION_CODEGEN_H__

#include <iostream>
#include <string>
#include <memory>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>

#include <llvm/ADT/APInt.h>
#include <llvm/ADT/APFloat.h>
#include <llvm/ADT/STLExtras.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/Support/Error.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/Instructions.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <llvm/ExecutionEngine/Orc/LLJIT.h>

#include "codegen_value.h"
#include "codegen_context.h"
#include "expression/expressions.h"
#include "expression/parser/parser.h"
#include "expression/expr_printer.h"
#include "util/debug.h"
#include "llvm_types.h"
#include "decimal_ir_builder.h"
#include "llvm_engine.h"


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
    void Visit(const omniruntime::expressions::LiteralExpr &e) override;
    void Visit(const omniruntime::expressions::FieldExpr &e) override;
    void Visit(const omniruntime::expressions::UnaryExpr &e) override;
    void Visit(const omniruntime::expressions::BinaryExpr &e) override;
    void Visit(const omniruntime::expressions::InExpr &e) override;
    void Visit(const omniruntime::expressions::BetweenExpr &e) override;
    void Visit(const omniruntime::expressions::IfExpr &e) override;
    void Visit(const omniruntime::expressions::CoalesceExpr &e) override;
    void Visit(const omniruntime::expressions::IsNullExpr &e) override;
    void Visit(const omniruntime::expressions::FuncExpr &e) override;
    void Visit(const omniruntime::expressions::SwitchExpr &e) override;

    // returns llvm value ptr of codegen functions
    CodeGenValuePtr VisitExpr(const omniruntime::expressions::Expr &e);
    void ExtractVectorIndexes();
    std::set<int32_t> vectorIndexes;

    std::vector<llvm::Value*> GetFunctionArgValues(const omniruntime::expressions::FuncExpr &fExpr,
                                                   llvm::Value **isAnyNull, bool &isInvalidExpr);
    // TODO: Figure out which of these can be private
protected:
    // Util functions
    std::vector<llvm::Type *> GetFunctionArgTypeVector(std::vector<omniruntime::type::DataTypeId> &params,
        omniruntime::type::DataTypeId &retTypeId, bool needsContext);

    llvm::Value *GetIntToPtr(omniruntime::type::DataTypeId typeId, llvm::Value *elementAddr);
    llvm::Constant *CreateStringConstant(std::string s);
    void PrintValues(std::string format, const std::vector<llvm::Value *> &values);
    // Helper functions for generating IR for operators and special forms
    llvm::Value *StringCmp(llvm::Value *lhs, llvm::Value *lLen, llvm::Value *rhs, llvm::Value *rLen);
    // Helper functions and main function for parsing binary expressions
    llvm::Value *HandleDivisionByZero(llvm::Value *divisorValue, omniruntime::type::DataTypeId type);
    llvm::Value *BinaryExprIntHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    llvm::Value *BinaryExprDoubleHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    llvm::Value *BinaryExprLongHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    llvm::Value *BinaryExprStringHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *leftVal,
        llvm::Value *leftLen, llvm::Value *rightVal, llvm::Value *rightLen, llvm::Value *leftIsNull,
        llvm::Value *rightIsNull);
    void BinaryExprDecimalHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    void BinaryExprNullHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull, llvm::PHINode **leftPhi,
        llvm::PHINode **rightPhi, llvm::Value **isNeitherNull);
    void DivExprNullHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull, llvm::PHINode **leftPhi,
        llvm::PHINode **rightPhi);
    void HandleCoalesceDecimals(CodeGenValue &v1, CodeGenValue &v2, llvm::BasicBlock &isNotNullBlock,
        llvm::BasicBlock &isNullBlock, llvm::PHINode &pn, llvm::PHINode &pnNull);
    // Helper functions and main function for parsing constant data expressions
    CodeGenValue *LiteralExprConstantHelper(const omniruntime::expressions::LiteralExpr &lExpr);
    static bool AreInvalidDataTypes(omniruntime::type::DataTypeId type1, omniruntime::type::DataTypeId type2);

    std::pair<llvm::Value *, llvm::Value *> RescaleDecimals(omniruntime::expressions::Expr &expr, CodeGenValue &left,
        CodeGenValue &right, int scaleDiff, omniruntime::type::DataTypeId typeId);

    bool VisitBetweenExprHelper(omniruntime::expressions::BetweenExpr &bExpr, const std::shared_ptr<CodeGenValue> &val,
        const std::shared_ptr<CodeGenValue> &lowerVal, const std::shared_ptr<CodeGenValue> &upperVal,
        std::pair<llvm::Value **, llvm::Value **> cmpPair);

    void Decimal64Helper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left, llvm::Value *right,
        llvm::Value *leftIsNull, llvm::Value *rightIsNull);

    virtual llvm::Function *CreateFunction();
    llvm::LLVMContext* GetContext() {return llvmEngine->GetContext();}
    llvm::IRBuilder<>* GetIRBuilder() {return llvmEngine->GetIRBuilder();}
    llvm::Module* GetModule() {return llvmEngine->GetModule();}
    llvm::orc::LLJIT* GetJit() {return llvmEngine->GetJit();}
    LLVMTypes* GetTypes() {return llvmEngine->GetTypes();}
    std::unique_ptr<DecimalIRBuilder> GetDecimalIRBuilder() {return std::make_unique<DecimalIRBuilder>(*llvmEngine);}

    const omniruntime::expressions::Expr *expr;
    std::unique_ptr<LLVMEngine> llvmEngine;
    llvm::LLVMContext* context;
    llvm::IRBuilder<>* builder;
    llvm::Module* module;
    llvm::orc::LLJIT* jit;
    llvm::ExitOnError eoe;
    LLVMTypes* llvmTypes;
    std::unique_ptr<DecimalIRBuilder> decimalIRBuilder;
    llvm::orc::ResourceTrackerSP rt;
    llvm::Function *func = nullptr;
    CodeGenValuePtr value = nullptr;
    std::unique_ptr<CodegenContext> codegenContext;
    int numGlobalValues = 0;

private:
    std::string funcName;
    bool InitializeCodegenContext(llvm::iterator_range<llvm::Function::arg_iterator> args);
    llvm::Value *GetDictionaryVectorValue(const omniruntime::type::DataType &dataType, llvm::Value *rowIdx,
        llvm::Value *dictionaryVectorPtr, llvm::AllocaInst *&lengthAllocaInst);
    void Decimal64MultiplyHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *output,
        llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    void InExprIntegerHelper(CodeGenValuePtr &argiValue, CodeGenValuePtr &valueToCompare, llvm::Value *&tmpCmpData,
        llvm::Value *&tmpCmpNull);
    void InExprDecimal64Helper(const omniruntime::expressions::InExpr &inExpr, size_t i,
        CodeGenValuePtr &valueToCompare, CodeGenValuePtr &argiValue, llvm::Value *&tmpCmpData,
        llvm::Value *&tmpCmpNull);
    void InExprDoubleHelper(CodeGenValuePtr &argiValue, CodeGenValuePtr &valueToCompare, llvm::Value *&tmpCmpData,
        llvm::Value *&tmpCmpNull);
    void InExprStringHelper(CodeGenValuePtr &argiValue, CodeGenValuePtr &valueToCompare, llvm::Value *&tmpCmpData,
        llvm::Value *&tmpCmpNull);
    void InExprDecimal128Helper(const omniruntime::expressions::InExpr &inExpr, llvm::Type *retType, size_t i,
        CodeGenValuePtr &valueToCompare, CodeGenValuePtr &argiValue, llvm::Value *&tmpCmpData,
        llvm::Value *&tmpCmpNull);
    std::vector<llvm::Value*> GetNullResultIfNullArgFunctionArgValues(const omniruntime::expressions::FuncExpr &fExpr,
                                                                   llvm::Value **isAnyNull, bool &isInvalidExpr);
    std::vector<llvm::Value*> GetValidNotNullResultFunctionArgValues(const omniruntime::expressions::FuncExpr &fExpr,
                                                                   llvm::Value **isAnyNull, bool &isInvalidExpr);
    std::vector<llvm::Value*> GetNotNullResultFunctionArgValues(const omniruntime::expressions::FuncExpr &fExpr,
                                                                   llvm::Value **isAnyNull, bool &isInvalidExpr);
    std::vector<llvm::Value*> GetDefaultFunctionArgValues(const omniruntime::expressions::FuncExpr &fExpr,
                                                                   llvm::Value **isAnyNull, bool &isInvalidExpr);
};

#endif