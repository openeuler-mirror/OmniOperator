/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch expression codegen
 */

#ifndef OMNI_RUNTIME_BATCH_EXPRESSION_CODEGEN_H
#define OMNI_RUNTIME_BATCH_EXPRESSION_CODEGEN_H

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
#include "batch_codegen_context.h"
#include "expression/expressions.h"
#include "expression/parser/parser.h"
#include "expression/expr_printer.h"
#include "util/debug.h"
#include "llvm_types.h"
#include "decimal_ir_builder.h"
#include "llvm_engine.h"
#include "operator/config/operator_config.h"

using CodeGenValuePtr = std::shared_ptr<CodeGenValue>;

// Given an expression generates the batch function for it.
class BatchExpressionCodeGen : public ExprVisitor {
public:
    BatchExpressionCodeGen(std::string name, const omniruntime::expressions::Expr &cpExpr,
        omniruntime::op::OverflowConfig *overflowConfig);
    ~BatchExpressionCodeGen() override
    {
        if (rt) {
            eoe(rt->remove());
        }
    }

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

    CodeGenValuePtr VisitExpr(const omniruntime::expressions::Expr &e);
    void ExtractVectorIndexes();
    std::set<int32_t> vectorIndexes;
    std::vector<llvm::Value *> GetFunctionArgValues(const omniruntime::expressions::FuncExpr &fExpr,
        llvm::AllocaInst *isAnyNull, bool &isInvalidExpr);

protected:
    // Util functions
    llvm::Constant *CreateConstantString(std::string s);
    CodeGenValue *LiteralExprConstantHelper(const omniruntime::expressions::LiteralExpr &lExpr);
    static bool AreInvalidDataTypes(omniruntime::type::DataTypeId type1, omniruntime::type::DataTypeId type2);

    virtual llvm::Function *CreateBatchFunction();

    llvm::LLVMContext *GetContext()
    {
        return llvmEngine->GetContext();
    }

    llvm::IRBuilder<> *GetIRBuilder()
    {
        return llvmEngine->GetIRBuilder();
    }

    llvm::Module *GetModule()
    {
        return llvmEngine->GetModule();
    }

    llvm::orc::LLJIT *GetJit()
    {
        return llvmEngine->GetJit();
    }

    LLVMTypes *GetTypes()
    {
        return llvmEngine->GetTypes();
    }

    std::unique_ptr<DecimalIRBuilder> GetDecimalIRBuilder()
    {
        return std::make_unique<DecimalIRBuilder>(*llvmEngine);
    }

    const omniruntime::expressions::Expr *expr;
    std::unique_ptr<LLVMEngine> llvmEngine;
    llvm::LLVMContext *context;
    llvm::IRBuilder<> *builder;
    llvm::Module *module;
    llvm::orc::LLJIT *jit;
    llvm::ExitOnError eoe;
    LLVMTypes *llvmTypes;
    std::unique_ptr<DecimalIRBuilder> decimalIRBuilder;
    llvm::orc::ResourceTrackerSP rt;
    llvm::Function *func = nullptr;
    CodeGenValuePtr value = nullptr;
    std::unique_ptr<BatchCodegenContext> batchCodegenContext;
    int numGlobalValues = 0;
    omniruntime::op::OverflowConfig *overflowConfig;

    llvm::AllocaInst *GetResultArray(omniruntime::type::DataTypeId dataTypeId, llvm::Value *rowCnt);

private:
    std::string funcName;
    bool InitializeCodegenContext(llvm::iterator_range<llvm::Function::arg_iterator> args);
    llvm::Value *GetDictionaryVectorValue(const omniruntime::expressions::DataType &dataType, llvm::Value *rowIdxArray,
        llvm::Value *rowCnt, llvm::Value *dictionaryVectorPtr, llvm::AllocaInst *lengthArrayPtr);
    llvm::Value *GetVectorValue(const omniruntime::expressions::DataType &dataType, llvm::Value *rowIdxArray,
        llvm::Value *rowCnt, llvm::Value *dataVectorPtr, llvm::Value *offsetArray, llvm::Value *lengthArrayPtr);
    void BinaryExprIntHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    void BinaryExprDoubleHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    void BinaryExprLongHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    void BinaryExprStringHelper(const omniruntime::expressions::BinaryExpr *binaryExpr, llvm::Value *left,
        llvm::Value *leftLen, llvm::Value *right, llvm::Value *rightLen, llvm::Value *leftIsNull,
        llvm::Value *rightIsNull);
    void BinaryExprDecimal64Helper(const omniruntime::expressions::BinaryExpr *binaryExpr, DecimalValue &left,
        DecimalValue &right, llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    void BinaryExprDecimal128Helper(const omniruntime::expressions::BinaryExpr *binaryExpr, DecimalValue &left,
        DecimalValue &right, llvm::Value *leftIsNull, llvm::Value *rightIsNull);
    bool VisitBetweenExprHelper(omniruntime::expressions::BetweenExpr &bExpr, const std::shared_ptr<CodeGenValue> &val,
        const std::shared_ptr<CodeGenValue> &lowerVal, const std::shared_ptr<CodeGenValue> &upperVal,
        std::pair<llvm::AllocaInst **, llvm::AllocaInst **> cmpPair);
    std::vector<llvm::Value *> GetDataArgs(const omniruntime::expressions::FuncExpr &fExpr, llvm::AllocaInst *isAnyNull,
        bool &isInvalidExpr);
    std::vector<llvm::Value *> GetDataAndNullArgs(const omniruntime::expressions::FuncExpr &fExpr,
        llvm::AllocaInst *isAnyNull, bool &isInvalidExpr);
    std::vector<llvm::Value *> GetDefaultFunctionArgValues(const omniruntime::expressions::FuncExpr &fExpr,
        llvm::AllocaInst *isAnyNull, bool &isInvalidExpr);
    std::vector<llvm::Value *> GetDataAndOverflowNullArgs(const omniruntime::expressions::FuncExpr &fExpr,
        llvm::AllocaInst *isAnyNull, bool &isInvalidExpr, llvm::AllocaInst *overflowNull);

    void FuncExprOverflowNullHelper(const omniruntime::expressions::FuncExpr &e);
};

#endif // OMNI_RUNTIME_BATCH_EXPRESSION_CODEGEN_H
