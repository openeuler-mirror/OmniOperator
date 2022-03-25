/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#ifndef OMNI_RUNTIME_DECIMAL_IR_BUILDER_H
#define OMNI_RUNTIME_DECIMAL_IR_BUILDER_H


#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Value.h>
#include "llvm/IR/IRBuilder.h"
#include "codegen_value.h"
#include "../type/data_type.h"
#include "codegen_utils.h"

class DecimalIRBuilder {
public:
    explicit DecimalIRBuilder(llvm::LLVMContext &context, llvm::Module &module, llvm::IRBuilder<> &builder,
        CodeGenUtils &codeGenUtils)
        : context(context), module(module), builder(builder), codeGenUtils(codeGenUtils)
    {
        this->AddScaleMultiplier();
    }
    virtual ~DecimalIRBuilder() = default;
    llvm::Value *CallDecimalFunction(const std::string &function_name, llvm::Type *return_type,
        const std::vector<llvm::Value *> &args);
    std::shared_ptr<DecimalValue> BuildDecimalValue(llvm::Value *data, omniruntime::type::DataType &retType,
        llvm::Value *isNull = nullptr);
    // Make from i128 value
    DecimalSplitValue Split(llvm::Value *fullValue);
    // Combine the two parts into an i128
    llvm::Value *ToInt128(llvm::Value *high, llvm::Value *low) const;
    void AddScaleMultiplier() const;
    void ScaleValues(llvm::Value &leftValue, llvm::Value &leftScale, llvm::Value &rightValue, llvm::Value &rightScale,
        llvm::Value **scaledLeft, llvm::Value **scaledRight);
    llvm::Value *ScaleValue(llvm::Value &value, llvm::Value &delta);
    llvm::Value *GetScaleMultiplier(llvm::Value &delta);
    llvm::Value *BuildIfElse(llvm::Value &condition, llvm::Type &return_type, std::function<llvm::Value *()> then_func,
        std::function<llvm::Value *()> else_func);
    friend class ExpressionCodeGen;

private:
    llvm::LLVMContext &context;
    llvm::Module &module;
    llvm::IRBuilder<> &builder;
    CodeGenUtils &codeGenUtils;
    const std::string scaleMultipliersName = "SCALE_MULTIPLIERS";
};


#endif // OMNI_RUNTIME_DECIMAL_IR_BUILDER_H
