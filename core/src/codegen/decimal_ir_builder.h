/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#ifndef OMNI_RUNTIME_DECIMAL_IR_BUILDER_H
#define OMNI_RUNTIME_DECIMAL_IR_BUILDER_H

#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Value.h>
#include <expression/expressions.h>
#include "llvm/IR/IRBuilder.h"
#include "codegen_value.h"
#include "type/data_type.h"
#include "llvm_engine.h"

class DecimalIRBuilder {
public:
    explicit DecimalIRBuilder(LLVMEngine &engine) : engine(engine)
    {
        context = engine.GetContext();
        builder = engine.GetIRBuilder();
        module = engine.GetModule();
    }
    virtual ~DecimalIRBuilder() = default;

    llvm::Value *CallDecimalFunction(const std::string &function_name, llvm::Type *return_type,
        const std::vector<llvm::Value *> &args, llvm::Value *executionContextPtr = nullptr,
        omniruntime::op::OverflowConfig *overflowConfig = nullptr, llvm::Value *overflowNull = nullptr);

    std::shared_ptr<DecimalValue> BuildDecimalValue(llvm::Value *data, omniruntime::type::DataType &retType,
        llvm::Value *isNull = nullptr);
    // Make from i128 value
    DecimalSplitValue Split(llvm::Value *fullValue);
    // Combine the two parts into an i128
    llvm::Value *ToInt128(llvm::Value *high, llvm::Value *low) const;

    friend class ExpressionCodeGen;


private:
    LLVMEngine &engine;
    llvm::LLVMContext *context;
    llvm::IRBuilder<> *builder;
    llvm::Module *module;
};


#endif // OMNI_RUNTIME_DECIMAL_IR_BUILDER_H
