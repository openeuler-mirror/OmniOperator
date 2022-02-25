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
#include "../vector/vector_type.h"

class DecimalIRBuilder {
public:
    explicit DecimalIRBuilder(llvm::LLVMContext& context, llvm::Module& module, llvm::IRBuilder<> &builder) : context(context),
                module(module), builder(builder) {}
    virtual ~DecimalIRBuilder() = default;
    llvm::Value* CallDecimalFunction(const std::string& function_name,
                                     llvm::Type* return_type,
                                     const std::vector<llvm::Value*>& args);
    std::shared_ptr<DecimalValue> BuildDecimalValue(llvm::Value *data, omniruntime::vec::VecType &retType,
                                                    llvm::Value *isNull = nullptr);
    // Make from i128 value
    DecimalSplitValue Split(llvm::Value *fullValue);
    // Combine the two parts into an i128
    llvm::Value* ToInt128(llvm::Value *high, llvm::Value *low) const;
    friend class ExpressionCodeGen;
private:
    llvm::LLVMContext& context;
    llvm::Module& module;
    llvm::IRBuilder<> &builder;
};


#endif //OMNI_RUNTIME_DECIMAL_IR_BUILDER_H
