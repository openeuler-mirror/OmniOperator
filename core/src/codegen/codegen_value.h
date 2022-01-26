/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Value object contains reference to data, isNull and varchar length
 */
#ifndef OMNI_RUNTIME_CODEGEN_VALUE_H
#define OMNI_RUNTIME_CODEGEN_VALUE_H

#include <llvm/IR/Value.h>

class CodeGenValue {
public:
    explicit CodeGenValue(llvm::Value *data,
                          llvm::Value *isNull,
                          llvm::Value *length = nullptr): data(data), isNull(isNull), length(length) {}

    virtual ~CodeGenValue() = default;
    bool IsValidValue() { return this->data != nullptr;}

    friend class ExpressionCodeGen;
    friend class RowExpressionCodeGen;

private:
    llvm::Value* data;
    llvm::Value* isNull;
    llvm::Value* length;
};


#endif // OMNI_RUNTIME_CODEGEN_VALUE_H

