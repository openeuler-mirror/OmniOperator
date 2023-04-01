/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#ifndef OMNI_RUNTIME_CODEGEN_CONTEXT_H
#define OMNI_RUNTIME_CODEGEN_CONTEXT_H

#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Value.h"

namespace omniruntime::codegen {
class CodegenContext {
public:
    explicit CodegenContext()
        : data(nullptr),
          nullBitmap(nullptr),
          offsets(nullptr),
          rowIdx(nullptr),
          executionContext(nullptr),
          dictionaryVectors(nullptr),
          print(nullptr)
    {}

    explicit CodegenContext(llvm::Value *data, llvm::Value *nullBitmap, llvm::Value *offsets, llvm::Value *rowIdx,
        llvm::Value *executionContext, llvm::Value *dictionaryVectors)
        : data(data),
          nullBitmap(nullBitmap),
          offsets(offsets),
          rowIdx(rowIdx),
          executionContext(executionContext),
          dictionaryVectors(dictionaryVectors),
          print(nullptr)
    {}

    ~CodegenContext() = default;

    friend class ExpressionCodeGen;

    friend class SimpleFilterCodeGen;

    friend class CodegenBase;

private:
    llvm::Value *data;
    llvm::Value *nullBitmap;
    llvm::Value *offsets;
    llvm::Value *rowIdx;
    llvm::Value *executionContext;
    llvm::Value *dictionaryVectors;
    llvm::FunctionCallee print;
};
}

#endif // OMNI_RUNTIME_CODEGEN_CONTEXT_H
