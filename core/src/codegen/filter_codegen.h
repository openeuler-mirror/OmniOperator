/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: filter code generation methods
 */
#ifndef FILTER_CODEGEN_H
#define FILTER_CODEGEN_H

#include "llvm_codegen.h"

class FilterCodeGen : public LLVMCodeGen {
public:
    FilterCodeGen(std::string name, omniruntime::expressions::Expr &expression,
                  std::vector<omniruntime::expressions::DataType> &datatypes)
        :LLVMCodeGen(name, expression, datatypes) {}
    ~FilterCodeGen() {}
    int64_t GetFunction() override;

    int64_t GetExpressionEvaluator();

private:
    int64_t CreateWrapper(llvm::Function &filter);
};
#endif