/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: filter code generation methods
 */
#ifndef FILTER_CODEGEN_H
#define FILTER_CODEGEN_H

#include "expression_codegen.h"

class FilterCodeGen : public ExpressionCodeGen {
public:
    FilterCodeGen(std::string name, const omniruntime::expressions::Expr &expression)
        : ExpressionCodeGen(name, expression) {}
    ~FilterCodeGen() {}
    int64_t GetFunction() override;

    int64_t GetExpressionEvaluator();

private:
    int64_t CreateWrapper(llvm::Function &filter);
};
#endif