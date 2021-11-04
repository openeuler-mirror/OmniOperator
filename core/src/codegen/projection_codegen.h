/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: project  codegen
 */
#ifndef PROJECTION_CODEGEN_H
#define PROJECTION_CODEGEN_H

#include "expression_codegen.h"

class ProjectionCodeGen : public ExpressionCodeGen {
public:
    ProjectionCodeGen(std::string name, omniruntime::expressions::Expr &expr, bool filter)
        : ExpressionCodeGen(name, expr), filter(filter) {}
    ~ProjectionCodeGen() {}
    int64_t GetFunction() override;
    bool IsFilterEnabled() const
    {
        return filter;
    }
    int64_t GetExpressionEvaluator();

private:
    int64_t CreateWrapper(llvm::Function &proj);
    bool filter;
};

#endif