/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: project  codegen
 */
#ifndef PROJECTION_CODEGEN_H
#define PROJECTION_CODEGEN_H

#include <utility>

#include "expression_codegen.h"
#include "util/type_util.h"

namespace omniruntime {
class ProjectionCodeGen : public ExpressionCodeGen {
public:
    /* *
     * Method to create and initialize a ProjectionCodeGen instance
     *
     * @param name Name for ProjectionCodeGen module
     * @param expression the projection expression
     * @return unique_ptr to the ProjectionCodeGen instance
     */
    static std::unique_ptr<ProjectionCodeGen> Create(std::string name, const omniruntime::expressions::Expr &expression,
        bool filter);

    ~ProjectionCodeGen() override = default;

    int64_t GetFunction() override;

    int64_t GetExpressionEvaluator();

private:
    ProjectionCodeGen(std::string name, const omniruntime::expressions::Expr &expr, bool filter)
        : ExpressionCodeGen(std::move(name), expr), filter(filter)
    {}
    int64_t CreateWrapper(llvm::Function &proj);
    bool filter;
};
}
#endif