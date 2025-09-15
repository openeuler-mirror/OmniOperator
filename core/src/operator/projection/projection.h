/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Projection operator header
 */
#ifndef __PROJECTION_H__
#define __PROJECTION_H__

#include <vector>
#include "codegen/expr_evaluator.h"
#include "expression/expressions.h"
#include "operator/execution_context.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "operator/status.h"
#include "plannode/planNode.h"
#include "type/data_types.h"
#include "util/config/QueryConfig.h"
#include "vector/vector_common.h"

namespace omniruntime {
namespace op {
using namespace vec;
using namespace codegen;

class ProjectionOperator : public Operator {
public:
    explicit ProjectionOperator(std::shared_ptr<ExpressionEvaluator> &exprEvaluator)
        : projectedVecs(nullptr), exprEvaluator(exprEvaluator)
    {}

    ~ProjectionOperator() override = default;

    int32_t AddInput(VectorBatch *vecBatch) override;

    int32_t GetOutput(VectorBatch **outputVecBatch) override;

    OmniStatus Close() override;

private:
    VectorBatch *projectedVecs = nullptr;
    std::shared_ptr<ExpressionEvaluator> &exprEvaluator;
};

class ProjectionOperatorFactory : public OperatorFactory {
public:
    explicit ProjectionOperatorFactory(std::shared_ptr<ExpressionEvaluator> &&exprEvaluator)
        : exprEvaluator(std::move(exprEvaluator))
    {
        this->exprEvaluator->ProjectFuncGeneration();
    }

    ~ProjectionOperatorFactory() override = default;

    omniruntime::op::Operator *CreateOperator() override;

private:
    std::shared_ptr<ExpressionEvaluator> exprEvaluator;
};

OperatorFactory *CreateProjectOperatorFactory(
    std::shared_ptr<const ProjectNode> projectNode, const config::QueryConfig &queryConfig);
} // namespace op
} // namespace omniruntime

#endif
