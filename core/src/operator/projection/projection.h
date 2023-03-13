/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Projection operator header
 */
#ifndef __PROJECTION_H__
#define __PROJECTION_H__

#include <vector>
#include "operator/operator_factory.h"
#include "operator/operator.h"
#include "operator/status.h"
#include "vector/vector_common.h"
#include "type/data_types.h"
#include "expression/expressions.h"
#include "operator/execution_context.h"
#include "codegen/expr_evaluator.h"

namespace omniruntime {
namespace op {
using namespace vec;
using namespace codegen;

class ProjectionOperator : public Operator {
public:
    explicit ProjectionOperator(ExecutionContext *context, std::shared_ptr<ExpressionEvaluator> &exprEvaluator)
        : projectedVecs(nullptr), exprEvaluator(exprEvaluator)
    {
        this->context = context;
        this->context->GetArena()->SetAllocator(vecAllocator);
    }

    ~ProjectionOperator() override
    {
        delete context;
    }

    int32_t AddInput(VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<VectorBatch *> &ret) override;
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
}
}

#endif