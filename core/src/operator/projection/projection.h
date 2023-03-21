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
#include "codegen/row_projection_codegen.h"

namespace omniruntime {
namespace op {
using namespace vec;
using namespace codegen;
/**
 * vector value addresses
 * vector null value addresses
 * vector offsets addresses
 * row index
 * int pointer to return length of varchar result
 * address of ExecutionContext
 * dictionary vector addresses
 * boolean pointer to return if results is null
 */
using RowProjFunc = void *(*)(int64_t *, int64_t *, int64_t *, int32_t, int32_t *, int64_t, int64_t *, bool *);

class RowProjection {
public:
    explicit RowProjection(const omniruntime::expressions::Expr &expression);
    ~RowProjection();
    RowProjFunc Create(OverflowConfig *overflowConfig);
    DataTypePtr GetReturnType();
    bool IsColumnProjection();
    int GetIndexIfColumnProjection();

private:
    std::unique_ptr<codegen::RowProjectionCodeGen> codegen = nullptr;
    const expressions::Expr *expression;
};

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