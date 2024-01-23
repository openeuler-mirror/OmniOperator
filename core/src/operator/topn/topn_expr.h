/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_TOPN_EXPR_H
#define OMNI_RUNTIME_TOPN_EXPR_H

#include "operator/projection/projection.h"
#include "operator/topn/topn.h"

namespace omniruntime::op {
class TopNWithExprOperatorFactory : public OperatorFactory {
public:
    TopNWithExprOperatorFactory(const type::DataTypes &sourceDataTypes, int32_t n,
        const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts,
        int32_t sortKeyCount, OverflowConfig *overflowConfig);

    ~TopNWithExprOperatorFactory() override;

    Operator *CreateOperator() override;

private:
    std::unique_ptr<DataTypes> sourceTypes;
    std::vector<int32_t> sortCols;
    std::vector<std::unique_ptr<Projection>> projections;
    std::unique_ptr<TopNOperatorFactory> topNOperatorFactory;
};

class TopNWithExprOperator : public Operator {
public:
    TopNWithExprOperator(const type::DataTypes &sourceTypes, std::vector<std::unique_ptr<Projection>> &projections,
        TopNOperator *topNOperator);

    ~TopNWithExprOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *inputVecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;

    OmniStatus Close() override;

private:
    omniruntime::type::DataTypes sourceTypes;
    std::vector<std::unique_ptr<Projection>> &projections;
    TopNOperator *topNOperator;
    ExecutionContext *executionContext;
};
}
#endif // OMNI_RUNTIME_TOPN_EXPR_H
