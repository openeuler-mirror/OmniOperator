/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_TOPN_EXPR_H
#define OMNI_RUNTIME_TOPN_EXPR_H

#include "../operator_factory.h"
#include "../projection/projection.h"
#include "../topn/topn.h"
#include "../../vector/vector_types.h"

namespace omniruntime {
namespace op {
class TopNWithExprOperatorFactory : public OperatorFactory {
public:
    TopNWithExprOperatorFactory(const vec::VecTypes &sourceVecTypes, int32_t n,
        const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings,
        int32_t *sortNullFirsts, int32_t sortKeyCount);

    ~TopNWithExprOperatorFactory() override;

    Operator *CreateOperator() override;

private:
    std::unique_ptr<vec::VecTypes> sourceTypes;
    std::vector<int32_t> sortCols;
    std::vector<std::unique_ptr<RowProjection>> rowProjections;
    std::vector<RowProjFunc> projectFuncs;
    std::unique_ptr<TopNOperatorFactory> topNOperatorFactory;
};

class TopNWithExprOperator : public Operator {
public:
    TopNWithExprOperator(const vec::VecTypes &sourceTypes, std::vector<int32_t> &sortCols,
        std::vector<RowProjFunc> &projectFuncs, TopNOperator *topNOperator);

    ~TopNWithExprOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *inputVecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatches) override;

    OmniStatus Close() override;

private:
    const omniruntime::vec::VecTypes &sourceTypes;
    std::vector<int32_t> sortCols;
    std::vector<RowProjFunc> projectFuncs;
    TopNOperator *topNOperator;
    std::vector<VectorBatch *> inputVecBatches;
};
}
}
#endif //OMNI_RUNTIME_TOPN_EXPR_H
