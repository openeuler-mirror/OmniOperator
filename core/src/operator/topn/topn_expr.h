/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_TOPN_EXPR_H
#define OMNI_RUNTIME_TOPN_EXPR_H

#include "operator/operator_factory.h"
#include "operator/projection/projection.h"
#include "operator/topn/topn.h"
#include "type/data_types.h"

namespace omniruntime {
namespace op {
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
    std::vector<ProjFunc> projectFuncs;
    std::unique_ptr<TopNOperatorFactory> topNOperatorFactory;
};

class TopNWithExprOperator : public Operator {
public:
    TopNWithExprOperator(type::DataTypes sourceTypes, std::vector<int32_t> &sortCols,
        std::vector<ProjFunc> &projectFuncs, TopNOperator *topNOperator);

    ~TopNWithExprOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *inputVecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatches) override;

    OmniStatus Close() override;

private:
    omniruntime::type::DataTypes sourceTypes;
    std::vector<int32_t> sortCols;
    std::vector<ProjFunc> projectFuncs;
    TopNOperator *topNOperator;
    std::vector<VectorBatch *> inputVecBatches;
};
}
}
#endif // OMNI_RUNTIME_TOPN_EXPR_H
