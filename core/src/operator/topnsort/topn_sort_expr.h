/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#ifndef OMNI_RUNTIME_TOPN_SORT_EXPR_H
#define OMNI_RUNTIME_TOPN_SORT_EXPR_H

#include "operator/topnsort/topn_sort.h"
#include "operator/projection/projection.h"

namespace omniruntime::op {
class TopNSortWithExprOperatorFactory : public OperatorFactory {
public:
    TopNSortWithExprOperatorFactory(const type::DataTypes &sourceDataTypes, int32_t n,
        const std::vector<omniruntime::expressions::Expr *> &partitionKeys,
        const std::vector<omniruntime::expressions::Expr *> &sortKeys, std::vector<int32_t> &sortAscendings,
        std::vector<int32_t> &sortNullFirsts, OverflowConfig *overflowConfig);

    ~TopNSortWithExprOperatorFactory() override;

    Operator *CreateOperator() override;

private:
    std::unique_ptr<DataTypes> sourceTypes;
    std::vector<int32_t> partitionCols;
    std::vector<int32_t> sortCols;
    std::vector<std::unique_ptr<Projection>> projections;
    std::vector<ProjFunc> projectFuncs;
    std::unique_ptr<TopNSortOperatorFactory> topNSortOperatorFactory;
};

class TopNSortWithExprOperator : public Operator {
public:
    TopNSortWithExprOperator(const type::DataTypes &sourceTypes, std::vector<int32_t> &partitionCols,
        std::vector<int32_t> &sortCols, std::vector<ProjFunc> &projectFuncs, TopNSortOperator *topNSortOperator);

    ~TopNSortWithExprOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *inputVecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;

    OmniStatus Close() override;

private:
    omniruntime::type::DataTypes sourceTypes;
    std::vector<int32_t> partitionCols;
    std::vector<int32_t> sortCols;
    std::vector<ProjFunc> projectFuncs;
    TopNSortOperator *topNSortOperator;
    std::vector<vec::VectorBatch *> inputVecBatches;
};
}
#endif // OMNI_RUNTIME_TOPN_SORT_EXPR_H
