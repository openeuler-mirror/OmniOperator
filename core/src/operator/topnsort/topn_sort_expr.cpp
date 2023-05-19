/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#include "topn_sort_expr.h"
#include "operator/util/operator_util.h"

namespace omniruntime::op {
TopNSortWithExprOperatorFactory::TopNSortWithExprOperatorFactory(const type::DataTypes &sourceDataTypes, int32_t n,
    const std::vector<omniruntime::expressions::Expr *> &partitionKeys,
    const std::vector<omniruntime::expressions::Expr *> &sortKeys, std::vector<int32_t> &sortAscendings,
    std::vector<int32_t> &sortNullFirsts, OverflowConfig *overflowConfig)
{
    std::vector<DataTypePtr> newSourceTypes;
    OperatorUtil::CreateProjectFuncs(sourceDataTypes, sortKeys, sortKeys.size(), newSourceTypes, this->projections,
        this->sortCols, this->projectFuncs, overflowConfig);

    this->sourceTypes = std::make_unique<DataTypes>(newSourceTypes);
    this->topNSortOperatorFactory = std::make_unique<TopNSortOperatorFactory>(*sourceTypes, n, this->partitionCols,
        this->sortCols, sortAscendings, sortNullFirsts);
}

TopNSortWithExprOperatorFactory::~TopNSortWithExprOperatorFactory() = default;

Operator *TopNSortWithExprOperatorFactory::CreateOperator()
{
    auto topNSortOperator = static_cast<TopNSortOperator *>(topNSortOperatorFactory->CreateOperator());
    auto pOperator =
        new TopNSortWithExprOperator(*sourceTypes, partitionCols, sortCols, projectFuncs, topNSortOperator);
    return pOperator;
}

TopNSortWithExprOperator::TopNSortWithExprOperator(const type::DataTypes &sourceTypes,
    std::vector<int32_t> &partitionCols, std::vector<int32_t> &sortCols, std::vector<ProjFunc> &projectFuncs,
    TopNSortOperator *topNSortOperator)
    : sourceTypes(sourceTypes),
      partitionCols(partitionCols),
      sortCols(sortCols),
      projectFuncs(projectFuncs),
      topNSortOperator(topNSortOperator)
{
    outputTypes = topNSortOperator->GetOutputType();
}

TopNSortWithExprOperator::~TopNSortWithExprOperator()
{
    delete topNSortOperator;
}

int32_t TopNSortWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    VectorBatch *newInputVecBatch = OperatorUtil::ProjectVectors(inputVecBatch, sourceTypes, projectFuncs, sortCols);
    if (newInputVecBatch != nullptr) {
        topNSortOperator->AddInput(newInputVecBatch);
        VectorHelper::FreeVecBatch(inputVecBatch);
    } else {
        topNSortOperator->AddInput(inputVecBatch);
    }
    SetStatus(topNSortOperator->GetStatus());
    return 0;
}

int32_t TopNSortWithExprOperator::GetOutput(VectorBatch **outputVecBatch)
{
    topNSortOperator->GetOutput(outputVecBatch);
    SetStatus(topNSortOperator->GetStatus());
    return 0;
}

OmniStatus TopNSortWithExprOperator::Close()
{
    return OMNI_STATUS_NORMAL;
}
}
