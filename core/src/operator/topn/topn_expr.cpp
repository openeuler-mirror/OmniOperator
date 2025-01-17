/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include "topn_expr.h"
#include "operator/util/operator_util.h"

namespace omniruntime::op {
TopNWithExprOperatorFactory::TopNWithExprOperatorFactory(const type::DataTypes &sourceDataTypes, int32_t limit,
    int32_t offset, const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAsc,
    int32_t *sortNullFirsts, int32_t sortKeyCount, OverflowConfig *overflowConfig)
{
    std::vector<DataTypePtr> newSourceTypes;
    OperatorUtil::CreateProjections(sourceDataTypes, sortKeys, newSourceTypes, this->projections, this->sortCols,
        overflowConfig);

    this->sourceTypes = std::make_unique<DataTypes>(newSourceTypes);
    this->topNOperatorFactory = std::make_unique<TopNOperatorFactory>(*sourceTypes,
    limit, offset, this->sortCols.data(), sortAsc, sortNullFirsts, sortKeyCount);
}

TopNWithExprOperatorFactory::~TopNWithExprOperatorFactory() = default;

Operator *TopNWithExprOperatorFactory::CreateOperator()
{
    auto topNOperator = static_cast<TopNOperator *>(topNOperatorFactory->CreateOperator());
    auto pOperator = new TopNWithExprOperator(*sourceTypes, projections, topNOperator);
    return pOperator;
}

TopNWithExprOperator::TopNWithExprOperator(const type::DataTypes &sourceTypes,
    std::vector<std::unique_ptr<Projection>> &projections, TopNOperator *topNOperator)
    : sourceTypes(sourceTypes), projections(projections), topNOperator(topNOperator)
{}

TopNWithExprOperator::~TopNWithExprOperator()
{
    delete topNOperator;
}

int32_t TopNWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    if (inputVecBatch->GetRowCount() <= 0) {
        VectorHelper::FreeVecBatch(inputVecBatch);
        this->ResetInputVecBatch();
        return 0;
    }

    auto *newInputVecBatch =
        OperatorUtil::ProjectVectors(inputVecBatch, sourceTypes, projections, executionContext.get());
    VectorHelper::FreeVecBatch(inputVecBatch);
    this->inputVecBatch = nullptr;

    topNOperator->SetInputVecBatch(newInputVecBatch);
    topNOperator->AddInput(newInputVecBatch);
    SetStatus(topNOperator->GetStatus());
    return 0;
}

int32_t TopNWithExprOperator::GetOutput(VectorBatch **outputVecBatch)
{
    topNOperator->GetOutput(outputVecBatch);
    SetStatus(topNOperator->GetStatus());
    return 0;
}

OmniStatus TopNWithExprOperator::Close()
{
    topNOperator->Close();
    return OMNI_STATUS_NORMAL;
}
}
