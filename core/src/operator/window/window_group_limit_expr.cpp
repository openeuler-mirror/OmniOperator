/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 * @Description: window group limit operator implementations
 */

#include "window_group_limit_expr.h"
#include "operator/util/operator_util.h"

namespace omniruntime::op {
WindowGroupLimitWithExprOperatorFactory::WindowGroupLimitWithExprOperatorFactory(const type::DataTypes &sourceDataTypes,
    int32_t n, const std::string funcName, const std::vector<omniruntime::expressions::Expr *> &partitionKeys,
    const std::vector<omniruntime::expressions::Expr *> &sortKeys, std::vector<int32_t> &sortAscendings,
    std::vector<int32_t> &sortNullFirsts, OverflowConfig *overflowConfig)
{
    std::vector<DataTypePtr> sourceTypesForPartition;
    OperatorUtil::CreateProjections(sourceDataTypes, partitionKeys, sourceTypesForPartition, this->projections,
        this->partitionCols, overflowConfig);

    DataTypes newSourceDataTypes(sourceTypesForPartition);
    std::vector<DataTypePtr> sourceTypesForSort;
    OperatorUtil::CreateProjections(newSourceDataTypes, sortKeys, sourceTypesForSort, this->projections, this->sortCols,
        overflowConfig);

    this->sourceTypes = std::make_unique<DataTypes>(sourceTypesForSort);
    this->windowGroupLimitOperatorFactory = std::make_unique<WindowGroupLimitOperatorFactory>(*sourceTypes, n, funcName,
        this->partitionCols, this->sortCols, sortAscendings, sortNullFirsts);
}

WindowGroupLimitWithExprOperatorFactory::~WindowGroupLimitWithExprOperatorFactory() = default;

Operator *WindowGroupLimitWithExprOperatorFactory::CreateOperator()
{
    auto windowGroupLimitOperator =
        static_cast<WindowGroupLimitOperator *>(windowGroupLimitOperatorFactory->CreateOperator());
    auto opOperator = new WindowGroupLimitWithExprOperator(*sourceTypes, partitionCols, sortCols, projections,
        windowGroupLimitOperator);
    return opOperator;
}

WindowGroupLimitWithExprOperator::WindowGroupLimitWithExprOperator(const type::DataTypes &sourceTypes,
    std::vector<int32_t> &partitionCols, std::vector<int32_t> &sortCols,
    std::vector<std::unique_ptr<Projection>> &projections, WindowGroupLimitOperator *windowGroupLimitOperator)
    : sourceTypes(sourceTypes), projections(projections), windowGroupLimitOperator(windowGroupLimitOperator)
{}

WindowGroupLimitWithExprOperator::~WindowGroupLimitWithExprOperator()
{
    delete windowGroupLimitOperator;
}

int32_t WindowGroupLimitWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    if (inputVecBatch->GetRowCount() <= 0) {
        VectorHelper::FreeVecBatch(inputVecBatch);
        this->ResetInputVecBatch();
        return 0;
    }
    auto *newInputVecBatch =
        OperatorUtil::ProjectVectors(inputVecBatch, sourceTypes, projections, executionContext.get());
    VectorHelper::FreeVecBatch(inputVecBatch);
    this->ResetInputVecBatch();
    windowGroupLimitOperator->AddInput(newInputVecBatch);
    SetStatus(windowGroupLimitOperator->GetStatus());
    return 0;
}

int32_t WindowGroupLimitWithExprOperator::GetOutput(VectorBatch **outputVecBatch)
{
    windowGroupLimitOperator->GetOutput(outputVecBatch);
    SetStatus(windowGroupLimitOperator->GetStatus());
    return 0;
}

OmniStatus WindowGroupLimitWithExprOperator::Close()
{
    windowGroupLimitOperator->Close();
    return OMNI_STATUS_NORMAL;
}
}
