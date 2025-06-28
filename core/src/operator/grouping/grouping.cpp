/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "grouping.h"
#include "operator/aggregation/group_aggregation_expr.h"

namespace omniruntime::op {
Operator *GroupingOperatorFactory::CreateOperator()
{
    return new GroupingOperator(aggPlanNode_, expandNode_, queryConfig_);
}

GroupingOperatorFactory *GroupingOperatorFactory::CreateGroupingOperatorFactory(
    const std::shared_ptr<const AggregationNode> &aggPlanNode, const std::shared_ptr<const ExpandNode> &expandNode,
    const config::QueryConfig &queryConfig)
{
    return new GroupingOperatorFactory(aggPlanNode, expandNode, queryConfig);
}

GroupingOperator::GroupingOperator(std::shared_ptr<const AggregationNode> &aggPlanNode,
    const std::shared_ptr<const ExpandNode> &expandNode, config::QueryConfig &queryConfig): aggPlanNode_(aggPlanNode),
    queryConfig_(queryConfig)
{
    auto projections = expandNode->GetProjections();
    auto sourceTypes = *(expandNode->InputType());
    auto overflowConfig = queryConfig_.IsOverFlowASNull() == true
                          ? std::make_shared<OverflowConfig>(OVERFLOW_CONFIG_NULL)
                          : std::make_shared<OverflowConfig>(OVERFLOW_CONFIG_EXCEPTION);
    aggFactor_ = CreateOperatorFactory(aggPlanNode_, queryConfig_);
    for (const auto &projection : projections) {
        auto exprEvaluator = std::make_shared<ExpressionEvaluator>(projection, sourceTypes, overflowConfig.get());
        exprEvaluator->ProjectFuncGeneration();
        expressionEvaluators_.push_back(exprEvaluator);
    }
}

int32_t GroupingOperator::AddInput(VectorBatch *vecBatch)
{
    if (vecBatch->GetRowCount() <= 0) {
        VectorHelper::FreeVecBatch(vecBatch);
        ResetInputVecBatch();
        return 0;
    }
    inputVecs_.push_back(vecBatch);
    return 0;
}

int32_t GroupingOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (!noMoreInput_) {
        SetStatus(OMNI_STATUS_NORMAL);
        return 0;
    }
    if (aggOperator_) {
        SetStatus(OMNI_STATUS_NORMAL);
        aggOperator_->GetOutput(outputVecBatch);
        if (*outputVecBatch != nullptr) {
            return 1;
        }
        index_++;
        aggOperator_->Close();
        aggOperator_ = nullptr;
    }

    const auto size = expressionEvaluators_.size();
    while (index_ < size) {
        aggOperator_ = std::shared_ptr<Operator>(aggFactor_->CreateOperator());
        for (auto inputVec : inputVecs_) {
            auto projectedVecBatch = this->expressionEvaluators_[index_]->Evaluate(inputVec, executionContext.get());
            aggOperator_->AddInput(projectedVecBatch);
        }

        aggOperator_->GetOutput(outputVecBatch);
        if (*outputVecBatch != nullptr) {
            SetStatus(OMNI_STATUS_NORMAL);
            return 1;
        }
        aggOperator_->Close();
        aggOperator_ = nullptr;
        index_++;
    }
    for (auto inputVec : inputVecs_) {
        VectorHelper::FreeVecBatch(inputVec);
    }
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

std::shared_ptr<OperatorFactory> GroupingOperator::CreateOperatorFactory(
    const std::shared_ptr<const AggregationNode> &aggregationNode, const config::QueryConfig &queryConfig)
{
    if (aggregationNode->GetGroupByKeys().empty()) {
        return std::shared_ptr<AggregationWithExprOperatorFactory>(
            AggregationWithExprOperatorFactory::CreateAggregationWithExprOperatorFactory(aggregationNode, queryConfig));
    }
    return std::shared_ptr<HashAggregationWithExprOperatorFactory>(
        HashAggregationWithExprOperatorFactory::CreateAggregationWithExprOperatorFactory(aggregationNode, queryConfig));
}

OmniStatus GroupingOperator::Close()
{
    return OMNI_STATUS_FINISHED;
}
}
