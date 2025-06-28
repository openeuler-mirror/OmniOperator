/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#pragma once
#include "plannode/planNode.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "operator/expand/expand.h"
#include "operator/aggregation/non_group_aggregation_expr.h"

namespace omniruntime::op {
class GroupingOperatorFactory final : public OperatorFactory {
public:
    GroupingOperatorFactory(const std::shared_ptr<const AggregationNode> &aggPlanNode,
        const std::shared_ptr<const ExpandNode> &expandNode,
        const config::QueryConfig &queryConfig): aggPlanNode_(aggPlanNode), expandNode_(expandNode),
        queryConfig_(queryConfig) {}

    Operator *CreateOperator() override;

    static GroupingOperatorFactory *CreateGroupingOperatorFactory(
        const std::shared_ptr<const AggregationNode> &aggPlanNode, const std::shared_ptr<const ExpandNode> &expandNode,
        const config::QueryConfig &queryConfig);

private:
    std::shared_ptr<const AggregationNode> aggPlanNode_;
    std::shared_ptr<const ExpandNode> expandNode_;
    config::QueryConfig queryConfig_;
};

class GroupingOperator final : public Operator {
public:
    GroupingOperator(std::shared_ptr<const AggregationNode> &aggPlanNode,
        const std::shared_ptr<const ExpandNode> &expandNode, config::QueryConfig &queryConfig);

    ~GroupingOperator() override = default;

    int32_t AddInput(VectorBatch *vecBatch) override;

    int32_t GetOutput(VectorBatch **resultVecBatch) override;

    OmniStatus Close() override;

    static std::shared_ptr<OperatorFactory> CreateOperatorFactory(
        const std::shared_ptr<const AggregationNode> &aggregationNode, const config::QueryConfig &queryConfig);

private:
    uint32_t index_ = 0;
    std::shared_ptr<OperatorFactory> aggFactor_;
    std::vector<VectorBatch *> inputVecs_;
    std::vector<std::shared_ptr<ExpressionEvaluator>> expressionEvaluators_;
    std::shared_ptr<AggregationWithExprOperator> aggregationWithExprOperator_;
    std::shared_ptr<const AggregationNode> aggPlanNode_;
    std::shared_ptr<ExpandOperator> expandOperator_;
    std::shared_ptr<Operator> aggOperator_;
    config::QueryConfig queryConfig_;
};
}
