/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "local_planner.h"

#include <operator/aggregation/group_aggregation_expr.h>

#include "operator/join/hash_builder.h"
#include "operator/join/lookup_join.h"
#include "operator/join/lookup_outer_join.h"
#include "operator/join/lookup_join_wrapper.h"
#include "operator/limit/limit.h"
#include "operator/sort/sort.h"
#include "operator/topn/topn.h"
#include "operator/union/union.h"
#include "operator/window/window.h"
#include "operator/join/sortmergejoin/sort_merge_join_expr.h"
#include <operator/aggregation/non_group_aggregation_expr.h>
#include "operator/expand/expand.h"

namespace omniruntime::compute {

// Returns ture if source nodes must run in a separate pipeline
bool MustStartNewPipeline(int sourceId) { return sourceId != 0; }

std::shared_ptr<omniruntime::op::Operator> createOperator(
    OperatorFactory* factory, const std::shared_ptr<const PlanNode>& planNode)
{
    std::shared_ptr<omniruntime::op::Operator> operatorPtr(factory->CreateOperator());
    operatorPtr->setNoMoreInput(false);
    operatorPtr->SetPlanNodeId(planNode->Id());
    if (operatorPtr->operatorType().empty()) {
        operatorPtr->SetOperatorType(string(planNode->Name()));
    }
    return std::move(operatorPtr);
}

OperatorFactory* createOperatorFactory(
    const std::shared_ptr<const PlanNode>& planNode,
    const config::QueryConfig& queryConfig)
{
    if (auto orderByNode = std::dynamic_pointer_cast<const OrderByNode>(planNode)) {
        return SortOperatorFactory::CreateSortOperatorFactory(orderByNode, queryConfig);
    } else if (auto projectNode = std::dynamic_pointer_cast<const ProjectNode>(planNode)) {
        return CreateProjectOperatorFactory(projectNode, queryConfig);
    } else if (auto filterNode = std::dynamic_pointer_cast<const FilterNode>(planNode)) {
        return CreateFilterOperatorFactory(filterNode, queryConfig);
    } else if (auto windowNode = std::dynamic_pointer_cast<const WindowNode>(planNode)) {
        return WindowOperatorFactory::CreateWindowOperatorFactory(windowNode, queryConfig);
    } else if (auto topNNode = std::dynamic_pointer_cast<const TopNNode>(planNode)) {
        return TopNOperatorFactory::CreateTopNOperatorFactory(topNNode);
    } else if (auto limitNode = std::dynamic_pointer_cast<const LimitNode>(planNode)) {
        return LimitOperatorFactory::CreateLimitOperatorFactory(limitNode);
    } else if (auto unionNode = std::dynamic_pointer_cast<const UnionNode>(planNode)) {
        return UnionOperatorFactory::CreateUnionOperatorFactory(unionNode);
    } else if (auto valueStreamNode = std::dynamic_pointer_cast<const ValueStreamNode>(planNode)) {
        return ValueStreamFactory::CreateValueStreamFactory(valueStreamNode);
    } else if (auto aggregationNode = std::dynamic_pointer_cast<const AggregationNode>(planNode)) {
        if (aggregationNode->GetGroupByKeys().empty()) {
            return AggregationWithExprOperatorFactory::CreateAggregationWithExprOperatorFactory(
                aggregationNode, queryConfig);
        }
        return HashAggregationWithExprOperatorFactory::CreateAggregationWithExprOperatorFactory(
            aggregationNode, queryConfig);
    } else if (auto expandNode = std::dynamic_pointer_cast<const ExpandNode>(planNode)) {
        return CreateExpandOperatorFactory(expandNode, queryConfig);
    } else {
        throw omniruntime::exception::OmniException(
            "PLANNODE_NOT_SUPPORT", "The plannode is not supported yet." + planNode->Id());
    }
}

void planDetail(
    const std::shared_ptr<const PlanNode>& planNode,
    std::vector<std::shared_ptr<omniruntime::op::Operator>>* currentOperators,
    std::vector<std::shared_ptr<OmniDriver>>* drivers,
    std::vector<OperatorFactory*>* factories,
    bool isUnionDriver,
    const config::QueryConfig& queryConfig)
{
    OperatorFactory* factory = nullptr;
    if (!currentOperators) {
        drivers->emplace_back(std::make_unique<OmniDriver>());
        drivers->back()->unionDriver = isUnionDriver;
        currentOperators = drivers->back()->operators();
    }

    const auto &sources = planNode->Sources();
    if (sources.empty()) {
        drivers->back()->inputDriver = true;
    } else {
        if (auto unionNode = std::dynamic_pointer_cast<const UnionNode>(planNode)) {
            isUnionDriver = true;
        }
        for (int32_t i = 0; i < sources.size(); ++i) {
            planDetail(sources[i], MustStartNewPipeline(i) ? nullptr : currentOperators,
                drivers, factories, isUnionDriver, queryConfig);
        }
    }

    if (auto joinNode = std::dynamic_pointer_cast<const HashJoinNode>(planNode)) {
        auto hashBuilderOperatorFactory =
            HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(joinNode);
        auto builderDriver = drivers->back();
        builderDriver->operators()->emplace_back(createOperator(hashBuilderOperatorFactory, joinNode));
        factories->emplace_back(hashBuilderOperatorFactory);

        auto joinType = joinNode->GetJoinType();
        if (joinType == JoinType::OMNI_JOIN_TYPE_FULL || joinType == JoinType::OMNI_JOIN_TYPE_RIGHT) {
            factory =
                LookupJoinWrapperOperatorFactory::CreateLookupJoinWrapperOperatorFactory(joinNode, hashBuilderOperatorFactory, queryConfig);
        } else {
            factory =
                LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(joinNode, hashBuilderOperatorFactory, queryConfig);
        }
    } else if (auto sortMergejoinNode = std::dynamic_pointer_cast<const MergeJoinNode>(planNode)) {
        auto streamedTableWithExprOperatorFactory =
            StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(
                sortMergejoinNode, queryConfig);
        auto bufferedTableWithExprOperatorFactory =
            BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(
                sortMergejoinNode, reinterpret_cast<int64_t>(streamedTableWithExprOperatorFactory), queryConfig);
        auto builderDriver = drivers->back();
        builderDriver->operators()->emplace_back(createOperator(bufferedTableWithExprOperatorFactory, sortMergejoinNode));
        factories->emplace_back(bufferedTableWithExprOperatorFactory);
        factory = streamedTableWithExprOperatorFactory;
    } else {
        factory = createOperatorFactory(planNode, queryConfig);
    }

    for (auto& driver : *drivers) {
        if (driver->unionDriver && driver->operators() != currentOperators) {
            driver->operators()->emplace_back(createOperator(factory, planNode));
        }
    }
    currentOperators->emplace_back(createOperator(factory, planNode));
    factories->emplace_back(factory);
}

void LocalPlanner::buildOperatorStats(std::vector<std::shared_ptr<OmniDriver>>* drivers)
{
    if (drivers->empty()) {
        LogError("drivers is empty");
        return;
    }

    for (auto index = 0; index <drivers->size(); ++index) {
        const auto operators = (*drivers)[index]->operators();
        if (operators->empty()) {
            LogError("operators is empty");
            return;
        }
        for (auto i = 0; i < operators->size(); ++i) {
            (*operators)[i]->SetOperatorId(i);
            (*operators)[i]->stats_ = OperatorStats(
                i,
                index,
                (*operators)[i]->planNodeId(),
                (*operators)[i]->operatorType());
        }
    }
}

void LocalPlanner::plan(
    const PlanFragment& fragment,
    std::vector<std::shared_ptr<OmniDriver>>* drivers,
    std::vector<OperatorFactory*>* factories,
    const config::QueryConfig& queryConfig)
{
    planDetail(fragment.planNode,
        nullptr,
        drivers,
        factories,
        false,
        queryConfig);
    (*drivers)[0]->outputDriver = true;

    buildOperatorStats(drivers);
}
} // namespace omniruntime::compute
