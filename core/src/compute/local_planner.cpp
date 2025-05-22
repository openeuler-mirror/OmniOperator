/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "local_planner.h"
#include "operator/sort/sort.h"
#include "operator/join/lookup_join.h"
#include "operator/join/hash_builder.h"
#include "operator/limit/limit.h"
#include "operator/topn/topn.h"
#include "operator/union/union.h"
#include "operator/window/window.h"

namespace omniruntime::compute {

// Returns ture if source nodes must run in a separate pipeline
bool MustStartNewPipeline(int sourceId)
{
    return sourceId != 0;
}

void planDetail(
    const std::shared_ptr<const PlanNode>& planNode,
    std::vector<OperatorFactory*>* currentOperatorFactories,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
    const config::QueryConfig& queryConfig)
{
    OperatorFactory* factory = nullptr;
    if (!currentOperatorFactories) {
        driverFactories->emplace_back(std::make_unique<DriverFactory>());
        currentOperatorFactories = &driverFactories->back()->operatorFactories;
    }

    const auto& sources = planNode->Sources();
    if (sources.empty()) {
        driverFactories->back()->inputDriver = true;
    } else {
        for (int32_t i = 0; i < sources.size(); ++i) {
            planDetail(
                sources[i],
                MustStartNewPipeline(i) ? nullptr : currentOperatorFactories,
                driverFactories,
                queryConfig);
        }
    }

    if (auto orderByNode = std::dynamic_pointer_cast<const OrderByNode>(planNode)) {
        factory = SortOperatorFactory::CreateSortOperatorFactory(orderByNode, queryConfig);
    } else if (auto windowNode = std::dynamic_pointer_cast<const WindowNode>(planNode)) {
        factory = WindowOperatorFactory::CreateWindowOperatorFactory(windowNode, queryConfig);
    } else if (auto topNNode = std::dynamic_pointer_cast<const TopNNode>(planNode)) {
        factory = TopNOperatorFactory::CreateTopNOperatorFactory(topNNode);
    } else if (auto limitNode = std::dynamic_pointer_cast<const LimitNode>(planNode)) {
        factory = LimitOperatorFactory::CreateLimitOperatorFactory(limitNode);
    } else if (auto unionNode = std::dynamic_pointer_cast<const UnionNode>(planNode)) {
        factory = UnionOperatorFactory::CreateUnionOperatorFactory(unionNode);
    } else if (auto joinNode = std::dynamic_pointer_cast<const HashJoinNode>(planNode)) {
        // The overflowConfig now is nullptr, need to update it later.
        auto hashBuilderOperatorFactory =
            HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(joinNode);
        factory =
            LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(joinNode, hashBuilderOperatorFactory, queryConfig);
        auto builderFactories = &driverFactories->back()->operatorFactories;
        builderFactories->emplace_back(hashBuilderOperatorFactory);
    } else if (auto valueStreamNode = std::dynamic_pointer_cast<const ValueStreamNode>(planNode)) {
        factory = ValueStreamFactory::CreateValueStreamFactory(valueStreamNode);
    } else {
        throw omniruntime::exception::OmniException("PLANNODE_NOT_SUPPORT",
            "The plannode is not supported yet." + planNode->Id());
    }

    currentOperatorFactories->emplace_back(factory);
}

void LocalPlanner::plan(
    const PlanFragment& fragment,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
    const config::QueryConfig& queryConfig,
    uint32_t maxDrivers)
{
    planDetail(fragment.planNode,
        nullptr,
        driverFactories,
        queryConfig);
    (*driverFactories)[0]->outputDriver = true;

    // Determine the number of drivers for each pipeline
    for (auto& factory : *driverFactories) {
        // Implement the logic to determine the number of drivers based on the query
        // configuration and the factory's properties. Placeholder for now.
        factory->maxDrivers = 1;
        factory->numDrivers = std::min(factory->maxDrivers, maxDrivers);
        factory->numTotalDrivers = factory->numDrivers;
    }
}
}  // end of omniruntime