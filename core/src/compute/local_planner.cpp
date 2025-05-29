/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "local_planner.h"
#include "operator/join/hash_builder.h"
#include "operator/join/lookup_join.h"
#include "operator/join/lookup_outer_join.h"
#include "operator/join/lookup_join_wrapper.h"
#include "operator/limit/limit.h"
#include "operator/sort/sort.h"
#include "operator/topn/topn.h"
#include "operator/union/union.h"
#include "operator/window/window.h"

namespace omniruntime::compute {

// Returns ture if source nodes must run in a separate pipeline
bool MustStartNewPipeline(int sourceId) { return sourceId != 0; }

void planDetail(const std::shared_ptr<const PlanNode> &planNode,
    std::vector<OperatorFactory *> *currentOperatorFactories,
    std::vector<std::unique_ptr<DriverFactory>> *driverFactories, const config::QueryConfig &queryConfig)
{
    if (!currentOperatorFactories) {
        driverFactories->emplace_back(std::make_unique<DriverFactory>());
        currentOperatorFactories = &driverFactories->back()->operatorFactories;
    }

    const auto &sources = planNode->Sources();
    if (sources.empty()) {
        driverFactories->back()->inputDriver = true;
    } else {
        for (int32_t i = 0; i < sources.size(); ++i) {
            planDetail(
                sources[i], MustStartNewPipeline(i) ? nullptr : currentOperatorFactories, driverFactories, queryConfig);
        }
    }

    if (auto orderByNode = std::dynamic_pointer_cast<const OrderByNode>(planNode)) {
        currentOperatorFactories->emplace_back(SortOperatorFactory::CreateSortOperatorFactory(orderByNode, queryConfig));
    } else if (auto projectNode = std::dynamic_pointer_cast<const ProjectNode>(planNode)) {
	    currentOperatorFactories->emplace_back(CreateProjectOperatorFactory(projectNode, queryConfig));
    } else if (auto filterNode = std::dynamic_pointer_cast<const FilterNode>(planNode)) {
	    currentOperatorFactories->emplace_back(CreateFilterOperatorFactory(filterNode, queryConfig));
    } else if (auto windowNode = std::dynamic_pointer_cast<const WindowNode>(planNode)) {
        currentOperatorFactories->emplace_back(WindowOperatorFactory::CreateWindowOperatorFactory(windowNode, queryConfig));
    } else if (auto topNNode = std::dynamic_pointer_cast<const TopNNode>(planNode)) {
        currentOperatorFactories->emplace_back(TopNOperatorFactory::CreateTopNOperatorFactory(topNNode));
    } else if (auto limitNode = std::dynamic_pointer_cast<const LimitNode>(planNode)) {
        currentOperatorFactories->emplace_back(LimitOperatorFactory::CreateLimitOperatorFactory(limitNode));
    } else if (auto unionNode = std::dynamic_pointer_cast<const UnionNode>(planNode)) {
        currentOperatorFactories->emplace_back(UnionOperatorFactory::CreateUnionOperatorFactory(unionNode));
    } else if (auto joinNode = std::dynamic_pointer_cast<const HashJoinNode>(planNode)) {
        // The overflowConfig now is nullptr, need to update it later.
        auto hashBuilderOperatorFactory =
            HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(joinNode);

        auto joinType = joinNode->GetJoinType();
        if (joinType == JoinType::OMNI_JOIN_TYPE_FULL || joinType == JoinType::OMNI_JOIN_TYPE_RIGHT) {
            currentOperatorFactories->emplace_back(LookupJoinWrapperOperatorFactory::CreateLookupJoinWrapperOperatorFactory(joinNode, hashBuilderOperatorFactory, queryConfig));
        } else {
            currentOperatorFactories->emplace_back(LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(joinNode, hashBuilderOperatorFactory, queryConfig));
        }

        auto builderFactories = &driverFactories->back()->operatorFactories;
        builderFactories->emplace_back(hashBuilderOperatorFactory);
    } else if (auto valueStreamNode = std::dynamic_pointer_cast<const ValueStreamNode>(planNode)) {
        currentOperatorFactories->emplace_back(ValueStreamFactory::CreateValueStreamFactory(valueStreamNode));
    } else {
        throw omniruntime::exception::OmniException(
            "PLANNODE_NOT_SUPPORT", "The plannode is not supported yet." + planNode->Id());
    }
}

void LocalPlanner::plan(const PlanFragment &fragment, std::vector<std::unique_ptr<DriverFactory>> *driverFactories,
    const config::QueryConfig &queryConfig, uint32_t maxDrivers)
{
    planDetail(fragment.planNode, nullptr, driverFactories, queryConfig);
    (*driverFactories)[0]->outputDriver = true;

    // Determine the number of drivers for each pipeline
    for (auto &factory : *driverFactories) {
        // Implement the logic to determine the number of drivers based on the query
        // configuration and the factory's properties. Placeholder for now.
        factory->maxDrivers = 1;
        factory->numDrivers = std::min(factory->maxDrivers, maxDrivers);
        factory->numTotalDrivers = factory->numDrivers;
    }
}
} // namespace omniruntime::compute
