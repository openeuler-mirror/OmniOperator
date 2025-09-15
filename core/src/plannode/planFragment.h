/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#pragma once
#include "planNode.h"

namespace omniruntime {

using PlanNodeId = std::string;

/// Gives hints on how to execute the fragment of a plan.
enum class ExecutionStrategy {
    /// Process splits as they come in any available driver.
    K_UNGROUPED,
    /// Process splits from each split group only in one driver.
    /// It is used when split groups represent separate partitions of the data on
    /// the grouping keys or join keys. In that case it is sufficient to keep only
    /// the keys from a single split group in a hash table used by group-by or
    /// join.
    K_GROUPED,
};

/// Contains some information on how to execute the fragment of a plan.
/// Used to construct Task.
struct PlanFragment {
    /// Top level (root) Plan Node.
    std::shared_ptr<const PlanNode> planNode;
    ExecutionStrategy executionStrategy{ExecutionStrategy::K_UNGROUPED};
    int numSplitGroups{0};

    PlanFragment() = default;

    explicit PlanFragment(std::shared_ptr<const PlanNode> topNode)
        : planNode(std::move(topNode))
    {
    }

    PlanFragment(
        std::shared_ptr<const PlanNode> topNode,
        ExecutionStrategy strategy,
        int numberOfSplitGroups,
        const std::unordered_set<PlanNodeId> &groupedExecLeafNodeIds)
        : planNode(std::move(topNode)),
          executionStrategy(strategy),
          numSplitGroups(numberOfSplitGroups)
    {
    }
};
}
