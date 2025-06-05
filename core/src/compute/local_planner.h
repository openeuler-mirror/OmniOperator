/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef __LOCAL_PLANNER_H__
#define __LOCAL_PLANNER_H__

#include <vector>
#include <memory>
#include "compute/driver.h"
#include "plannode/planFragment.h"
#include "plannode/planNode.h"


namespace omniruntime {
namespace compute {

std::shared_ptr<omniruntime::op::Operator> createOperator(OperatorFactory* factory);
OperatorFactory* createOperatorFactory(
    const std::shared_ptr<const PlanNode>& planNode,
    const config::QueryConfig& queryConfig);

class LocalPlanner {
public:
    static void buildOperatorStats(std::vector<std::shared_ptr<OmniDriver>>* drivers);
    static void plan(
        const omniruntime::PlanFragment& fragment,
        std::vector<std::shared_ptr<omniruntime::compute::OmniDriver>>* drivers,
        std::vector<omniruntime::op::OperatorFactory*>* factories,
        const config::QueryConfig& queryConfig);
};
}  // namespace compute
}  // namespace omniruntime
#endif  // __LOCAL_PLANNER_H__