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
class LocalPlanner {
public:
    static void plan(
        const omniruntime::PlanFragment& fragment,
        std::vector<std::unique_ptr<omniruntime::compute::DriverFactory>>* driverFactories,
        const config::QueryConfig& queryConfig,
        uint32_t maxDrivers);
};
}  // namespace compute
}  // namespace omniruntime
#endif  // __LOCAL_PLANNER_H__