/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef __TASK_H__
#define __TASK_H__
 
#include <vector>
#include <memory>
#include <string>
#include <functional>
#include <exception>

#include "compute/driver.h"
#include "vector/vector_batch.h"
#include "operator/operator.h"
#include "plannode/planFragment.h"
 
namespace omniruntime {
namespace compute {

struct OmniDriver;
struct DriverFactory;

class OmniTask {
public:
    OmniTask(const PlanFragment& planFragment, const OperatorConfig& operatorConfig)
        : planFragment_(planFragment), operatorConfig_(operatorConfig) {};
    ~OmniTask() {};
 
    vec::VectorBatch* Next(ContinueFuture* future = nullptr);

private:
    std::vector<std::shared_ptr<OmniDriver>> drivers_;
    std::vector<std::unique_ptr<DriverFactory>> driverFactories_;
    PlanFragment planFragment_;
    OperatorConfig operatorConfig_;
};
} // end of compute
} // end of omniruntime
#endif