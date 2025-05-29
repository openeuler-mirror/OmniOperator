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

class OmniTask {
public:
    OmniTask(const PlanFragment& planFragment,  config::QueryConfig&& queryConfig)
        : planFragment_(planFragment), queryConfig_(std::move(queryConfig)) {}

    ~OmniTask()
    {
        for (auto& driver : drivers_) {
            if (driver) {
                driver->close();
            }
        }
        for (auto& factory : operatorFactories_) {
            delete factory;
        }
    }

    vec::VectorBatch* Next(ContinueFuture* future = nullptr);

private:
    std::vector<std::shared_ptr<OmniDriver>> drivers_;
    std::vector<OperatorFactory*> operatorFactories_;
    PlanFragment planFragment_;
    OperatorConfig operatorConfig_;
    const config::QueryConfig queryConfig_;
};
} // end of compute
} // end of omniruntime
#endif