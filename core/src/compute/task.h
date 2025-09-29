/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __TASK_H__
#define __TASK_H__
 
#include <vector>
#include <memory>
#include <string>
#include <functional>
#include <exception>

#include "compute/driver.h"
#include "compute/task_stats.h"
#include "vector/vector_batch.h"
#include "operator/operator.h"
#include "plannode/planFragment.h"
 
namespace omniruntime {
namespace compute {

class OmniDriver;

class OmniTask {
public:
    OmniTask(const PlanFragment& planFragment,  config::QueryConfig&& queryConfig)
        : planFragment_(planFragment), queryConfig_(std::move(queryConfig))
    {
        taskStats_ = TaskStats();
    }

    ~OmniTask()
    {
        for (auto& driver : drivers_) {
            if (driver) {
                driver->shouldStop = true;
                driver->close();
            }
        }
        for (auto& factory : operatorFactories_) {
            delete factory;
        }
    }

    vec::VectorBatch* Next(ContinueFuture* future = nullptr);

    /// Returns Task Stats by copy as other threads might be updating the
    /// structure.
    TaskStats GetTaskStats() const;

private:
    std::vector<std::shared_ptr<OmniDriver>> drivers_;
    std::vector<OperatorFactory*> operatorFactories_;
    PlanFragment planFragment_;
    OperatorConfig operatorConfig_;
    TaskStats taskStats_;
    const config::QueryConfig queryConfig_;
};
} // end of compute
} // end of omniruntime
#endif