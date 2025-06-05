/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "task.h"
#include "local_planner.h"
 
namespace omniruntime::compute {
 
vec::VectorBatch* OmniTask::Next(ContinueFuture* future)
{
    if (drivers_.empty()) {
        LocalPlanner::plan(
            planFragment_, &drivers_, &operatorFactories_, queryConfig_);
        std::reverse(drivers_.begin(), drivers_.end());
    }
    const auto numDrivers = drivers_.size();
    auto futures = OmniFuture::createValidFutures(numDrivers);
    for (;;) {
        int runableDrivers = 0;
        for (auto i = 0; i < numDrivers; ++i) {
            if (drivers_[i] == nullptr) {
                // This driver has finished processing.
                continue;
            }
 
            ++runableDrivers;
 
            ContinueFuture driverFuture = OmniFuture::makeEmpty();
            StopReason stopReason = StopReason::kNone;
            auto result = drivers_[i]->Next(&driverFuture, &stopReason);
            if (stopReason == StopReason::kAtEnd) {
                drivers_[i] = nullptr;
            }
            if (result) {
                return result;
            }
 
            futures[i] = std::move(driverFuture);
        }
 
        if (runableDrivers == 0) {
            return nullptr;
        }
    }
}

} // end of omniruntime