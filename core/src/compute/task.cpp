/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
 
#include "task.h"
#include "local_planner.h"
 
namespace omniruntime::compute {
 
vec::VectorBatch* OmniTask::Next(ContinueFuture* future)
{
    if (driverFactories_.empty()) {
        LocalPlanner::plan(
            planFragment_, &driverFactories_, operatorConfig_, 1);
        std::vector<std::shared_ptr<OmniDriver>> drivers;
        for (auto& factory : driverFactories_) {
            drivers.emplace_back(factory->CreateDriver());
        }
        drivers_ = std::move(drivers);
    }
    const auto numDrivers = drivers_.size();
    auto futures = OmniFuture::createValidFutures(numDrivers);
    for (;;) {
        int runableDrivers = 0;
        int blockedDrivers = 0;
        for (auto i = 0; i < numDrivers; ++i) {
            if (drivers_[i] == nullptr) {
                // This driver has finished processing.
                continue;
            }

            if (!OmniFuture::isReady(futures[i])) {
                // This driver is still blocked.
                ++blockedDrivers;
                continue;
            }
 
            ++runableDrivers;
 
            ContinueFuture driverFuture = OmniFuture::makeEmpty();
            auto result = drivers_[i]->Next(&driverFuture);
            if (result) {
                return result;
            }
 
            futures[i] = std::move(driverFuture);
        }
 
        if (runableDrivers == 0) {
            if (blockedDrivers > 0) {
                std::vector<ContinueFuture> notReadyFutures;
                for (auto& continueFuture : futures) {
                    if (continueFuture.valid() && !OmniFuture::isReady(continueFuture)) {
                        notReadyFutures.emplace_back(std::move(continueFuture));
                    }
                }
                *future = OmniFuture::collectAll(std::move(notReadyFutures));
            }
            return nullptr;
        }
    }
}

} // end of omniruntime