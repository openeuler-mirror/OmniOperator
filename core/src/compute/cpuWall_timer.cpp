/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "cpuWall_timer.h"

namespace omniruntime::compute {

    CpuWallTimer::CpuWallTimer(CpuWallTiming& timing) : timing_(timing)
    {
        ++timing_.count;
        cpuTimeStart_ = ThreadCpuNanos();
        wallTimeStart_ = std::chrono::steady_clock::now();
    }

    CpuWallTimer::~CpuWallTimer()
    {
        timing_.cpuNanos += ThreadCpuNanos() - cpuTimeStart_;
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - wallTimeStart_);
        timing_.wallNanos += duration.count();
    }

} // namespace omniruntime::compute