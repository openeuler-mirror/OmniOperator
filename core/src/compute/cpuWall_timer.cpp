/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "cpuWall_timer.h"

namespace omniruntime::compute {

    CpuWallTimer::CpuWallTimer(CpuWallTiming& timing) : timing_(timing)
    {
        ++timing_.count;
        cpuTimeStart_ = ThreadCpuNanos();
        wallTimeStart_ = 0;
    }

    CpuWallTimer::~CpuWallTimer()
    {
        timing_.cpuNanos += ThreadCpuNanos() - cpuTimeStart_;
        timing_.wallNanos += 0;
    }

} // namespace omniruntime::compute