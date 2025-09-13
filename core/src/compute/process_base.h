/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef PROCESS_BASE_H
#define PROCESS_BASE_H

#pragma once

#include <cstdint>

namespace omniruntime::compute {

    /// Returns elapsed CPU nanoseconds on the calling thread
    int64_t ThreadCpuNanos();
} // namespace omniruntime
#endif
