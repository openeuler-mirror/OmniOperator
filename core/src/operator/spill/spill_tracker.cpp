/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * @Description: spill tracker implementation
 */

#include "spill_tracker.h"
#include "util/debug.h"

namespace omniruntime {
namespace op {
RootSpillTracker::~RootSpillTracker() = default;

bool RootSpillTracker::CheckIfExceedAndReserve(uint64_t bytes)
{
    if (spilledBytes + bytes >= maxBytes) {
        LogInfo("Spilled size exceeded the spill limit of %lu.", maxBytes);
        return true;
    }

    spilledBytes.fetch_add(bytes);
    totalSpilledBytes += bytes;
    return false;
}

void InitRootSpillTracker(uint64_t maxSize)
{
    GetRootSpillTracker().SetMaxBytes(maxSize);
}

RootSpillTracker &GetRootSpillTracker()
{
    static RootSpillTracker rootSpillTracker;
    return rootSpillTracker;
}
}
}