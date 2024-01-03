/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * @Description: spill tracker implementation
 */

#include "spill_tracker.h"
#include <unistd.h>
#include "util/debug.h"

namespace omniruntime {
namespace op {
RootSpillTracker::~RootSpillTracker()
{
    for (const auto &spillPath : spillPaths) {
        rmdir(spillPath.c_str());
    }
}

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

void InitRootSpillTracker(std::string &spillPath, uint64_t maxSize)
{
    GetRootSpillTracker().AddSpillPath(spillPath);
    GetRootSpillTracker().SetMaxBytes(maxSize);
}

RootSpillTracker &GetRootSpillTracker()
{
    static RootSpillTracker rootSpillTracker;
    return rootSpillTracker;
}
}
}