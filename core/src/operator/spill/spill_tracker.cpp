/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: spill unit iterator
 */
#include "spill_tracker.h"
#include <unistd.h>
#include "util/debug.h"

namespace omniruntime {
namespace op {
RootSpillTracker::~RootSpillTracker()
{
    for (auto iter = spillPaths.begin(); iter != spillPaths.end(); iter++) {
        rmdir((*iter).c_str());
    }
}

bool RootSpillTracker::CheckIfExceedAndReserve(uint64_t bytes)
{
    if (spilledBytes + bytes >= maxBytes) {
        LogInfo("Spilled size exceeded the spill limit of %lu.", maxBytes);
        return true;
    }

    spilledBytes.fetch_add(bytes);
    return false;
}

RootSpillTracker rootSpillTracker;

void InitRootSpillTracker(std::string &spillPath, uint64_t maxSize)
{
    rootSpillTracker.AddSpillPath(spillPath);
    rootSpillTracker.SetMaxBytes(maxSize);
}

RootSpillTracker &GetRootSpillTracker()
{
    return rootSpillTracker;
}
}
}