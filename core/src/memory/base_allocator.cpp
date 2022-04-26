/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "base_allocator.h"

namespace omniruntime {
namespace mem {
using namespace exception;
BaseAllocator::~BaseAllocator()
{
    Close();
}

int64_t BaseAllocator::AllocatedBytesInternal(int64_t size)
{
    // fetch_add returns the original value,
    // here we need to calculate the original value plus size as new Allocated
    int64_t newAllocated = allocatedBytes.fetch_add(size, std::memory_order_relaxed) + size;
    int64_t beyondReservation = newAllocated - reservation;
    int64_t parentLimit = UNLIMIT;
    if (beyondReservation > 0 && parentAllocator) {
        int64_t increment = std::min(beyondReservation, size);
        parentLimit = parentAllocator->AllocatedBytesInternal(increment);
    }

    bool beyondLimit = (allocationLimit != UNLIMIT) && (newAllocated > allocationLimit);
    int64_t resultLimit = (size > 0 && beyondLimit) ? allocationLimit.load() : parentLimit;
    // set peakAllocated
    UpdatePeak();
    return resultLimit;
}

void BaseAllocator::UpdatePeak()
{
    int64_t newAllocated = allocatedBytes.load(std::memory_order_relaxed);
    int64_t oldPeakAllocated = peakAllocated;
    while (oldPeakAllocated < newAllocated && !peakAllocated.compare_exchange_weak(oldPeakAllocated, newAllocated)) {
        oldPeakAllocated = peakAllocated;
    }
}

void BaseAllocator::Close()
{
    // release child allocator first
    std::vector<BaseAllocator *> childs = GetChildAllocators();
    for (auto &child : childs) {
        delete child;
    }

    // release current allocator
    int64_t currentAllocated = allocatedBytes.load(std::memory_order_relaxed);
    if (currentAllocated > 0) {
        ReleaseBytes(currentAllocated);
        LogWarn("Memory leak in allocator:%s,leak size in bytes is:%ld, stack is:%s", scope.c_str(), currentAllocated,
            TraceUtil::GetStack().c_str());
    }

    ReleaseReservation();
    RemoveFromParent();
}

void BaseAllocator::RemoveFromParent()
{
    // remove this allocator from parent
    if (parentAllocator) {
        std::lock_guard<std::mutex> l(parentAllocator->childAllocatorsLock);
        parentAllocator->childAllocators.erase(childAllocatorIt);
        childAllocatorIt = parentAllocator->childAllocators.end();
    }
}

BaseAllocator *GetProcessRootAllocator()
{
    return BaseAllocator::GetRootAllocator();
}

void SetRootAllocatorLimit(int64_t limit)
{
    LogInfo("set root allocator limit:%ld Byte", limit);
    GetProcessRootAllocator()->SetLimit(limit);
}
} // namespace mem
} // namespace omniruntime