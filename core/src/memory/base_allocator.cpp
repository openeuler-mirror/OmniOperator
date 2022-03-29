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
    bool beyondLimit;
    {
        std::lock_guard<std::mutex> l(mutex);
        int64_t newAllocated = allocatedBytes.fetch_add(size, std::memory_order_relaxed) + size;
        int64_t beyondReservation = newAllocated - reservation;
        beyondLimit = (allocationLimit != UNLIMIT) && (newAllocated > allocationLimit);
        if (beyondReservation > 0 && parentAllocator) {
            int64_t increment = std::min(beyondReservation, size);
            parentAllocator->AllocatedBytesInternal(increment);
        }
    }

    if (size > 0 && beyondLimit) {
        return allocationLimit;
    }
    // set peakAllocated
    UpdatePeak();
    return UNLIMIT;
}

void BaseAllocator::UpdateInternal(int64_t size)
{
    if (parentAllocator != nullptr) {
        parentAllocator->UpdateInternal(size);
    }
    int64_t newAllocated = allocatedBytes.fetch_add(size, std::memory_order_relaxed) + size;
    if (newAllocated < 0) {
        std::cout << "newAllocated:" << newAllocated << std::endl;
    }
    // check memory usage
    if (size > 0 && allocationLimit != UNLIMIT && newAllocated > allocationLimit) {
        if (parentAllocator != nullptr) {
            parentAllocator->UpdateInternal(-size);
        }
        allocatedBytes.fetch_add(-size, std::memory_order_relaxed);
        // throw memory cap exceeded exception
        int64_t limit = allocationLimit.load(std::memory_order_relaxed);
        auto message = "Exceeded memory cap of MB:" + std::to_string(limit / 1024 / 1024);
        throw OmniException(kMemCapExceeded, message);
    }
    // set peakAllocated
    UpdatePeak();
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
    if (currentAllocated > 0 || currentAllocated < 0) {
        //        if (parentAllocator) {
        //            parentAllocator->UpdateInternal(-currentAllocated);
        //        }
        ReleaseBytes(currentAllocated);
        LogError("Memory leak in allocator:%s,leak size in bytes is:%ld, stack is:%s", scope.c_str(), currentAllocated,
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
} // namespace mem
} // namespace omniruntime