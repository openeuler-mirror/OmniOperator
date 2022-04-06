/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef __BASE_ALLOCATOR_H__
#define __BASE_ALLOCATOR_H__
#pragma once

#include <string>
#include <vector>
#include <map>
#include <atomic>
#include <list>
#include <mutex>
#include "memory_pool.h"
#include "util/debug.h"
#include "util/omni_exception.h"

namespace omniruntime {
namespace mem {
using namespace exception;
class BaseAllocator {
public:
    virtual ~BaseAllocator();

    std::string GetScope() const
    {
        return scope;
    }

    void *alloc(int64_t size)
    {
        int64_t preferredSize = pool->GetPreferredSize(size);
        // try allocate
        TryAllocate(preferredSize);
        uint8_t *data = nullptr;
        pool->Allocate(preferredSize, &data);
        return data;
    }

    void free(void *addr, int64_t size)
    {
        pool->Release(reinterpret_cast<uint8_t *>(addr));
        ReleaseBytes(pool->GetPreferredSize(size));
    }

    BaseAllocator *GetParentAllocator()
    {
        return parentAllocator;
    }

    int64_t GetAllocatedMemory()
    {
        return allocatedBytes;
    }

    virtual BaseAllocator *NewChildAllocator(const std::string &scope, int64_t limit = UNLIMIT, int64_t reservation = 0)
    {
        return new BaseAllocator(this, scope, limit, reservation);
    }

    void SetLimit(int64_t limit)
    {
        allocationLimit.store(limit, std::memory_order_relaxed);
    }

    int64_t GetLimit()
    {
        return allocationLimit;
    }

    std::vector<BaseAllocator *> GetChildAllocators()
    {
        std::vector<BaseAllocator *> childs;
        std::lock_guard<std::mutex> l(childAllocatorsLock);
        for (BaseAllocator *allocator : childAllocators) {
            childs.push_back(allocator);
        }
        return childs;
    }

    int64_t GetPeakAllocated()
    {
        return peakAllocated;
    }

    void TryAllocate(int64_t size)
    {
        if (size == 0) {
            return;
        }
        int64_t limit = AllocatedBytesInternal(size);
        if (limit != UNLIMIT) {
            ReleaseBytes(size);
            // throw memory cap exceeded exception
            auto message = "Exceeded memory cap of MB:" + std::to_string(limit / 1024 / 1024);
            throw OmniException(kMemCapExceeded, message);
        }
    }

    void ReleaseBytes(int64_t size)
    {
        const int64_t newAllocated = allocatedBytes.fetch_add(-size, std::memory_order_relaxed) - size;
        const int64_t originalSize = newAllocated + size;
        if (originalSize > reservation && parentAllocator) {
            // release memory to  parent
            const int64_t possibleAmountToReleaseToParent = originalSize - reservation;
            const int64_t actualToReleaseToParent = std::min(size, possibleAmountToReleaseToParent);
            parentAllocator->ReleaseBytes(actualToReleaseToParent);
        }
    }

    static BaseAllocator *GetRootAllocator()
    {
        static std::string ROOT_SCOPE_NAME = "___ROOT_SCOPE___";
        static BaseAllocator ROOT_ALLOCATOR(nullptr, ROOT_SCOPE_NAME, UNLIMIT);
        return &ROOT_ALLOCATOR;
    }

    static void SetRootAllocatorLimit(int64_t limit)
    {
        LogInfo("set root allocator limit:%ld", limit);
        GetRootAllocator()->SetLimit(limit);
    }

protected:
    BaseAllocator(BaseAllocator *parent, const std::string &scope, int64_t limit = UNLIMIT, int64_t reservation = 0)
        : scope(scope), allocationLimit(limit), reservation(reservation), parentAllocator(parent)
    {
        if (reservation != 0 && parentAllocator) {
            parentAllocator->TryAllocate(reservation);
        }
        if (parentAllocator) {
            parentAllocator->AddChildAllocator(this);
        }
    }

    // unlimited memory usage
    const static int64_t UNLIMIT = -1;

private:
    void AddChildAllocator(BaseAllocator *childAllocator)
    {
        std::lock_guard<std::mutex> l(childAllocatorsLock);
        childAllocator->childAllocatorIt = childAllocators.insert(childAllocators.end(), childAllocator);
    }

    void ReleaseReservation()
    {
        // return memory reservation to parent allocator
        if (parentAllocator && reservation != 0) {
            parentAllocator->ReleaseBytes(reservation);
        }
    }

    int64_t AllocatedBytesInternal(int64_t size);

    void UpdatePeak();

    void Close();

    void RemoveFromParent();

    const std::string scope;
    std::mutex childAllocatorsLock;
    std::list<BaseAllocator *> childAllocators;
    std::list<BaseAllocator *>::iterator childAllocatorIt;

    std::atomic<int64_t> allocatedBytes { 0 };
    std::atomic<int64_t> allocationLimit { 0 };
    std::atomic<int64_t> peakAllocated { 0 };
    std::atomic<int64_t> reservation { 0 };

    BaseAllocator *parentAllocator;
    MemoryPool *pool = GetMemoryPool();
};
}
}
#endif // __BASE_ALLOCATOR_H__
