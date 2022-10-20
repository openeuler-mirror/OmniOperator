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
#include "cpu_checker/omniruntime_cpu_checker.h"
#include "util/error_code.h"

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

    int64_t GetUnTrackedBytes()
    {
        return unTrackedBytes;
    }

    void TryAllocate(int64_t size)
    {
        if (size == 0) {
            return;
        }

        if (!BaseAllocator::isOperatorAllocator.load(std::memory_order_relaxed)) {
            // except for root and global level allocator, only operator level and task level allocator left for OLK
            // task level allocator do not alloc memory, so only operator level allocator can hit condition
            // spark engine do not use operator level and task level allocator
            if (scope.compare("___ROOT_SCOPE___") && scope.compare("___GLOBAL_SCOPE___")) {
                BaseAllocator::isOperatorAllocator.store(true);
            }
        }

        int64_t limit = AllocatedBytesInternal(size);
        if (limit != UNLIMIT) {
            ReleaseBytes(size);
            // throw memory cap exceeded exception
            auto message = op::GetErrorMessage(op::ErrorCode::MEM_CAP_EXCEEDED) + std::to_string(limit / 1024 / 1024);
            throw OmniException(op::GetErrorCode(op::ErrorCode::MEM_CAP_EXCEEDED), message);
        }
    }

    void OperatorReleaseBytes(int64_t size)
    {
        allocatedBytes.fetch_add(-size, std::memory_order_relaxed);
        while (lock.test_and_set()) {
            // acquire lock when run thread
        }
        unFreedBytes += size;
        if (unFreedBytes > reservation && parentAllocator) {
            // release memory to  parent
            parentAllocator->ReleaseBytes(unFreedBytes);
            unFreedBytes = 0;
        }
        // release lock when finish thread
        lock.clear();
    }

    void ReleaseBytes(int64_t size)
    {
        if (BaseAllocator::isOperatorAllocator.load(std::memory_order_relaxed)) {
            OperatorReleaseBytes(size);
            return;
        }

        allocatedBytes.fetch_add(-size, std::memory_order_relaxed);
        if (parentAllocator) {
            // release memory to  parent
            parentAllocator->ReleaseBytes(size);
        }
    }

    static BaseAllocator *GetRootAllocator()
    {
        static int ret = (KunpengCpuCheck() == 0 || QingsongCpuCheck() == 0);
        if (!ret) {
            throw OmniException("CPU_CHECK_ERROR", "CPU check failed");
        }
        static std::string ROOT_SCOPE_NAME = "___ROOT_SCOPE___";
        static BaseAllocator ROOT_ALLOCATOR(nullptr, ROOT_SCOPE_NAME, UNLIMIT);
        return &ROOT_ALLOCATOR;
    }

protected:
    BaseAllocator(BaseAllocator *parent, const std::string &scope, int64_t limit = UNLIMIT, int64_t reservation = 0)
        : scope(scope), allocationLimit(limit), reservation(1 << 20), parentAllocator(parent)
    {
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

    int64_t AllocatedBytesInternal(int64_t size);

    void UpdatePeak();

    void Close();

    void RemoveFromParent();

    int64_t OperatorAllocatedBytesInternal(int64_t size);

    bool IsOperatorAllocator();

    const std::string scope;
    std::mutex childAllocatorsLock;
    std::list<BaseAllocator *> childAllocators;
    std::list<BaseAllocator *>::iterator childAllocatorIt;

    std::atomic<int64_t> allocatedBytes { 0 };
    std::atomic<int64_t> allocationLimit { UNLIMIT };
    std::atomic<int64_t> peakAllocated { 0 };
    std::atomic<int64_t> reservation { 0 };

    BaseAllocator *parentAllocator;
    MemoryPool *pool = GetMemoryPool();
    int64_t unTrackedBytes { 0 };
    int64_t unFreedBytes { 0 };
    std::atomic<bool> isOperatorAllocator {false };
    std::atomic_flag lock = ATOMIC_FLAG_INIT;
};
BaseAllocator *GetProcessRootAllocator();

void SetRootAllocatorLimit(int64_t limit);
}
}
#endif // __BASE_ALLOCATOR_H__
