/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */
#include "memory_manager.h"

namespace omniruntime::mem {
// constructor for globalMemoryManager
MemoryManager::MemoryManager()
{
    parent.store(nullptr, std::memory_order_relaxed);
    memoryAmount.store(0, std::memory_order_relaxed);
    memoryLimit.store(UNLIMIT, std::memory_order_relaxed);
    memoryPeak.store(0, std::memory_order_relaxed);
    isBlocked.store(false, std::memory_order_relaxed);
}

// constructor for threadMemoryManager
MemoryManager::MemoryManager(MemoryManager *parentMemoryManager)
{
    // The capacity of each thread can also be limited. It needs to be set the corresponding value if necessary.
    int64_t globalMemoryLimit = parentMemoryManager->GetMemoryLimit();
    parent.store(parentMemoryManager, std::memory_order_relaxed);
    memoryAmount.store(0, std::memory_order_relaxed);
    memoryLimit.store(globalMemoryLimit, std::memory_order_relaxed);
    memoryPeak.store(0, std::memory_order_relaxed);
    isBlocked.store(false, std::memory_order_relaxed);
}

MemoryManager::~MemoryManager()
{
    if (parent == nullptr) {
        int64_t finalSize = memoryAmount.load(std::memory_order_relaxed);
        if (finalSize != 0) {
            std::cout << "it may has memory leak, leak size is " << finalSize << std::endl;
        }
#ifdef TRACE
        // if has memoryLeak, it will print and free leaked memory.
        MemoryTrace *trace = MemoryTrace::GetMemoryTrace();
        int64_t vectorMemory = trace->GetVectorAllocated();
        if (vectorMemory != 0) {
            std::cout << "vector has memory leak: leak size is " << vectorMemory << std::endl;
        }
        int64_t arenaMemory = trace->GetArenaAllocated();
        if (arenaMemory != 0) {
            std::cout << "arena has memory leak: leak size is " << arenaMemory << std::endl;
        }
        if (trace->HasMemoryLeak()) {
            trace->FreeLeakedMemory();
        }
        delete trace;
#endif
    }
}

void MemoryManager::UpdatePeak(int64_t size)
{
    int64_t peak = memoryPeak.load(std::memory_order_relaxed);
    if (size > peak) {
        memoryPeak.store(size, std::memory_order_relaxed);
    }
}

#ifdef DEBUG
void MemoryManager::AddScopeAmount(const std::string &scope, int64_t size)
{
    // keep thread safety.
    std::lock_guard<std::mutex> lock(mapLock);
    scopeMap[scope] += size;
    if (auto parentMemoryManager = parent.load(std::memory_order_relaxed)) {
        parentMemoryManager->scopeMap[scope] += size;
    }
}

void MemoryManager::SubScopeAmount(const std::string &scope, int64_t size)
{
    // keep thread safety.
    std::lock_guard<std::mutex> lock(mapLock);
    scopeMap[scope] -= size;
    if (auto parentMemoryManager = parent.load(std::memory_order_relaxed)) {
        parentMemoryManager->scopeMap[scope] -= size;
    }
}
#endif

void MemoryManager::SetParent(MemoryManager *parentMemoryManager)
{
    parent.store(parentMemoryManager, std::memory_order_relaxed);
}

MemoryManager *MemoryManager::GetParent()
{
    return parent.load(std::memory_order_relaxed);
}

void MemoryManager::SetMemoryAmount(int64_t amount)
{
    memoryAmount.store(amount, std::memory_order_relaxed);
}

int64_t MemoryManager::GetMemoryAmount()
{
    return memoryAmount.load(std::memory_order_relaxed);
}

void MemoryManager::SetMemoryLimit(int64_t limit)
{
    memoryLimit.store(limit, std::memory_order_relaxed);
}

int64_t MemoryManager::GetMemoryLimit()
{
    return memoryLimit.load(std::memory_order_relaxed);
}

void MemoryManager::SetMemoryPeak(int64_t peak)
{
    memoryPeak.store(peak, std::memory_order_relaxed);
}

int64_t MemoryManager::GetMemoryPeak()
{
    return memoryPeak.load(std::memory_order_relaxed);
}

bool MemoryManager::IsMemoryManagerBlocked()
{
    return isBlocked.load(std::memory_order_relaxed);
}

#ifdef DEBUG
std::unordered_map<std::string, int64_t, std::hash<std::string>, std::equal_to<std::string>,
    MemoryManagerAllocator<std::pair<std::string, int64_t>>>
MemoryManager::GetScopeMap()
{
    return scopeMap;
}
#endif

void MemoryManager::Clear()
{
    memoryAmount.store(0, std::memory_order_relaxed);
    memoryLimit.store(UNLIMIT, std::memory_order_relaxed);
    memoryPeak.store(0, std::memory_order_relaxed);
    isBlocked.store(false, std::memory_order_relaxed);
#ifdef DEBUG
    // keep thread safety.
    std::lock_guard<std::mutex> lock(mapLock);
    if (auto parentMemoryManager = parent.load(std::memory_order_relaxed)) {
        for (const auto &it : scopeMap) {
            parentMemoryManager->scopeMap.erase(it.first);
        }
    }
    scopeMap.clear();
#endif
}
}