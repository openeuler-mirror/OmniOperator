/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#include "memory_trace.h"
#include "allocator.h"
#include "vector/vector.h"

namespace omniruntime::mem {
void MemoryTrace::AddVectorMemory(uintptr_t ptr, int64_t size)
{
    auto trace = MemoryTrace::GetMemoryTrace();
    trace->SetVectorAllocated(size);
    trace->AddVectorPtrAllocated(ptr, std::make_pair(size, TraceUtil::GetStack()));
}

void MemoryTrace::SubVectorMemory(uintptr_t ptr, int64_t size)
{
    auto trace = MemoryTrace::GetMemoryTrace();
    trace->SetVectorAllocated(-size);
    trace->SubVectorPtrAllocated(ptr, size);
}

void MemoryTrace::AddArenaMemory(uintptr_t ptr, int64_t size)
{
    auto trace = MemoryTrace::GetMemoryTrace();
    trace->SetArenaAllocated(size);
    trace->AddArenaPtrAllocated(ptr, std::make_pair(size, TraceUtil::GetStack()));
}

void MemoryTrace::SubArenaMemory(uintptr_t ptr, int64_t size)
{
    auto trace = MemoryTrace::GetMemoryTrace();
    trace->SetArenaAllocated(-size);
    trace->SubArenaPtrAllocated(ptr, size);
}

void MemoryTrace::SetVectorAllocated(int64_t size)
{
    curVectorAllocated.fetch_add(size, std::memory_order_relaxed);
}

int64_t MemoryTrace::GetVectorAllocated()
{
    return curVectorAllocated.load(std::memory_order_relaxed);
}

void MemoryTrace::SetArenaAllocated(int64_t size)
{
    curArenaAllocated.fetch_add(size, std::memory_order_relaxed);
}

int64_t MemoryTrace::GetArenaAllocated()
{
    return curArenaAllocated.load(std::memory_order_relaxed);
}

/**
 * stack will be replaced if vector is created by jni
 *   */
void MemoryTrace::ReplaceVectorPtrAllocated(uintptr_t ptr, const std::string &stack)
{
    std::lock_guard<std::mutex> lock(vectorLock);
    PtrMap::iterator iter;
    if ((iter = curVectorPtrAllocated.find(ptr)) != curVectorPtrAllocated.end()) {
        iter->second.second = stack;
    } else {
        throw OmniException("Memory Trace Error", "vector create failed!");
    }
}

void MemoryTrace::AddVectorPtrAllocated(uintptr_t ptr, const std::pair<int64_t, std::string> &pair)
{
    std::lock_guard<std::mutex> lock(vectorLock);
    curVectorPtrAllocated.emplace(ptr, pair);
}

void MemoryTrace::SubVectorPtrAllocated(uintptr_t ptr, int64_t size)
{
    std::lock_guard<std::mutex> lock(vectorLock);
    PtrMap::iterator iter;
    if ((iter = curVectorPtrAllocated.find(ptr)) != curVectorPtrAllocated.end()) {
        if (iter->second.first != size) {
            auto message =
                "wrong vector size, alloc: " + std::to_string(iter->second.first) + ", free: " + std::to_string(size);
            throw OmniException("Memory Trace Error", message);
        }
        curVectorPtrAllocated.erase(ptr);
    } else {
        throw OmniException("Memory Trace Error", "vector ptr is wrong!");
    }
}

PtrMap MemoryTrace::GetVectorPtrAllocated()
{
    return curVectorPtrAllocated;
}

void MemoryTrace::AddArenaPtrAllocated(uintptr_t ptr, const std::pair<int64_t, std::string> &pair)
{
    std::lock_guard<std::mutex> lock(arenaLock);
    curArenaPtrAllocated.emplace(ptr, pair);
}

void MemoryTrace::SubArenaPtrAllocated(uintptr_t ptr, int64_t size)
{
    std::lock_guard<std::mutex> lock(arenaLock);
    PtrMap::iterator iter;
    if ((iter = curArenaPtrAllocated.find(ptr)) != curArenaPtrAllocated.end()) {
        if (iter->second.first != size) {
            auto message =
                "wrong arena size, alloc: " + std::to_string(iter->second.first) + ", free: " + std::to_string(size);
            LogError("%s.", message.c_str());
            // throw OmniException("Memory Trace Error", message);
        }
        curArenaPtrAllocated.erase(ptr);
    } else {
        LogError("arena ptr is wrong!");
        // throw OmniException("Memory Trace Error", "arena ptr is wrong!");
    }
}

PtrMap MemoryTrace::GetArenaPtrAllocated()
{
    return curArenaPtrAllocated;
}

/**
 * check and print stack if memory leak.
 *   */
bool MemoryTrace::HasMemoryLeak()
{
    int64_t vectorMemory = curVectorAllocated.load(std::memory_order_relaxed);
    int64_t arenaMemory = curArenaAllocated.load(std::memory_order_relaxed);
#ifdef TRACE
    if (!curVectorPtrAllocated.empty()) {
        // print leak stackLog
        std::cout << "memory leak of vector count: " << curVectorPtrAllocated.size() << " total size: " <<
            vectorMemory << std::endl;
        PtrMap::iterator iter;
        for (iter = curVectorPtrAllocated.begin(); iter != curVectorPtrAllocated.end(); ++iter) {
            std::cout << "leaked memory: " << iter->second.first << ", stack is: " << iter->second.second << std::endl;
        }
    }
    if (!curArenaPtrAllocated.empty()) {
        // print leak stackLog
        std::cout << "memory leak of arena count: " << curArenaPtrAllocated.size() << " total size: " << arenaMemory <<
            std::endl;
        PtrMap::iterator iter;
        for (iter = curArenaPtrAllocated.begin(); iter != curArenaPtrAllocated.end(); ++iter) {
            std::cout << "leaked memory: " << iter->second.first << ", stack is: " << iter->second.second << std::endl;
        }
    }
#endif
    return vectorMemory != 0 || arenaMemory != 0;
}

void MemoryTrace::FreeLeakedMemory()
{
#ifdef TRACE
    PtrMap::iterator iter;
    for (iter = curVectorPtrAllocated.begin(); iter != curVectorPtrAllocated.end(); ++iter) {
        // free vector and buffer if vector type is varchar.
        delete reinterpret_cast<vec::BaseVector *>(iter->first);
    }
    Allocator *allocator = Allocator::GetAllocator();
    for (iter = curArenaPtrAllocated.begin(); iter != curArenaPtrAllocated.end(); ++iter) {
        // free arena ptr
        allocator->Free(reinterpret_cast<uint8_t *>(iter->first), iter->second.first);
    }
    Clear();
#endif
}

void MemoryTrace::Clear()
{
    curVectorAllocated.store(0, std::memory_order_relaxed);
    curArenaAllocated.store(0, std::memory_order_relaxed);
    curVectorPtrAllocated.clear();
    curArenaPtrAllocated.clear();
}
}