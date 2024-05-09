/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

#include "memory_trace.h"
#include "allocator.h"
#include "vector/vector.h"

namespace omniruntime::mem {
void MemoryTrace::AddVectorMemory(uintptr_t ptr, int64_t size)
{
    ThreadMemoryTrace *threadMemoryTrace = ThreadMemoryTrace::GetThreadMemoryTrace();
    threadMemoryTrace->AddVectorMemory(ptr, size);
    if (!threadMemoryTrace->GetInsertGlobalFlag()) {
        MemoryTrace *globalMemoryTrace = MemoryTrace::GetMemoryTrace();
        globalMemoryTrace->AddThreadMemoryTrace(threadMemoryTrace);
        threadMemoryTrace->SetInsertGlobalFlag();
    }
}

void MemoryTrace::SubVectorMemory(uintptr_t ptr, int64_t size)
{
    ThreadMemoryTrace *threadMemoryTrace = ThreadMemoryTrace::GetThreadMemoryTrace();
    threadMemoryTrace->RemoveVectorMemory(ptr, size);
}

void MemoryTrace::AddArenaMemory(uintptr_t ptr, int64_t size)
{
    ThreadMemoryTrace *threadMemoryTrace = ThreadMemoryTrace::GetThreadMemoryTrace();
    threadMemoryTrace->AddArenaMemory(ptr, size);
    if (!threadMemoryTrace->GetInsertGlobalFlag()) {
        MemoryTrace *globalMemoryTrace = MemoryTrace::GetMemoryTrace();
        globalMemoryTrace->AddThreadMemoryTrace(threadMemoryTrace);
        threadMemoryTrace->SetInsertGlobalFlag();
    }
}

void MemoryTrace::SubArenaMemory(uintptr_t ptr, int64_t size)
{
    ThreadMemoryTrace *threadMemoryTrace = ThreadMemoryTrace::GetThreadMemoryTrace();
    threadMemoryTrace->RemoveArenaMemory(ptr, size);
}

MemoryTrace::~MemoryTrace() {}

void MemoryTrace::AddThreadMemoryTrace(ThreadMemoryTrace *threadMemoryTrace)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    threadMemoryTraceSet.emplace(threadMemoryTrace);
}

void MemoryTrace::SubThreadMemoryTrace(ThreadMemoryTrace *threadMemoryTrace)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    threadMemoryTraceSet.erase(threadMemoryTrace);
}

std::unordered_set<ThreadMemoryTrace *> MemoryTrace::GetThreadMemoryTraceSet()
{
    return threadMemoryTraceSet;
}
}