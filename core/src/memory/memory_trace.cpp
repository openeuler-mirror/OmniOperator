/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

#include "memory_trace.h"
#include "allocator.h"
#include "vector/vector.h"

namespace omniruntime::mem {
void MemoryTrace::AddVectorMemory(uintptr_t ptr, int64_t size)
{
#ifdef TRACE
    ThreadMemoryTrace *threadMemoryTrace = ThreadMemoryTrace::GetThreadMemoryTrace();
    threadMemoryTrace->AddVectorMemory(ptr, size);
#endif
}

void MemoryTrace::SubVectorMemory(uintptr_t ptr, int64_t size)
{
#ifdef TRACE
    ThreadMemoryTrace *threadMemoryTrace = ThreadMemoryTrace::GetThreadMemoryTrace();
    threadMemoryTrace->RemoveVectorMemory(ptr, size);
#endif
}

void MemoryTrace::AddArenaMemory(uintptr_t ptr, int64_t size)
{
#ifdef TRACE
    ThreadMemoryTrace *threadMemoryTrace = ThreadMemoryTrace::GetThreadMemoryTrace();
    threadMemoryTrace->AddArenaMemory(ptr, size);
#endif
}

void MemoryTrace::SubArenaMemory(uintptr_t ptr, int64_t size)
{
#ifdef TRACE
    ThreadMemoryTrace *threadMemoryTrace = ThreadMemoryTrace::GetThreadMemoryTrace();
    threadMemoryTrace->RemoveArenaMemory(ptr, size);
#endif
}

MemoryTrace::MemoryTrace() {}

MemoryTrace::~MemoryTrace()
{
    threadMemoryTraceSet.clear();
}

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

const std::unordered_set<ThreadMemoryTrace *> &MemoryTrace::GetThreadMemoryTraceSet()
{
    return threadMemoryTraceSet;
}

static MemoryTrace g_globalMemoryTrace;
MemoryTrace *GetMemoryTrace()
{
    return &g_globalMemoryTrace;
}
}