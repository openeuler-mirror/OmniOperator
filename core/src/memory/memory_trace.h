/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#ifndef OMNI_RUNTIME_MEMORY_TRACE_H
#define OMNI_RUNTIME_MEMORY_TRACE_H

#include <atomic>
#include <mutex>
#include <unordered_set>
#include <unordered_map>
#include <type/data_type.h>
#include "util/compiler_util.h"
#include "memory_manager_allocator.h"
#include "thread_memory_trace.h"

namespace omniruntime {
namespace mem {
/**
 * it is responsible for memory usage trace of each thread and the global memory usage.
 **/
class MemoryTrace {
public:
    static void AddVectorMemory(uintptr_t ptr, int64_t size);

    static void SubVectorMemory(uintptr_t ptr, int64_t size);

    static void AddArenaMemory(uintptr_t ptr, int64_t size);

    static void SubArenaMemory(uintptr_t ptr, int64_t size);

    MemoryTrace();

    ~MemoryTrace();

    void AddThreadMemoryTrace(ThreadMemoryTrace *threadMemoryTrace);

    void SubThreadMemoryTrace(ThreadMemoryTrace *threadMemoryTrace);

    const std::unordered_set<ThreadMemoryTrace *> &GetThreadMemoryTraceSet();

private:
    std::unordered_set<ThreadMemoryTrace *> threadMemoryTraceSet;
    std::mutex m_mutex;
};

MemoryTrace *GetMemoryTrace();
}
}


#endif // OMNI_RUNTIME_MEMORY_TRACE_H
