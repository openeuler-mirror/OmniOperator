/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

#ifndef OMNI_RUNTIME_THREAD_MEMORY_TRACE_H
#define OMNI_RUNTIME_THREAD_MEMORY_TRACE_H

#include <iostream>
#include <unordered_map>

namespace omniruntime {
namespace mem {
/**
 * TLS Object, it is responsible for memory trace per thread.
 *      */
class ThreadMemoryTrace {
public:
    static ThreadMemoryTrace *GetThreadMemoryTrace()
    {
        thread_local ThreadMemoryTrace threadMemoryTrace;
        return &threadMemoryTrace;
    }

    ThreadMemoryTrace();

    ~ThreadMemoryTrace();

    void AddVectorMemory(uintptr_t ptr, int64_t size);

    void RemoveVectorMemory(uintptr_t ptr, int64_t size);

    void AddArenaMemory(uintptr_t ptr, int64_t size);

    void RemoveArenaMemory(uintptr_t ptr, int64_t size);

    std::unordered_map<uintptr_t, int64_t> GetVectorTraced();

    std::unordered_map<uintptr_t, int64_t> GetArenaTraced();

    std::unordered_map<uintptr_t, std::pair<int64_t, std::string>> GetVectorTracedWithLog();

    std::unordered_map<uintptr_t, std::pair<int64_t, std::string>> GetArenaTracedWithLog();

    void ReplaceVectorTracedLog(uintptr_t ptr, const std::string &stack);

    bool HasMemoryLeak();

    void FreeLeakedMemory();

    void Clear();

private:
    // <uintptr, size>, record the size of each vector.
    std::unordered_map<uintptr_t, int64_t> vectorTraced;
    // <uintptr, size>, record the size of each arena.
    std::unordered_map<uintptr_t, int64_t> arenaTraced;

    // <uintptr : pair<size, stackLog>>, record the size and stack of each vector.
    std::unordered_map<uintptr_t, std::pair<int64_t, std::string >> vectorTracedWithLog;
    // <uintptr : pair<size, stackLog>>, record the size and stack of each arena.
    std::unordered_map<uintptr_t, std::pair<int64_t, std::string >> arenaTracedWithLog;
};
}
}
#endif // OMNI_RUNTIME_THREAD_MEMORY_TRACE_H
