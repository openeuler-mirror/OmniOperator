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

namespace omniruntime {
namespace mem {
using PtrMapAllocator = MemoryManagerAllocator<std::pair<uintptr_t, std::pair<int64_t, std::string>>>;
using PtrMap = std::unordered_map<uintptr_t, std::pair<int64_t, std::string>, std::hash<uintptr_t>, std::equal_to<>,
    PtrMapAllocator>;

class MemoryTrace {
public:
    static ALWAYS_INLINE MemoryTrace *GetMemoryTrace()
    {
        static MemoryTrace *memoryTrace = new MemoryTrace();
        return memoryTrace;
    }

    static void AddVectorMemory(uintptr_t ptr, int64_t size);

    static void SubVectorMemory(uintptr_t ptr, int64_t size);

    static void AddArenaMemory(uintptr_t ptr, int64_t size);

    static void SubArenaMemory(uintptr_t ptr, int64_t size);

    void SetVectorAllocated(int64_t size);

    int64_t GetVectorAllocated();

    void SetArenaAllocated(int64_t size);

    int64_t GetArenaAllocated();

    void AddVectorPtrAllocated(uintptr_t ptr, std::pair<int64_t, std::string> pair);

    void SubVectorPtrAllocated(uintptr_t ptr, int64_t size);

    void AddArenaPtrAllocated(uintptr_t ptr, std::pair<int64_t, std::string> pair);

    void SubArenaPtrAllocated(uintptr_t ptr, int64_t size);

    bool HasMemoryLeak();

    void FreeLeakedMemory();

private:
    std::mutex vectorLock;                   // keep memoryAllocatedByDataType thread-safely
    std::mutex arenaLock;                    // keep arenaAllocated thread-safely
    std::atomic<int64_t> curVectorAllocated; // current vector allocated memory size
    std::atomic<int64_t> curArenaAllocated;  // current arena allocated memory size
    PtrMap curVectorPtrAllocated;            // <uintptr : pair<size, stackLog>>
    PtrMap curArenaPtrAllocated;             // <uintptr : pair<size, stackLog>>
};
}
}


#endif // OMNI_RUNTIME_MEMORY_TRACE_H
