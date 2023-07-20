/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#include <string>
#include <atomic>
#include "util/compiler_util.h"
#include "memory/thread_memory_manager.h"

#ifndef OMNI_RUNTIME_STRING_UTILS_H
#define OMNI_RUNTIME_STRING_UTILS_H
namespace omniruntime::vec {
static constexpr int32_t STRING_SIZE_THRESHOLD = 12;
static constexpr int32_t INITIAL_STRING_SIZE = 1 << 15; // 32K

enum StringWidthType {
    OMNI_SMALL_WIDTH = STRING_SIZE_THRESHOLD,
    OMNI_LARGE_WIDTH
};

enum StringEncoding {
    OMNI_SMALL_STRING = 0,
    OMNI_LARGE_STRING = 1,
    OMNI_INVALID_STRING
};

struct SmallStringBuffer {
    char buffer[STRING_SIZE_THRESHOLD];
};

class LargeStringBuffer {
public:
    LargeStringBuffer(size_t size)
    {
        // Empty strings also need to allocate 1 byte of memory to deal with the null pointer exception.
        capacity = size == 0 ? 1 : size;
        data.reserve(capacity);
        int64_t bufferCapacity = sizeof(LargeStringBuffer) + capacity;
        omniruntime::mem::ThreadMemoryManager::ReportMemory(bufferCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::AddArenaMemory(reinterpret_cast<uintptr_t>(this), bufferCapacity);
#endif
    }

    ~LargeStringBuffer()
    {
        int64_t bufferCapacity = sizeof(LargeStringBuffer) + capacity;
        omniruntime::mem::ThreadMemoryManager::ReclaimMemory(bufferCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::SubArenaMemory(reinterpret_cast<uintptr_t>(this), bufferCapacity);
#endif
    }

    ALWAYS_INLINE uint64_t Capacity()
    {
        return capacity;
    }

    ALWAYS_INLINE uint64_t Size()
    {
        return usedSize;
    }

    ALWAYS_INLINE void SetSize(uint64_t size)
    {
        usedSize = size;
    }

    ALWAYS_INLINE char *Data()
    {
        return data.data();
    };

private:
    std::vector<char> data;
    uint64_t usedSize = 0;
    uint64_t capacity = 0;
    std::atomic<int32_t> referenceCount;
};
}

#endif // OMNI_RUNTIME_STRING_UTILS_H
