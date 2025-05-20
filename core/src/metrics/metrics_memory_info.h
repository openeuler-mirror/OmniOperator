/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: Metrics memory info header
 */

#ifndef OMNI_RUNTIME_MEMORY_INFO_H
#define OMNI_RUNTIME_MEMORY_INFO_H
#include "type/data_types.h"
#include "unistd.h"
#include "memory/thread_memory_manager.h"
namespace omniruntime {
namespace op {
class MetricsMemoryInfo {
public:
    MetricsMemoryInfo() {}

    ~MetricsMemoryInfo() = default;

    std::string &SetMemoryInfo(const std::unique_ptr<omniruntime::op::ExecutionContext> &executionContext)
    {
        threadAllocMemory = omniruntime::mem::ThreadMemoryManager::GetThreadMemoryManager()->GetAllocMemory();
        threadFreeMemory = omniruntime::mem::ThreadMemoryManager::GetThreadMemoryManager()->GetFreeMemory();
        processAllocMemory = omniruntime::mem::MemoryManager::GetGlobalAccountedMemory();
        FillMemoryInfoStr(executionContext);
        return memoryInfoStr;
    }

private:
    uint64_t threadAllocMemory = 0;
    uint64_t threadFreeMemory = 0;
    uint64_t processAllocMemory = 0;
    std::string memoryInfoStr = "";

    void FillMemoryInfoStr(const std::unique_ptr<omniruntime::op::ExecutionContext> &executionContext)
    {
        memoryInfoStr = "processUsedMemory=" + std::to_string(processAllocMemory) +
            ".ThreadInfo:"
            "allocMemory=" +
            std::to_string(threadAllocMemory) + ",freeMemory=" + std::to_string(threadFreeMemory) + ",remainMemory=" +
            std::to_string(threadAllocMemory - threadFreeMemory) +
            ".ArenaInfo:"
            "totalBytes=" +
            std::to_string(executionContext->GetArena()->TotalBytes()) +
            ",usedBytes=" + std::to_string(executionContext->GetArena()->UsedBytes()) + ",remainingBytes=" +
            std::to_string(executionContext->GetArena()->TotalBytes() - executionContext->GetArena()->UsedBytes()) +
            ".";
    }
};
}
}
#endif // OMNI_RUNTIME_MEMORY_INFO_H
