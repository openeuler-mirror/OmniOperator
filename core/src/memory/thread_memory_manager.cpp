/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#include "thread_memory_manager.h"

namespace omniruntime::mem {
ThreadMemoryManager::ThreadMemoryManager() noexcept
{
    MemoryManager *globalMemoryManager = MemoryManager::GetGlobalMemoryManager();
    thread_local MemoryManager memoryManger(globalMemoryManager);
    currentMemoryManager = &memoryManger;
#ifdef DEBUG
    char threadName[16] = {0};
    prctl(PR_GET_NAME, threadName);
    StartScope(threadName);
#endif
}

ThreadMemoryManager::~ThreadMemoryManager() noexcept
{
    currentMemoryManager->Account(untrackedMemory);
    untrackedMemory = 0;
#ifdef DEBUG
    char threadName[16] = {0};
    prctl(PR_GET_NAME, threadName);
    RemoveScope(threadName);
    DeleteScope(threadName);
#endif
}

#ifdef DEBUG
void ThreadMemoryManager::StartScope(const std::string &scope)
{
    currentScope = scope;
}

void ThreadMemoryManager::RemoveScope(const std::string &scope)
{
    currentScope = "";
}

/**
 * The current logic is that when the scope ends,
 * the value of the thread's scopeMap in thread is set to 0, and the value of the global scopeMap decreases.
 * todo: In the future, reference count may be introduced to clear key-value pair.
 *   */
void ThreadMemoryManager::DeleteScope(const std::string &scope)
{
    std::unordered_map<std::string, int64_t, std::hash<std::string>, std::equal_to<std::string>,
        MemoryManagerAllocator<std::pair<std::string, int64_t>>>
        map = currentMemoryManager->GetScopeMap();
    if (map.find(scope) != map.end()) {
        int64_t size = map.find(scope)->second;
        currentMemoryManager->SubScopeAmount(scope, size);
    }
}
#endif

void ThreadMemoryManager::Clear()
{
    currentMemoryManager->Clear();
    untrackedMemory = 0;
    if (auto parentMemoryManager = currentMemoryManager->GetParent()) {
        parentMemoryManager->Clear();
    }
}

int64_t ThreadMemoryManager::GetUntrackedMemory() const
{
    return untrackedMemory;
}

int64_t ThreadMemoryManager::GetThreadAccountedMemory()
{
    return currentMemoryManager->GetMemoryAmount();
}
}
