/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */
#include "thread_memory_manager.h"

namespace omniruntime::mem {
ThreadMemoryManager::ThreadMemoryManager() noexcept
{
    MemoryManager *globalMemoryManager = MemoryManager::GetGlobalMemoryManager();
    thread_local MemoryManager memoryManger(globalMemoryManager);
    currentMemoryManager = &memoryManger;
#ifdef DEBUG
    pthread_getname_np(pthread_self(), currentScope, THREAD_NAME_SIZE);
#endif
}

ThreadMemoryManager::~ThreadMemoryManager() noexcept
{
    currentMemoryManager->SubMemory(untrackedMemory);
    untrackedMemory = 0;
#ifdef DEBUG
    DeleteScope(currentScope);
#endif
}

#ifdef DEBUG
/**
 * The current logic is that when the scope ends,
 * the value of the thread's scopeMap in thread is set to 0, and the value of the global scopeMap decreases.
 * todo: In the future, reference count may be introduced to clear key-value pair.
 *         */
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
