/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_allocator_factory.h"
#include "../../config.h"

namespace omniruntime {
namespace vec {
VectorAllocatorFactory::VectorAllocatorFactory() = default;

VectorAllocator *VectorAllocatorFactory::GetOrCreateAllocator(std::string scope)
{
#ifdef DEBUG_VECTOR
    auto &allocatorMap = GetAllocatorMap();
    auto &mutex = GetMutex();
    mutex.lock();
    auto iterator = allocatorMap.find(scope);
    VectorAllocator *allocator = nullptr;
    if (iterator == allocatorMap.end()) {
        allocator = new VectorAllocator(scope);
        allocatorMap[scope] = allocator;
    } else {
        allocator = iterator->second;
    }
    mutex.unlock();
    return allocator;
#else
    return GetGlobalAllocator();
#endif
}

void VectorAllocatorFactory::DeleteAllocator(VectorAllocator **allocator)
{
#ifdef DEBUG_VECTOR
    auto &allocatorMap = GetAllocatorMap();
    auto &mutex = GetMutex();
    mutex.lock();
    if (allocator != nullptr && *allocator != nullptr) {
        if (allocatorMap.find((*allocator)->GetScope()) != allocatorMap.end()) {
            allocatorMap.erase((*allocator)->GetScope());
            delete *allocator;
        }
        *allocator = nullptr;
    }
    mutex.unlock();
#else
    if (allocator != nullptr) {
        *allocator = nullptr;
    }
#endif
}

VectorAllocator *VectorAllocatorFactory::GetGlobalAllocator()
{
#ifdef DEBUG_VECTOR
    static VectorAllocator *globalAllocator = VectorAllocatorFactory::GetOrCreateAllocator(GLOBAL_SCOPE_NAME);
#else
    static VectorAllocator *globalAllocator = new VectorAllocator(GLOBAL_SCOPE_NAME);
#endif
    return globalAllocator;
}
}
}