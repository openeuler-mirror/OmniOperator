/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/1/21.
//

#include "vector_allocator_manager.h"

namespace omniruntime {
namespace vec {
VectorAllocatorManager::VectorAllocatorManager() = default;

VectorAllocator *VectorAllocatorManager::GetOrCreateAllocator(std::string scope)
{
    auto iterator = allocatorList.find(scope);
    VectorAllocator *allocator = nullptr;
    if (iterator == allocatorList.end()) {
        allocator = new VectorAllocator(scope);
        allocatorList[scope] = allocator;
    } else {
        allocator = iterator->second;
    }
    return allocator;
}

void VectorAllocatorManager::DeleteAllocator(VectorAllocator **allocator)
{
    if (allocator != nullptr && *allocator != nullptr) {
        // TODO: *allocator is no longer a valid pointer if it was freed previously.
        if (allocatorList.find((*allocator)->GetScope()) != allocatorList.end()) {
            (*allocator)->FreeAllVectors();
            allocatorList.erase((*allocator)->GetScope());
            delete *allocator;
        }
        *allocator = nullptr;
    }
}

const VectorAllocatorManager &VectorAllocatorManager::GetInstance()
{
    static VectorAllocatorManager instance;
    return instance;
}
}
}