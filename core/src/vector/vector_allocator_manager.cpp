//
// Created by root on 6/1/21.
//

#include "vector_allocator_manager.h"

VectorAllocatorManager::VectorAllocatorManager() {
}

VectorAllocator *VectorAllocatorManager::getOrCreateAllocator(std::string scope) {
    std::map<std::string, VectorAllocator *>::iterator iterator = allocatorList.find(scope);
    VectorAllocator *allocator = nullptr;
    if (iterator == allocatorList.end()) {
        allocator = new VectorAllocator(scope);
        allocatorList[scope] = allocator;
    } else {
        allocator = iterator->second;
    }
    return allocator;
}

void VectorAllocatorManager::deleteAllocator(VectorAllocator **allocator) {
    if (allocator != nullptr && *allocator != nullptr) {
        // TODO: *allocator is no longer a valid pointer if it was freed previously.
        if (allocatorList.find((*allocator)->getScope()) != allocatorList.end()) {
            (*allocator)->freeAllVectors();
            allocatorList.erase((*allocator)->getScope());
            delete *allocator;
        }
        *allocator = nullptr;
    }
}

const VectorAllocatorManager &VectorAllocatorManager::getInstance() {
    static VectorAllocatorManager instance;
    return instance;
}