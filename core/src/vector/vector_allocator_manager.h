/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/1/21.
//

#ifndef __VECTOR_ALLOCATOR_MANAGER_H__
#define __VECTOR_ALLOCATOR_MANAGER_H__

#include <map>
#include "vector_allocator.h"

const static std::string GLOBAL_SCOPE_NAME = "___GLOBAL_SCOPE___";

namespace omniruntime {
namespace vec {
class VectorAllocatorManager {
public:
    VectorAllocatorManager();

    VectorAllocator *GetOrCreateAllocator(std::string scope);

    void DeleteAllocator(VectorAllocator **allocator);

    static const VectorAllocatorManager &GetInstance();

    ~VectorAllocatorManager() {}

public:
private:
    std::map<std::string, VectorAllocator *> allocatorList;
};
}
}
#endif // __VECTOR_ALLOCATOR_MANAGER_H__
