/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef __VECTOR_ALLOCATOR_FACTORY_H__
#define __VECTOR_ALLOCATOR_FACTORY_H__

#include <map>
#include "vector_allocator.h"

const static std::string GLOBAL_SCOPE_NAME = "___GLOBAL_SCOPE___";

namespace omniruntime {
namespace vec {
class VectorAllocatorFactory {
public:
    VectorAllocatorFactory();

    static VectorAllocator *GetOrCreateAllocator(std::string scope);

    static void DeleteAllocator(VectorAllocator **allocator);

    static VectorAllocator *GetGlobalAllocator();

    ~VectorAllocatorFactory() {}

private:
    static std::map<std::string, VectorAllocator *> &GetAllocatorMap()
    {
        static std::map<std::string, VectorAllocator *> allocators;
        return allocators;
    }
    static std::mutex &GetMutex()
    {
        static std::mutex mutex;
        return mutex;
    }
};
}
}
#endif // __VECTOR_ALLOCATOR_FACTORY_H__
