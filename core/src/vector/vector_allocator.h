/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/1/21.
//

#ifndef __VECTOR_ALLOCATOR_H__
#define __VECTOR_ALLOCATOR_H__

#include <string>
#include <list>
#include <atomic>
#include "chunk.h"
#include "vector_reference.h"
#include "vector_type.h"

namespace omniruntime {
namespace vec {
class VectorAllocator {
public:
    VectorAllocator(const std::string scope);

    VectorReference *NewVector(int capacityInBytes, int size, VecType type);

    void FreeAllVectors();

    std::string GetScope() const;

    int64_t GetAllocatedBytes();

    ~VectorAllocator() {}

private:
    bool IsVariableWidthType(int type);

private:
    const std::string scope;
    // TODO: per list per CPU core.
    std::list<VectorReference *> vectorList;
    std::atomic<int64_t> allocatedBytes;
};
}
}
#endif // __VECTOR_ALLOCATOR_H__
