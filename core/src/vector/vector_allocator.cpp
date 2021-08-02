/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/1/21.
//

#include "vector_allocator.h"

namespace omniruntime {
namespace vec {
VectorAllocator::VectorAllocator(std::string scope) : scope(scope)
{
    vectorList.clear();
}

// TODO: support different type of vector.
VectorReference *VectorAllocator::NewVector(int capacityInBytes, int size, VecType type)
{
    Chunk *values = new Chunk(capacityInBytes);
    Chunk *valueNulls = new Chunk(size);
    Chunk *valueOffsets = nullptr;
    if (IsVariableWidthType(type)) {
        // 4-byte length storage variable length type offset
        valueOffsets = new Chunk((size + 1) * sizeof(int32_t));
    }
    return new VectorReference(values, valueNulls, valueOffsets, capacityInBytes, type);
}

void VectorAllocator::FreeAllVectors() {}

std::string VectorAllocator::GetScope() const
{
    return scope;
}

int64_t VectorAllocator::GetAllocatedBytes()
{
    return allocatedBytes;
}

bool VectorAllocator::IsVariableWidthType(int type)
{
    switch (type) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DOUBLE:
        case OMNI_VEC_TYPE_SHORT:
        case OMNI_VEC_TYPE_BOOLEAN:
            return false;
        case OMNI_VEC_TYPE_VARCHAR:
            return true;
        default:
            // TODO: throw error.
            return false;
    }
}
}
}