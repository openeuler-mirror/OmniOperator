/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <cstring>
#include <stdint.h>
#include "vector.h"
#include "../util/debug.h"
#include "vector_allocator_manager.h"

omniruntime::vec::VectorAllocatorManager g_vectorAllocatorManager =
    omniruntime::vec::VectorAllocatorManager::GetInstance();
omniruntime::vec::VectorAllocator *g_vectorAllocator = g_vectorAllocatorManager.GetOrCreateAllocator(GLOBAL_SCOPE_NAME);
namespace omniruntime {
namespace vec {
Vector::Vector(VectorAllocator *allocator, int capacityInBytes, int size, VecType type)
    : size(size), positionOffset(0), capacityInBytes(capacityInBytes), type(type)
{
    if (allocator != nullptr) {
        reference = allocator->NewVector(capacityInBytes, size, type);
        this->allocator = allocator;
    } else {
        reference = g_vectorAllocator->NewVector(capacityInBytes, size, type);
        this->allocator = g_vectorAllocator;
    }
    valuesAddress = reference->GetValuesAddress();
    valueNullsAddress = reference->GetValueNullsAddress();
    valueOffsetsAddress = reference->GetValueOffsetsAddress();
}

Vector::Vector(Vector *vector, int size, int positionOffset)
    : allocator(vector->allocator),
      reference(vector->reference),
      size(size),
      positionOffset(vector->positionOffset + positionOffset),
      capacityInBytes(vector->GetCapacityInBytes()),
      type(vector->type)

{
    reference->IncRef();
    valuesAddress = reference->GetValuesAddress();
    valueNullsAddress = reference->GetValueNullsAddress();
    valueOffsetsAddress = reference->GetValueOffsetsAddress();
}

Vector::~Vector()
{
    if (reference == nullptr) {
        return;
    }
    if (0 == reference->DecRef()) {
        delete reference;
    }
}

void Vector::SetValueNulls(int startIndex, bool *nulls, int length)
{
    errno_t ret = memcpy_s(((bool *)valueNullsAddress) + startIndex, capacityInBytes, nulls, length);
    if (ret != EOK) {
        std::cerr << "set value nulls failed." << std::endl;
    }
}

void Vector::SetValueNullBitMap(int index)
{
    if (valueNullsAddress != nullptr) {
        // std::cout << "set value null BitMap" << std::endl;
        BitMapUtil::Set(reinterpret_cast<uint8_t *>(valueNullsAddress), index);
    }
}
}
}
