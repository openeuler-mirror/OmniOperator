/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/1/21.
//

#include <cstring>
#include <stdint.h>
#include "vector.h"
#include "../util/debug.h"
#include "vector_allocator_manager.h"

VectorAllocatorManager g_vector_allocator_manager = VectorAllocatorManager::GetInstance();
VectorAllocator *g_vector_allocator = g_vector_allocator_manager.GetOrCreateAllocator(GLOBAL_SCOPE_NAME);

Vector::Vector(VectorAllocator *allocator, int capacityInBytes, int size, VecType type)
: size(size), positionOffset(0), capacityInBytes(capacityInBytes)
{
    if (allocator != nullptr) {
        reference = allocator->NewVector(capacityInBytes, size, type);
        this->allocator = allocator;
    } else {
        reference = g_vector_allocator->NewVector(capacityInBytes, size, type);
        this->allocator = g_vector_allocator;
    }
    valuesAddress = reference->GetValuesAddress();
    valueNullsAddress = reference->GetValueNullsAddress();
    valueOffsetsAddress = reference->GetValueOffsetsAddress();
}

Vector::Vector(Vector *vector, int size, int positionOffset)
    : allocator(vector->allocator),
      reference(vector->reference),
      size(size),
      positionOffset(vector->positionOffset + positionOffset)
{
    reference->IncRef();
    valuesAddress = reference->GetValuesAddress();
    valueNullsAddress = reference->GetValueNullsAddress();
    valueOffsetsAddress = reference->GetValueOffsetsAddress();
    capacityInBytes = reference->GetCapacityInBytes();
}

Vector::~Vector()
{
    if (reference == nullptr) {
        std::cerr << "reference is null" << std::endl;
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

int Vector::GetSize()
{
    return size;
}

int Vector::GetPositionOffset()
{
    return positionOffset;
}

VectorReference *Vector::GetReference() const
{
    return reference;
}

VectorAllocator *Vector::GetAllocator() const
{
    return allocator;
}

VecType Vector::GetType()
{
    return reference->GetType();
}

void *Vector::GetValues() const
{
    return valuesAddress;
}

void *Vector::GetValueNulls() const
{
    return valueNullsAddress;
}

void *Vector::GetValueOffsets() const
{
    return valueOffsetsAddress;
}

void Vector::SetSize(int size)
{
    this->size = size;
}

void Vector::SetValueNullBitMap(int index)
{
    if (valueNullsAddress != nullptr) {
        // std::cout << "set value null BitMap" << std::endl;
        BitMapUtil::Set(reinterpret_cast<uint8_t *>(valueNullsAddress), index);
    }
}
