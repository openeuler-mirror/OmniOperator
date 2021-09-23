/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <stdint.h>
#include "vector.h"
#include "vector_allocator_factory.h"

namespace omniruntime {
namespace vec {
Vector::Vector(VectorAllocator *allocator, int capacityInBytes, int size, VecType type)
    : allocator(allocator), size(size), positionOffset(0), capacityInBytes(capacityInBytes), type(type)
{
    ASSERT(allocator != nullptr);
    reference = allocator->NewVector(capacityInBytes, size, type);
    valuesAddress = reference->GetValuesAddress();
    valueNullsAddress = reference->GetValueNullsAddress();
    valueOffsetsAddress = reference->GetValueOffsetsAddress();
#ifdef DEBUG_VECTOR
    allocator->GetLeakDetector().Record(this, "", VecOpType::NEW);
#endif
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
#ifdef DEBUG_VECTOR
    allocator->GetLeakDetector().Record(this, "", VecOpType::NEW);
#endif
}

Vector::Vector(VectorAllocator *allocator, int capacityInBytes, int size, VecType type, int32_t positionOffset)
    : allocator(allocator), capacityInBytes(capacityInBytes), size(size), type(type), positionOffset(positionOffset)
{
#ifdef DEBUG_VECTOR
    allocator->GetLeakDetector().Record(this, "", VecOpType::NEW);
#endif
}

Vector::~Vector()
{
#ifdef DEBUG_VECTOR
    this->allocator->GetLeakDetector().Record(this, "", VecOpType::FREE);
#endif
    if (reference == nullptr) {
        return;
    }
    if (0 == reference->DecRef()) {
        delete reference;
        reference = nullptr;
    }
}

void Vector::SetValueNulls(int startIndex, bool *nulls, int length)
{
    errno_t ret = EOK;
    if (length > 0) {
        ret = memcpy_s(((bool *)valueNullsAddress) + startIndex, size, nulls, length);
    }
    if (ret != EOK) {
        std::cerr << "set value nulls failed." << ret << std::endl;
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
