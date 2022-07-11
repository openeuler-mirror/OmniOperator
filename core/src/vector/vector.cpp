/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "vector.h"
#include <huawei_secure_c/include/securec.h>

namespace omniruntime {
namespace vec {
Vector::Vector(VectorAllocator *allocator, int capacityInBytes, int size, DataTypeId dataTypeId)
    : positionOffset(0),
      capacityInBytes(capacityInBytes),
      size(size),
      dataTypeId(dataTypeId),
      reference(nullptr),
      allocator(allocator),
      hasNull(false),
      nullCount(UNKNOWN_NULL_COUNT)
{
    ASSERT(allocator != nullptr);
    allocator->NewVector(this, capacityInBytes, size, dataTypeId);
    valuesAddress = reference->GetValuesAddress();
    valueNullsAddress = reference->GetValueNullsAddress();
    valueOffsetsAddress = reference->GetValueOffsetsAddress();
}

Vector::Vector(Vector *vector, int size, int positionOffset)
    : positionOffset(vector->positionOffset + positionOffset),
      capacityInBytes(vector->GetCapacityInBytes()),
      size(size),
      dataTypeId(vector->dataTypeId),
      reference(vector->reference),
      allocator(vector->allocator),
      hasNull(vector->hasNull),
      nullCount(vector->nullCount == 0 ? 0 : UNKNOWN_NULL_COUNT)
{
    allocator->SliceVector(vector, this);
    valuesAddress = reference->GetValuesAddress();
    valueNullsAddress = reference->GetValueNullsAddress();
    valueOffsetsAddress = reference->GetValueOffsetsAddress();
}

Vector::~Vector()
{
    allocator->DeleteVector(this);
    valuesAddress = nullptr;
    valueNullsAddress = nullptr;
    valueOffsetsAddress = nullptr;
}

void Vector::SetValueNulls(int startIndex, bool *nulls, int length)
{
    for (int32_t i = 0; i < length; i++) {
        SetValueNull(i + startIndex, nulls[i]);
    }
}

void Vector::RecordStack(std::string &stack, VecOpType opType)
{
    tracer->Record(stack, opType);
}

void Vector::SetVectorTracer(VectorTracer *vectorTracer)
{
    this->tracer = vectorTracer;
}

VectorTracer *Vector::GetVectorTracer()
{
    return this->tracer;
}
}
}
