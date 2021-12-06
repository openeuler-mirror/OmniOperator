/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "vector.h"

#include <stdint.h>
#include "../util/bitmap_util.h"
#include "../../thirdparty/huawei_secure_c/include/securec.h"

namespace omniruntime {
namespace vec {
Vector::Vector(VectorAllocator *allocator, int capacityInBytes, int size, VecTypeId typeId)
    : allocator(allocator),
      reference(nullptr),
      size(size),
      positionOffset(0),
      capacityInBytes(capacityInBytes),
      typeId(typeId)
{
    ASSERT(allocator != nullptr);
    allocator->NewVector(this, capacityInBytes, size, typeId);
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
      typeId(vector->typeId)
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

void Vector::RecordStack(std::string &stack, VecOpType opType)
{
    tracer->Record(stack, opType);
}

void Vector::SetVectorTracer(VectorTracer *tracer)
{
    this->tracer = tracer;
}

VectorTracer *Vector::GetVectorTracer()
{
    return this->tracer;
}
}
}
