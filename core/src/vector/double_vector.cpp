/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "debug.h"
#include <cstring>
#include "double_vector.h"

namespace omniruntime {
namespace vec {
DoubleVector::DoubleVector(VectorAllocator *allocator, int size)
    : FixedWidthVector(allocator, size * BYTES, size, OMNI_VEC_TYPE_DOUBLE)
{}

void DoubleVector::SetValues(int startIndex, const double *values, int length)
{
    if (!const_cast<VectorReference *>(GetReference())->IsWritable() || startIndex + length > size) {
        return;
    }
    void *startAddress = &(((double *)valuesAddress)[startIndex]);
    errno_t ret = memcpy_s(startAddress, capacityInBytes, values, length * BYTES);
    if (ret != EOK) {
        std::cerr << "set values failed in double vector." << std::endl;
    }
}

DoubleVector *DoubleVector::Slice(int positionOffset, int length)
{
    return new DoubleVector(this, length, positionOffset);
}

DoubleVector *DoubleVector::CopyPositions(const int *positions, int offset, int length)
{
    if (length > size) {
        return nullptr;
    }
    DoubleVector *vector = new DoubleVector(GetAllocator(), length);
    for (int i = 0; i < length; ++i) {
        int position = positions[offset + i];
        vector->SetValue(i, GetValue(position));
        vector->SetValueNulls(i, ((bool *)valueNullsAddress) + position + positionOffset, 1);
    }
    return vector;
}

DoubleVector *DoubleVector::CopyRegion(int positionOffset, int length)
{
    if (positionOffset + length > size) {
        return nullptr;
    }
    DoubleVector *vector = new DoubleVector(GetAllocator(), length);
    vector->SetValues(0, (double *)valuesAddress + positionOffset + this->positionOffset, length);
    vector->SetValueNulls(0, (bool *)valueNullsAddress + positionOffset + this->positionOffset, length);
    return vector;
}

void DoubleVector::Append(Vector *other, int positionOffset, int length)
{
    if (positionOffset + length > size) {
        return;
    }
    uint8_t *destination = (uint8_t *)this->GetValues() + positionOffset * BYTES;
    uint8_t *src = (other->GetPositionOffset() * BYTES) + (reinterpret_cast<uint8_t *>(other->GetValues()));
    errno_t ret = memcpy_s(destination, capacityInBytes, src, length * BYTES);
    if (ret != EOK) {
        std::cerr << "append failed in double vector." << std::endl;
    }
}
}
}