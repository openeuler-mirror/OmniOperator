/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "debug.h"
#include <cstring>
#include "int_vector.h"

IntVector::IntVector(VectorAllocator *allocator, int size)
    : FixedWidthVector(allocator, size * BYTES, size, OMNI_VEC_TYPE_INT)
{}

void IntVector::SetValues(int startIndex, const int32_t *values, int length)
{
    if (!GetReference()->IsWritable() || startIndex + length > size) {
        return;
    }
    void *startAddress = &(((int32_t *)valuesAddress)[startIndex]);
    errno_t ret = memcpy_s(startAddress, capacityInBytes, values, length * BYTES);
    if (ret != EOK) {
        std::cerr << "set values failed in int vector." << std::endl;
    }
}

IntVector *IntVector::Slice(int positionOffset, int length)
{
    return new IntVector(this, length, positionOffset);
}
IntVector *IntVector::CopyPositions(const int *positions, int offset, int length)
{
    if (length > size) {
        return nullptr;
    }
    IntVector *vector = new IntVector(GetAllocator(), length);
    for (int i = 0; i < length; ++i) {
        int position = positions[offset + i];
        vector->SetValue(i, GetValue(position));
        vector->SetValueNulls(i, ((bool *)valueNullsAddress) + position + positionOffset, 1);
    }
    return vector;
}

IntVector *IntVector::CopyRegion(int positionOffset, int length)
{
    if (positionOffset + length > size) {
        return nullptr;
    }
    IntVector *vector = new IntVector(GetAllocator(), length);
    vector->SetValues(0, (int32_t *)valuesAddress + positionOffset + this->positionOffset, length);
    vector->SetValueNulls(0, (bool *)valueNullsAddress + positionOffset + this->positionOffset, length);
    return vector;
}

void IntVector::Append(Vector *other, int positionOffset, int length)
{
    if (positionOffset + length > size) {
        return;
    }
    uint8_t *destination = (uint8_t *)this->GetValues() + positionOffset * BYTES;
    uint8_t *src = (other->GetPositionOffset() * BYTES) + (reinterpret_cast<uint8_t *>(other->GetValues()));
    errno_t ret = memcpy_s(destination, capacityInBytes, src, length * BYTES);
    if (ret != EOK) {
        std::cerr << "append failed in int vector." << std::endl;
    }
}
