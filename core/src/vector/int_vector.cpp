/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "debug.h"
#include <cstring>
#include "int_vector.h"

namespace omniruntime {
namespace vec {
IntVector::IntVector(VectorAllocator *allocator, int size)
    : FixedWidthVector(allocator, size * BYTES, size, IntVecType::Instance())
{}

void IntVector::SetValues(int startIndex, const int32_t *values, int length)
{
    if (!reference->IsWritable() || startIndex + length > size) {
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
    IntVector *vector = new IntVector(GetAllocator(), length);
    for (int i = 0; i < length; ++i) {
        int position = positions[offset + i];
        vector->SetValue(i, GetValue(position));
        vector->SetValueNull(i, IsValueNull(position));
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

    int32_t otherPositionOffset = other->GetPositionOffset();
    int32_t *otherValues = static_cast<int32_t *>(other->GetValues()) + otherPositionOffset;
    bool *otherValueNulls = static_cast<bool *>(other->GetValueNulls()) + otherPositionOffset;
    SetValues(positionOffset, otherValues, length);
    SetValueNulls(positionOffset, otherValueNulls, length);
}
} // namespace vec
} // namespace omniruntime