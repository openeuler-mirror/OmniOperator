/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 * Description: boolean vector, store boolean value
 */

#include "debug.h"
#include "boolean_vector.h"

namespace omniruntime {
namespace vec {
BooleanVector::BooleanVector(VectorAllocator *allocator, int size)
    : FixedWidthVector(allocator, size, size, BooleanVecType::Instance())
{}

void BooleanVector::SetValues(int startIndex, const bool *values, int length)
{
    if (!reference->IsWritable() || length > size) {
        return;
    }
    bool *startAddr = reinterpret_cast<bool *>(valuesAddress);
    errno_t ret = memcpy_s(startAddr + startIndex, capacityInBytes, values, length);
    if (ret != EOK) {
        std::cerr << "set values failed in boolean vector." << std::endl;
    }
}

BooleanVector *BooleanVector::Slice(int positionOffset, int length)
{
    return new BooleanVector(this, length, positionOffset);
}

BooleanVector *BooleanVector::CopyPositions(const int *positions, int offset, int length)
{
    BooleanVector *vector = new BooleanVector(GetAllocator(), length);
    for (int i = 0; i < length; ++i) {
        int position = positions[offset + i];
        vector->SetValue(i, GetValue(position));
        vector->SetValueNulls(i, ((bool *)valueNullsAddress) + position + positionOffset, 1);
    }
    return vector;
}

BooleanVector *BooleanVector::CopyRegion(int positionOffset, int length)
{
    if (positionOffset + length > size) {
        return nullptr;
    }
    BooleanVector *vector = new BooleanVector(GetAllocator(), length);
    vector->SetValues(0, (bool *)valuesAddress + positionOffset + this->positionOffset, length);
    vector->SetValueNulls(0, (bool *)valueNullsAddress + positionOffset + this->positionOffset, length);
    return vector;
}

void BooleanVector::Append(Vector *other, int positionOffset, int length)
{
    if (positionOffset + length > size) {
        return;
    }
    uint8_t *destination = reinterpret_cast<uint8_t *>(this->GetValues()) + positionOffset;
    uint8_t *src = (other->GetPositionOffset()) + (reinterpret_cast<uint8_t *>(other->GetValues()));
    errno_t ret = memcpy_s(destination, capacityInBytes, src, length);
    if (ret != EOK) {
        std::cerr << "append failed in boolean vector." << std::endl;
    }
}
} // namespace vec
} // namespace omniruntime