/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "debug.h"
#include "long_vector.h"

namespace omniruntime {
namespace vec {
LongVector::LongVector(VectorAllocator *allocator, int size)
    : FixedWidthVector(allocator, size * BYTES, size, LongVecType::Instance())
{}

void LongVector::SetValues(int startIndex, const int64_t *values, int length)
{
    if (!reference->IsWritable() || startIndex + length > size) {
        return;
    }
    void *startAddress = &(((int64_t *)valuesAddress)[startIndex]);
    errno_t ret = memcpy_s(startAddress, capacityInBytes, values, length * BYTES);
    if (ret != EOK) {
        std::cerr << "memcpy failed in long vector set values." << std::endl;
    }
}

LongVector *LongVector::Slice(int positionOffset, int length)
{
    return new LongVector(this, length, positionOffset);
}

LongVector *LongVector::CopyPositions(const int *positions, int offset, int length)
{
    LongVector *vector = new LongVector(GetAllocator(), length);
    for (int i = 0; i < length; ++i) {
        int position = positions[offset + i];
        vector->SetValue(i, GetValue(position));
        vector->SetValueNulls(i, ((bool *)valueNullsAddress) + position + positionOffset, 1);
    }
    return vector;
}

LongVector *LongVector::CopyRegion(int positionOffset, int length)
{
    if (positionOffset + length > size) {
        return nullptr;
    }
    LongVector *vector = new LongVector(GetAllocator(), length);
    vector->SetValues(0, (int64_t *)valuesAddress + positionOffset + this->positionOffset, length);
    vector->SetValueNulls(0, (bool *)valueNullsAddress + positionOffset + this->positionOffset, length);
    return vector;
}

void LongVector::Append(Vector *other, int positionOffset, int length)
{
    if (positionOffset + length > size) {
        return;
    }

    int32_t otherPositionOffset = other->GetPositionOffset();
    int64_t *otherValues = static_cast<int64_t *>(other->GetValues()) + otherPositionOffset;
    bool *otherValueNulls = static_cast<bool *>(other->GetValueNulls()) + otherPositionOffset;
    SetValues(positionOffset, otherValues, length);
    SetValueNulls(positionOffset, otherValueNulls, length);
}
} // namespace vec
} // namespace omniruntime
