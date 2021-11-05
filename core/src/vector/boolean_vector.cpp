/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 * Description: boolean vector, store boolean value
 */

#include "debug.h"
#include "boolean_vector.h"
#include "dictionary_vector.h"

namespace omniruntime {
namespace vec {
BooleanVector::BooleanVector(VectorAllocator *allocator, int size)
    : FixedWidthVector(allocator, size, size, BooleanVecType::Instance())
{}

void BooleanVector::SetValues(int startIndex, const bool *values, int length)
{
    if (!reference->IsWritable() || startIndex + length > size) {
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
        vector->SetValueNull(i, IsValueNull(position));
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
    if (other->GetTypeId() != OMNI_VEC_TYPE_DICTIONARY) {
        int32_t otherPositionOffset = other->GetPositionOffset();
        bool *otherValues = static_cast<bool *>(other->GetValues()) + otherPositionOffset;
        bool *otherValueNulls = static_cast<bool *>(other->GetValueNulls()) + otherPositionOffset;
        SetValues(positionOffset, otherValues, length);
        SetValueNulls(positionOffset, otherValueNulls, length);
    } else {
        DictionaryVector *src = static_cast<DictionaryVector *>(other);
        int32_t originalIds[length];
        BooleanVector *dictionary = static_cast<BooleanVector *>(src->ExtractDictionaryAndIds(0, length, originalIds));
        for (int32_t i = 0; i < length; i++) {
            if (dictionary->IsValueNull(originalIds[i])) {
                SetValueNull(positionOffset + i);
            } else {
                SetValue(positionOffset + i, dictionary->GetValue(originalIds[i]));
            }
        }
    }
}
} // namespace vec
} // namespace omniruntime