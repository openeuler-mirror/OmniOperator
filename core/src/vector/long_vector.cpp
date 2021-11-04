/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "debug.h"
#include "long_vector.h"
#include "dictionary_vector.h"

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
        vector->SetValueNull(i, IsValueNull(position));
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
    if (other->GetTypeId() != OMNI_VEC_TYPE_DICTIONARY) {
        int32_t otherPositionOffset = other->GetPositionOffset();
        int64_t *otherValues = static_cast<int64_t *>(other->GetValues()) + otherPositionOffset;
        bool *otherValueNulls = static_cast<bool *>(other->GetValueNulls()) + otherPositionOffset;
        SetValues(positionOffset, otherValues, length);
        SetValueNulls(positionOffset, otherValueNulls, length);
    } else {
        DictionaryVector *src = static_cast<DictionaryVector *>(other);
        int32_t originalIds[length];
        LongVector *dictionary = static_cast<LongVector *>(src->ExtractDictionaryAndIds(0, length, originalIds));
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
