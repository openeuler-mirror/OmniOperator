/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "decimal128_vector.h"
#include "vector_type.h"
#include "dictionary_vector.h"
#include "../../thirdparty/huawei_secure_c/include/securec.h"
namespace omniruntime {
namespace vec {
Decimal128Vector::Decimal128Vector(VectorAllocator *allocator, int32_t size)
    : Vector(allocator, size * BYTES, size, Decimal128VecType::Instance())
{}

void Decimal128Vector::SetValues(int32_t startIndex, const int64_t *values, int32_t length)
{
    if (!reference->IsWritable() || startIndex + length > size) {
        return;
    }
    void *startAddress = &(((int64_t *)valuesAddress)[startIndex * DECIMAL128_TYPE_WIDTH]);
    errno_t ret = memcpy_s(startAddress, capacityInBytes, values, length * BYTES);
    if (ret != EOK) {
        std::cerr << "set values failed in decimal vector." << std::endl;
    }
}

Decimal128Vector *Decimal128Vector::Slice(int32_t positionOffset, int32_t length)
{
    return new Decimal128Vector(this, length, positionOffset);
}

Decimal128Vector *Decimal128Vector::CopyPositions(const int32_t *positions, int32_t offset, int32_t length)
{
    auto vector = new Decimal128Vector(GetAllocator(), length);
    for (int32_t i = 0; i < length; ++i) {
        int32_t position = positions[offset + i];
        vector->SetValue(i, GetValue(position));
        vector->SetValueNull(i, IsValueNull(position));
    }
    return vector;
}

Decimal128Vector *Decimal128Vector::CopyRegion(int32_t positionOffset, int32_t length)
{
    if (positionOffset + length > size) {
        return nullptr;
    }
    auto vector = new Decimal128Vector(GetAllocator(), length);
    vector->SetValues(0, (int64_t *)valuesAddress + (positionOffset + this->positionOffset) * DECIMAL128_TYPE_WIDTH,
        length);
    vector->SetValueNulls(0, (bool *)valueNullsAddress + positionOffset + this->positionOffset, length);
    return vector;
}

void Decimal128Vector::Append(Vector *other, int32_t positionOffset, int32_t length)
{
    if (positionOffset + length > size) {
        return;
    }
    if (other->GetTypeId() != OMNI_VEC_TYPE_DICTIONARY) {
        int32_t otherPositionOffset = other->GetPositionOffset();
        int64_t *otherValues = static_cast<int64_t *>(other->GetValues()) + otherPositionOffset * DECIMAL128_TYPE_WIDTH;
        bool *otherValueNulls = static_cast<bool *>(other->GetValueNulls()) + otherPositionOffset;
        SetValues(positionOffset, otherValues, length);
        SetValueNulls(positionOffset, otherValueNulls, length);
    } else {
        DictionaryVector *src = static_cast<DictionaryVector *>(other);
        int32_t originalIds[length];
        Decimal128Vector *dictionary =
            static_cast<Decimal128Vector *>(src->ExtractDictionaryAndIds(0, length, originalIds));
        for (int32_t i = 0; i < length; i++) {
            if (dictionary->IsValueNull(originalIds[i])) {
                SetValueNull(positionOffset + i);
            } else {
                SetValue(positionOffset + i, dictionary->GetValue(originalIds[i]));
            }
        }
    }
}
}
}