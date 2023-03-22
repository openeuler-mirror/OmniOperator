/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#include "fixed_width_vector.h"

namespace omniruntime {
namespace vec {
template <DataTypeId TYPE_ID>
FixedWidthVector<TYPE_ID>::FixedWidthVector(VectorAllocator *allocator, int size)
    : Vector(allocator, BYTES * size, size, TYPE_ID)
{}

template <DataTypeId TYPE_ID>
FixedWidthVector<TYPE_ID>::FixedWidthVector(Vector *vector, int size, int positionOffset)
    : Vector(vector, size, positionOffset)
{}

template <DataTypeId TYPE_ID> void FixedWidthVector<TYPE_ID>::SetValues(int startIndex, const void *values, int length)
{
    if (!reference->IsWritable() || startIndex + length > size) {
        LogError("vector is not writable(%d) or out of range(needed size:%d, real size:%d).", reference->IsWritable(),
            startIndex + length, size);
        return;
    }
    T *startAddr = reinterpret_cast<T *>(valuesAddress);
    errno_t ret = memcpy_s(startAddr + startIndex, capacityInBytes, values, length * BYTES);
    if (ret != EOK) {
        LogError("memory copy failed.");
    }
}

template <DataTypeId TYPE_ID> FixedWidthVector<TYPE_ID> *FixedWidthVector<TYPE_ID>::Slice(int startIndex, int length)
{
    if (startIndex + length > size) {
        LogError("slice vector out of range(needed size:%d, real size:%d).", startIndex + length, size);
        return nullptr;
    }
    return new FixedWidthVectorImpl(this, length, startIndex);
}

template <DataTypeId TYPE_ID>
FixedWidthVector<TYPE_ID> *FixedWidthVector<TYPE_ID>::CopyPositions(const int *positions, int offset, int length)
{
    auto *vector = new FixedWidthVectorImpl(GetAllocator(), length);
    for (int i = 0; i < length; ++i) {
        int position = positions[offset + i];
        vector->SetValue(i, GetValue(position));
        vector->SetValueNull(i, IsValueNull(position));
    }
    return vector;
}

template <DataTypeId TYPE_ID>
FixedWidthVector<TYPE_ID> *FixedWidthVector<TYPE_ID>::CopyRegion(int startIndex, int length)
{
    if (startIndex + length > size) {
        LogError("copy region out of range(needed size:%d, real size:%d).", startIndex + length, size);
        return nullptr;
    }
    auto *vector = new FixedWidthVectorImpl(GetAllocator(), length);
    vector->SetValues(0, static_cast<T *>(valuesAddress) + startIndex + this->positionOffset, length);
    vector->SetValueNulls(0, static_cast<bool *>(valueNullsAddress) + startIndex + this->positionOffset, length);
    return vector;
}

template <DataTypeId TYPE_ID> void FixedWidthVector<TYPE_ID>::Append(Vector *other, int startIndex, int length)
{
    if (startIndex + length > size) {
        LogError("append vector out of range(needed size:%d, real size:%d).", startIndex + length, size);
        return;
    }

    if (other->GetEncoding() != OMNI_VEC_ENCODING_DICTIONARY) {
        int32_t otherPositionOffset = other->GetPositionOffset();
        void *otherValues = static_cast<T *>(other->GetValues()) + otherPositionOffset;
        bool *otherValueNulls = static_cast<bool *>(other->GetValueNulls()) + otherPositionOffset;
        SetValues(startIndex, otherValues, length);
        SetValueNulls(startIndex, otherValueNulls, length);
    } else {
        auto *src = static_cast<DictionaryVector *>(other);
        int32_t originalIds[length];
        auto *dictionary = static_cast<FixedWidthVectorImpl *>(src->ExtractDictionaryAndIds(0, length, originalIds));
        for (int32_t i = 0; i < length; i++) {
            if (dictionary->IsValueNull(originalIds[i])) {
                SetValueNull(startIndex + i);
            } else {
                SetValue(startIndex + i, dictionary->GetValue(originalIds[i]));
            }
        }
    }
}

template <DataTypeId TYPE_ID>
StringRef FixedWidthVector<TYPE_ID>::SerializeValue(size_t rowId, SimpleArenaAllocator &arenaAllocator,
    const uint8_t *&begin)
{
    StringRef res;
    res.size = sizeof(bool) + sizeof(T);
    auto *pos = arenaAllocator.AllocateContinue(res.size, begin);
    bool isNull = IsValueNull(rowId);
    std::copy(reinterpret_cast<uint8_t *>(&isNull), reinterpret_cast<uint8_t *>(&isNull) + sizeof(bool), pos);

    if (not isNull) {
        auto value = GetValue(rowId);
        std::copy(reinterpret_cast<uint8_t *>(&value), reinterpret_cast<uint8_t *>(&value) + BYTES, pos + sizeof(bool));
    } else {
        memset_sp(pos + sizeof(bool), BYTES, 0, BYTES);
    }
    res.data = reinterpret_cast<char *>(pos);
    return res;
}

template <DataTypeId TYPE_ID>
const uint8_t *FixedWidthVector<TYPE_ID>::DeserializeValueIntoThis(size_t rowId, const uint8_t *pos)
{
    bool isNull = *(reinterpret_cast<const bool *>(pos));
    auto *copyPointer = reinterpret_cast<const T *>(pos + 1);
    if (not isNull) {
        SetValue(rowId, *copyPointer);
    } else {
        SetValueNull(rowId);
    }
    return pos + BYTES + 1;
}

template class FixedWidthVector<type::OMNI_BOOLEAN>;
template class FixedWidthVector<type::OMNI_INT>;
template class FixedWidthVector<type::OMNI_SHORT>;
template class FixedWidthVector<type::OMNI_LONG>;
template class FixedWidthVector<type::OMNI_DOUBLE>;
template class FixedWidthVector<type::OMNI_DECIMAL128>;
} // namespace vec
} // namespace omniruntime