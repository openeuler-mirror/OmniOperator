/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef __VARIABLE_WIDTH_VECTOR_OPERATOR_H__
#define __VARIABLE_WIDTH_VECTOR_OPERATOR_H__

#include <huawei_secure_c/include/securec.h>

#include "vector.h"
#include "dictionary_vector.h"

namespace omniruntime {
namespace vec {
template <DataTypeId TYPE_ID> class VariableWidthVector : public Vector {
    using T = typename type::NativeType<TYPE_ID>::type;
    using VariableWidthVectorImpl = VariableWidthVector<TYPE_ID>;

public:
    VariableWidthVector(Vector *vector, int size, int positionOffset) : Vector(vector, size, positionOffset) {};

    VariableWidthVector(VectorAllocator *pAllocator, int capacityInBytes, int size)
        : Vector(pAllocator, capacityInBytes, size, TYPE_ID)
    {}

    VariableWidthVector(VectorAllocator *pAllocator, int size)
        : Vector(pAllocator, INI_CAPACITY_IN_BYTES, size, TYPE_ID)
    {}

    int ALWAYS_INLINE GetValue(int index, T **dst)
    {
        int actualIndex = index + positionOffset;
        int startOffset = GetValueOffset(actualIndex);
        int dataLen = GetValueOffset(actualIndex + 1) - startOffset;
        if (dataLen < 0) {
            LogError("get value by index %d failed, actual index:%d, start offset:%d, data length:%d ", index,
                actualIndex, startOffset, dataLen);
            return -1;
        }
        *dst = static_cast<T *>(valuesAddress) + startOffset;
        return dataLen;
    }

    void ALWAYS_INLINE SetValue(int index, const T *value, int length)
    {
        FillSlots(index);
        SetData(index, value, 0, length);
        lastOffsetPosition = index;
    }

    void ALWAYS_INLINE SetValueNull(int index)
    {
        FillSlots(index);
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = true;
        SetValueOffset(index + 1, GetValueOffset(index));
        lastOffsetPosition = index;
    }

    void ALWAYS_INLINE SetValueNull(int index, bool value)
    {
        FillSlots(index);
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = value;
        if (value) {
            SetValueOffset(index + 1, GetValueOffset(index));
        }
        lastOffsetPosition = index;
    }

    VariableWidthVectorImpl *Slice(int startIndex, int length) override
    {
        if (startIndex + length > size) {
            LogError("slice vector out of range(needed size:%d, real size:%d).", startIndex + length, size);
            return nullptr;
        }
        return new VariableWidthVectorImpl(this, length, startIndex);
    }

    VariableWidthVectorImpl *CopyPositions(const int *positions, int offset, int length) override
    {
        int totalDataLen = 0;
        for (int i = 0; i < length; i++) {
            if (IsValueNull(positions[offset + i])) {
                continue;
            }
            int position = positions[offset + i] + positionOffset;
            totalDataLen += GetValueOffset(position + 1) - GetValueOffset(position);
        }
        auto *vector = new VariableWidthVectorImpl(GetAllocator(), totalDataLen, length);
        for (int i = 0; i < length; i++) {
            if (IsValueNull(positions[offset + i])) {
                vector->SetValueNull(i);
                continue;
            }
            int position = positions[offset + i] + positionOffset;
            int startOffset = GetValueOffset(position);
            int dataLen = GetValueOffset(position + 1) - startOffset;
            T *data = reinterpret_cast<T *>(valuesAddress);
            vector->SetValue(i, data + startOffset, dataLen);
        }
        return vector;
    }

    VariableWidthVectorImpl *CopyRegion(int startIndex, int length) override
    {
        if (startIndex + length > size) {
            LogError("copy region out of range(needed size:%d, real size:%d).", startIndex + length, size);
            return nullptr;
        }

        int newPosition = startIndex + this->positionOffset;
        int startOffset = GetValueOffset(newPosition);
        int totalDataLen = GetValueOffset(newPosition + length) - GetValueOffset(newPosition);

        auto *vector = new VariableWidthVectorImpl(GetAllocator(), totalDataLen, length);

        // copy offset
        auto *offsets = reinterpret_cast<int32_t *>(vector->GetValueOffsets());
        for (int32_t i = 1; i <= length; i++) {
            offsets[i] = GetValueOffset(newPosition + i) - GetValueOffset(newPosition);
        }

        // copy nulls
        vector->SetValueNulls(0, static_cast<bool *>(valueNullsAddress) + startIndex + this->positionOffset, length);

        // copy data
        errno_t ret = EOK;
        if (totalDataLen > 0) {
            ret = memcpy_s(reinterpret_cast<T *>(vector->GetValues()), totalDataLen,
                (reinterpret_cast<T *>(valuesAddress)) + startOffset, totalDataLen);
        }

        if (ret != EOK) {
            LogError("copy region failed in varchar vector: %d", ret);
            delete vector;
            return nullptr;
        }
        return vector;
    }

    void Append(Vector *other, int startIndex, int length) override
    {
        if (startIndex + length > size) {
            LogError("append vector out of range(needed size:%d, real size:%d).", startIndex + length, size);
            return;
        }

        T *value = nullptr;
        int32_t valueLen = 0;
        if (other->GetEncoding() != OMNI_VEC_ENCODING_DICTIONARY) {
            VariableWidthVectorImpl *src = static_cast<VariableWidthVectorImpl *>(other);
            for (int32_t i = 0; i < length; i++) {
                if (other->IsValueNull(i)) {
                    SetValueNull(startIndex + i);
                } else {
                    valueLen = src->GetValue(i, &value);
                    SetValue(startIndex + i, value, valueLen);
                }
            }
        } else {
            DictionaryVector *src = static_cast<DictionaryVector *>(other);
            int32_t originalIds[length];
            VariableWidthVectorImpl *dictionary =
                static_cast<VariableWidthVectorImpl *>(src->ExtractDictionaryAndIds(0, length, originalIds));
            for (int32_t i = 0; i < length; i++) {
                if (dictionary->IsValueNull(originalIds[i])) {
                    SetValueNull(startIndex + i);
                } else {
                    valueLen = dictionary->GetValue(originalIds[i], &value);
                    SetValue(startIndex + i, value, valueLen);
                }
            }
        }
    }

    int64_t ExpandDataCapacity(int32_t toCapacityInBytes) override
    {
        allocator->ResizeVectorData(this, toCapacityInBytes);
        valuesAddress = reference->GetValuesAddress();
        capacityInBytes = toCapacityInBytes;
        int64_t newAddress = reinterpret_cast<uintptr_t>(valuesAddress);
        return newAddress;
    }

private:
    static const int32_t INI_CAPACITY_IN_BYTES = 32 * 1024; // 32K

    static const int32_t EXPAND_FACTOR = 2;

    void CheckCapacity(int32_t needCapacityInBytes)
    {
        if (needCapacityInBytes <= 0) {
            return;
        }
        int32_t toCapacityInBytes = (capacityInBytes > 0) ? capacityInBytes : INI_CAPACITY_IN_BYTES;
        while (toCapacityInBytes < needCapacityInBytes) {
            toCapacityInBytes = toCapacityInBytes * EXPAND_FACTOR;
        }
        if (toCapacityInBytes != capacityInBytes) {
            ExpandDataCapacity(toCapacityInBytes);
        }
    }

    void SetData(int index, const T *value, int start, int dataLen)
    {
        int startOffset = GetValueOffset(index);
        CheckCapacity(startOffset + dataLen);
        SetValueOffset(index + 1, startOffset + dataLen);

        // empty vector or empty string no need to copy data
        if (value == nullptr || dataLen == 0 || capacityInBytes == 0) {
            return;
        }
        T *data = reinterpret_cast<T *>(valuesAddress);
        errno_t ret = memcpy_s(data + startOffset, capacityInBytes, value + start, dataLen);
        if (ret != EOK) {
            LogError("set data failed in variable vector. %d", ret);
        }
    }

    void FillSlots(int index)
    {
        for (int i = lastOffsetPosition + 1; i < index; i++) {
            SetValueOffset(i + 1, GetValueOffset(i));
        }
        lastOffsetPosition = index - 1;
    }

    int32_t lastOffsetPosition = -1;
};
using VarcharVector = VariableWidthVector<type::OMNI_VARCHAR>;
}
}
#endif // __VARIABLE_WIDTH_VECTOR_OPERATOR_H__
