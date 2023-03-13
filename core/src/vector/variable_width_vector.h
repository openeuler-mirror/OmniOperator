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
    static const int32_t initCapacityInBytes = 32 * 1024; // 32K

    VariableWidthVector(Vector *vector, int size, int positionOffset) : Vector(vector, size, positionOffset){};

    VariableWidthVector(VectorAllocator *pAllocator, int capacityInBytes, int size)
        : Vector(pAllocator, capacityInBytes, size, TYPE_ID)
    {}

    VariableWidthVector(VectorAllocator *pAllocator, int size) : Vector(pAllocator, initCapacityInBytes, size, TYPE_ID)
    {}

    ~VariableWidthVector() override = default;

    int ALWAYS_INLINE GetValue(int index, T **dst) const
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
        Vector::SetValueNull(index);
        FillSlots(index);
        SetValueOffset(index + 1, GetValueOffset(index));
        lastOffsetPosition = index;
    }

    void ALWAYS_INLINE SetValueNull(int index, bool value) override
    {
        Vector::SetValueNull(index, value);
        FillSlots(index);
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
            auto *src = static_cast<VariableWidthVectorImpl *>(other);
            for (int32_t i = 0; i < length; i++) {
                if (other->IsValueNull(i)) {
                    SetValueNull(startIndex + i);
                } else {
                    valueLen = src->GetValue(i, &value);
                    SetValue(startIndex + i, value, valueLen);
                }
            }
        } else {
            auto *src = static_cast<DictionaryVector *>(other);
            int32_t originalIds[length];
            auto *dictionary =
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
        auto newAddress = static_cast<int64_t>(reinterpret_cast<uintptr_t>(valuesAddress));
        return newAddress;
    }

    /* *
     * if nth value is null,we will set -1 as value 's length to distinguish 0 byte varchar
     * @param rowId
     * @param executionContext
     * @param begin
     * @return
     */
    virtual StringRef SerializeValue(size_t rowId, mem::SimpleArenaAllocator &arenaAllocator,
        const uint8_t *&begin) override final
    {
        T *str = nullptr;
        StringRef res{};
        int stringLen;
        auto isNull = Vector::IsValueNull((int)rowId);
        if (not isNull) {
            stringLen = GetValue(rowId, &str);
            res.size = sizeof(stringLen) + stringLen;
        } else {
            stringLen = -1;
            res.size = sizeof(stringLen);
        }
        auto *pos = arenaAllocator.AllocateContinue(res.size, begin);
        std::copy(reinterpret_cast<uint8_t *>(&stringLen),
                  reinterpret_cast<uint8_t *>(&stringLen) + sizeof(stringLen), pos);
        if (stringLen > 0) {
            std::copy(str, str + stringLen, pos + sizeof(stringLen));
            res.data = reinterpret_cast<char *>(pos);
        }
        return res;
    }

    const uint8_t *DeserializeValueIntoThis(size_t rowId, const uint8_t *pos) override final
    {
        int stringSize = 0;
        std::copy(pos, pos + sizeof(int), reinterpret_cast<uint8_t *>(&stringSize));
        pos += sizeof(stringSize);

        if (stringSize >= 0) {
            auto *copyPointer = reinterpret_cast<const T *>(pos);
            SetValue(rowId, copyPointer, stringSize);
            return pos + stringSize;
        } else {
            // string_size < 0 means null pointer
            SetValueNull(rowId);
            return pos;
        }
    }

private:
    static const int32_t expandFactor = 2;

    void CheckCapacity(int32_t needCapacityInBytes)
    {
        if (needCapacityInBytes <= 0) {
            return;
        }
        int32_t toCapacityInBytes = (capacityInBytes > 0) ? capacityInBytes : initCapacityInBytes;
        while (toCapacityInBytes < needCapacityInBytes) {
            toCapacityInBytes = toCapacityInBytes * expandFactor;
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
            throw OmniException("memcpy_s error", " when SetData");
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
