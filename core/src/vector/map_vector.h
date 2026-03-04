/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: MapVector  implementation
 */

#ifndef OMNI_RUNTIME_MAP_VECTOR_H
#define OMNI_RUNTIME_MAP_VECTOR_H

#include <vector>
#include <memory>
#include "vector.h"

namespace omniruntime::vec {

class MapVector : public BaseVector {
public:
    MapVector(int64_t size, std::shared_ptr<BaseVector> keyVector, std::shared_ptr<BaseVector> valueVector)
        : BaseVector(size, OMNI_ENCODING_MAP, OMNI_MAP),
          keys(std::move(keyVector)),
          values(std::move(valueVector)), capacity(static_cast<int32_t>(size))
    {
        offsetsBuffer = std::make_shared<AlignedBuffer<int64_t>>(size + 1);
        offsets = offsetsBuffer->GetBuffer();
        offsets[0] = 0;
    }

    MapVector(int64_t size)
        : BaseVector(size, OMNI_ENCODING_MAP, OMNI_MAP), capacity(static_cast<int32_t>(size))
    {
        offsetsBuffer = std::make_shared<AlignedBuffer<int64_t>>(size + 1);
        offsets = offsetsBuffer->GetBuffer();
        offsets[0] = 0;
    }

    const std::shared_ptr<AlignedBuffer<int64_t>>& GetOffsetsBuffer() const
    {
        return offsetsBuffer;
    }

    int64_t* GetOffsets()
    {
        return offsets;
    }

    int64_t GetOffset(int64_t index)
    {
        return offsets[index];
    }

    using BaseVector::GetSize;

    int64_t GetSize(int64_t index)
    {
        return offsets[index + 1] - offsets[index];
    }

    const std::shared_ptr<BaseVector> GetKeyVector() const
    {
        return keys;
    }

    const std::shared_ptr<BaseVector> GetValueVector() const
    {
        return values;
    }

    void Append(MapVector* other, int32_t offset);

    std::vector<DataTypeId> ALWAYS_INLINE GetTypeIds() const override
    {
        return {keys->GetTypeId(), values->GetTypeId()};
    }

    void SetKeyVector(std::shared_ptr<BaseVector> keyVector)
    {
        keys = std::move(keyVector);
    }
    
    void SetValueVector(std::shared_ptr<BaseVector> valueVector)
    {
        values = std::move(valueVector);
    }

    void SetOffset(int32_t index, int32_t offset)
    {
        offsets[index] = offset;
    }

    void SetSize(int32_t index, int32_t size)
    {
        offsets[index + 1] = offsets[index] + size;
    }

    void AddKeys(BaseVector* addedKeys)
    {
        keys = std::shared_ptr<BaseVector>(addedKeys);
    }

    void AddValues(BaseVector* addedValues)
    {
        values = std::shared_ptr<BaseVector>(addedValues);
    }

    MapVector *Slice(int positionOffset, int length, bool isCopy = false) override
    {
        if (UNLIKELY(positionOffset + length > size)) {
            std::string message("slice vector out of range(needed size:%d, real size:%d).", positionOffset + length,
                                size);
            throw OmniException("OPERATOR_RUNTIME_ERROR", message);
        }
        auto sliced = new MapVector(length);
        sliced->isSliced = true;
        int32_t startOffset = GetOffset(positionOffset);
        for (int i = 0; i < length; ++i) {
            sliced->SetOffset(i + 1, GetOffset(positionOffset + 1 + i) - startOffset);
        }
        for (int i = 0; i < length; ++i) {
            if (IsNull(positionOffset + i)) {
                sliced->SetNull(i);
            }
        }
        sliced->SetKeyVector(
            std::shared_ptr<BaseVector>(GetKeyVector()->Slice(startOffset, sliced->GetOffset(length), isCopy)));
        sliced->SetValueVector(
            std::shared_ptr<BaseVector>(GetValueVector()->Slice(startOffset, sliced->GetOffset(length), isCopy)));
        return sliced;
    }

    /* *
     * Copies the values of the vector at the indicated positions
     * @param positions
     * @param offset
     * @param length
     */
    MapVector* CopyPositions(const int *positions, int positionOffset, int length) ;

    static void UpdateKeyPositions(std::vector<int> &keyPositions, int index, int size)
    {
        for (int i = 0; i < size; i++) {
            keyPositions.push_back(index++);
        }
    }

    void ALWAYS_INLINE SetNull(int64_t index)
    {
        BaseVector::SetNull(index);
        SetSize(index, 0);
    }

    void Expand(int32_t needCapacity) override
    {
        if (needCapacity <= size) {
            return;
        }

        if (needCapacity <= capacity) {
            size = needCapacity;
            return;
        }

        int32_t newCapacity = std::max(capacity * 2, needCapacity);
        int32_t oldSize = size;

        auto oldOffsetsBuffer = offsetsBuffer;
        offsetsBuffer = std::make_shared<AlignedBuffer<int64_t>>(newCapacity + 1);
        offsets = offsetsBuffer->GetBuffer();

        if (oldOffsetsBuffer != nullptr) {
            memcpy(
                offsets,
                oldOffsetsBuffer->GetBuffer(),
                (oldSize + 1) * sizeof(int64_t)
            );
        } else {
            memset(offsets, 0, (newCapacity + 1) * sizeof(int64_t));
        }

        auto oldNullsBuffer = nullsBuffer;
        nullsBuffer = std::make_shared<NullsBuffer>(newCapacity);
        if (oldNullsBuffer != nullptr) {
            nullsBuffer->SetNulls(0, oldNullsBuffer.get(), oldSize);
        } else {
            nullsBuffer->SetNulls(0, false, newCapacity);
        }

        capacity = newCapacity;
        size = needCapacity;
    }

protected:
    int64_t* offsets;
    std::shared_ptr<AlignedBuffer<int64_t>> offsetsBuffer;
    std::shared_ptr<BaseVector> keys;
    std::shared_ptr<BaseVector> values;
    int32_t capacity;
};
}

#endif // OMNI_RUNTIME_MAP_VECTOR_H