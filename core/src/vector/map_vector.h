/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: MapVector  implementation
 */

#ifndef OMNI_RUNTIME_MAP_VECTOR_H
#define OMNI_RUNTIME_MAP_VECTOR_H

#include <vector>
#include <memory>

namespace omniruntime::vec {

class MapVector : public BaseVector {
public:
    MapVector(int64_t size, std::shared_ptr<BaseVector> keyVector, std::shared_ptr<BaseVector> valueVector)
        : BaseVector(size, OMNI_ENCODING_MAP, OMNI_MAP),
          keys(std::move(keyVector)),
          values(std::move(valueVector))
    {
        offsetsBuffer = std::make_shared<AlignedBuffer<int64_t>>(size + 1);
        offsets = offsetsBuffer->GetBuffer();
        offsets[0] = 0;
    }

    MapVector(int64_t size) : BaseVector(size, OMNI_ENCODING_MAP, OMNI_MAP)
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
    MapVector* CopyPositions(const int *positions, int positionOffset, int length) override
    {
        if ((positions == nullptr) || (length < 0)) {
            std::string message("MapVector positions is null or the input length is incorrect: %d.", length);
            throw OmniException("OPERATOR_RUNTIME_ERROR", message);
        }

        MapVector* newMapVector = new MapVector(length);
        auto startPositions = positions + positionOffset;

        std::vector<int> keyPositions;
        int keyLength = 0;
        for (int32_t i = 0; i < length; i++) {
            int position = startPositions[i];
            if (UNLIKELY(IsNull(position))) {
                newMapVector->SetNull(i);
            }
            int keyIndex = this->GetOffset(position);
            int keySize = this->GetSize(position);

            newMapVector->SetOffset(i, keyLength);
            keyLength += keySize;

            UpdateKeyPositions(keyPositions, keyIndex, keySize);
        }
        newMapVector->SetOffset(length, keyLength);

        auto keyVector = this->GetKeyVector();
        if (UNLIKELY(keyLength == 0)) {
            newMapVector->AddKeys(new BaseVector(0, keyVector->GetEncoding(), keyVector->GetTypeId()));
        } else {
            auto newKeyVector = keyVector->CopyPositions(keyPositions.data(), 0, keyLength);
            newMapVector->AddKeys(newKeyVector);
        }

        auto valueVector = this->GetValueVector();
        if (UNLIKELY(keyLength == 0)) {
            newMapVector->AddValues(new BaseVector(0, valueVector->GetEncoding(), valueVector->GetTypeId()));
        } else {
            auto newValueVector = valueVector->CopyPositions(keyPositions.data(), 0, keyLength);
            newMapVector->AddValues(newValueVector);
        }
        return newMapVector;
    }

    static void UpdateKeyPositions(std::vector<int> &keyPositions, int index, int size)
    {
        for (int i = 0; i < size; i++) {
            keyPositions.push_back(index++);
        }
    }

protected:
    int64_t* offsets;
    std::shared_ptr<AlignedBuffer<int64_t>> offsetsBuffer;
    std::shared_ptr<BaseVector> keys;
    std::shared_ptr<BaseVector> values;
};
}

#endif // OMNI_RUNTIME_MAP_VECTOR_H