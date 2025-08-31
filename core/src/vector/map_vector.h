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

    const std::shared_ptr<BaseVector> GetKeyVector() const {
        return keys;
    }

    const std::shared_ptr<BaseVector> GetValueVector() const {
        return values;
    }

    void SetKeyVector(std::shared_ptr<BaseVector> keyVector) {
        keys = std::move(keyVector);
    }
    
    void SetValueVector(std::shared_ptr<BaseVector> valueVector) {
        values = std::move(valueVector);
    }

    void SetOffset(int32_t index, int32_t offset) {
        offsets[index] = offset;
    }

    void AddKeys(BaseVector* addedKeys) {
        keys = std::shared_ptr<BaseVector>(addedKeys);
    }

    void AddValues(BaseVector* addedValues) {
        values = std::shared_ptr<BaseVector>(addedValues);
    }

protected:
    int64_t* offsets;
    std::shared_ptr<AlignedBuffer<int64_t>> offsetsBuffer;
    std::shared_ptr<BaseVector> keys;
    std::shared_ptr<BaseVector> values;
};
}

#endif // OMNI_RUNTIME_MAP_VECTOR_H
