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
    MapVector(int32_t size, std::shared_ptr<BaseVector> keyVector, std::shared_ptr<BaseVector> valueVector)
        : BaseVector(size, OMNI_ENCODING_MAP),
          keys(std::move(keyVector)),
          values(std::move(valueVector))
    {
        offsetsBuffer = std::make_shared<AlignedBuffer<int32_t>>(size + 1);
        offsets = offsetsBuffer->GetBuffer();
        offsets[0] = 0;
    }

    const std::shared_ptr<AlignedBuffer<int32_t>>& GetOffsetsBuffer() const
    {
        return offsetsBuffer;
    }

    const int32_t* GetOffsets() const
    {
        return offsets;
    }

    int32_t GetOffset(int32_t index)
    {
        return offsets[index];
    }

    int32_t GetSize(int32_t index)
    {
        return offsets[index + 1] - offsets[index];
    }

    const std::shared_ptr<BaseVector> GetKeys const {
        return keys;
    }

    const std::shared_ptr<BaseVector> GetValues const {
        return values;
    }

protected:
    int32_t* offsets;
    std::shared_ptr<AlignedBuffer<int32_t>> offsetsBuffer;
    std::shared_ptr<BaseVector> keys;
    std::shared_ptr<BaseVector> values;
};
}

#endif // OMNI_RUNTIME_MAP_VECTOR_H