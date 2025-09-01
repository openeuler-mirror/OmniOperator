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

    BaseVector* Slice(int offset, int length) {
        // TODO
        return nullptr;
    }

    /* *
     * Copies the values of the vector at the indicated positions
     * @param positions
     * @param offset
     * @param length
     */
    MapVector* CopyPositions(const int *positions, int positionOffset, int length)
    {
        if ((positions == nullptr) || (length < 0)) {
            std::string message("MapVector positions is null or the input length is incorrect: %d.", length);
            throw OmniException("OPERATOR_RUNTIME_ERROR", message);
        }

        MapVector* newMapVector = new MapVector(length);
        int* startPositions = positions + positionOffset;

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

            updateKeyPositions(keyPositions, keyIndex, keySize);
        }
        newMapVector->SetOffset(length, keyLength);

        auto keyVector = this->GetKeyVector();
        auto newKeyVector = CopyChildPositionsVector(keyVector.get(), keyPositions.data(), 0, keyLength);
        newMapVector->AddKeys(newKeyVector);

        auto valueVector = this->GetValueVector();
        auto newValueVector = CopyChildPositionsVector(valueVector.get(), keyPositions.data(), 0, keyLength);
        newMapVector->AddValues(newValueVector);

        return newMapVector;
    }

protected:
    int64_t* offsets;
    std::shared_ptr<AlignedBuffer<int64_t>> offsetsBuffer;
    std::shared_ptr<BaseVector> keys;
    std::shared_ptr<BaseVector> values;

private:
    void updateKeyPositions(std::vector<int> keyPositions, int index, int size){
        for(int i = 0; i < size; i++) {
            keyPositions.push_back(index++);
        }
    }

    BaseVector *CopyChildPositionsVector(BaseVector *vector, int *positions, int offset, int length)
    {
        DataTypeId dataTypeId = vector->GetTypeId();
        switch (dataTypeId) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                return reinterpret_cast<Vector<int32_t> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_SHORT: {
                return reinterpret_cast<Vector<int16_t> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                return reinterpret_cast<Vector<int64_t> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_DOUBLE: {
                return reinterpret_cast<Vector<double> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_BOOLEAN: {
                return reinterpret_cast<Vector<bool> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                return reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->CopyPositions(
                    positions, offset, length);
            }
            case type::OMNI_DECIMAL128: {
                return reinterpret_cast<Vector<type::Decimal128> *>(vector)->CopyPositions(positions, offset, length);
            }
            default: {
                std::string omniExceptionInfo =
                    "In function CopyChildPositionsVector, no such data type " + std::to_string(dataTypeId);
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
            }
        }
    }
};
}

#endif // OMNI_RUNTIME_MAP_VECTOR_H
