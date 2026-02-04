/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: arrayVector  implementation
 */

#ifndef OMNI_RUNTIME_ARRAY_VECTOR_H
#define OMNI_RUNTIME_ARRAY_VECTOR_H

#include <vector>
#include <memory>
#include "vector.h"

namespace omniruntime::vec {
class ArrayVector : public BaseVector {
public:
    ArrayVector(int64_t size, std::shared_ptr<BaseVector> elementVector)
        : BaseVector(size, OMNI_ENCODING_ARRAY, OMNI_ARRAY),
          elements(std::move(elementVector)), capacity(size)
    {
        offsetsBuffer = std::make_shared<AlignedBuffer<int64_t>>(size + 1);
        offsets = offsetsBuffer->GetBuffer();
        offsets[0] = 0;
    }

    ArrayVector(int64_t size)
        : BaseVector(size, OMNI_ENCODING_ARRAY, OMNI_ARRAY), capacity(size)
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

    using BaseVector::GetSize;

    const std::shared_ptr<BaseVector> GetElementVector() const
    {
        return elements;
    }

    void SetElementVector(std::shared_ptr<BaseVector> elementVector)
    {
        elements = std::move(elementVector);
    }

    void SetOffset(int32_t index, int32_t offset)
    {
        offsets[index] = offset;
    }

    void SetSize(int32_t index, int32_t size)
    {
        offsets[index + 1] = offsets[index] + size;
    }

    void AddElements(BaseVector* addedElements)
    {
        elements = std::shared_ptr<BaseVector>(addedElements);
    }

    /* *
     * Get the array at the specified index with optional copying.
     *
     * @param index The index of the array to retrieve
     * @param copy If true, return a copy instead of a view
     * @return std::shared_ptr<BaseVector> GetArrayAt(int64_t index, bool copy)
     */
    std::shared_ptr<BaseVector> GetArrayAt(int64_t index, bool copy)
    {
        if (UNLIKELY(index < 0 || index >= size)) {
            std::string message("slice vector out of range(needed size:%d, real size:%d).", index,
                size);
            throw OmniException("OPERATOR_RUNTIME_ERROR", message);
        }

        int64_t startOffset = GetOffset(index);
        int64_t arraySize = GetSize(index);

        return std::shared_ptr<BaseVector>(GetElementVector()->Slice(startOffset, arraySize, false));
    }

    /* *
     * Create a new vector based on a slice of the vector. The returned Vector is
     * a read-only vector which shares data memory with the original vector,
     * if the vector data is modified, the original vector data is also modified.
     *
     * @param positionOffset
     * @param length
     * @param isCopy reserved parameters
     */
    ArrayVector *Slice(int positionOffset, int length, bool isCopy = false) override
    {
        if (UNLIKELY(positionOffset + length > size)) {
            std::string message("slice vector out of range(needed size:%d, real size:%d).", positionOffset + length,
                size);
            throw OmniException("OPERATOR_RUNTIME_ERROR", message);
        }
        auto sliced = new ArrayVector(length);
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
        sliced->SetElementVector(std::shared_ptr<BaseVector>(GetElementVector()->Slice(startOffset, sliced->GetOffset(length), isCopy)));
        return sliced;
    }

       /* *
        * Copies the values of the vector at the indicated positions
        * @param positions
        * @param offset
        * @param length
        */
    ArrayVector *CopyPositions(const int *positions, int positionOffset, int length);

    static void updateElementPositions(std::vector<int> &elementPositions, int index, int size)
    {
        for (int i = 0; i < size; ++i) {
            elementPositions.push_back(index++);
        }
    }

    void Expand(int64_t needCapacity)
    {
        if (needCapacity <= size) {
            return;
        }

        if (needCapacity <= capacity) {
            size = needCapacity;
            return;
        }

        int64_t newCapacity = std::max(capacity * 2, needCapacity);
        int64_t oldSize = size;

        auto oldOffsetsBuffer = offsetsBuffer;
        offsetsBuffer = std::make_shared<AlignedBuffer<int64_t>>(newCapacity + 1);
        offsets = offsetsBuffer->GetBuffer();

        if (oldOffsetsBuffer != nullptr) {
            errno_t res = memcpy_s(
                    offsets,
                    (newCapacity + 1) * sizeof(int64_t),
                    oldOffsetsBuffer->GetBuffer(),
                    (oldSize + 1) * sizeof(int64_t)
            );
            if (res != EOK) {
                throw OmniException("ERROR : ArrayVector Expand memcpy_s failed ! ", std::to_string(res));
            }
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

    /* *
     * append another arrayVector to the current arrayVector
     *
     * @param other the dst data from
     * @param positionOffset element position
     * @param length number of elements
     */
    void Append(BaseVector *other, int positionOffset, int length)
    {
        auto *otherArrayVector = static_cast<ArrayVector *>(other);

        if (length <= 0) {
            return;
        }
        if (positionOffset < 0) {
            std::string message = "Invalid append position";
            throw OmniException("ARRAYVECTOR_APPEND_ERROR", message);
        }

        int64_t newSize = positionOffset + length;
        Expand(newSize);

        int newIndex = positionOffset;
        for (int i = 0; i < length; i++) {
            if (otherArrayVector->IsNull(i)) {
                SetNull(newIndex);
            } else {
                auto tmp = otherArrayVector->GetValue(i);
                SetValue(newIndex, tmp);
                delete tmp;
            }
            newIndex++;
        }
    }

    void SetValue(int index, BaseVector* elements);

    BaseVector* GetValue(int index);

    void ALWAYS_INLINE SetNull(int64_t index)
    {
        BaseVector::SetNull(index);
        SetSize(index, 0);
    }

protected:
    int64_t* offsets;
    std::shared_ptr<AlignedBuffer<int64_t>> offsetsBuffer;
    std::shared_ptr<BaseVector> elements;
    int64_t capacity;
};
}

#endif // OMNI_RUNTIME_ARRAY_VECTOR_H