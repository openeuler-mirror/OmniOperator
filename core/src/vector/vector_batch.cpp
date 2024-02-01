/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#include "unsafe_vector.h"
#include "vector_batch.h"

namespace omniruntime::vec {
/**
 * @param rowCnt
 */
VectorBatch::VectorBatch(size_t rowCnt) : rowCnt(rowCnt) {}

VectorBatch::~VectorBatch() = default;

/**
 * Set the vector at the indicated index, need ResizeVectorCount before SetVector
 * @param vector
 */
void VectorBatch::SetVector(int32_t index, BaseVector *vector)
{
    vectors[index] = vector;
}

/**
 * @param vector
 *     */
void VectorBatch::Append(BaseVector *vector)
{
    vectors.emplace_back(vector);
}

BaseVector **VectorBatch::GetVectors()
{
    return vectors.data();
}

int32_t VectorBatch::GetRowCount() const
{
    return static_cast<int32_t>(rowCnt);
}

int32_t VectorBatch::GetVectorCount()
{
    return static_cast<int32_t>(vectors.size());
}

/**
 * @param vectorCnt
 */
void VectorBatch::ResizeVectorCount(size_t vectorCnt)
{
    vectors.resize(vectorCnt);
}

void VectorBatch::FreeAllVectors()
{
    auto vectorSize = vectors.size();
    for (size_t vecIndex = 0; vecIndex < vectorSize; ++vecIndex) {
        delete vectors[vecIndex];
        vectors[vecIndex] = nullptr;
    }
    vectors.clear();
}

void VectorBatch::Resize(size_t rowCount)
{
    if (rowCnt == rowCount) {
        return;
    }

    if (rowCnt < rowCount) {
        throw OmniException("UNSUPPORTED_ERROR", "Can only resize vector batch to a smaller value.");
    }

    for (auto *vector: vectors) {
        unsafe::UnsafeBaseVector::SetSize(vector, static_cast<int>(rowCount));
    }

    rowCnt = rowCount;
}
}
