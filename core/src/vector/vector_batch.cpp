/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#include <cstdint>
#include "vector_batch.h"

namespace omniruntime::vec {
/**
 * auto v1 = std::make_unique<Vector<int32_t>>(8).release();
 * auto v2 = std::make_unique<Vector<std::string>>(8).release();
 * int32_t ids[8] = {0, 1, 2, 3, 0, 1, 2, 3};
 * auto dict = createDictionary<TYPE_UTIL<int32_t>::DICTIONARY_TYPE>(4);
 * auto container = std::make_shared<DictionaryArrayContainer<int32_t>>(ids, 8, dict, 4);
 * auto v3 = std::make_unique<Vector<DictionaryArrayContainer<int32_t>>>(8, container).release();
 * VectorBatch vb(8);
 * vb.Append(v1);
 * vb.Append(v2);
 * vb.Append(v3);
 * auto col0 = reinterpret_cast<Vector<int32_t> *>(vb.Get(0));
 * auto col1 = reinterpret_cast<Vector<std::string> *>(vb.Get(1));
 * auto col2 = reinterpret_cast<Vector<DictionaryArrayContainer<int32_t>> *>(vb.Get(2));
 * vb.FreeAllVectors();
 * delete vb;
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

/**
 * @param index
 */
BaseVector *VectorBatch::Get(int32_t index)
{
    return vectors[index];
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
}
