/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_BATCH_H
#define OMNI_RUNTIME_VECTOR_BATCH_H

#include "vector.h"
namespace omniruntime::vec {
class VectorBatch {
public:
    /* *
     * @param rowCnt
     */
    explicit VectorBatch(size_t rowCnt);

    ~VectorBatch();

    /* *
     * Set the vector at the indicated index, need ResizeVectorCount before SetVector
     * @param vector
     */
    void SetVector(int32_t index, BaseVector *vector);

    /* *
     * @param vector
     *     */
    void Append(BaseVector *vector);

    /* *
     * @param index
     */
    BaseVector *Get(int32_t index)
    {
        return vectors[index];
    }

    BaseVector **GetVectors();

    int32_t GetRowCount() const;

    int32_t GetVectorCount();

    void Resize(size_t rowCount);

    /* *
     * @param vectorCnt
     */
    void ResizeVectorCount(size_t vectorCnt);

    void FreeAllVectors();

    void ClearVectors();

    size_t GetCapacity();

    uint64_t CalculateTotalSize() const;

protected:
    size_t capacity; // max row count that can be held
    size_t rowCnt;
    std::vector<BaseVector *> vectors;
};
}

#endif // OMNI_RUNTIME_VECTOR_BATCH_H
