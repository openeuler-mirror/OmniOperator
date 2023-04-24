/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_BATCH_H
#define OMNI_RUNTIME_VECTOR_BATCH_H

#include "vector.h"
namespace omniruntime::vec {
class VectorBatch {
public:
    /* *
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
    BaseVector *Get(int32_t index);

    BaseVector **GetVectors();

    int GetRowCount() const;

    int GetVectorCount();

    /* *
     * @param vectorCnt
     */
    void ResizeVectorCount(size_t vectorCnt);

    void FreeAllVectors();

private:
    size_t rowCnt;
    std::vector<BaseVector *> vectors;
};
}

#endif // OMNI_RUNTIME_VECTOR_BATCH_H
