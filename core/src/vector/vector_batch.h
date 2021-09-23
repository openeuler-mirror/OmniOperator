/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_BATCH_H
#define OMNI_RUNTIME_VECTOR_BATCH_H


#include "vector.h"

namespace omniruntime {
namespace vec {
class VectorBatch {
public:
    VectorBatch(int vectorCount);

    VectorBatch(int vectorCount, int rowCount);

    ~VectorBatch();

    Vector *GetVector(int index)
    {
        return vectors[index];
    }

    Vector **GetVectors() const
    {
        return vectors;
    }

    void SetVector(int index, Vector *vector);

    int GetVectorCount()
    {
        return vectorCount;
    }

    int GetRowCount()
    {
        return rowCount;
    }

    const VecType *GetVectorTypes() const
    {
        return vectorTypes;
    }

    void GetVectorTypeIds(int32_t *typeIds);

    void NewVectors(VectorAllocator *vecAllocator, const int *types);

    void NewVectors(VectorAllocator *vecAllocator, const std::vector<VecType> &types);

    void ReleaseAllVectors();

    void TraceRecord(VectorLeakDetector &leakDetector, std::string opName, VecOpType opType);

private:
    void Init();
    Vector *NewContainerVec(VectorAllocator *vecAllocator);

    int vectorCount;
    int rowCount;
    Vector **vectors;
    VecType *vectorTypes;
};
} // namespace vec
} // namespace omniruntime
#endif // OMNI_RUNTIME_VECTOR_BATCH_H
