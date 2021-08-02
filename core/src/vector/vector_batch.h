/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/9/21.
//

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

    omniruntime::vec::Vector *GetVector(int index);

    omniruntime::vec::Vector **GetVectors() const;

    void SetVector(int index, omniruntime::vec::Vector *vector);

    int GetVectorCount();

    int GetRowCount();

    omniruntime::vec::VecType *GetVectorTypes() const;

    void FreeAllVectors();

    void SetVectors(int *types);

private:
    void Init();

    int vectorCount;
    int rowCount;
    omniruntime::vec::Vector **vectors;
    omniruntime::vec::VecType *vectorTypes;
};
} // namespace vec
} // namespace omniruntime
#endif // OMNI_RUNTIME_VECTOR_BATCH_H
