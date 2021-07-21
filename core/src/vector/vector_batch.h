/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/9/21.
//

#ifndef OMNI_RUNTIME_VECTOR_BATCH_H
#define OMNI_RUNTIME_VECTOR_BATCH_H


#include "vector.h"

class VectorBatch {
public:
    VectorBatch(int vectorCount);

    VectorBatch(int vectorCount, int rowCount);

    ~VectorBatch();

    Vector *GetVector(int index);

    Vector **GetVectors() const;

    void SetVector(int index, Vector *vector);

    int GetVectorCount();

    int GetRowCount();

    VecType *GetVectorTypes() const;

    void FreeAllVectors();

    void SetVectors(int *types);

private:
    void Init();
    int vectorCount;
    int rowCount;
    Vector **vectors;
    VecType *vectorTypes;
};

#endif // OMNI_RUNTIME_VECTOR_BATCH_H
