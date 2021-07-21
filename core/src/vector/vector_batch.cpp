/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/9/21.
//

#include "vector_batch.h"
#include "int_vector.h"
#include "long_vector.h"
#include "double_vector.h"
#include "varchar_vector.h"
#include "container_vector.h"

namespace omniruntime {
namespace vec {
VectorBatch::VectorBatch(int vectorCount) : vectorCount(vectorCount), rowCount(0)
{
    vectors = nullptr;
    vectorTypes = nullptr;
    Init();
}

void VectorBatch::Init()
{
    if (vectorCount < 0) {
        return;
    }
    vectors = new Vector *[vectorCount];
    vectorTypes = new VecType[vectorCount];
    rowCount = 0;
}

VectorBatch::~VectorBatch()
{
    delete[] vectors;
    delete[] vectorTypes;
}

VectorBatch::VectorBatch(int vectorCount, int rowCount) : vectorCount(vectorCount), rowCount(rowCount)
{
    vectors = nullptr;
    vectorTypes = nullptr;
}

void VectorBatch::SetVectors(int *types)
{
    Init();
    for (int colIndex = 0; colIndex < vectorCount; ++colIndex) {
        vectorTypes[colIndex] = (VecType)types[colIndex];
        switch (types[colIndex]) {
            case OMNI_VEC_TYPE_INT: {
                SetVector(colIndex, new IntVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                SetVector(colIndex, new LongVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                SetVector(colIndex, new DoubleVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_CONTAINER: {
                DoubleVector *doubleVector = new DoubleVector(nullptr, rowCount);
                LongVector *longVector = new LongVector(nullptr, rowCount);
                Vector **vectorAddresses = new Vector *[2];
                vectorAddresses[0] = doubleVector;
                vectorAddresses[1] = longVector;
                VecType *vecTypes = new VecType[2];
                vecTypes[0] = OMNI_VEC_TYPE_DOUBLE;
                vecTypes[1] = OMNI_VEC_TYPE_LONG;
                ContainerVector *containerVector = new ContainerVector(nullptr, rowCount, vectorAddresses, 2, vecTypes);
                SetVector(colIndex, containerVector);
                break;
            }
                // TODO: add short support to codegen
            case OMNI_VEC_TYPE_SHORT: {
                SetVector(colIndex, new IntVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_VARCHAR: {
                VectorAllocator *va = nullptr;
                // TODO: set capacity appropriately
                // capacity = rowCount * 50 can't handle a vector of strings with average length above 50
                SetVector(colIndex, new VarcharVector(va, rowCount * 50, rowCount));
            }
                // TODO: support other types!!!
            default: {
                break;
            }
        }
    }
}

void VectorBatch::SetVector(int index, Vector *vector)
{
    vectors[index] = vector;
    vectorTypes[index] = vector->GetType();
    if (rowCount == 0) {
        rowCount = vector->GetSize();
    }
}

Vector *VectorBatch::GetVector(int index)
{
    return vectors[index];
}

Vector **VectorBatch::GetVectors() const
{
    return vectors;
}

int VectorBatch::GetVectorCount()
{
    return vectorCount;
}

int VectorBatch::GetRowCount()
{
    return rowCount;
}

VecType *VectorBatch::GetVectorTypes() const
{
    return vectorTypes;
}

void VectorBatch::FreeAllVectors()
{
    for (int vecIndex = 0; vecIndex < vectorCount; ++vecIndex) {
        delete vectors[vecIndex];
        vectors[vecIndex] = nullptr;
    }
}
} // namespace vec
} // namespace omniruntime