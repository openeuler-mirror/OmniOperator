/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_batch.h"
#include "int_vector.h"
#include "long_vector.h"
#include "double_vector.h"
#include "varchar_vector.h"
#include "container_vector.h"
#include "decimal128_vector.h"

namespace omniruntime {
namespace vec {
VectorBatch::VectorBatch(int vectorCount, int rowCount) : vectorCount(vectorCount), rowCount(rowCount)
{
    vectors = nullptr;
    vectorTypes = nullptr;
    Init();
}

VectorBatch::VectorBatch(int vectorCount) : VectorBatch(vectorCount, 0) {}

void VectorBatch::Init()
{
    if (vectorCount < 0) {
        return;
    }
    vectors = new Vector *[vectorCount];
    vectorTypes = new VecType[vectorCount];
}

VectorBatch::~VectorBatch()
{
    for (int vecIndex = 0; vecIndex < vectorCount; ++vecIndex) {
        delete vectors[vecIndex];
    }
    delete[] vectors;
    delete[] vectorTypes;
}

void VectorBatch::NewVectors(int *types)
{
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
                vecTypes[0] = DoubleVecType::Instance();
                vecTypes[1] = LongVecType::Instance();
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
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                SetVector(colIndex,
                    new Decimal128Vector(nullptr, rowCount, Decimal128Vector::DECIMAL128_TYPE_WIDTH, 0));
                break;
            }
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

void VectorBatch::GetVectorTypeIds(int32_t *typeIds)
{
    for (int32_t i = 0; i < vectorCount; ++i) {
        typeIds[i] = vectorTypes[i].GetId();
    }
}

void VectorBatch::ReleaseAllVectors()
{
    for (int vecIndex = 0; vecIndex < vectorCount; ++vecIndex) {
        delete vectors[vecIndex];
        vectors[vecIndex] = nullptr;
    }
}
} // namespace vec
} // namespace omniruntime