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
#include "boolean_vector.h"

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
    delete[] vectors;
    delete[] vectorTypes;
}

Vector *VectorBatch::NewContainerVec(VectorAllocator *vecAllocator)
{
    DoubleVector *doubleVector = new DoubleVector(vecAllocator, rowCount);
    LongVector *longVector = new LongVector(vecAllocator, rowCount);
    Vector **vectorAddresses = new Vector *[2];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    VecType *vecTypes = new VecType[2];
    vecTypes[0] = DoubleVecType::Instance();
    vecTypes[1] = LongVecType::Instance();
    return new ContainerVector(vecAllocator, rowCount, vectorAddresses, 2, vecTypes);
}

// deprecation, will remove when all operator complete refactor.
void VectorBatch::NewVectors(VectorAllocator *vecAllocator, const int *types)
{
    for (int colIndex = 0; colIndex < vectorCount; ++colIndex) {
        vectorTypes[colIndex] = (VecType)types[colIndex];
        switch (types[colIndex]) {
            case OMNI_VEC_TYPE_BOOLEAN:
                SetVector(colIndex, new BooleanVector(vecAllocator, rowCount));
                break;
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32: {
                SetVector(colIndex, new IntVector(vecAllocator, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64: {
                SetVector(colIndex, new LongVector(vecAllocator, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                SetVector(colIndex, new DoubleVector(vecAllocator, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_CONTAINER: {
                Vector *containerVector = NewContainerVec(vecAllocator);
                SetVector(colIndex, containerVector);
                break;
            }
                // TODO: add short support to codegen
            case OMNI_VEC_TYPE_SHORT: {
                SetVector(colIndex, new IntVector(vecAllocator, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_VARCHAR: {
                // TODO: set capacity appropriately
                // capacity = rowCount * 50 can't handle a vector of strings with average length above 50
                SetVector(colIndex, new VarcharVector(vecAllocator, rowCount * 50, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                SetVector(colIndex, new Decimal128Vector(vecAllocator, rowCount));
                break;
            }
            default: {
                break;
            }
        }
    }
}

void VectorBatch::NewVectors(VectorAllocator *vecAllocator, const std::vector<VecType> &types)
{
    for (int colIndex = 0; colIndex < vectorCount; ++colIndex) {
        vectorTypes[colIndex] = (VecType)types[colIndex];
        switch (types[colIndex].GetId()) {
            case OMNI_VEC_TYPE_BOOLEAN:
                SetVector(colIndex, new BooleanVector(vecAllocator, rowCount));
                break;
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32: {
                SetVector(colIndex, new IntVector(vecAllocator, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64: {
                SetVector(colIndex, new LongVector(vecAllocator, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                SetVector(colIndex, new DoubleVector(vecAllocator, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_CONTAINER: {
                Vector *containerVector = NewContainerVec(vecAllocator);
                SetVector(colIndex, containerVector);
                break;
            }
            case OMNI_VEC_TYPE_SHORT: {
                SetVector(colIndex, new IntVector(vecAllocator, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_VARCHAR: {
                int32_t width = (static_cast<const VarcharVecType *>(&types[colIndex]))->GetWidth();
                SetVector(colIndex, new VarcharVector(vecAllocator, rowCount * width, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                SetVector(colIndex, new Decimal128Vector(vecAllocator, rowCount));
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

void VectorBatch::TraceRecord(VectorLeakDetector &leakDetector, std::string opName, VecOpType opType)
{
    for (int i = 0; i < vectorCount; ++i) {
        leakDetector.Record(vectors[i], opName, opType);
    }
}
} // namespace vec
} // namespace omniruntime