/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_batch.h"

#include "fixed_width_vector.h"
#include "variable_width_vector.h"
#include "container_vector.h"

namespace omniruntime {
namespace vec {
VectorBatch::VectorBatch(int vectorCount, int rowCount)
    : vectorCount(vectorCount), rowCount(rowCount), vectors(nullptr), vectorTypeIds(nullptr)
{
    Init();
}

VectorBatch::VectorBatch(int vectorCount) : VectorBatch(vectorCount, 0) {}

void VectorBatch::Init()
{
    if (vectorCount < 0) {
        return;
    }
    vectors = new Vector *[vectorCount];
    vectorTypeIds = new int32_t[vectorCount];
}

VectorBatch::~VectorBatch()
{
    delete[] vectors;
    delete[] vectorTypeIds;
}

Vector *VectorBatch::NewContainerVec(VectorAllocator *vecAllocator)
{
    DoubleVector *doubleVector = new DoubleVector(vecAllocator, rowCount);
    LongVector *longVector = new LongVector(vecAllocator, rowCount);
    std::vector<uintptr_t> vectorAddresses(2);
    vectorAddresses[0] = reinterpret_cast<uintptr_t>(doubleVector);
    vectorAddresses[1] = reinterpret_cast<uintptr_t>(longVector);
    std::vector<VecType> vecTypes = {DoubleVecType(), LongVecType()};
    return new ContainerVector(vecAllocator, rowCount, vectorAddresses, 2, vecTypes);
}

void VectorBatch::NewVectors(VectorAllocator *vecAllocator, const std::vector<VecType> &types)
{
    for (int colIndex = 0; colIndex < vectorCount; ++colIndex) {
        vectorTypeIds[colIndex] = types[colIndex].GetId();
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
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR: {
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
    vectorTypeIds[index] = vector->GetTypeId();
    if (rowCount == 0) {
        rowCount = vector->GetSize();
    }
}

const int32_t *VectorBatch::GetVectorTypeIds()
{
    return vectorTypeIds;
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