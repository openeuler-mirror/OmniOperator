/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_batch.h"
#include "vector_helper.h"

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

void VectorBatch::NewVectors(VectorAllocator *vecAllocator, const std::vector<DataTypePtr> &types)
{
    for (int colIndex = 0; colIndex < vectorCount; ++colIndex) {
        vectorTypeIds[colIndex] = types[colIndex]->GetId();
        auto currVecType = types[colIndex];
        int32_t vectorEncodingId =
            currVecType->GetId() == type::OMNI_CONTAINER ? OMNI_VEC_ENCODING_CONTAINER : OMNI_VEC_ENCODING_FLAT;
        SetVector(colIndex, VectorHelper::CreateVector(vecAllocator, vectorEncodingId, *currVecType, rowCount));
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