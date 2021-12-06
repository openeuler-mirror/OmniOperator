/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "container_vector.h"

namespace omniruntime {
namespace vec {
ContainerVector::ContainerVector(VectorAllocator *allocator, int32_t positionCount, Vector **fieldVectors,
    int32_t vectorCount, VecType *types)
    : Vector(allocator, vectorCount * BYTES, vectorCount, OMNI_VEC_TYPE_CONTAINER),
      vectorCount(vectorCount),
      positionCount(positionCount)
{
    for (int32_t i = 0; i < vectorCount; ++i) {
        SetValue(i, reinterpret_cast<int64_t>(fieldVectors[i]));
        this->vecTypes.push_back(types[i]);
    }
}

ContainerVector::ContainerVector(VectorAllocator *allocator, int32_t vectorCount)
    : vectorCount(vectorCount),
      positionCount(0),
      Vector(allocator, vectorCount * BYTES, vectorCount, OMNI_VEC_TYPE_CONTAINER)
{}

ContainerVector *ContainerVector::Slice(int32_t positionOffset, int32_t length)
{
    return new ContainerVector(this, length, positionOffset, this->vecTypes.data());
}

ContainerVector *ContainerVector::CopyPositions(const int *positions, int offset, int length)
{
    if (length <= 0) {
        return nullptr;
    }
    Vector **vectorAddresses = new Vector *[length];
    VecType *copyTypes = new VecType[length];

    for (int32_t i = offset; i < offset + length; ++i) {
        vectorAddresses[i] = reinterpret_cast<Vector *>(GetValue(positions[i]));
        copyTypes[i] = this->vecTypes[positions[i]];
    }
    return new ContainerVector(GetAllocator(), positionCount, vectorAddresses, length, copyTypes);
}

ContainerVector *ContainerVector::CopyRegion(int positionOffset, int length)
{
    if (length <= 0) {
        return nullptr;
    }
    Vector **vectorAddresses = new Vector *[length];
    VecType *copyTypes = new VecType[length];

    for (int32_t i = positionOffset; i < positionOffset + length; ++i) {
        vectorAddresses[i] = reinterpret_cast<Vector *>(GetValue(i));
        copyTypes[i] = this->vecTypes[i];
    }
    return new ContainerVector(GetAllocator(), positionCount, vectorAddresses, length, copyTypes);
}

void ContainerVector::Append(Vector *other, int positionOffset, int length) {}
} // namespace vec
} // namespace omniruntime