/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "container_vector.h"

namespace omniruntime {
namespace vec {
ContainerVector::ContainerVector(VectorAllocator *allocator, int32_t positionCount,
                                 std::vector<uintptr_t>& fieldVectors, int32_t vectorCount,
                                 std::vector<VecType>& types)
    : Vector(allocator, vectorCount * BYTES, positionCount, OMNI_VEC_TYPE_CONTAINER),
      vectorCount(vectorCount),
      positionCount(positionCount),
      vecTypes(types)
{
    for (int32_t i = 0; i < vectorCount; ++i) {
        SetValue(i, fieldVectors[i]);
    }
}

ContainerVector::ContainerVector(VectorAllocator *allocator, int32_t capacityInBytes, int32_t positionCount)
    : vectorCount(capacityInBytes / BYTES),
      positionCount(positionCount),
      Vector(allocator, capacityInBytes, positionCount, OMNI_VEC_TYPE_CONTAINER)
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
    std::vector<uintptr_t> vectorAddresses(length);
    std::vector<VecType> copyTypes(this->vecTypes.begin(), this->vecTypes.end());

    for (int32_t i = offset; i < offset + length; ++i) {
        vectorAddresses[i] = static_cast<uintptr_t>(GetValue(positions[i]));
    }
    auto containerVec = new ContainerVector(GetAllocator(), positionCount, vectorAddresses, length, copyTypes);
    for (int32_t i = 0; i < positionCount; ++i) {
        containerVec->SetValueNull(i, IsValueNull(i));
    }
    return containerVec;
}

ContainerVector *ContainerVector::CopyRegion(int positionOffset, int length)
{
    if (length <= 0) {
        return nullptr;
    }
    std::vector<uintptr_t> vectorAddresses(length);
    std::vector<VecType> copyTypes(this->vecTypes.begin(), this->vecTypes.end());

    for (int32_t i = positionOffset; i < positionOffset + length; ++i) {
        vectorAddresses[i] = static_cast<uintptr_t>(GetValue(i));
    }
    return new ContainerVector(GetAllocator(), positionCount, vectorAddresses, length, copyTypes);
}

void ContainerVector::Append(Vector *other, int positionOffset, int length) {}
} // namespace vec
} // namespace omniruntime