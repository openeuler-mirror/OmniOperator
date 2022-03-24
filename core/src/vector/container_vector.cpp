/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "container_vector.h"

namespace omniruntime {
namespace vec {
ContainerVector::ContainerVector(VectorAllocator *allocator, int32_t positionCount,
    std::vector<uintptr_t> &fieldVectors, int32_t vectorCount, std::vector<DataType> &dataTypes)
    : Vector(allocator, vectorCount * BYTES, positionCount, type::OMNI_CONTAINER),
      vectorCount(vectorCount),
      positionCount(positionCount),
      dataTypes(dataTypes)
{
    for (int32_t i = 0; i < vectorCount; ++i) {
        SetValue(i, fieldVectors[i]);
    }
}

ContainerVector::ContainerVector(VectorAllocator *allocator, int32_t capacityInBytes, int32_t positionCount)
    : vectorCount(capacityInBytes / BYTES),
      positionCount(positionCount),
      Vector(allocator, capacityInBytes, positionCount, type::OMNI_CONTAINER)
{
    // init vec is null in container
    for (int i = 0; i < vectorCount; i++) {
        SetValue(i, 0);
    }
}

ContainerVector *ContainerVector::Slice(int32_t positionOffset, int32_t length)
{
    // get fieldVecOffsets
    std::vector<int32_t> fieldVecOffsets = GetFieldVecOffsets();

    int32_t fieldVecStartIndex = fieldVecOffsets[positionOffset];
    int32_t filedVecEndIndex = fieldVecOffsets[positionOffset + length];
    int32_t fieldVecLength = filedVecEndIndex - fieldVecStartIndex;

    // get new fieldVec
    std::vector<uintptr_t> newVecAddr(vectorCount);
    for (int32_t i = 0; i < vectorCount; ++i) {
        newVecAddr[i] = reinterpret_cast<uintptr_t>(
            (reinterpret_cast<Vector *>(GetValue(i)))->Slice(fieldVecStartIndex, fieldVecLength));
    }

    auto newContainerVec = new ContainerVector(GetAllocator(), length, newVecAddr, vectorCount, dataTypes);
    newContainerVec->SetValueNulls(0, (bool *)valueNullsAddress + positionOffset + this->positionOffset, length);
    return newContainerVec;
}

ContainerVector *ContainerVector::CopyPositions(const int *positions, int offset, int length)
{
    if (length <= 0) {
        return nullptr;
    }
    // get fieldVecOffsets
    std::vector<int32_t> fieldVecOffsets = GetFieldVecOffsets();

    // get fieldVecPositions
    std::vector<int32_t> fieldVecPositions;
    for (int i = 0; i < length; i++) {
        int position = positions[offset + i];
        if (!IsValueNull(position)) {
            fieldVecPositions.push_back(fieldVecOffsets[position]);
        }
    }

    // get new fieldVec
    std::vector<uintptr_t> vecAddr(vectorCount);
    for (int32_t i = 0; i < vectorCount; ++i) {
        vecAddr[i] =
            reinterpret_cast<uintptr_t>((reinterpret_cast<Vector *>(GetValue(i)))
                                            ->CopyPositions(fieldVecPositions.data(), 0, fieldVecPositions.size()));
    }

    auto newContainerVec = new ContainerVector(GetAllocator(), length, vecAddr, vectorCount, dataTypes);
    for (int32_t i = 0; i < length; ++i) {
        int position = positions[offset + i];
        newContainerVec->SetValueNull(position, IsValueNull(position));
    }
    return newContainerVec;
}

ContainerVector *ContainerVector::CopyRegion(int positionOffset, int length)
{
    if (length <= 0) {
        return nullptr;
    }

    // get fieldVecOffsets
    std::vector<int32_t> fieldVecOffsets = GetFieldVecOffsets();

    int32_t fieldVecStartIndex = fieldVecOffsets[positionOffset];
    int32_t filedVecEndIndex = fieldVecOffsets[positionOffset + length];
    int32_t fieldVecLength = filedVecEndIndex - fieldVecStartIndex;

    // get new fieldVec
    std::vector<uintptr_t> newVecAddr(vectorCount);
    for (int32_t i = 0; i < vectorCount; ++i) {
        newVecAddr[i] = reinterpret_cast<uintptr_t>(
            (reinterpret_cast<Vector *>(GetValue(i)))->CopyRegion(fieldVecStartIndex, fieldVecLength));
    }

    auto newContainerVec = new ContainerVector(GetAllocator(), length, newVecAddr, vectorCount, dataTypes);
    newContainerVec->SetValueNulls(0, (bool *)valueNullsAddress + positionOffset + this->positionOffset, length);
    return newContainerVec;
}

void ContainerVector::Append(Vector *other, int positionOffset, int length)
{
    auto *otherContainer = reinterpret_cast<ContainerVector *>(other);
    if (otherContainer->GetVectorCount() != vectorCount) {
        LogError("this vec count %d is not equal other vec count %d, container vec append failed.", vectorCount,
            otherContainer->GetVectorCount());
        return;
    }
    if (other->GetTypeId() != dataTypeId) {
        LogError("this vec type %d is not equal other type %d, container vec append failed.", dataTypeId,
            other->GetTypeId());
        return;
    }

    for (int32_t i = 0; i < vectorCount; i++) {
        auto *thisVector = reinterpret_cast<Vector *>(GetValue(i));
        auto *otherVector = reinterpret_cast<Vector *>(otherContainer->GetValue(i));
        thisVector->Append(otherVector, positionOffset, length);
    }
    // set nulls
    bool *otherValueNulls = static_cast<bool *>(other->GetValueNulls()) + other->GetPositionOffset();
    SetValueNulls(positionOffset, otherValueNulls, length);
}

ContainerVector::~ContainerVector()
{
    // release the vector in container vector
    for (int32_t i = 0; i < vectorCount; i++) {
        auto *field = reinterpret_cast<Vector *>(GetValue(i));
        if (field != nullptr) {
            delete field;
            // set null vec
            SetValue(i, 0);
        }
    }
}
} // namespace vec
} // namespace omniruntime