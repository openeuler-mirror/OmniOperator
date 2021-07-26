/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
// Created by root on 6/28/21.
//
#include "container_vector.h"

namespace omniruntime {
namespace vec {
void ContainerVector::SetValues(int32_t startIndex, const int64_t *values, int32_t length)
{
    void *startAddress = &(((int64_t *)valuesAddress)[startIndex]);
    errno_t ret = memcpy_s(startAddress, capacityInBytes, values, length * BYTES);
    if (ret != EOK) {
        std::cerr << "setvalues failed in container vector" << std::endl;
    }
}

ContainerVector *ContainerVector::Slice(int32_t positionOffset, int32_t length)
{
    return new ContainerVector(this, length, positionOffset, this->vecTypes.data());
}

void ContainerVector::Append(Vector *other, int positionOffset, int length)
{
    return;
}

ContainerVector *ContainerVector::CopyPositions(const int32_t *positions, int32_t offset, int32_t length)
{
    if (length <= 0) {
        return nullptr;
    }
    Vector **vectorAddresses = new Vector *[length];
    VecType *copyTypes = new VecType[length];

    for (int32_t i = offset; i < offset + length; ++i) {
        vectorAddresses[i] = reinterpret_cast<Vector *>(getValue(positions[i]));
        copyTypes[i] = this->vecTypes[positions[i]];
    }
    return new ContainerVector(GetAllocator(), positionCount, vectorAddresses, length, copyTypes);
}

ContainerVector *ContainerVector::CopyRegion(int32_t positionOffset, int32_t length)
{
    if (length <= 0) {
        return nullptr;
    }
    Vector **vectorAddresses = new Vector *[length];
    VecType *copyTypes = new VecType[length];

    for (int32_t i = positionOffset; i < positionOffset + length; ++i) {
        vectorAddresses[i] = reinterpret_cast<Vector *>(getValue(i));
        copyTypes[i] = this->vecTypes[i];
    }
    return new ContainerVector(GetAllocator(), positionCount, vectorAddresses, length, copyTypes);
}
} // namespace vec
} // namespace omniruntime