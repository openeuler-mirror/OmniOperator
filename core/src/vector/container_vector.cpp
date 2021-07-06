//
// Created by root on 6/28/21.
//
#include "container_vector.h"

void ContainerVector::setValues(int32_t startIndex, int64_t *values, int32_t length) {
    ASSERT(getReference()->isWritable());
    ASSERT(startIndex + length <= getSize());
    void *startAddress = &(((int64_t *) valuesAddress)[startIndex]);
    std::memcpy(startAddress, values, length * BYTES);
}

ContainerVector *ContainerVector::slice(int32_t positionOffset, int32_t length) {
    return new ContainerVector(this, length, positionOffset, this->vecTypes.data());
}

ContainerVector *ContainerVector::copyPositions(int32_t *positions, int32_t offset, int32_t length) {
    ASSERT(offset + length < getSize());
    Vector** vectorAddresses = new Vector*[length];
    VecType* copyTypes = new VecType[length];

    for (int32_t i = offset; i < offset + length; ++i) {
        vectorAddresses[i] = reinterpret_cast<Vector*>(getValue(positions[i]));
        copyTypes[i] = this->vecTypes[positions[i]];
    }
    return new ContainerVector(getAllocator(), positionCount, vectorAddresses, length, copyTypes);
}

ContainerVector *ContainerVector::copyRegion(int32_t positionOffset, int32_t length) {
    ASSERT(offset + length < getSize());
    Vector** vectorAddresses = new Vector*[length];
    VecType* copyTypes = new VecType[length];

    for (int32_t i = positionOffset; i < positionOffset + length; ++i) {
        vectorAddresses[i] = reinterpret_cast<Vector*>(getValue(i));
        copyTypes[i] = this->vecTypes[i];
    }
    return new ContainerVector(getAllocator(), positionCount, vectorAddresses, length, copyTypes);
}