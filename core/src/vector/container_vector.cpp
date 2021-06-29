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
    return new ContainerVector(this, length, positionOffset, this->vectorOffsets.data(), this->vecTypes.data());
}

ContainerVector *ContainerVector::copyPositions(int32_t *positions, int32_t offset, int32_t length) {
    ASSERT(offset + length < getSize());
    Vector** vectors = new Vector*[this->vectorCount];
    ContainerVector *vector = new ContainerVector(getAllocator(), length, this->valuesAddress, vecTypes.data());
    for (int32_t i = 0; i < length; ++i) {
        int32_t position = positions[offset + i];
        vector->setValue(i, getValue(position));
    }
    return vector;
}

ContainerVector *ContainerVector::copyRegion(int32_t positionOffset, int32_t length) {
    ASSERT(positionOffset + length < getSize());
    ContainerVector *vector = new ContainerVector(getAllocator(), length, vecTypes.data());
    vector->setValues(0, (int64_t *) valuesAddress + positionOffset, length);
    vector->setValueNulls(0, (bool *) valueNullsAddress + positionOffset, length);
    return vector;
}

void ContainerVector::fromFieldVectors(int64_t positionCount, Vector *fieldVectors, int32_t* vectorOffsets, int32_t vectorCount) {

}
