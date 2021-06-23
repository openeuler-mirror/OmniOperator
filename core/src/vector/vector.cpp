//
// Created by root on 6/1/21.
//

#include <cstring>
#include "vector.h"
#include "../util/debug.h"
#include "vector_allocator_manager.h"

VectorAllocatorManager g_vector_allocator_manager = VectorAllocatorManager::getInstance();
VectorAllocator *g_vector_allocator = g_vector_allocator_manager.getOrCreateAllocator(GLOBAL_SCOPE_NAME);

Vector::Vector(VectorAllocator *allocator, int capacityInBytes, int size, VecType type)
        : size(size), positionOffset(0) {
    if (allocator != nullptr) {
        reference = allocator->newVector(capacityInBytes, size, type);
        this->allocator = allocator;
    } else {
        reference = g_vector_allocator->newVector(capacityInBytes, size, type);
        this->allocator = g_vector_allocator;
    }
    valuesAddress = reference->getValuesAddress();
    valueNullsAddress = reference->getValueNullsAddress();
    valueOffsetsAddress = reference->getValueOffsetsAddress();
}

Vector::Vector(Vector *vector, int size, int positionOffset)
        : allocator(vector->allocator), reference(vector->reference), size(size), positionOffset(positionOffset){
    reference->incRef();
    valuesAddress = reference->getValuesAddress();
    valueNullsAddress = reference->getValueNullsAddress();
    valueOffsetsAddress = reference->getValueOffsetsAddress();
}

Vector::~Vector() {
    if (0 == reference->decRef()) {
        delete reference;
    }
}

bool Vector::isValueNull(int index) {
    ASSERT(index < size);
    return ((bool *) (valueNullsAddress))[index];
}

void Vector::setValueNull(int index) {
    ASSERT(index < size);
    ((bool *) (valueNullsAddress))[index] = true;
}

void Vector::setValueNulls(int startIndex, bool *nulls, int length) {
    ASSERT(startIndex + length < size);
    std::memcpy(((bool *) valueNullsAddress) + startIndex, nulls, length);
}

int Vector::getValueOffset(int index) {
    ASSERT(index < size + 1);
    return ((int *) (valueOffsetsAddress))[index];
}

void Vector::setValueOffset(int index, int valueOffset) {
    ASSERT(index < size + 1);
    ((int *) (valueOffsetsAddress))[index] = valueOffset;
}

int Vector::getSize() {
    return size;
}

int Vector::getPositionOffset() {
    return positionOffset;
}

VectorReference *Vector::getReference() {
    return reference;
}

VectorAllocator *Vector::getAllocator() {
    return allocator;
}

VecType Vector::getType() {
    return reference->getType();
}

void *Vector::getValues() {
    return reference->getValuesAddress();
}

void *Vector::getValueNulls() {
    return valueNullsAddress;
}

void Vector::setSize(int size) {
    this->size = size;
}

void Vector::setValueNullBitMap(int index) {
    if (valueNullsAddress != nullptr) {
        BitMapUtil::set(reinterpret_cast<uint8_t *>(valueNullsAddress), index);
    }
}
