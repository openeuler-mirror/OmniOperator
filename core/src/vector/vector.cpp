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
        : allocator(vector->allocator), reference(vector->reference), size(size), positionOffset(vector->positionOffset + positionOffset) {
    reference->incRef();
    valuesAddress = reference->getValuesAddress();
    valueNullsAddress = reference->getValueNullsAddress();
    valueOffsetsAddress = reference->getValueOffsetsAddress();
}

Vector::~Vector() {
    if (reference == nullptr) {
        std::cerr << "reference is null" << std::endl;
    }
    if (0 == reference->decRef()) {
        delete reference;
    }
}

void Vector::setValueNulls(int startIndex, bool *nulls, int length) {
    ASSERT(startIndex + length < size);
    std::memcpy(((bool *) valueNullsAddress) + startIndex, nulls, length);
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
    return valuesAddress;
}

void *Vector::getValueNulls() {
    return valueNullsAddress;
}

void *Vector::getValueOffsets() {
    return valueOffsetsAddress;
}

void Vector::setSize(int size) {
    this->size = size;
}

void Vector::setValueNullBitMap(int index) {
    if (valueNullsAddress != nullptr) {
        // std::cout << "set value null BitMap" << std::endl;
        BitMapUtil::set(reinterpret_cast<uint8_t *>(valueNullsAddress), index);
    }
}
