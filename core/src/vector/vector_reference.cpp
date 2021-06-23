//
// Created by root on 6/1/21.
//

#include <stddef.h>
#include "vector_reference.h"

VectorReference::VectorReference(Chunk *values, Chunk *valueNulls, Chunk *valueOffsets, int capacityInBytes, VecType type)
        : values(values), valueNulls(valueNulls), valueOffsets(valueOffsets), reference(1),
          capacityInBytes(capacityInBytes), type(type), writable(true) {
}

VectorReference::~VectorReference() {
}

void VectorReference::incRef() {
    reference++;
    writable = false;
}

int64_t VectorReference::decRef() {
    if (--reference == 0) {
        if (values != nullptr) {
            delete values;
            values = nullptr;
        }
        if (valueNulls != nullptr) {
            delete valueNulls;
            valueNulls = nullptr;
        }
        if (valueOffsets != nullptr) {
            delete valueOffsets;
            valueOffsets = nullptr;
        }
    }
    return reference;
}

int64_t VectorReference::getRef() {
    return reference;
}

void *VectorReference::getValuesAddress() {
    if (values != nullptr) {
        return values->getAddress();
    }
    return nullptr;
}

void *VectorReference::getValueNullsAddress() {
    if (valueNulls != nullptr) {
        return valueNulls->getAddress();
    }
    return nullptr;
}

void *VectorReference::getValueOffsetsAddress() {
    if (valueOffsets != nullptr) {
        return valueOffsets->getAddress();
    }
    return nullptr;
}

VecType VectorReference::getType() {
    return type;
}

int VectorReference::getCapacityInBytes() {
    return capacityInBytes;
}

bool VectorReference::isWritable() {
    return writable;
}

Chunk* VectorReference::getValueChunk() {
    return values;
}

Chunk* VectorReference::getValueNullChunk() {
    return valueNulls;
}

Chunk* VectorReference::getValueOffsetChunk() {
    return valueOffsets;
}