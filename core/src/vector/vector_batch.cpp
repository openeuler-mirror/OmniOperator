//
// Created by root on 6/9/21.
//

#include "vector_batch.h"
#include "int_vector.h"
#include "long_vector.h"
#include "double_vector.h"

VectorBatch::VectorBatch(int vectorCount) : vectorCount(vectorCount) {
    vectors = new Vector *[vectorCount];
    vectorTypes = new VecType[vectorCount];
    rowCount = 0;
}

VectorBatch::~VectorBatch() {
    delete[] vectors;
    delete[] vectorTypes;
}

VectorBatch::VectorBatch(int *types, int vectorCount, int rowCount) : vectorCount(vectorCount) {
    vectors = new Vector *[vectorCount];
    vectorTypes = new VecType[vectorCount];
    this->rowCount = rowCount;
    for (int colIndex = 0; colIndex < vectorCount; ++colIndex) {
        vectorTypes[colIndex] = (VecType) types[colIndex];
        switch (types[colIndex]) {
            case OMNI_VEC_TYPE_INT: {
                setVector(colIndex, new IntVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                setVector(colIndex, new LongVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                setVector(colIndex, new DoubleVector(nullptr, rowCount));
                break;
            }
            // TODO: support other types!!!
            default: {
                break;
            }
        }
    }
}

void VectorBatch::setVector(int index, Vector *vector) {
    vectors[index] = vector;
    vectorTypes[index] = vector->getType();
    if (rowCount == 0) {
        rowCount = vector->getSize();
    }
    ASSERT(rowCount == vector->getSize())
}

Vector *VectorBatch::getVector(int index) {
    return vectors[index];
}

Vector **VectorBatch::getVectors() {
    return vectors;
}

int VectorBatch::getVectorCount() {
    return vectorCount;
}

int VectorBatch::getRowCount() {
    return rowCount;
}

VecType * VectorBatch::getVectorTypes() {
    return vectorTypes;
}

void VectorBatch::freeAllVectors() {
    for (int vecIndex = 0; vecIndex < vectorCount; ++vecIndex) {
        delete vectors[vecIndex];
        vectors[vecIndex] = nullptr;
    }
}