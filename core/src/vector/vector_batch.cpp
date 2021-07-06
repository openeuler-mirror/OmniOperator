//
// Created by root on 6/9/21.
//

#include "vector_batch.h"
#include "int_vector.h"
#include "long_vector.h"
#include "double_vector.h"
#include "container_vector.h"

VectorBatch::VectorBatch(int vectorCount) : vectorCount(vectorCount) {
    vectors = new Vector *[vectorCount];
    vectorTypes = new VecType[vectorCount];
}

VectorBatch::~VectorBatch() {
    delete[] vectors;
    delete[] vectorTypes;
}

// This constructor should not exist!
VectorBatch::VectorBatch(int *types, int vectorCount, int rowCount) : vectorCount(vectorCount) {
    vectors = new Vector *[vectorCount];
    vectorTypes = new VecType[vectorCount];
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
            case OMNI_VEC_TYPE_CONTAINER: {
                DoubleVector* doubleVector = new DoubleVector(nullptr, rowCount);
                LongVector* longVector = new LongVector(nullptr, rowCount);
                Vector** vectorAddresses = new Vector*[2];
                vectorAddresses[0] = doubleVector;
                vectorAddresses[1] = longVector;
                VecType* vecTypes = new VecType[2];
                vecTypes[0] = OMNI_VEC_TYPE_DOUBLE;
                vecTypes[1] = OMNI_VEC_TYPE_LONG;
                ContainerVector* containerVector = new ContainerVector(nullptr, rowCount, vectorAddresses, 2, vecTypes);
                setVector(colIndex, containerVector);
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
    rowCount = vector->getSize();
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