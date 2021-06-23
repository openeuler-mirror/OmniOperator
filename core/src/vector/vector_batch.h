//
// Created by root on 6/9/21.
//

#ifndef OMNI_RUNTIME_VECTOR_BATCH_H
#define OMNI_RUNTIME_VECTOR_BATCH_H


#include "vector.h"

class VectorBatch {
public:
    VectorBatch(int vectorCount);

    VectorBatch(int *types, int vectorCount, int rowCount);

    ~VectorBatch();

    Vector *getVector(int index);

    Vector **getVectors();

    void setVector(int index, Vector *vector);

    int getVectorCount();

    int getRowCount();

    VecType *getVectorTypes();

    void freeAllVectors();
private:
    int vectorCount;
    int rowCount;
    Vector **vectors;
    VecType *vectorTypes;
};

#endif //OMNI_RUNTIME_VECTOR_BATCH_H
