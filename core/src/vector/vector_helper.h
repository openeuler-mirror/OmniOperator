//
// Created by root on 6/16/21.
//

#ifndef OMNI_RUNTIME_VECTOR_HELPER_H
#define OMNI_RUNTIME_VECTOR_HELPER_H

#include "vector.h"
#include "vector_batch.h"
#include <vector>

class VectorHelper {
public:
    static void setValue(Vector *vector, int index, void *value);

    static void getValue(Vector *vector, int index, void *value);

    static void freeVecBatch(VectorBatch *vecBatch);

    static void freeVecBatches(VectorBatch **vecBatches, int32_t vecBatchCount);

    static void freeVecBatches(vector<VectorBatch *> &vecBatches);
};

#endif //OMNI_RUNTIME_VECTOR_HELPER_H
