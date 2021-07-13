//
// Created by root on 6/16/21.
//

#ifndef OMNI_RUNTIME_VECTOR_HELPER_H
#define OMNI_RUNTIME_VECTOR_HELPER_H

#include "vector.h"
#include "vector_batch.h"
#include "int_vector.h"
#include "long_vector.h"
#include "double_vector.h"
#include <vector>

class VectorHelper {
public:

    static void setValue(Vector *vector, int32_t index, void *value)
    {
        switch (vector->getType()) {
            case OMNI_VEC_TYPE_INT:
                ((IntVector *) vector)->setValue(index, *(int32_t *) value);
                break;
            case OMNI_VEC_TYPE_LONG:
                ((LongVector *) vector)->setValue(index, *(int64_t *) value);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                ((DoubleVector *) vector)->setValue(index, *(double *) value);
                break;
            default:
                break;
        }
    }

    static void getValue(Vector *vector, int32_t index, void *value)
    {
        switch (vector->getType()) {
            case OMNI_VEC_TYPE_INT:
                *(int32_t *) value = ((IntVector *) vector)->getValue(index);
                break;
            case OMNI_VEC_TYPE_LONG:
                *(int64_t *) value = ((LongVector *) vector)->getValue(index);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                *(double *) value = ((DoubleVector *) vector)->getValue(index);
                break;
            default:
                break;
        }
    }

    static void freeVecBatch(VectorBatch *vecBatch);

    static void freeVecBatches(VectorBatch **vecBatches, int32_t vecBatchCount);

    static void freeVecBatches(vector<VectorBatch *> &vecBatches);
};

#endif //OMNI_RUNTIME_VECTOR_HELPER_H
