//
// Created by root on 6/16/21.
//

#include "vector_helper.h"
#include "int_vector.h"
#include "long_vector.h"
#include "double_vector.h"

void VectorHelper::setValue(Vector *vector, int index, void *value) {
    switch (vector->getType()) {
        case OMNI_VEC_TYPE_INT:
            ((IntVector *) vector)->setValue(index, *(int *) value);
            break;
        case OMNI_VEC_TYPE_LONG:
            ((LongVector *) vector)->setValue(index, *(long *) value);
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            ((DoubleVector *) vector)->setValue(index, *(double *) value);
            break;
        default:
            break;
    }
}

void VectorHelper::getValue(Vector *vector, int index, void *value) {
    switch (vector->getType()) {
        case OMNI_VEC_TYPE_INT:
            *(int *) value = ((IntVector *) vector)->getValue(index);
            break;
        case OMNI_VEC_TYPE_LONG:
            *(long *) value = ((LongVector *) vector)->getValue(index);
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            *(double *) value = ((DoubleVector *) vector)->getValue(index);
            break;
        default:
            break;
    }
}

void VectorHelper::freeVecBatch(VectorBatch *vecBatch)
{
    vecBatch->freeAllVectors();
    delete vecBatch;
}

void VectorHelper::freeVecBatches(VectorBatch **vecBatches, int32_t vecBatchCount)
{
    for (int i = 0; i < vecBatchCount; ++i) {
        vecBatches[i]->freeAllVectors();
        delete vecBatches[i];
    }
    delete[] vecBatches;
}

void VectorHelper::freeVecBatches(vector<VectorBatch *> &vecBatches)
{
    for (int i = 0; i < vecBatches.size(); ++i) {
        vecBatches[i]->freeAllVectors();
        delete vecBatches[i];
    }
}