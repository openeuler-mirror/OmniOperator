//
// Created by root on 6/16/21.
//

#include "vector_helper.h"

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