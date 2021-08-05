/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_helper.h"

namespace omniruntime {
namespace vec {
void VectorHelper::FreeVecBatch(VectorBatch *vecBatch)
{
    vecBatch->ReleaseAllVectors();
    delete vecBatch;
}

void VectorHelper::FreeVecBatches(VectorBatch **vecBatches, int32_t vecBatchCount)
{
    for (int i = 0; i < vecBatchCount; ++i) {
        delete vecBatches[i];
    }
    delete[] vecBatches;
}

void VectorHelper::FreeVecBatches(std::vector<VectorBatch *> &vecBatches)
{
    for (int i = 0; i < vecBatches.size(); ++i) {
        delete vecBatches[i];
    }
}
} // namespace vec
} // namespace omniruntime