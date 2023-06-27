/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "limit.h"
#include "util/type_util.h"
#include "vector/vector_helper.h"

using namespace std;
using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
LimitOperatorFactory::LimitOperatorFactory(int64_t limit) : limit(limit) {}

LimitOperatorFactory::~LimitOperatorFactory() {}

LimitOperatorFactory *LimitOperatorFactory::CreateLimitOperatorFactory(int64_t limitNum)
{
    return new LimitOperatorFactory(limitNum);
}

Operator *LimitOperatorFactory::CreateOperator()
{
    return new LimitOperator(limit);
}

LimitOperator::LimitOperator(int64_t limit) : remainingLimit(limit), outputVecBatch(nullptr) {}

LimitOperator::~LimitOperator() {}

int32_t LimitOperator::AddInput(VectorBatch *vecBatch)
{
    if (vecBatch == nullptr) {
        return 0;
    }
    if ((vecBatch->GetRowCount() == 0) || (outputVecBatch != nullptr) || (remainingLimit <= 0)) {
        VectorHelper::FreeVecBatch(vecBatch);
        return 0;
    }

    int32_t rowCount = vecBatch->GetRowCount();
    if (remainingLimit >= rowCount) {
        outputVecBatch = vecBatch;
        remainingLimit -= rowCount;
        return 0;
    }

    int32_t vectorCount = vecBatch->GetVectorCount();
    outputVecBatch = new VectorBatch(remainingLimit);
    for (int32_t i = 0; i < vectorCount; ++i) {
        BaseVector *inputVector = vecBatch->Get(i);
        outputVecBatch->Append(
            VectorHelper::SliceVector(inputVector, 0, remainingLimit));
    }
    remainingLimit = 0;
    VectorHelper::FreeVecBatch(vecBatch);
    return 0;
}

int32_t LimitOperator::GetOutput(VectorBatch **resultVecBatch)
{
    if (outputVecBatch != nullptr) {
        *resultVecBatch = outputVecBatch;
        outputVecBatch = nullptr;
    }

    if (remainingLimit <= 0) {
        SetStatus(OMNI_STATUS_FINISHED);
    }

    return 0;
}

OmniStatus LimitOperator::Close()
{
    if (outputVecBatch != nullptr) {
        VectorHelper::FreeVecBatch(outputVecBatch);
        outputVecBatch = nullptr;
    }

    return OMNI_STATUS_NORMAL;
}
}
}
