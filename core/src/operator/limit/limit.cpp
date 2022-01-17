/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "limit.h"
#include "vector/vector_helper.h"

using namespace std;
using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
LimitOperatorFactory::LimitOperatorFactory(int64_t limitVal) : limit(limitVal) {}

LimitOperatorFactory::~LimitOperatorFactory() {}

LimitOperatorFactory *LimitOperatorFactory::CreateLimitOperatorFactory(int64_t limit)
{
    auto operatorFactory = std::make_unique<LimitOperatorFactory>(limit);
    return operatorFactory.release();
}

Operator *LimitOperatorFactory::CreateOperator()
{
    LimitOperator *limitOperator = std::make_unique<LimitOperator>(limit).release();
    return limitOperator;
}

LimitOperator::LimitOperator(int64_t limitVal) : remainingLimit(limitVal), outputVecBatch(nullptr) {}

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

    int32_t vectorCount = vecBatch->GetVectorCount();
    int32_t rowCount = vecBatch->GetRowCount();
    int32_t fetchSize = (remainingLimit >= rowCount) ? rowCount : remainingLimit;

    outputVecBatch = std::make_unique<VectorBatch>(vectorCount).release();
    for (int32_t i = 0; i < vectorCount; ++i) {
        Vector *inputVector = vecBatch->GetVector(i);
        outputVecBatch->SetVector(i, inputVector->Slice(0, fetchSize));
    }
    remainingLimit -= fetchSize;
    VectorHelper::FreeVecBatch(vecBatch);
    return 0;
}

int32_t LimitOperator::GetOutput(std::vector<VectorBatch *> &outputPages)
{
    outputPages.push_back(outputVecBatch);
    outputVecBatch = nullptr;

    if (remainingLimit <= 0) {
        SetStatus(OMNI_STATUS_FINISHED);
    }

    return 0;
}

OmniStatus LimitOperator::Close()
{
    if (outputVecBatch != nullptr) {
        outputVecBatch->ReleaseAllVectors();
        delete outputVecBatch;
        outputVecBatch = nullptr;
    }

    return OMNI_STATUS_NORMAL;
}
}
}
