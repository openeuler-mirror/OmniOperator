/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#include "limit.h"
#include "util/type_util.h"
#include "vector/vector_helper.h"

using namespace std;
using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
LimitOperatorFactory::LimitOperatorFactory(int32_t limit, int32_t offset) : limit(limit), offset(offset) {}

LimitOperatorFactory::~LimitOperatorFactory() {}

LimitOperatorFactory *LimitOperatorFactory::CreateLimitOperatorFactory(int32_t limitNum, int32_t offsetNum)
{
    return new LimitOperatorFactory(limitNum, offsetNum);
}

LimitOperatorFactory *LimitOperatorFactory::CreateLimitOperatorFactory(std::shared_ptr<const LimitNode> planNode)
{
    return new LimitOperatorFactory(planNode->Count(), planNode->Offset());
}

Operator *LimitOperatorFactory::CreateOperator()
{
    return new LimitOperator(limit, offset);
}

LimitOperator::LimitOperator(int32_t limit, int32_t offset)
    : remainingLimit(limit), remainingOffset(offset), outputVecBatch(nullptr) {}

LimitOperator::~LimitOperator() {}

int32_t LimitOperator::AddInput(VectorBatch *vecBatch)
{
    if (vecBatch == nullptr) {
        return 0;
    }
    if ((vecBatch->GetRowCount() == 0) || (outputVecBatch != nullptr) ||
        (remainingLimit == 0) || (remainingLimit < 0 && remainingOffset <= 0)) {
        VectorHelper::FreeVecBatch(vecBatch);
        ResetInputVecBatch();
        return 0;
    }

    int32_t rowCount = vecBatch->GetRowCount();
    int32_t vectorCount = vecBatch->GetVectorCount();
    int32_t limitSize;
    int32_t realLimit;
    int32_t positionOffset;
    int32_t remainingRowCount = remainingOffset > rowCount ? 0 : rowCount - remainingOffset;
    if (remainingLimit < 0) {
        limitSize = remainingRowCount;
    } else {
        realLimit = remainingLimit - remainingOffset;
        limitSize = realLimit > remainingRowCount ? remainingRowCount : realLimit;
    }
    positionOffset = remainingOffset < rowCount ? remainingOffset : rowCount;
    remainingOffset = remainingOffset < rowCount ? 0 : remainingOffset - rowCount;
    remainingLimit = realLimit - limitSize + remainingOffset;
    auto result = make_unique<VectorBatch>(limitSize);
    for (int32_t i = 0; i < vectorCount; ++i) {
        BaseVector *inputVector = vecBatch->Get(i);
        result->Append(VectorHelper::SliceVector(inputVector, positionOffset, limitSize));
    }
    VectorHelper::FreeVecBatch(vecBatch);
    ResetInputVecBatch();
    outputVecBatch = result.release();
    return 0;
}

int32_t LimitOperator::GetOutput(VectorBatch **resultVecBatch)
{
    if (outputVecBatch != nullptr) {
        *resultVecBatch = outputVecBatch;
        outputVecBatch = nullptr;
        return 0;
    }

    if (remainingLimit <= 0) {
        SetStatus(OMNI_STATUS_FINISHED);
        return 0;
    }
    if (noMoreInput_) {
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
