/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "union.h"
#include "vector/vector_helper.h"

using namespace std;
using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
UnionOperatorFactory::UnionOperatorFactory(const type::DataTypes &sourceTypes, int32_t sourceTypesCount,
    bool isDistinct)
    : sourceTypes(sourceTypes), sourceTypesCount(sourceTypesCount), isDistinct(isDistinct)
{}

UnionOperatorFactory::~UnionOperatorFactory() {}

UnionOperatorFactory *UnionOperatorFactory::CreateUnionOperatorFactory(const type::DataTypes &sourceTypesField,
    int32_t sourceTypesCountField, bool distinct)
{
    auto uOperatorFactory = new UnionOperatorFactory(sourceTypesField, sourceTypesCountField, distinct);
    return uOperatorFactory;
}

Operator *UnionOperatorFactory::CreateOperator()
{
    UnionOperator *unionOperator = new UnionOperator(sourceTypes, sourceTypesCount, isDistinct);
    return unionOperator;
}

UnionOperator::UnionOperator(const type::DataTypes &sourceTypes, int32_t sourceTypesCount, bool isDistinct)
    : sourceTypes(sourceTypes), sourceTypesCount(sourceTypesCount), isDistinct(isDistinct)
{}

UnionOperator::~UnionOperator() {}

int32_t UnionOperator::AddInput(VectorBatch *vecBatch)
{
    int32_t vectorCount = vecBatch->GetVectorCount();
    int32_t rowCount = vecBatch->GetRowCount();
    auto outBatch = new VectorBatch(rowCount);
    for (int32_t i = 0; i < vectorCount; ++i) {
        BaseVector *inputVector = vecBatch->Get(i);
        outBatch->Append(inputVector);
    }
    inputVecBatches.push_back(outBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    vecBatchCount++;
    return 0;
}

int32_t UnionOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (vecBatchCount == 0 || vecBatchIndex == vecBatchCount) {
        vecBatchCount = 0;
        vecBatchIndex = 0;
        inputVecBatches.clear();
        SetStatus(OMNI_STATUS_FINISHED);
        return 0;
    }

    this->outputTypes = sourceTypes.Get();
    *outputVecBatch = inputVecBatches[vecBatchIndex];
    vecBatchIndex++;
    if (vecBatchIndex == vecBatchCount) {
        vecBatchCount = 0;
        vecBatchIndex = 0;
        inputVecBatches.clear();
        SetStatus(OMNI_STATUS_FINISHED);
    }

    return 0;
}

OmniStatus UnionOperator::Close()
{
    if (!inputVecBatches.empty()) {
        VectorHelper::FreeVecBatches(inputVecBatches);
        inputVecBatches.clear();
    }
    return OMNI_STATUS_NORMAL;
}
}
}
