/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "union.h"
#include "vector/vector_helper.h"

using namespace std;
using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
UnionOperatorFactory::UnionOperatorFactory(const vec::VecTypes &sourceTypes, int32_t sourceTypesCount, bool isDistinct)
    : sourceTypes(sourceTypes), sourceTypesCount(sourceTypesCount), isDistinct(isDistinct)
{}

UnionOperatorFactory::~UnionOperatorFactory() {}

UnionOperatorFactory *UnionOperatorFactory::CreateUnionOperatorFactory(const vec::VecTypes &sourceTypes,
    int32_t sourceTypesCount, bool isDistinct)
{
    auto uOperatorFactory = std::make_unique<UnionOperatorFactory>(sourceTypes, sourceTypesCount, isDistinct);
    return uOperatorFactory.release();
}

Operator *UnionOperatorFactory::CreateOperator()
{
    UnionOperator *unionOperator = std::make_unique<UnionOperator>(sourceTypes, sourceTypesCount, isDistinct).release();
    return unionOperator;
}

UnionOperator::UnionOperator(const vec::VecTypes &sourceTypes, int32_t sourceTypesCount, bool isDistinct)
    : sourceTypes(sourceTypes), sourceTypesCount(sourceTypesCount), isDistinct(isDistinct)
{}

UnionOperator::~UnionOperator() {}

int32_t UnionOperator::AddInput(VectorBatch *vecBatch)
{
    int32_t vectorCount = vecBatch->GetVectorCount();
    auto outBatch = std::make_unique<VectorBatch>(vectorCount);
    for (int32_t i = 0; i < vectorCount; ++i) {
        Vector *inputVector = vecBatch->GetVector(i);
        outBatch->SetVector(i, inputVector->Slice(0, inputVector->GetSize()));
    }
    inputVecBatches.push_back(outBatch.release());
    VectorHelper::FreeVecBatch(vecBatch);
    return 0;
}

int32_t UnionOperator::GetOutput(std::vector<VectorBatch *> &outputPages)
{
    for (auto item : inputVecBatches) {
        outputPages.push_back(item);
    }
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}
}
}
