/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include "union.h"

using namespace std;
using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
UnionOperatorFactory::UnionOperatorFactory(const vec::VecTypes &sourceTypes,
    int32_t sourceTypesCount, bool isDistinct)
    : sourceTypes(sourceTypes), sourceTypesCount(sourceTypesCount), isDistinct(isDistinct) {}

UnionOperatorFactory::~UnionOperatorFactory() {}

UnionOperatorFactory *UnionOperatorFactory::CreateUnionOperatorFactory(
    const vec::VecTypes &sourceTypes, int32_t sourceTypesCount, bool isDistinct)
{
    auto uOperatorFactory = std::make_unique<UnionOperatorFactory>(sourceTypes, sourceTypesCount, isDistinct);
    return uOperatorFactory.release();
}

Operator *UnionOperatorFactory::CreateOperator()
{
    UnionOperator *unionOperator = std::make_unique<UnionOperator>(
        sourceTypes,
    	sourceTypesCount,
    	isDistinct).release();
    return unionOperator;
}

UnionOperator::UnionOperator(
    const vec::VecTypes &sourceTypes,
    int32_t sourceTypesCount,
    bool isDistinct)
    : sourceTypes(sourceTypes), sourceTypesCount(sourceTypesCount), isDistinct(isDistinct) {}

UnionOperator::~UnionOperator() {}

int32_t UnionOperator::AddInput(VectorBatch *vecBatch)
{
    int32_t vectorCount = vecBatch->GetVectorCount();
    auto outBatch = std::make_unique<VectorBatch>(vectorCount);
    for (int32_t i = 0; i < vectorCount; ++i) {
        Vector *outCol = vecBatch->GetVector(i);
        outBatch->SetVector(i, outCol->Slice(0, outCol->GetSize()));
    }
    inputVecBatches.push_back(outBatch.release());
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
