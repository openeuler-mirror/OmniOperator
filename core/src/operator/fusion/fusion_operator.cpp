/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: Fusion Operator Source File
 */

#include "fusion_operator.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::type;

FusionOperatorFactory::FusionOperatorFactory(std::vector<OperatorFactory *> &operatorFactories,
    std::vector<OperatorType> &operatorTypes, OverflowConfig *overflowConfig)
    : filterAndProjectFactory(nullptr), hashAggregationFactory(nullptr), overflowConfig(overflowConfig)
{
    auto operatorCount = operatorFactories.size();
    if (operatorTypes[0] == OMNI_FILTER_AND_PROJECT) {
        filterAndProjectFactory = static_cast<FilterAndProjectOperatorFactory *>(operatorFactories[0]);
    } else {
        lookupJoinFactories.emplace_back(static_cast<LookupJoinWithExprOperatorFactory *>(operatorFactories[0]));
    }
    auto lastIndex = operatorCount - 1;
    for (uint32_t i = 1; i < lastIndex; i++) {
        lookupJoinFactories.emplace_back(static_cast<LookupJoinWithExprOperatorFactory *>(operatorFactories[i]));
    }
    if (operatorTypes[lastIndex] == OMNI_HASH_AGGREGATION) {
        hashAggregationFactory = static_cast<HashAggregationWithExprOperatorFactory *>(operatorFactories[lastIndex]);
    } else {
        lookupJoinFactories.emplace_back(
            static_cast<LookupJoinWithExprOperatorFactory *>(operatorFactories[lastIndex]));
    }
}

Operator *FusionOperatorFactory::CreateOperator()
{
    FilterAndProjectOperator *filterAndProjectOperator = nullptr;
    if (filterAndProjectFactory != nullptr) {
        filterAndProjectOperator = static_cast<FilterAndProjectOperator *>(filterAndProjectFactory->CreateOperator());
    }
    std::vector<LookupJoinWithExprOperator *> lookupJoinOperators;
    for (auto lookupJoin : lookupJoinFactories) {
        auto lookupJoinOperator = static_cast<LookupJoinWithExprOperator *>(lookupJoin->CreateOperator());
        lookupJoinOperators.emplace_back(lookupJoinOperator);
    }
    HashAggregationWithExprOperator *hashAggregationOperator = nullptr;
    if (hashAggregationFactory != nullptr) {
        hashAggregationOperator =
            static_cast<HashAggregationWithExprOperator *>(hashAggregationFactory->CreateOperator());
    }
    return new FusionOperator(filterAndProjectOperator, lookupJoinOperators, hashAggregationOperator);
}

FusionOperator::FusionOperator(FilterAndProjectOperator *filterAndProjectOperator,
    std::vector<LookupJoinWithExprOperator *> &lookupJoinOperators,
    HashAggregationWithExprOperator *hashAggregationOperator)
    : filterAndProjectOperator(filterAndProjectOperator),
      lookupJoinOperators(lookupJoinOperators),
      hashAggregationOperator(hashAggregationOperator)
{}

int32_t FusionOperator::AddInput(omniruntime::vec::VectorBatch *vecBatch)
{
    VectorHelper::FreeVecBatch(vecBatch);
    return 0;
}

int32_t FusionOperator::GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatches)
{
    return 0;
}
}
}