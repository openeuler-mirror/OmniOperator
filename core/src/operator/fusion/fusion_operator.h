/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: Fusion Operator Header File
 */

#ifndef OMNI_RUNTIME_FUSION_OPERATOR_H
#define OMNI_RUNTIME_FUSION_OPERATOR_H

#include "operator/operator_factory.h"
#include "operator/filter/filter_and_project.h"
#include "operator/join/lookup_join_expr.h"
#include "operator/aggregation/group_aggregation_expr.h"
#include "type/data_types.h"

namespace omniruntime {
namespace op {
class FusionOperatorFactory : public OperatorFactory {
public:
    FusionOperatorFactory(std::vector<OperatorFactory *> &operatorFactories, std::vector<OperatorType> &operatorTypes,
        OverflowConfig *overflowConfig);

    Operator *CreateOperator() override;

private:
    FilterAndProjectOperatorFactory *filterAndProjectFactory;
    std::vector<LookupJoinWithExprOperatorFactory *> lookupJoinFactories;
    HashAggregationWithExprOperatorFactory *hashAggregationFactory;
    OverflowConfig *overflowConfig;
};

class FusionOperator : public Operator {
public:
    FusionOperator(FilterAndProjectOperator *filterAndProjectOperator,
        std::vector<LookupJoinWithExprOperator *> &lookupJoinOperators,
        HashAggregationWithExprOperator *hashAggregationOperator);

    ~FusionOperator() override = default;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatches) override;

private:
    FilterAndProjectOperator *filterAndProjectOperator;
    std::vector<LookupJoinWithExprOperator *> lookupJoinOperators;
    HashAggregationWithExprOperator *hashAggregationOperator;
};
}
}
#endif // OMNI_RUNTIME_FUSION_OPERATOR_H
