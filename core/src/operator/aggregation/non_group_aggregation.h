/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation Header
 */
#ifndef NON_GROUP_AGGREGATION_H
#define NON_GROUP_AGGREGATION_H

#include "aggregation.h"
#include "vector/vector_types.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"

namespace omniruntime {
namespace op {
class AggregationOperator : public AggregationCommonOperator {
public:
    AggregationOperator(std::vector<std::unique_ptr<Aggregator>> aggs, std::vector<int32_t> &aggInputCols,
        std::vector<int32_t> &maskColIds, omniruntime::vec::VecTypes &aggOutputTypes, bool inputRaw, bool outputPartial)
        : AggregationCommonOperator(std::move(aggs), inputRaw, outputPartial),
          aggInputCols(aggInputCols),
          maskCols(maskColIds),
          aggOutputTypes(aggOutputTypes)
    {
        for (int32_t i = 0; i < aggregators.size(); i++) {
            aggStates.push_back(AggregateState());
        }
    }

    ~AggregationOperator() override {}
    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data) override;

private:
    std::vector<int32_t> aggInputCols;
    std::vector<int32_t> maskCols;
    omniruntime::vec::VecTypes aggOutputTypes;
    std::vector<AggregateState> aggStates;
};

class AggregationOperatorFactory : public AggregationCommonOperatorFactory {
public:
    Operator *CreateOperator() override;

public:
    AggregationOperatorFactory(omniruntime::vec::VecTypes &sourceTypes, PrepareContext aggFuncTypesContext,
        PrepareContext aggInputColsContext, PrepareContext maskColsContext, omniruntime::vec::VecTypes &aggOutputTypes,
        bool inputRaw, bool outputPartial)
        : sourceTypes(sourceTypes),
          aggFuncTypesContext(aggFuncTypesContext),
          aggInputColsContext(aggInputColsContext),
          maskColsContext(maskColsContext),
          aggOutputTypes(aggOutputTypes),
          AggregationCommonOperatorFactory(inputRaw, outputPartial)
    {}

    ~AggregationOperatorFactory() override {}
    OmniStatus Init() override;
    OmniStatus Close() override;

private:
    template <class T> void CreateAggregatorFactory(int32_t maskCol);

private:
    omniruntime::vec::VecTypes sourceTypes;
    PrepareContext aggFuncTypesContext;
    PrepareContext aggInputColsContext;
    PrepareContext maskColsContext;
    omniruntime::vec::VecTypes aggOutputTypes;
    std::vector<omniruntime::vec::VecType> aggInputTypes;
    std::vector<int32_t> aggInputCols;
    std::vector<int32_t> maskCols;
    std::vector<std::unique_ptr<AggregatorFactory>> aggregatorFactories;
};
} // end op
} // edn omniruntime

#endif