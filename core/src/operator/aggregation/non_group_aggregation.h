/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation Header
 */
#ifndef NON_GROUP_AGGREGATION_H
#define NON_GROUP_AGGREGATION_H

#include "aggregation.h"

#include <utility>
#include "type/data_types.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"

namespace omniruntime {
namespace op {
class AggregationOperator : public AggregationCommonOperator {
public:
    AggregationOperator(std::vector<std::unique_ptr<Aggregator>> aggs, omniruntime::type::ContainerDataTypePtr aggOutputTypes,
        bool inputRaw, bool outputPartial)
        : AggregationCommonOperator(std::move(aggs), inputRaw, outputPartial), aggOutputTypes(std::move(aggOutputTypes))
    {
        for (uint32_t i = 0; i < aggregators.size(); i++) {
            aggStates.push_back(AggregateState());
        }
    }

    ~AggregationOperator() override {}
    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data) override;

private:
    omniruntime::type::ContainerDataTypePtr aggOutputTypes;
    std::vector<AggregateState> aggStates;
};

class AggregationOperatorFactory : public AggregationCommonOperatorFactory {
public:
    Operator *CreateOperator() override;

public:
    AggregationOperatorFactory(omniruntime::type::ContainerDataTypePtr sourceTypes, std::vector<uint32_t> &aggFuncTypesVector,
        std::vector<uint32_t> &aggInputColsVector, std::vector<uint32_t> &maskColsVector,
        omniruntime::type::ContainerDataTypePtr aggOutputTypes, bool inputRaw, bool outputPartial)
        : AggregationCommonOperatorFactory(inputRaw, outputPartial, maskColsVector),
          sourceTypes(std::move(sourceTypes)),
          aggFuncTypesVector(aggFuncTypesVector),
          aggInputColsVector(aggInputColsVector),
          aggOutputTypes(std::move(aggOutputTypes))
    {}

    ~AggregationOperatorFactory() override {}
    OmniStatus Init() override;
    OmniStatus Close() override;

private:
    omniruntime::type::ContainerDataTypePtr sourceTypes;
    std::vector<uint32_t> aggFuncTypesVector;
    std::vector<uint32_t> aggInputColsVector;
    omniruntime::type::ContainerDataTypePtr aggOutputTypes;
    std::vector<omniruntime::type::DataTypePtr> aggInputTypes;
    std::vector<int32_t> aggInputCols;
    std::vector<std::unique_ptr<AggregatorFactory>> aggregatorFactories;
};
} // end op
} // edn omniruntime

#endif