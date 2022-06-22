/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation Header
 */
#ifndef NON_GROUP_AGGREGATION_H
#define NON_GROUP_AGGREGATION_H

#include "aggregation.h"
#include "type/data_types.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"

namespace omniruntime {
namespace op {
class AggregationOperator : public AggregationCommonOperator {
public:
    AggregationOperator(std::vector<std::unique_ptr<Aggregator>> aggs, omniruntime::type::DataTypes &aggOutputTypes,
        bool inputRaw, bool outputPartial)
        : AggregationCommonOperator(std::move(aggs), inputRaw, outputPartial), aggOutputTypes(aggOutputTypes)
    {
        for (uint32_t i = 0; i < aggregators.size(); i++) {
            aggStates.push_back(AggregateState());
        }
    }

    ~AggregationOperator() override {}
    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data) override;

private:
    omniruntime::type::DataTypes aggOutputTypes;
    std::vector<AggregateState> aggStates;
};

class AggregationOperatorFactory : public AggregationCommonOperatorFactory {
public:
    Operator *CreateOperator() override;

public:
    AggregationOperatorFactory(omniruntime::type::DataTypes &sourceTypes, std::vector<uint32_t> &aggFuncTypesVector,
        std::vector<uint32_t> &aggInputColsVector, std::vector<uint32_t> &maskColsVector,
        omniruntime::type::DataTypes &aggOutputTypes, bool inputRaw, bool outputPartial)
        : AggregationCommonOperatorFactory(inputRaw, outputPartial, maskColsVector),
          sourceTypes(sourceTypes),
          aggFuncTypesVector(aggFuncTypesVector),
          aggInputColsVector(aggInputColsVector),
          aggOutputTypes(aggOutputTypes)
    {}

    ~AggregationOperatorFactory() override {}
    OmniStatus Init() override;
    OmniStatus Close() override;

private:
    omniruntime::type::DataTypes sourceTypes;
    std::vector<uint32_t> aggFuncTypesVector;
    std::vector<uint32_t> aggInputColsVector;
    omniruntime::type::DataTypes aggOutputTypes;
    std::vector<omniruntime::type::DataTypeRawPtr> aggInputTypes;
    std::vector<int32_t> aggInputCols;
    std::vector<std::unique_ptr<AggregatorFactory>> aggregatorFactories;
};
} // end op
} // edn omniruntime

#endif