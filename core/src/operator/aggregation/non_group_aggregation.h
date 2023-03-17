/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation Header
 */
#ifndef NON_GROUP_AGGREGATION_H
#define NON_GROUP_AGGREGATION_H

#include "aggregation.h"

#include <utility>
#include "type/data_types.h"

namespace omniruntime {
namespace op {
class AggregationOperator : public AggregationCommonOperator {
public:
    AggregationOperator(std::vector<std::unique_ptr<Aggregator>> aggs,
        std::vector<omniruntime::type::DataTypes> &aggsOutputTypes, std::vector<bool> &inputRaws,
        std::vector<bool> &outputPartials)
        : AggregationCommonOperator(std::move(aggs), inputRaws, outputPartials), aggsOutputTypes(aggsOutputTypes)
    {
        aggsStates.reserve(aggregators.size());
        for (uint32_t i = 0; i < aggregators.size(); i++) {
            aggsStates.push_back(AggregateState());
        }
    }

    ~AggregationOperator() override {}
    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data) override;

private:
    std::vector<omniruntime::type::DataTypes> aggsOutputTypes;
    std::vector<AggregateState> aggsStates;
};

class AggregationOperatorFactory : public AggregationCommonOperatorFactory {
public:
    Operator *CreateOperator() override;

public:
    AggregationOperatorFactory(omniruntime::type::DataTypes &sourceTypes, std::vector<uint32_t> &aggFuncTypesVector,
        std::vector<std::vector<uint32_t>> &aggsInputColsVector, std::vector<uint32_t> &maskColsVector,
        std::vector<omniruntime::type::DataTypes> &aggsOutputTypes, std::vector<bool> inputRaws,
        std::vector<bool> outputPartials, bool overflowAsNull = false)
        : AggregationCommonOperatorFactory(inputRaws, outputPartials, maskColsVector, overflowAsNull),
          sourceTypes(sourceTypes),
          aggFuncTypesVector(aggFuncTypesVector),
          aggsInputColsVector(aggsInputColsVector),
          aggsOutputTypes(aggsOutputTypes)
    {}

    ~AggregationOperatorFactory() override = default;
    OmniStatus Init() override;
    OmniStatus Close() override;

private:
    omniruntime::type::DataTypes sourceTypes;
    std::vector<uint32_t> aggFuncTypesVector;
    std::vector<std::vector<uint32_t>> aggsInputColsVector;
    std::vector<omniruntime::type::DataTypes> aggsOutputTypes;
    std::vector<omniruntime::type::DataTypesPtr> aggsInputTypes;
    std::vector<std::vector<int32_t>> aggsInputCols;
    std::vector<std::unique_ptr<AggregatorFactory>> aggregatorFactories;
};
} // end op
} // edn omniruntime

#endif