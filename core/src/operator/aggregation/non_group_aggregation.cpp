/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Aggregation Source File
 */
#include "non_group_aggregation.h"
#include "vector/vector_common.h"
#include "operator/status.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

OmniStatus AggregationOperatorFactory::Init()
{
    OmniStatus ret = OMNI_STATUS_NORMAL;
    std::vector<DataTypePtr> types = sourceTypes.Get();
    for (uint32_t i = 0; i < aggInputColsVector.size(); i++) {
        aggInputCols.push_back(aggInputColsVector[i]);
        aggInputTypes.push_back(types[aggInputColsVector[i]]);
    }

    ret = CreateAggregatorFactories(aggregatorFactories, aggFuncTypesVector, GetMaskColumns());

    return ret;
}

OmniStatus AggregationOperatorFactory::Close()
{
    return OMNI_STATUS_NORMAL;
}

Operator *AggregationOperatorFactory::CreateOperator()
{
    std::vector<std::unique_ptr<Aggregator>> aggs;

    uint32_t aggInputChannelIndex = 0;
    for (int32_t i = 0; i < this->aggOutputTypes.GetSize(); i++) {
        uint32_t aggregateType = aggFuncTypesVector[i];
        DataTypePtr inputType;
        int32_t aggInputCol;
        if (aggregateType == OMNI_AGGREGATION_TYPE_COUNT_ALL) {
            inputType = std::make_shared<NoneDataType>();
            aggInputCol = Aggregator::INVALID_INPUT_COL;
        } else {
            inputType = aggInputTypes[aggInputChannelIndex];
            aggInputCol = aggInputCols[aggInputChannelIndex];
            aggInputChannelIndex++;
        }

        auto outputType = aggOutputTypes.Get()[i];
        auto aggregator =
            aggregatorFactories[i]->CreateAggregator(inputType, outputType, aggInputCol, inputRaw, outputPartial);
        aggs.push_back(std::move(aggregator));
    }

    return new AggregationOperator(std::move(aggs), aggOutputTypes, inputRaw, outputPartial);
}

int32_t AggregationOperator::AddInput(VectorBatch *vecBatch)
{
    auto aggCount = aggregators.size();
    int32_t rowCount = vecBatch->GetRowCount();
    for (size_t aggIdx = 0; aggIdx < aggCount; aggIdx++) {
        auto aggregator = aggregators[aggIdx].get();
        auto &state = aggStates[aggIdx];
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            aggregator->ProcessGroup(state, vecBatch, rowIdx);
        }
    }
    VectorHelper::FreeVecBatch(vecBatch);
    return 0;
}

int AggregationOperator::GetOutput(std::vector<VectorBatch *> &result)
{
    // always output one row
    auto aggCount = aggregators.size();
    auto outputVecBatch = new VectorBatch(aggCount, 1);
    outputVecBatch->NewVectors(this->vecAllocator, aggOutputTypes.Get());

    // set result value
    for (size_t aggIdx = 0; aggIdx < aggCount; ++aggIdx) {
        auto aggregator = aggregators[aggIdx].get();
        auto outputVec = outputVecBatch->GetVector(aggIdx);
        auto state = aggStates[aggIdx];
        aggregator->ExtractValue(state, outputVec, 0);
    }

    result.push_back(outputVecBatch);
    SetStatus(OMNI_STATUS_FINISHED);
    return OMNI_STATUS_FINISHED;
}
}
}