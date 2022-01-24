/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation Source File
 */
#include "non_group_aggregation.h"
#include "jit/annotation.h"
#include "operator/optimization.h"
#include "vector/vector_common.h"
#include "operator/status.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

template <class T> void AggregationOperatorFactory::CreateAggregatorFactory(int32_t maskCol)
{
    if (maskCol == Aggregator::INVALID_MASK_COL) {
        aggregatorFactories.push_back(std::make_unique<T>());
    } else {
        aggregatorFactories.push_back(std::make_unique<MaskAggregatorFactory<T>>(maskCol));
    }
}

OmniStatus AggregationOperatorFactory::Init()
{
    OmniStatus ret = OMNI_STATUS_NORMAL;
    uint32_t *aggInputColsPtr = aggInputColsContext.context;
    std::vector<VecType> types = sourceTypes.Get();
    for (int32_t i = 0; i < aggFuncTypesContext.len; i++) {
        aggInputCols.push_back(aggInputColsPtr[i]);
        maskCols.push_back(maskColsContext.context[i]);
        aggInputTypes.push_back(types[aggInputColsPtr[i]]);
    }

    uint32_t *aggFuncTypesPtr = aggFuncTypesContext.context;
    for (int32_t i = 0; i < aggFuncTypesContext.len; i++) {
        switch (aggFuncTypesPtr[i]) {
            case OMNI_AGGREGATION_TYPE_SUM: {
                CreateAggregatorFactory<SumAggregatorFactory>(maskCols[i]);
                break;
            }
            case OMNI_AGGREGATION_TYPE_COUNT: {
                CreateAggregatorFactory<CountAggregatorFactory>(maskCols[i]);
                break;
            }
            case OMNI_AGGREGATION_TYPE_MAX: {
                CreateAggregatorFactory<MaxAggregatorFactory>(maskCols[i]);
                break;
            }
            case OMNI_AGGREGATION_TYPE_MIN: {
                CreateAggregatorFactory<MinAggregatorFactory>(maskCols[i]);
                break;
            }
            case OMNI_AGGREGATION_TYPE_AVG: {
                CreateAggregatorFactory<AverageAggregatorFactory>(maskCols[i]);
                break;
            }
            default: {
                LogError("No such agg func type %d", aggFuncTypesPtr[i]);
                ret = OMNI_STATUS_ERROR;
            }
        }
    }
    return ret;
}

OmniStatus AggregationOperatorFactory::Close()
{
    return OMNI_STATUS_NORMAL;
}

Operator *AggregationOperatorFactory::CreateOperator()
{
    std::vector<std::unique_ptr<Aggregator>> aggs;

    for (int32_t i = 0; i < this->aggOutputTypes.GetSize(); i++) {
        auto inputType = aggInputTypes[i];
        auto outputType = aggOutputTypes.Get()[i];
        auto aggregator = aggregatorFactories[i]->CreateAggregator(inputType.GetId(), outputType.GetId(),
            aggInputCols[i], inputRaw, outputPartial);
        aggs.push_back(std::move(aggregator));
    }

    AggregationOperator *aggOp =
        new AggregationOperator(std::move(aggs), aggInputCols, maskCols, aggOutputTypes, inputRaw, outputPartial);
    return aggOp;
}

int32_t AggregationOperator::AddInput(VectorBatch *vecBatch)
{
    int32_t aggCount = aggregators.size();
    int32_t rowCount = vecBatch->GetRowCount();
    auto vectors = vecBatch->GetVectors();
    for (int32_t aggIdx = 0; aggIdx < aggCount; aggIdx++) {
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
    int32_t aggCount = aggregators.size();
    auto outputVecBatch = new VectorBatch(aggCount, 1);
    outputVecBatch->NewVectors(this->vecAllocator, aggOutputTypes.Get());

    // set result value
    for (int32_t aggIdx = 0; aggIdx < aggCount; ++aggIdx) {
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