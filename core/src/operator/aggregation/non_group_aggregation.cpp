/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Aggregation Source File
 */
#include "non_group_aggregation.h"
#include "vector/vector_common.h"
#include "operator/status.h"
#include "util/type_util.h"
#ifdef ENABLE_HMPP
#include "util/config_util.h"
#endif

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

OmniStatus AggregationOperatorFactory::Init()
{
    auto &types = sourceTypes.Get();
    aggsInputCols.reserve(aggsInputColsVector.size());
    aggsInputTypes.reserve(aggsInputColsVector.size());
    for (auto aggInputCol : aggsInputColsVector) {
        std::vector<DataTypePtr> aggInputTypeVec;
        std::vector<int32_t> aggsInputVec;
        for (uint32_t i = 0; i < aggInputCol.size(); i++) {
            aggInputTypeVec.push_back(types[aggInputCol[i]]);
            aggsInputVec.push_back(aggInputCol[i]);
        }
        aggsInputCols.push_back(aggsInputVec);
        aggsInputTypes.push_back(std::make_unique<DataTypes>(aggInputTypeVec));
    }

    return CreateAggregatorFactories(aggregatorFactories, aggFuncTypesVector, GetMaskColumns());
}

OmniStatus AggregationOperatorFactory::Close()
{
    return OMNI_STATUS_NORMAL;
}

Operator *AggregationOperatorFactory::CreateOperator()
{
    std::vector<std::unique_ptr<Aggregator>> aggs;

    uint32_t aggCountAllSkipCnt = 0;
    for (uint32_t i = 0; i < this->aggsOutputTypes.size(); i++) {
        uint32_t aggregateType = aggFuncTypesVector[i];
        std::vector<int32_t> aggInputColIdxVec;
        std::vector<DataTypePtr> inputDataTypesPtr;

        // for COUNT_ALL aggregator no input(key and columnar index)
        // use aggCountAllSkipCnt to align with aggsInputCols and aggregatorFactories index not same
        if (aggregateType == OMNI_AGGREGATION_TYPE_COUNT_ALL) {
            inputDataTypesPtr.push_back(NoneType());
            aggInputColIdxVec.push_back(-1);
            aggCountAllSkipCnt++;
        } else {
            for (uint32_t j = 0; j < this->aggsInputCols[i - aggCountAllSkipCnt].size(); j++) {
                aggInputColIdxVec.push_back(aggsInputCols[i - aggCountAllSkipCnt][j]);
                inputDataTypesPtr.push_back(aggsInputTypes[i - aggCountAllSkipCnt]->GetType(j));
            }
        }
        auto inputTypes = DataTypes(inputDataTypesPtr).Instance();
        auto outputTypes = aggsOutputTypes[i].Instance();
        auto aggregator = aggregatorFactories[i]->CreateAggregator(*inputTypes, *outputTypes, aggInputColIdxVec,
            inputRaws[i], outputPartials[i]);
        if (aggregator == nullptr) {
            throw OmniException("create non_group aggregation operator", "return nullptr when create aggregator ");
        }
        aggs.push_back(std::move(aggregator));
    }

    return new AggregationOperator(std::move(aggs), aggsOutputTypes, inputRaws, outputPartials);
}

int32_t AggregationOperator::AddInput(VectorBatch *vecBatch)
{
    auto aggCount = aggregators.size();
    for (size_t aggIdx = 0; aggIdx < aggCount; aggIdx++) {
        auto aggregator = aggregators[aggIdx].get();
        auto &state = aggsStates[aggIdx];

#ifdef ENABLE_HMPP
        if (ConfigUtil::IsEnableHMPP() && aggregator->CanProcessWithHMPP(state, vecBatch)) {
            aggregator->ProcessGroupWithHMPP(state, vecBatch);
        } else {
            aggregator->ProcessGroup(state, vecBatch, 0, vecBatch->GetRowCount());
        }
#else
        aggregator->ProcessGroup(state, vecBatch, 0, vecBatch->GetRowCount());
#endif
    }
    VectorHelper::FreeVecBatch(vecBatch);
    return 0;
}

int AggregationOperator::GetOutput(std::vector<VectorBatch *> &result)
{
    // always output one row
    int32_t aggsCount = 0;
    std::vector<DataTypePtr> aggsOutputDataTypePtrs;
    for (auto aggOutputTypes : aggsOutputTypes) {
        auto aggSize = aggOutputTypes.GetSize();
        aggsCount += aggSize;
        for (int i = 0; i < aggSize; ++i) {
            aggsOutputDataTypePtrs.push_back(aggOutputTypes.GetType(i));
        }
    }
    auto outputVecBatch = new VectorBatch(aggsCount, 1);
    outputVecBatch->NewVectors(this->vecAllocator, aggsOutputDataTypePtrs);

    // set result value
    int32_t aggOutputColsStart = 0;
    for (size_t aggIdx = 0; aggIdx < aggregators.size(); ++aggIdx) {
        auto aggregator = aggregators[aggIdx].get();
        auto &state = aggsStates[aggIdx];
        std::vector<Vector *> extractVectors;
        for (int i = 0; i < aggsOutputTypes[aggIdx].GetSize(); ++i) {
            extractVectors.push_back(outputVecBatch->GetVector(aggOutputColsStart + i));
        }
        aggOutputColsStart += aggsOutputTypes[aggIdx].GetSize();
        aggregator->ExtractValues(state, extractVectors, 0);
    }

    result.push_back(outputVecBatch);
    SetStatus(OMNI_STATUS_FINISHED);
    return OMNI_STATUS_FINISHED;
}
}
}