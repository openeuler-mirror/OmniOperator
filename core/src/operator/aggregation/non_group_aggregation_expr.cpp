/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: Hash Aggregation WithExpr Source File
 */
#include "non_group_aggregation_expr.h"
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"
#include "agg_util.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::type;

AggregationWithExprOperatorFactory::AggregationWithExprOperatorFactory(
    std::vector<omniruntime::expressions::Expr *> &groupByKeys, uint32_t groupByNum,
    std::vector<std::vector<omniruntime::expressions::Expr *>> &aggsKeys, DataTypes &sourceDataTypes,
    std::vector<DataTypes> &aggOutputTypes, std::vector<uint32_t> &aggFuncTypes,
    std::vector<omniruntime::expressions::Expr *> &aggFilters, std::vector<uint32_t> &maskColumns,
    std::vector<bool> &inputRaws, std::vector<bool> &outputPartial, OverflowConfig *overflowConfig)
{
    uint32_t aggColNum = 0;
    for (auto &aggKeys : aggsKeys) {
        aggColNum += aggKeys.size();
    }
    this->aggFilterNum = aggFilters.size();
    // do  aggsKeys expression handle, and get new sourceTypes,  and agg columnar index
    uint32_t projectColNum = aggColNum;
    omniruntime::expressions::Expr *projectKeys[projectColNum];
    uint32_t projectColIdx = 0;
    for (auto &aggKeys : aggsKeys) {
        for (uint32_t i = 0; i < aggKeys.size(); i++) {
            projectKeys[projectColIdx] = aggKeys.at(i);
            projectColIdx++;
        }
    }

    // do aggSimpleFilters
    for (int i = 0; i < aggFilterNum; ++i) {
        if (aggFilters[i] == nullptr) {
            aggSimpleFilters.push_back(nullptr);
            continue;
        }
        SimpleFilter *simpleFilter = new SimpleFilter(*aggFilters[i]);
        simpleFilter->Initialize((overflowConfig));
        aggSimpleFilters.push_back(simpleFilter);
    }

    std::vector<int32_t> groupByAndAggColumnarIdx;
    std::vector<DataTypePtr> newSourceTypes;
    OperatorUtil::CreateRequiredProjectFuncs(sourceDataTypes, projectKeys, projectColNum, newSourceTypes,
        this->projections, this->projectCols, groupByAndAggColumnarIdx, this->projectFuncs, *overflowConfig);
    uint32_t aggCols[aggColNum];
    for (uint32_t i = 0, j = groupByNum; i < aggColNum; i++, j++) {
        aggCols[i] = static_cast<uint32_t>(groupByAndAggColumnarIdx[j]);
    }
    // get agg columnar data types and index
    std::vector<std::vector<uint32_t>> aggColIdx;
    std::vector<DataTypes> aggInputDataTypes;
    std::vector<DataTypes> aggOutputDataTypes;
    uint32_t startIdx = 0;
    for (auto &aggKeys : aggsKeys) {
        // agg columnar index and data types
        std::vector<uint32_t> aggFuncColIdx;
        std::vector<DataTypePtr> aggInputTypeVec;
        uint32_t oneAggSize = aggKeys.size();
        for (uint32_t i = 0; i < oneAggSize; i++) {
            aggFuncColIdx.push_back(aggCols[startIdx + i]);
            aggInputTypeVec.push_back(newSourceTypes[aggCols[startIdx + i]]);
        }
        startIdx += oneAggSize;
        aggColIdx.push_back(aggFuncColIdx);
        aggInputDataTypes.push_back(*std::make_unique<DataTypes>(aggInputTypeVec));
    }
    originSourceTypes = std::make_unique<DataTypes>(sourceDataTypes);
    sourceTypes = std::make_unique<DataTypes>(newSourceTypes);
    aggOperatorFactory = new AggregationOperatorFactory(*sourceTypes, aggFuncTypes, aggColIdx, maskColumns,
        aggOutputTypes, inputRaws, outputPartial, overflowConfig->IsOverflowAsNull());
    aggOperatorFactory->Init();
}

AggregationWithExprOperatorFactory::~AggregationWithExprOperatorFactory()
{
    delete aggOperatorFactory;
    for (auto it : aggSimpleFilters) {
        delete it;
        it = nullptr;
    }
    aggSimpleFilters.clear();
}

Operator *AggregationWithExprOperatorFactory::CreateOperator()
{
    auto aggOperator = static_cast<AggregationOperator *>(aggOperatorFactory->CreateOperator());
    return new AggregationWithExprOperator(*originSourceTypes, *sourceTypes, projectCols, projectFuncs,
                                           aggSimpleFilters, aggOperator);
}

AggregationWithExprOperator::AggregationWithExprOperator(const DataTypes &originSourceTypes,
    const DataTypes &sourceTypes, std::vector<int32_t> &projectCols, std::vector<ProjFunc> &projectFuncs,
    std::vector<SimpleFilter *> &aggSimpleFilters, AggregationOperator *aggOperator)
    : originTypes(originSourceTypes),
      sourceTypes(sourceTypes),
      projectCols(projectCols),
      projectFuncs(projectFuncs),
      aggSimpleFilters(aggSimpleFilters),
      aggOperator(aggOperator)
{}

AggregationWithExprOperator::~AggregationWithExprOperator()
{
    delete aggOperator;
}

int32_t AggregationWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    VectorBatch *newInputVecBatch = AggUtil::AggFilterRequiredVectors(inputVecBatch, originTypes, sourceTypes,
        projectFuncs, projectCols);
    // do filter and update newInputVecBatch
    // if is true not filter
    AggUtil::AddFilterColumn(inputVecBatch, newInputVecBatch, projectCols, aggSimpleFilters, context, originTypes);
    aggOperator->AddInput(newInputVecBatch);
    VectorHelper::FreeVecBatch(inputVecBatch);
    return 0;
}

int32_t AggregationWithExprOperator::GetOutput(VectorBatch **outputVecBatch)
{
    aggOperator->GetOutput(outputVecBatch);
    this->outputTypes = aggOperator->GetOutputType();
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus AggregationWithExprOperator::Close()
{
    aggOperator->Close();
    return OMNI_STATUS_NORMAL;
}
}
}