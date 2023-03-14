/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: Hash Aggregation WithExpr Source File
 */

#include "non_group_aggregation_expr.h"
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::type;

AggregationWithExprOperatorFactory::AggregationWithExprOperatorFactory(
    std::vector<omniruntime::expressions::Expr *> &groupByKeys, uint32_t groupByNum,
    std::vector<std::vector<omniruntime::expressions::Expr *>> &aggsKeys, DataTypes &sourceDataTypes,
    std::vector<DataTypes> &aggOutputTypes, std::vector<uint32_t> &aggFuncTypes, std::vector<uint32_t> &maskColumns,
    std::vector<bool> &inputRaws, std::vector<bool> &outputPartial, const OverflowConfig &overflowConfig)
{
    uint32_t aggColNum = 0;
    for (auto &aggKeys : aggsKeys) {
        aggColNum += aggKeys.size();
    }
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
    std::vector<int32_t> groupByAndAggColumnarIdx;
    std::vector<DataTypePtr> newSourceTypes;
    OperatorUtil::CreateRequiredProjectFuncs(sourceDataTypes, projectKeys, projectColNum, newSourceTypes,
        this->rowProjections, this->projectCols, groupByAndAggColumnarIdx, this->projectFuncs, overflowConfig);
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
    sourceTypes = std::make_unique<DataTypes>(newSourceTypes);
    aggOperatorFactory = new AggregationOperatorFactory(*sourceTypes, aggFuncTypes, aggColIdx, maskColumns,
        aggOutputTypes, inputRaws, outputPartial, overflowConfig.IsOverflowAsNull());
    aggOperatorFactory->Init();
}

AggregationWithExprOperatorFactory::~AggregationWithExprOperatorFactory()
{
    delete aggOperatorFactory;
}

Operator *AggregationWithExprOperatorFactory::CreateOperator()
{
    auto aggOperator = static_cast<AggregationOperator *>(aggOperatorFactory->CreateOperator());
    return new AggregationWithExprOperator(*sourceTypes, projectCols, projectFuncs, aggOperator);
}

AggregationWithExprOperator::AggregationWithExprOperator(const DataTypes &sourceTypes,
    std::vector<int32_t> &projectCols, std::vector<RowProjFunc> &projectFuncs, AggregationOperator *aggOperator)
    : sourceTypes(sourceTypes), projectCols(projectCols), projectFuncs(projectFuncs), aggOperator(aggOperator)
{}

AggregationWithExprOperator::~AggregationWithExprOperator()
{
    delete aggOperator;
}

int32_t AggregationWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectRequiredVectors(inputVecBatch, sourceTypes, projectFuncs, projectCols, vecAllocator);
    aggOperator->AddInput(newInputVecBatch);
    VectorHelper::FreeVecBatch(inputVecBatch);
    return 0;
}

int32_t AggregationWithExprOperator::GetOutput(std::vector<VectorBatch *> &outputVecBatches)
{
    aggOperator->GetOutput(outputVecBatches);
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