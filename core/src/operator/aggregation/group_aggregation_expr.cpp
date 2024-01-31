/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * @Description: Hash Aggregation WithExpr Source File
 */

#include "group_aggregation_expr.h"
#include "agg_util.h"
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::type;

HashAggregationWithExprOperatorFactory::HashAggregationWithExprOperatorFactory(
    std::vector<omniruntime::expressions::Expr *> &groupByKeys, uint32_t groupByNum,
    std::vector<std::vector<omniruntime::expressions::Expr *>> &aggsKeys,
    std::vector<omniruntime::expressions::Expr *> &aggFilters, DataTypes &sourceDataTypes,
    std::vector<DataTypes> &aggOutputTypes, std::vector<uint32_t> &aggFuncTypes, std::vector<uint32_t> &maskColumns,
    std::vector<bool> &inputRaws, std::vector<bool> &outputPartial, const OperatorConfig &operatorConfig)
{
    OperatorConfig::CheckOperatorConfig(operatorConfig);

    uint32_t aggColNum = 0;
    for (auto &aggKeys : aggsKeys) {
        aggColNum += aggKeys.size();
    }

    // do groupByKeys and aggsKeys expression handle, and get new sourceTypes, groupby and agg columnar index
    uint32_t projectColNum = groupByNum + aggColNum;
    std::vector<omniruntime::expressions::Expr *> projectKeys(projectColNum);
    for (uint32_t i = 0; i < groupByNum; i++) {
        projectKeys[i] = groupByKeys[i];
    }

    uint32_t projectColIdx = groupByNum;
    for (auto &aggKeys : aggsKeys) {
        for (auto &aggKey : aggKeys) {
            projectKeys[projectColIdx] = aggKey;
            projectColIdx++;
        }
    }

    // do aggSimpleFilters
    this->aggFilterNum = static_cast<int32_t>(aggFilters.size());
    for (int32_t i = 0; i < aggFilterNum; ++i) {
        if (aggFilters[i] == nullptr) {
            aggSimpleFilters.push_back(nullptr);
            continue;
        }
        auto simpleFilter = new SimpleFilter(*aggFilters[i]);
        simpleFilter->Initialize(operatorConfig.GetOverflowConfig());
        aggSimpleFilters.push_back(simpleFilter);
    }

    std::vector<int32_t> groupByAndAggColumnarIdx;
    std::vector<DataTypePtr> newSourceTypes;
    OperatorUtil::CreateRequiredProjections(sourceDataTypes, projectKeys, newSourceTypes, this->projections,
        groupByAndAggColumnarIdx, *(operatorConfig.GetOverflowConfig()));
    uint32_t groupByCols[groupByNum];
    for (uint32_t i = 0; i < groupByNum; i++) {
        groupByCols[i] = static_cast<uint32_t>(groupByAndAggColumnarIdx[i]);
    }
    uint32_t aggCols[aggColNum];
    for (uint32_t i = 0, j = groupByNum; i < aggColNum; i++, j++) {
        aggCols[i] = static_cast<uint32_t>(groupByAndAggColumnarIdx[j]);
    }

    // get groupby columnar data types and index
    std::vector<DataTypePtr> groupByTypeVec;
    groupByTypeVec.reserve(groupByNum);
    for (uint32_t i = 0; i < groupByNum; i++) {
        groupByTypeVec.push_back(newSourceTypes[groupByCols[i]]);
    }
    this->groupByTypes = std::make_unique<DataTypes>(groupByTypeVec);
    std::vector<uint32_t> groupByCol =
        std::vector<uint32_t>(static_cast<uint32_t *>(groupByCols), static_cast<uint32_t *>(groupByCols) + groupByNum);

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

    this->originSourceTypes = std::make_unique<DataTypes>(sourceDataTypes);
    this->sourceTypes = std::make_unique<DataTypes>(newSourceTypes);
    this->hashAggOperatorFactory =
        new HashAggregationOperatorFactory(groupByCol, *groupByTypes, aggColIdx, aggInputDataTypes, aggOutputTypes,
        aggFuncTypes, maskColumns, inputRaws, outputPartial, operatorConfig);
    this->hashAggOperatorFactory->Init();
}

HashAggregationWithExprOperatorFactory::~HashAggregationWithExprOperatorFactory()
{
    delete hashAggOperatorFactory;
    for (auto it : aggSimpleFilters) {
        delete it;
    }
    aggSimpleFilters.clear();
}

Operator *HashAggregationWithExprOperatorFactory::CreateOperator()
{
    auto hashAggOperator = static_cast<HashAggregationOperator *>(hashAggOperatorFactory->CreateOperator());
    auto *op = new HashAggregationWithExprOperator(*originSourceTypes, *sourceTypes, projections, aggSimpleFilters,
        hashAggOperator);
    std::vector<type::DataTypeId> dataTypeIds;
    for (int32_t i = 0; i < originSourceTypes->GetSize(); ++i) {
        dataTypeIds.push_back(originSourceTypes->GetType(i)->GetId());
    }
    op->Init(dataTypeIds);
    return op;
}

HashAggregationWithExprOperator::HashAggregationWithExprOperator(const DataTypes &originSourceTypes,
    const DataTypes &sourceTypes, std::vector<std::unique_ptr<Projection>> &projections,
    std::vector<SimpleFilter *> &aggSimpleFilters, HashAggregationOperator *hashAggOperator)
    : originTypes(originSourceTypes),
      sourceTypes(sourceTypes),
      projections(projections),
      aggSimpleFilters(aggSimpleFilters),
      hashAggOperator(hashAggOperator),
      executionContext(new ExecutionContext())
{}

HashAggregationWithExprOperator::~HashAggregationWithExprOperator()
{
    delete hashAggOperator;
    delete executionContext;
}

int32_t HashAggregationWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    VectorBatch *newInputVecBatch =
        AggUtil::AggFilterRequiredVectors(inputVecBatch, originTypes, sourceTypes, projections, executionContext);
    // do filter and update newInputVecBatch
    // if is true not filter
    AggUtil::AddFilterColumn(inputVecBatch, newInputVecBatch, aggSimpleFilters, executionContext, originTypes);
    hashAggOperator->AddInput(newInputVecBatch);
    VectorHelper::FreeVecBatch(inputVecBatch);
    return 0;
}

void HashAggregationWithExprOperator::ProcessRow(uintptr_t rowValues[], int32_t lens[])
{
    auto inputVecBatch = oneRowAdaptor.Trans2VectorBatch(rowValues, lens);
    VectorBatch *newInputVecBatch =
        AggUtil::AggFilterRequiredVectors(inputVecBatch, originTypes, sourceTypes, projections, executionContext);
    hashAggOperator->AddInput(newInputVecBatch);
    // no need to delete inputVecBatch, it will be reused when this interface call again
}

int32_t HashAggregationWithExprOperator::GetOutput(VectorBatch **outputVecBatch)
{
    int32_t status = hashAggOperator->GetOutput(outputVecBatch);
    SetStatus(hashAggOperator->GetStatus());
    return status;
}

OmniStatus HashAggregationWithExprOperator::Close()
{
    hashAggOperator->Close();
    return OMNI_STATUS_NORMAL;
}

uint64_t HashAggregationWithExprOperator::GetSpilledBytes()
{
    return hashAggOperator->GetSpilledBytes();
}

OmniStatus HashAggregationWithExprOperator::Init(const std::vector<type::DataTypeId> &dataTypeIds)
{
    oneRowAdaptor.Init(dataTypeIds);
    return OMNI_STATUS_NORMAL;
}
}
}