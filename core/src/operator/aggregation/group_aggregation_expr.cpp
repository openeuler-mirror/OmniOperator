/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: Hash Aggregation WithExpr Source File
 */

#include "group_aggregation_expr.h"
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::type;

HashAggregationWithExprOperatorFactory::HashAggregationWithExprOperatorFactory(
    const std::vector<omniruntime::expressions::Expr *> &groupByKeys, uint32_t groupByNum,
    const std::vector<omniruntime::expressions::Expr *> &aggKeys, uint32_t aggNum, const DataTypes &sourceDataTypes,
    const DataTypes &aggOutputTypes, uint32_t *aggFuncTypes, uint32_t *maskColumns, bool inputRaw, bool outputPartial)
{
    uint32_t aggColNum = aggKeys.size();
    uint32_t projectColNum = groupByNum + aggColNum;
    omniruntime::expressions::Expr *projectKeys[projectColNum];
    for (uint32_t i = 0; i < groupByNum; ++i) {
        projectKeys[i] = groupByKeys.at(i);
    }
    for (uint32_t i = 0, j = groupByNum; i < aggColNum; ++i, ++j) {
        projectKeys[j] = aggKeys.at(i);
    }
    std::vector<int32_t> hashAggCols;
    std::vector<DataTypeRawPtr> newSourceTypes;
    OperatorUtil::CreateRequiredProjectFuncs(sourceDataTypes, projectKeys, projectColNum, newSourceTypes,
        this->rowProjections, this->projectCols, hashAggCols, this->projectFuncs);

    uint32_t groupByCols[groupByNum];
    for (uint32_t i = 0; i < groupByNum; ++i) {
        groupByCols[i] = static_cast<uint32_t>(hashAggCols[i]);
    }
    uint32_t aggCols[aggColNum];
    for (uint32_t i = 0, j = groupByNum; i < aggColNum; ++i, ++j) {
        aggCols[i] = static_cast<uint32_t>(hashAggCols[j]);
    }

    std::vector<DataTypeRawPtr> groupByTypeVec;
    groupByTypeVec.reserve(groupByNum);
    for (uint32_t i = 0; i < groupByNum; i++) {
        groupByTypeVec.push_back(newSourceTypes[groupByCols[i]]);
    }
    this->groupByTypes = std::make_unique<DataTypes>(groupByTypeVec);
    std::vector<DataTypeRawPtr> aggTypeVec;
    aggTypeVec.reserve(aggColNum);
    for (uint32_t i = 0; i < aggColNum; i++) {
        aggTypeVec.push_back(newSourceTypes[aggCols[i]]);
    }
    this->aggTypes = std::make_unique<DataTypes>(aggTypeVec);

    std::vector<uint32_t> groupByCol =
        std::vector<uint32_t>(static_cast<uint32_t *>(groupByCols), static_cast<uint32_t *>(groupByCols) + groupByNum);
    std::vector<uint32_t> aggCol =
        std::vector<uint32_t>(static_cast<uint32_t *>(aggCols), static_cast<uint32_t *>(aggCols) + aggColNum);
    std::vector<uint32_t> aggFunc = std::vector<uint32_t>(aggFuncTypes, aggFuncTypes + aggNum);
    std::vector<uint32_t> maskColumnContext = std::vector<uint32_t>(maskColumns, maskColumns + aggNum);

    this->sourceTypes = std::make_unique<DataTypes>(newSourceTypes);
    this->hashAggOperatorFactory = new HashAggregationOperatorFactory(groupByCol, *(this->groupByTypes.get()), aggCol,
        *(this->aggTypes.get()), aggOutputTypes, aggFunc, maskColumnContext, inputRaw, outputPartial);
    this->hashAggOperatorFactory->Init();
}

HashAggregationWithExprOperatorFactory::~HashAggregationWithExprOperatorFactory()
{
    delete hashAggOperatorFactory;
}

Operator *HashAggregationWithExprOperatorFactory::CreateOperator()
{
    auto hashAggOperator = static_cast<HashAggregationOperator *>(hashAggOperatorFactory->CreateOperator());
    return new HashAggregationWithExprOperator(*(sourceTypes), projectCols, projectFuncs, hashAggOperator);
}

HashAggregationWithExprOperator::HashAggregationWithExprOperator(const DataTypes &sourceTypes,
    std::vector<int32_t> &projectCols, std::vector<RowProjFunc> &projectFuncs, HashAggregationOperator *hashAggOperator)
    : sourceTypes(sourceTypes), projectCols(projectCols), projectFuncs(projectFuncs), hashAggOperator(hashAggOperator)
{}

HashAggregationWithExprOperator::~HashAggregationWithExprOperator()
{
    delete hashAggOperator;
}

int32_t HashAggregationWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectRequiredVectors(inputVecBatch, sourceTypes, projectFuncs, projectCols, vecAllocator);
    hashAggOperator->AddInput(newInputVecBatch);
    VectorHelper::FreeVecBatch(inputVecBatch);
    return 0;
}

int32_t HashAggregationWithExprOperator::GetOutput(std::vector<VectorBatch *> &outputVecBatches)
{
    hashAggOperator->GetOutput(outputVecBatches);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus HashAggregationWithExprOperator::Close()
{
    hashAggOperator->Close();
    return OMNI_STATUS_NORMAL;
}
}
}