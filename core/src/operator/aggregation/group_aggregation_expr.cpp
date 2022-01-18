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
using namespace omniruntime::vec;

HashAggregationWithExprOperatorFactory::HashAggregationWithExprOperatorFactory(
        const std::vector<omniruntime::expressions::Expr *> &groupByKeys, uint32_t groupByNum,
        const std::vector<omniruntime::expressions::Expr *> &aggKeys, uint32_t aggNum, const VecTypes &sourceVecTypes,
        const VecTypes &aggOutputTypes, uint32_t *aggFuncTypes, bool inputRaw, bool outputPartial)
{
    uint32_t projectColNum = groupByNum + aggNum;
    omniruntime::expressions::Expr *projectKeys[projectColNum];
    for (int i = 0; i < groupByNum; ++i) {
        projectKeys[i] = groupByKeys.at(i);
    }
    for (int i = 0, j = groupByNum; i < aggNum; ++i, ++j) {
        projectKeys[j] = aggKeys.at(i);
    }
    std::vector<int32_t> hashAggCols;
    std::vector<VecType> newSourceTypes;
    OperatorUtil::CreateRequiredProjectFuncs(sourceVecTypes, projectKeys, projectColNum, newSourceTypes,
        this->rowProjections, this->projectCols, hashAggCols, this->projectFuncs);

    uint32_t groupByCols[groupByNum];
    for (int i = 0; i < groupByNum; ++i) {
        groupByCols[i] = hashAggCols[i];
    }
    uint32_t aggCols[aggNum];
    for (int i = 0, j = groupByNum; i < aggNum; ++i, ++j) {
        aggCols[i] = hashAggCols[j];
    }

    std::vector<VecType> groupByTypeVec;
    groupByTypeVec.reserve(groupByNum);
    for (int i = 0; i < groupByNum; i++) {
        groupByTypeVec.push_back(newSourceTypes[groupByCols[i]]);
    }
    this->groupByTypes = std::make_unique<VecTypes>(groupByTypeVec);
    std::vector<VecType> aggTypeVec;
    aggTypeVec.reserve(aggNum);
    for (int i = 0; i < aggNum; i++) {
        aggTypeVec.push_back(newSourceTypes[aggCols[i]]);
    }
    this->aggTypes = std::make_unique<VecTypes>(aggTypeVec);

    PrepareContext groupByCol = { static_cast<uint32_t *>(groupByCols), groupByNum };
    PrepareContext aggCol = { static_cast<uint32_t *>(aggCols), aggNum };
    PrepareContext aggFunc = { aggFuncTypes, aggNum };

    this->sourceTypes = std::make_unique<VecTypes>(newSourceTypes);
    this->hashAggOperatorFactory = std::make_unique<HashAggregationOperatorFactory>(groupByCol,
         *(this->groupByTypes.get()), aggCol, *(this->aggTypes.get()), aggOutputTypes, aggFunc,
          inputRaw, outputPartial).release();
    this->hashAggOperatorFactory->Init();
}

HashAggregationWithExprOperatorFactory::~HashAggregationWithExprOperatorFactory()
{
    delete hashAggOperatorFactory;
}

Operator *HashAggregationWithExprOperatorFactory::CreateOperator()
{
    auto hashAggOperator = static_cast<HashAggregationOperator *>(
        hashAggOperatorFactory->CreateOperator());
    auto pOperator = std::make_unique<HashAggregationWithExprOperator>(*(sourceTypes), projectCols,
        projectFuncs, hashAggOperator);
    return pOperator.release();
}

HashAggregationWithExprOperator::HashAggregationWithExprOperator(const vec::VecTypes &sourceTypes,
    std::vector<int32_t> &projectCols, std::vector<RowProjFunc> &projectFuncs, HashAggregationOperator *hashAggOperator)
    : sourceTypes(sourceTypes), projectCols(projectCols), projectFuncs(projectFuncs), hashAggOperator(hashAggOperator)
{}

HashAggregationWithExprOperator::~HashAggregationWithExprOperator()
{
    delete hashAggOperator;
}

int32_t HashAggregationWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    VectorBatch *newInputVecBatch = OperatorUtil::ProjectRequiredVectors(inputVecBatch, sourceTypes, projectFuncs,
        projectCols);
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
    return OMNI_STATUS_NORMAL;
}
}
}