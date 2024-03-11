/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * @Description: sort implementations
 */

#include "sort_expr.h"
#include "sort.h"
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

SortWithExprOperatorFactory *SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(
    const type::DataTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts,
    int32_t sortKeysCount, const OperatorConfig &operatorConfig)
{
    auto pOperatorFactory = new SortWithExprOperatorFactory(sourceTypes, outputCols, outputColsCount, sortKeys,
        sortAscendings, sortNullFirsts, sortKeysCount, operatorConfig);
    return pOperatorFactory;
}

SortWithExprOperatorFactory::SortWithExprOperatorFactory(const type::DataTypes &sourceTypes, int32_t *outputCols,
    int32_t outputColsCount, const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortKeysCount, const OperatorConfig &operatorConfig)
{
    std::vector<DataTypePtr> newSourceTypes;
    OperatorUtil::CreateProjections(sourceTypes, sortKeys, newSourceTypes, this->projections, this->sortCols,
        operatorConfig.GetOverflowConfig());
    this->sourceTypes = std::make_unique<DataTypes>(newSourceTypes);
    this->sortOperatorFactory = SortOperatorFactory::CreateSortOperatorFactory(*(this->sourceTypes), outputCols,
        outputColsCount, sortCols.data(), sortAscendings, sortNullFirsts, sortKeysCount, operatorConfig);
}

SortWithExprOperatorFactory *SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(
    const type::DataTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts,
    int32_t sortKeysCount)
{
    auto pOperatorFactory = new SortWithExprOperatorFactory(sourceTypes, outputCols, outputColsCount, sortKeys,
        sortAscendings, sortNullFirsts, sortKeysCount, OperatorConfig());
    return pOperatorFactory;
}

SortWithExprOperatorFactory::~SortWithExprOperatorFactory()
{
    delete sortOperatorFactory;
}

Operator *SortWithExprOperatorFactory::CreateOperator()
{
    auto sortOperator = static_cast<SortOperator *>(sortOperatorFactory->CreateOperator());
    auto pOperator = new SortWithExprOperator(*sourceTypes, projections, sortOperator);
    return pOperator;
}

SortWithExprOperator::SortWithExprOperator(const type::DataTypes &sourceTypes,
    std::vector<std::unique_ptr<Projection>> &projections, SortOperator *sortOperator)
    : sourceTypes(sourceTypes),
      projections(projections),
      sortOperator(sortOperator),
      executionContext(new ExecutionContext())
{}

SortWithExprOperator::~SortWithExprOperator()
{
    delete sortOperator;
    delete executionContext;
}

std::atomic<int64_t> exceptionCount = 0;
int32_t SortWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectVectors(inputVecBatch, sourceTypes, projections, executionContext);
    if (exceptionCount.fetch_add(1, std::memory_order_relaxed) == 0) {
        throw OmniException("OPERATOR_RUNTIME_ERROR", "SortWithExprOperator::AddInput failed");
    }
    VectorHelper::FreeVecBatch(inputVecBatch);
    ResetInputVecBatch();
    sortOperator->AddInput(newInputVecBatch);
    return 0;
}

int32_t SortWithExprOperator::GetOutput(VectorBatch **outputVecBatch)
{
    int32_t status = sortOperator->GetOutput(outputVecBatch);
    SetStatus(sortOperator->GetStatus());
    return status;
}

OmniStatus SortWithExprOperator::Close()
{
    sortOperator->Close();
    return OMNI_STATUS_NORMAL;
}

uint64_t SortWithExprOperator::GetSpilledBytes()
{
    return sortOperator->GetSpilledBytes();
}
}
}
