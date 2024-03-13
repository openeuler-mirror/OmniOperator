/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: lookup join implementations
 */

#include "lookup_join_expr.h"

#include <utility>
#include "hash_builder_expr.h"
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

LookupJoinWithExprOperatorFactory *LookupJoinWithExprOperatorFactory::CreateLookupJoinWithExprOperatorFactory(
    const DataTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &probeHashKeys, int32_t probeHashKeysCount,
    int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes,
    int64_t hashBuilderFactoryAddr, omniruntime::expressions::Expr *filterExpr, OverflowConfig *overflowConfig)
{
    auto operatorFactory = new LookupJoinWithExprOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashKeys, probeHashKeysCount, buildOutputCols, buildOutputColsCount, buildOutputTypes,
        hashBuilderFactoryAddr, filterExpr, overflowConfig);
    return operatorFactory;
}

LookupJoinWithExprOperatorFactory::LookupJoinWithExprOperatorFactory(const DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &probeHashKeys, int32_t probeHashKeysCount,
    int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes,
    int64_t hashBuilderFactoryAddr, Expr *filterExpr, OverflowConfig *overflowConfig)
{
    std::vector<DataTypePtr> newProbeTypes;
    OperatorUtil::CreateProjections(probeTypes, probeHashKeys, newProbeTypes, this->projections, this->probeHashCols,
        overflowConfig);
    this->probeTypes = std::make_unique<DataTypes>(newProbeTypes);
    auto hashBuilderWithExprOperatorFactory =
        reinterpret_cast<HashBuilderWithExprOperatorFactory *>(hashBuilderFactoryAddr);
    this->operatorFactory =
        LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(*(this->probeTypes), probeOutputCols,
        probeOutputColsCount, this->probeHashCols.data(), probeHashKeysCount, buildOutputCols, buildOutputColsCount,
        std::move(buildOutputTypes), (int64_t)(hashBuilderWithExprOperatorFactory->GetHashBuilderOperatorFactory()),
        filterExpr, probeTypes.GetSize(), overflowConfig);
}

LookupJoinWithExprOperatorFactory::~LookupJoinWithExprOperatorFactory()
{
    delete this->operatorFactory;
}

Operator *LookupJoinWithExprOperatorFactory::CreateOperator()
{
    auto lookupJoinOperator = static_cast<LookupJoinOperator *>(operatorFactory->CreateOperator());
    return new LookupJoinWithExprOperator(*probeTypes, projections, lookupJoinOperator);
}

LookupJoinWithExprOperator::LookupJoinWithExprOperator(const type::DataTypes &probeTypes,
    std::vector<std::unique_ptr<Projection>> &projections, LookupJoinOperator *lookupJoinOperator)
    : probeTypes(probeTypes),
      projections(projections),
      lookupJoinOperator(lookupJoinOperator),
      executionContext(new ExecutionContext())
{}

LookupJoinWithExprOperator::~LookupJoinWithExprOperator()
{
    delete lookupJoinOperator;
    delete executionContext;
}

int32_t LookupJoinWithExprOperator::AddInput(VectorBatch *vecBatch)
{
    VectorBatch *newInputVecBatch = OperatorUtil::ProjectVectors(vecBatch, probeTypes, projections, executionContext);
    VectorHelper::FreeVecBatch(vecBatch);
    inputVecBatch = nullptr;

    lookupJoinOperator->AddInput(newInputVecBatch);
    SetStatus(lookupJoinOperator->GetStatus());
    return 0;
}

int32_t LookupJoinWithExprOperator::GetOutput(VectorBatch **outputVecBatch)
{
    int32_t status = lookupJoinOperator->GetOutput(outputVecBatch);
    SetStatus(lookupJoinOperator->GetStatus());
    return status;
}

OmniStatus LookupJoinWithExprOperator::Close()
{
    lookupJoinOperator->Close();
    return OMNI_STATUS_NORMAL;
}
}
}