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
    int64_t hashBuilderFactoryAddr, omniruntime::expressions::Expr *filterExpr,
    bool isShuffleExchangeBuildPlan, OverflowConfig *overflowConfig)
{
    auto operatorFactory = new LookupJoinWithExprOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashKeys, probeHashKeysCount, buildOutputCols, buildOutputColsCount, buildOutputTypes,
        hashBuilderFactoryAddr, filterExpr, isShuffleExchangeBuildPlan, overflowConfig);
    return operatorFactory;
}

LookupJoinWithExprOperatorFactory *LookupJoinWithExprOperatorFactory::CreateLookupJoinWithExprOperatorFactory(
    std::shared_ptr<const HashJoinNode> planNode, HashBuilderWithExprOperatorFactory* hashBuilderOperatorFactory,
    const config::QueryConfig &queryConfig)
{
    auto buildOutputTypes = planNode->RightOutputType();
    auto buildOutputColsCount = buildOutputTypes->GetSize();
    if (planNode->IsLeftSemi() || planNode->IsLeftAnti()) {
        buildOutputColsCount = 0;
    } else if (planNode->IsExistence()) {
        buildOutputColsCount = 1;
        buildOutputTypes = std::make_shared<DataTypes>(DataTypes({BooleanType()}));
    }
    std::vector<int32_t> buildOutputCols;
    if (planNode->IsExistence()) {
        buildOutputCols.emplace_back(1);
    } else {
        for (size_t index = 0; index < buildOutputColsCount; index++) {
            buildOutputCols.emplace_back(index);
        }
    }

    auto probeOutputTypes = planNode->LeftOutputType();
    auto probeOutputColsCount = probeOutputTypes->GetSize();
    std::vector<int32_t> probeOutputCols;
    for (size_t index = 0; index < probeOutputColsCount; index++) {
        probeOutputCols.emplace_back(index);
    }

    auto probeHashCols = planNode->LeftKeys();
    auto probeHashColsCount = (int32_t) probeHashCols.size();

    auto filter = planNode->Filter();
    auto isShuffle = planNode->IsShuffle();

    auto overflowConfig = queryConfig.IsOverFlowASNull()
                          ? new OverflowConfig(OVERFLOW_CONFIG_NULL)
                          : new OverflowConfig(OVERFLOW_CONFIG_EXCEPTION);

    auto pLookupJoinWithExprOperatorFactory = new LookupJoinWithExprOperatorFactory(*probeOutputTypes, probeOutputCols.data(), probeOutputColsCount,
        probeHashCols, probeHashColsCount, buildOutputCols.data(), buildOutputColsCount, *buildOutputTypes,
    reinterpret_cast<int64_t>(hashBuilderOperatorFactory), filter, isShuffle, overflowConfig);

    delete overflowConfig;
    overflowConfig = nullptr;
    return pLookupJoinWithExprOperatorFactory;
}

LookupJoinWithExprOperatorFactory::LookupJoinWithExprOperatorFactory(const DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &probeHashKeys, int32_t probeHashKeysCount,
    int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes,
    int64_t hashBuilderFactoryAddr, Expr *filterExpr, bool isShuffleExchangeBuildPlan, OverflowConfig *overflowConfig)
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
        filterExpr, probeTypes.GetSize(), isShuffleExchangeBuildPlan, overflowConfig);
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
      lookupJoinOperator(lookupJoinOperator)
{
    SetOperatorName(opNameForLookUpJoin);
}

LookupJoinWithExprOperator::~LookupJoinWithExprOperator()
{
    delete lookupJoinOperator;
}

int32_t LookupJoinWithExprOperator::AddInput(VectorBatch *vecBatch)
{
    if (vecBatch->GetRowCount() <= 0) {
        VectorHelper::FreeVecBatch(vecBatch);
        ResetInputVecBatch();
        return 0;
    }
    auto *newInputVecBatch = OperatorUtil::ProjectVectors(vecBatch, probeTypes, projections, executionContext.get());
    VectorHelper::FreeVecBatch(vecBatch);
    ResetInputVecBatch();

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

BlockingReason LookupJoinWithExprOperator::IsBlocked(ContinueFuture* future)
{
    return lookupJoinOperator->IsBlocked(future);
}

OmniStatus LookupJoinWithExprOperator::Close()
{
    lookupJoinOperator->Close();
    return OMNI_STATUS_NORMAL;
}
}
}