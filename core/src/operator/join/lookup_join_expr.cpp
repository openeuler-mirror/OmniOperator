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
    int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes, JoinType joinType,
    int64_t hashBuilderFactoryAddr, OverflowConfig *overflowConfig)
{
    auto operatorFactory = new LookupJoinWithExprOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashKeys, probeHashKeysCount, buildOutputCols, buildOutputColsCount, buildOutputTypes, joinType,
        hashBuilderFactoryAddr, overflowConfig);
    return operatorFactory;
}

LookupJoinWithExprOperatorFactory::LookupJoinWithExprOperatorFactory(const DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &probeHashKeys, int32_t probeHashKeysCount,
    int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes, JoinType joinType,
    int64_t hashBuilderFactoryAddr, OverflowConfig *overflowConfig)
{
    std::vector<DataTypePtr> newProbeTypes;
    OperatorUtil::CreateProjectFuncs(probeTypes, probeHashKeys, probeHashKeysCount, newProbeTypes, this->projections,
        this->probeHashCols, this->projectFuncs, overflowConfig);
    this->probeTypes = std::make_unique<DataTypes>(newProbeTypes);
    auto hashBuilderWithExprOperatorFactory =
        reinterpret_cast<HashBuilderWithExprOperatorFactory *>(hashBuilderFactoryAddr);
    this->operatorFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(*(this->probeTypes.get()),
        probeOutputCols, probeOutputColsCount, this->probeHashCols.data(), probeHashKeysCount, buildOutputCols,
        buildOutputColsCount, std::move(buildOutputTypes), joinType,
        (int64_t)(hashBuilderWithExprOperatorFactory->GetHashBuilderOperatorFactory()), probeTypes.GetSize(),
        overflowConfig);
}

LookupJoinWithExprOperatorFactory::~LookupJoinWithExprOperatorFactory()
{
    delete this->operatorFactory;
}

Operator *LookupJoinWithExprOperatorFactory::CreateOperator()
{
    auto lookupJoinOperator = static_cast<LookupJoinOperator *>(operatorFactory->CreateOperator());
    return new LookupJoinWithExprOperator(*probeTypes, probeHashCols, projectFuncs, lookupJoinOperator);
}

LookupJoinWithExprOperator::LookupJoinWithExprOperator(const type::DataTypes &probeTypes,
    std::vector<int32_t> &probeHashCols, std::vector<ProjFunc> &projectFuncs, LookupJoinOperator *lookupJoinOperator)
    : probeTypes(probeTypes),
      probeHashCols(probeHashCols),
      projectFuncs(projectFuncs),
      lookupJoinOperator(lookupJoinOperator)
{}

LookupJoinWithExprOperator::~LookupJoinWithExprOperator()
{
    delete lookupJoinOperator;
}

int32_t LookupJoinWithExprOperator::AddInput(VectorBatch *vecBatch)
{
    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectVectors(vecBatch, probeTypes, projectFuncs, probeHashCols, vecAllocator);
    if (newInputVecBatch != nullptr) {
        lookupJoinOperator->AddInput(newInputVecBatch);
        VectorHelper::FreeVecBatch(vecBatch);
    } else {
        lookupJoinOperator->AddInput(vecBatch);
    }
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