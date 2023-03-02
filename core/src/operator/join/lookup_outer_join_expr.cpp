/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: lookup outer join implementations
 */

#include "lookup_outer_join_expr.h"
#include "hash_builder_expr.h"
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

LookupOuterJoinWithExprOperatorFactory *
LookupOuterJoinWithExprOperatorFactory::CreateLookupOuterJoinWithExprOperatorFactory(const type::DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &probeHashKeys, int32_t probeHashKeysCount,
    int32_t *buildOutputCols, const type::DataTypes &buildOutputTypes, int64_t hashBuilderFactoryAddr)
{
    auto operatorFactory = new LookupOuterJoinWithExprOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashKeys, probeHashKeysCount, buildOutputCols, buildOutputTypes, hashBuilderFactoryAddr);
    return operatorFactory;
}

LookupOuterJoinWithExprOperatorFactory::LookupOuterJoinWithExprOperatorFactory(const type::DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &probeHashKeys, int32_t probeHashKeysCount,
    int32_t *buildOutputCols, const type::DataTypes &buildOutputTypes, int64_t hashBuilderFactoryAddr)
{
    std::vector<DataTypePtr> newProbeTypes;
    OperatorUtil::CreateProjectFuncs(probeTypes, probeHashKeys, probeHashKeysCount, newProbeTypes, this->projections,
        this->probeHashCols, this->projectFuncs, nullptr);
    this->probeTypes = std::make_unique<DataTypes>(DataTypes(newProbeTypes));
    auto hashBuilderWithExprOperatorFactory =
        reinterpret_cast<HashBuilderWithExprOperatorFactory *>(hashBuilderFactoryAddr);
    this->operatorFactory = LookupOuterJoinOperatorFactory::CreateLookupOuterJoinOperatorFactory(*(this->probeTypes),
        probeOutputCols, probeOutputColsCount, buildOutputCols, buildOutputTypes,
        (int64_t)(hashBuilderWithExprOperatorFactory->GetHashBuilderOperatorFactory()));
}

LookupOuterJoinWithExprOperatorFactory::~LookupOuterJoinWithExprOperatorFactory()
{
    delete this->operatorFactory;
}

Operator *LookupOuterJoinWithExprOperatorFactory::CreateOperator()
{
    auto lookupOuterJoinOperator = static_cast<LookupOuterJoinOperator *>(operatorFactory->CreateOperator());
    return new LookupOuterJoinWithExprOperator(lookupOuterJoinOperator);
}

LookupOuterJoinWithExprOperator::LookupOuterJoinWithExprOperator(LookupOuterJoinOperator *lookupJoinOperator)
    : lookupOuterJoinOperator(lookupJoinOperator)
{
    this->outputTypes = lookupJoinOperator->GetOutputType();
}

LookupOuterJoinWithExprOperator::~LookupOuterJoinWithExprOperator()
{
    delete lookupOuterJoinOperator;
}

int32_t LookupOuterJoinWithExprOperator::AddInput(VectorBatch *vecBatch)
{
    // since LookupOuterJoinOperator do nothing in AddInput
    return 0;
}

int32_t LookupOuterJoinWithExprOperator::GetOutput(VectorBatch **outputVecBatch)
{
    lookupOuterJoinOperator->GetOutput(outputVecBatch);
    SetStatus(lookupOuterJoinOperator->GetStatus());
    return 0;
}

OmniStatus LookupOuterJoinWithExprOperator::Close()
{
    lookupOuterJoinOperator->Close();
    return OMNI_STATUS_NORMAL;
}
}
}