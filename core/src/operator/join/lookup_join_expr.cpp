/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */

#include "lookup_join_expr.h"
#include "hash_builder_expr.h"
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

LookupJoinWithExprOperatorFactory *LookupJoinWithExprOperatorFactory::CreateLookupJoinWithExprOperatorFactory(
    const type::DataTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &probeHashKeys, int32_t probeHashKeysCount,
    int32_t *buildOutputCols, const type::DataTypes &buildOutputTypes, JoinType joinType,
    int64_t hashBuilderFactoryAddr)
{
    auto operatorFactory = new LookupJoinWithExprOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashKeys, probeHashKeysCount, buildOutputCols, buildOutputTypes, joinType, hashBuilderFactoryAddr);
    return operatorFactory;
}

LookupJoinWithExprOperatorFactory::LookupJoinWithExprOperatorFactory(const type::DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &probeHashKeys, int32_t probeHashKeysCount,
    int32_t *buildOutputCols, const type::DataTypes &buildOutputTypes, JoinType joinType,
    int64_t hashBuilderFactoryAddr)
{
    std::vector<DataTypeRawPtr> newProbeTypes;
    OperatorUtil::CreateProjectFuncs(probeTypes, probeHashKeys, probeHashKeysCount, newProbeTypes, this->rowProjections,
        this->probeHashCols, this->projectFuncs);
    this->probeTypes = std::make_unique<DataTypes>(newProbeTypes);
    auto hashBuilderWithExprOperatorFactory =
        reinterpret_cast<HashBuilderWithExprOperatorFactory *>(hashBuilderFactoryAddr);
    this->operatorFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(*(this->probeTypes.get()),
        probeOutputCols, probeOutputColsCount, this->probeHashCols.data(), probeHashKeysCount, buildOutputCols,
        buildOutputTypes, joinType, (int64_t)(hashBuilderWithExprOperatorFactory->GetHashBuilderOperatorFactory()));
}

LookupJoinWithExprOperatorFactory::~LookupJoinWithExprOperatorFactory()
{
    delete this->operatorFactory;
}

Operator *LookupJoinWithExprOperatorFactory::CreateOperator()
{
    auto lookupJoinOperator = static_cast<LookupJoinOperator *>(operatorFactory->CreateOperator());
    return new LookupJoinWithExprOperator(*(probeTypes.get()), probeHashCols, projectFuncs, lookupJoinOperator);
}

LookupJoinWithExprOperator::LookupJoinWithExprOperator(const type::DataTypes &probeTypes,
    std::vector<int32_t> &probeHashCols, std::vector<RowProjFunc> &projectFuncs, LookupJoinOperator *lookupJoinOperator)
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
    return 0;
}

int32_t LookupJoinWithExprOperator::GetOutput(std::vector<VectorBatch *> &outputPages)
{
    lookupJoinOperator->GetOutput(outputPages);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus LookupJoinWithExprOperator::Close()
{
    lookupJoinOperator->Close();
    return OMNI_STATUS_NORMAL;
}
}
}