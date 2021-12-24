/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */

#include "lookup_join_expr.h"
#include <memory>
#include "hash_builder_expr.h"
#include "lookup_join.h"
#include "../util/operator_util.h"
#include "../../vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

LookupJoinWithExprOperatorFactory *LookupJoinWithExprOperatorFactory::CreateLookupJoinWithExprOperatorFactory(
    const vec::VecTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &probeHashKeys, int32_t probeHashKeysCount,
    int32_t *buildOutputCols, const vec::VecTypes &buildOutputTypes, JoinType joinType, int64_t hashBuilderFactoryAddr)
{
    auto operatorFactory =
        std::make_unique<LookupJoinWithExprOperatorFactory>(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashKeys, probeHashKeysCount, buildOutputCols, buildOutputTypes, joinType, hashBuilderFactoryAddr);
    return operatorFactory.release();
}

LookupJoinWithExprOperatorFactory::LookupJoinWithExprOperatorFactory(const vec::VecTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &probeHashKeys, int32_t probeHashKeysCount,
    int32_t *buildOutputCols, const vec::VecTypes &buildOutputTypes, JoinType joinType, int64_t hashBuilderFactoryAddr)
{
    std::vector<VecType> newProbeTypes;
    OperatorUtil::CreateProjectFuncs(probeTypes, probeHashKeys, probeHashKeysCount, newProbeTypes, this->rowProjections,
                                     this->probeHashCols, this->projectFuncs);
    this->probeTypes = std::make_unique<VecTypes>(newProbeTypes);
    auto hashBuilderWithExprOperatorFactory =
            reinterpret_cast<HashBuilderWithExprOperatorFactory *>(hashBuilderFactoryAddr);
    this->operatorFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
        *(this->probeTypes.get()), probeOutputCols, probeOutputColsCount,
        this->probeHashCols.data(), probeHashKeysCount, buildOutputCols, buildOutputTypes, joinType,
        (int64_t)(hashBuilderWithExprOperatorFactory->GetHashBuilderOperatorFactory()));
}

LookupJoinWithExprOperatorFactory::~LookupJoinWithExprOperatorFactory()
{
    delete this->operatorFactory;
}

Operator *LookupJoinWithExprOperatorFactory::CreateOperator()
{
    auto lookupJoinOperator = static_cast<LookupJoinOperator *>(operatorFactory->CreateOperator());
    auto lookupJoinWithExprOperator = std::make_unique<LookupJoinWithExprOperator>(*(probeTypes.get()), probeHashCols,
        projectFuncs, lookupJoinOperator);
    return lookupJoinWithExprOperator.release();
}

LookupJoinWithExprOperator::LookupJoinWithExprOperator(const vec::VecTypes &probeTypes,
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

int32_t LookupJoinWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectVectors(inputVecBatch, probeTypes, projectFuncs, probeHashCols);
    if (newInputVecBatch != nullptr) {
        lookupJoinOperator->AddInput(newInputVecBatch);
        this->newInputVecBatch = newInputVecBatch;
    } else {
        lookupJoinOperator->AddInput(inputVecBatch);
        this->newInputVecBatch = nullptr;
    }
    return 0;
}

int32_t LookupJoinWithExprOperator::GetOutput(std::vector<VectorBatch *> &outputPages)
{
    lookupJoinOperator->GetOutput(outputPages);
    if (newInputVecBatch != nullptr) {
        VectorHelper::FreeVecBatch(newInputVecBatch);
    }
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus LookupJoinWithExprOperator::Close()
{
    return OMNI_STATUS_NORMAL;
}
}
}