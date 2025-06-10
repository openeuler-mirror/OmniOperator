/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * @Description: lookup outer join implementations
 */

#include "hash_builder.h"
#include "lookup_join_wrapper.h"

namespace omniruntime {
namespace op {
LookupJoinWrapperOperatorFactory::LookupJoinWrapperOperatorFactory(LookupJoinOperatorFactory &lookupJoinOperatorFactory,
    LookupOuterJoinOperatorFactory &lookupOuterJoinOperatorFactory, bool isNeedOuterJoin)
    : lookupJoinOperatorFactory(&lookupJoinOperatorFactory),
      lookupOuterJoinOperatorFactory(&lookupOuterJoinOperatorFactory),
      isNeedOuterJoin(isNeedOuterJoin) {}

LookupJoinWrapperOperatorFactory::~LookupJoinWrapperOperatorFactory()
{
    delete lookupOuterJoinOperatorFactory;
    delete lookupJoinOperatorFactory;
    lookupOuterJoinOperatorFactory = nullptr;
    lookupJoinOperatorFactory = nullptr;
}

LookupJoinWrapperOperatorFactory *LookupJoinWrapperOperatorFactory::CreateLookupJoinWrapperOperatorFactory(std::shared_ptr<const HashJoinNode> planNode,
    HashBuilderOperatorFactory* hashBuilderOperatorFactory, const config::QueryConfig& queryConfig)
{
    auto isNeedOuterJoin = planNode->IsFullJoin() || (planNode->IsLeftJoin() && planNode->IsBuildLeft()) || (planNode->IsRightJoin() && planNode->IsBuildRight());
    auto lookupJoinOperatorFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(planNode, hashBuilderOperatorFactory, queryConfig);
    auto lookupOuterJoinOperatorFactory = LookupOuterJoinOperatorFactory::CreateLookupOuterJoinOperatorFactory(planNode, hashBuilderOperatorFactory, queryConfig);
    return new LookupJoinWrapperOperatorFactory(*lookupJoinOperatorFactory, *lookupOuterJoinOperatorFactory, isNeedOuterJoin);
}

Operator *LookupJoinWrapperOperatorFactory::CreateOperator()
{
    auto lookupJoinOperator = (LookupJoinOperator*) lookupJoinOperatorFactory.CreateOperator();
    auto lookupOuterJoinOperator = (LookupOuterJoinOperator*) lookupOuterJoinOperatorFactory.CreateOperator();
    auto pLookupJoinWrapperOperator = new LookupJoinWrapperOperator(*lookupJoinOperator, *lookupOuterJoinOperator, isNeedOuterJoin);
    return pLookupJoinWrapperOperator;
}

LookupJoinWrapperOperator::LookupJoinWrapperOperator(LookupJoinOperator &lookupJoinOperator,
    LookupOuterJoinOperator &lookupOuterJoinOperator, bool isNeedOuterJoin)
    : lookupOuterJoinOperator(&lookupOuterJoinOperator),
      lookupJoinOperator(&lookupJoinOperator),
      isNeedOuterJoin(isNeedOuterJoin) {}

LookupJoinWrapperOperator::~LookupJoinWrapperOperator()
{
    delete lookupOuterJoinOperator;
    delete lookupJoinOperator;
    lookupOuterJoinOperator = nullptr;
    lookupJoinOperator = nullptr;
}

int32_t LookupJoinWrapperOperator::AddInput(VectorBatch *vecBatch)
{
    auto result = lookupJoinOperator->AddInput(vecBatch);
    // set operator pipeline status
    SetStatus(OMNI_STATUS_NORMAL);
    return result;
}

int32_t LookupJoinWrapperOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (this->isFinished()) {
        return 0;
    }

    if (lookupJoinOperator->GetStatus() != OMNI_STATUS_FINISHED) {
        return lookupJoinOperator->GetOutput(outputVecBatch);
    } else {
        if (isNeedOuterJoin) {
            // when lookup join operator end can execute
            auto result = lookupOuterJoinOperator->GetOutput(outputVecBatch);
            if (lookupOuterJoinOperator->GetStatus() == OMNI_STATUS_FINISHED) {
                SetStatus(OMNI_STATUS_FINISHED);
            }
            return result;
        } else {
            SetStatus(OMNI_STATUS_FINISHED);
            return 0;
        }
    }
}

OmniStatus LookupJoinWrapperOperator::Close()
{
    lookupJoinOperator->Close();
    lookupOuterJoinOperator->Close();
    return OMNI_STATUS_NORMAL;
}

}
}