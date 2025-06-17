/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * @Description: lookup outer join implementations
 */

#include "hash_builder.h"
#include "nest_loop_join_lookup_wrapper.h"

namespace omniruntime {
namespace op {
NestLoopJoinLookupWrapperOperatorFactory::NestLoopJoinLookupWrapperOperatorFactory(NestLoopJoinLookupOperatorFactory &nestLoopJoinLookupOperatorFactory)
    : nestLoopJoinLookupOperatorFactory(&nestLoopJoinLookupOperatorFactory) {}

NestLoopJoinLookupWrapperOperatorFactory::~NestLoopJoinLookupWrapperOperatorFactory()
{
    if (nestLoopJoinLookupOperatorFactory != nullptr) {
        delete nestLoopJoinLookupOperatorFactory;
        nestLoopJoinLookupOperatorFactory = nullptr;
    }
}

NestLoopJoinLookupWrapperOperatorFactory *NestLoopJoinLookupWrapperOperatorFactory::CreateNestLoopJoinLookupWrapperOperatorFactory(std::shared_ptr<const NestedLoopJoinNode> planNode,
    NestedLoopJoinBuildOperatorFactory* builderOperatorFactory, const config::QueryConfig& queryConfig)
{
    auto nestLoopJoinLookupOperatorFactory = NestLoopJoinLookupOperatorFactory::CreateNestLoopJoinLookupOperatorFactory(planNode, builderOperatorFactory, queryConfig);
    return new NestLoopJoinLookupWrapperOperatorFactory(*nestLoopJoinLookupOperatorFactory);
}

Operator *NestLoopJoinLookupWrapperOperatorFactory::CreateOperator()
{
    auto pNestLoopJoinLookupWrapperOperator = new NestLoopJoinLookupWrapperOperator(*nestLoopJoinLookupOperatorFactory);
    return pNestLoopJoinLookupWrapperOperator;
}

NestLoopJoinLookupWrapperOperator::NestLoopJoinLookupWrapperOperator(NestLoopJoinLookupOperatorFactory &nestLoopJoinLookupOperatorFactory)
    : nestLoopJoinLookupOperator(nullptr), nestLoopJoinLookupOperatorFactory(&nestLoopJoinLookupOperatorFactory) {}

NestLoopJoinLookupWrapperOperator::~NestLoopJoinLookupWrapperOperator()
{
    if (nestLoopJoinLookupOperator != nullptr) {
        delete nestLoopJoinLookupOperator;
        nestLoopJoinLookupOperator = nullptr;
    }
}

void NestLoopJoinLookupWrapperOperator::InitOperator()
{
    this->nestLoopJoinLookupOperator = dynamic_cast<NestLoopJoinLookupOperator*>(nestLoopJoinLookupOperatorFactory->CreateOperator());
    this->nestLoopJoinLookupOperator->setNoMoreInput(false);
}

int32_t NestLoopJoinLookupWrapperOperator::AddInput(VectorBatch *vecBatch)
{
    if (this->nestLoopJoinLookupOperator == nullptr) {
        InitOperator();
    }

    auto result = nestLoopJoinLookupOperator->AddInput(vecBatch);
    // set operator pipeline status
    SetStatus(OMNI_STATUS_NORMAL);
    return result;
}

int32_t NestLoopJoinLookupWrapperOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (this->isFinished()) {
        return 0;
    }

    if (this->nestLoopJoinLookupOperator == nullptr) {
        if (noMoreInput_) {
            SetStatus(OMNI_STATUS_FINISHED);
        }
        return 0;
    }

    if (!nestLoopJoinLookupOperator->isFinished()) {
        auto result = nestLoopJoinLookupOperator->GetOutput(outputVecBatch);
        if (nestLoopJoinLookupOperator->isFinished()) {
            SetStatus(OMNI_STATUS_FINISHED);
        }
        return result;
    }

    return 0;
}

OmniStatus NestLoopJoinLookupWrapperOperator::Close()
{
    if (nestLoopJoinLookupOperator != nullptr) {
        nestLoopJoinLookupOperator->Close();
        delete nestLoopJoinLookupOperator;
        nestLoopJoinLookupOperator = nullptr;
    }
    return OMNI_STATUS_NORMAL;
}

}
}
