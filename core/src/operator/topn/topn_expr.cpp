/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include "topn_expr.h"
#include "../util/operator_util.h"
#include "../../vector/vector_helper.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

TopNWithExprOperatorFactory::TopNWithExprOperatorFactory(const vec::VecTypes &sourceVecTypes, int32_t n,
    std::string *sortKeys, int32_t *sortAsc, int32_t *sortNullFirsts, int32_t sortKeyCount)
{
    std::vector<VecType> newSourceTypes;
    OperatorUtil::CreateProjectFuncs(sourceVecTypes, sortKeys, sortKeyCount, newSourceTypes,
        this->rowProjections, this->sortCols, this->projectFuncs);

    this->sourceTypes = std::make_unique<VecTypes>(newSourceTypes);
    this->topNOperatorFactory = std::make_unique<TopNOperatorFactory>(*(this->sourceTypes.get()), n,
        this->sortCols.data(), sortAsc, sortNullFirsts, sortKeyCount);
}

TopNWithExprOperatorFactory::~TopNWithExprOperatorFactory()
{}

Operator *TopNWithExprOperatorFactory::CreateOperator()
{
    auto topNOperator = static_cast<TopNOperator *>(topNOperatorFactory->CreateOperator());
    auto pOperator = std::make_unique<TopNWithExprOperator>(*(sourceTypes.get()), sortCols,
        projectFuncs, topNOperator);
    return pOperator.release();
}

TopNWithExprOperator::TopNWithExprOperator(const vec::VecTypes &sourceTypes, std::vector<int32_t> &sortCols,
    std::vector<RowProjFunc> &projectFuncs, TopNOperator *topNOperator)
    : sourceTypes(sourceTypes), sortCols(sortCols), projectFuncs(projectFuncs), topNOperator(topNOperator)
{}

TopNWithExprOperator::~TopNWithExprOperator()
{
    delete topNOperator;
}

int32_t TopNWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    VectorBatch *newInputVecBatch = OperatorUtil::ProjectVectors(inputVecBatch, sourceTypes, projectFuncs, sortCols);
    if (newInputVecBatch != nullptr) {
        topNOperator->AddInput(newInputVecBatch);
        inputVecBatches.push_back(newInputVecBatch);
    } else {
        topNOperator->AddInput(inputVecBatch);
    }
    return 0;
}

int32_t TopNWithExprOperator::GetOutput(std::vector<VectorBatch *> &outputVecBatches)
{
    topNOperator->GetOutput(outputVecBatches);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus TopNWithExprOperator::Close()
{
    VectorHelper::FreeVecBatches(inputVecBatches);
    return OMNI_STATUS_NORMAL;
}
}
}

