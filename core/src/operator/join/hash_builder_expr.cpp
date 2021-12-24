/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */

#include "hash_builder_expr.h"
#include <memory>
#include "../util/operator_util.h"
#include "../../vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

HashBuilderWithExprOperatorFactory *HashBuilderWithExprOperatorFactory::CreateHashBuilderWithExprOperatorFactory(
    const VecTypes &buildTypes, const std::vector<omniruntime::expressions::Expr *> &buildHashKeys,
    int32_t buildHashKeysCount, std::string &filter, int32_t hashTableCount)
    {
    auto operatorFactory = std::make_unique<HashBuilderWithExprOperatorFactory>(buildTypes, buildHashKeys,
        buildHashKeysCount, filter, hashTableCount);
    return operatorFactory.release();
        }


HashBuilderWithExprOperatorFactory::HashBuilderWithExprOperatorFactory(const VecTypes &buildTypes,
    const std::vector<omniruntime::expressions::Expr *> &buildHashKeys, int32_t buildHashKeysCount,
    std::string &filter, int32_t hashTableCount)
{
    std::vector<VecType> newBuildTypes;
    OperatorUtil::CreateProjectFuncs(buildTypes, buildHashKeys, buildHashKeysCount, newBuildTypes,
                                     this->rowProjections, this->buildHashCols, this->projectFuncs);
    this->buildTypes = std::make_unique<VecTypes>(newBuildTypes);
    this->operatorFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(*(this->buildTypes.get()),
        this->buildHashCols.data(), buildHashKeysCount, filter, hashTableCount);
}

HashBuilderWithExprOperatorFactory::~HashBuilderWithExprOperatorFactory()
{
    delete this->operatorFactory;
}

Operator *HashBuilderWithExprOperatorFactory::CreateOperator()
{
    auto hashBuilderOperator = static_cast<HashBuilderOperator *>(operatorFactory->CreateOperator());
    auto hashBuilderWithExprOperator = std::make_unique<HashBuilderWithExprOperator>(*(buildTypes.get()),
        buildHashCols, projectFuncs, hashBuilderOperator);
    return hashBuilderWithExprOperator.release();
}

HashBuilderWithExprOperator::HashBuilderWithExprOperator(const VecTypes &buildTypes,
    const std::vector<int32_t> &buildHashCols, const std::vector<RowProjFunc> &projectFuncs,
    HashBuilderOperator *hashBuilderOperator)
    : buildTypes(buildTypes),
      buildHashCols(buildHashCols),
      projectFuncs(projectFuncs),
      hashBuilderOperator(hashBuilderOperator)
{}

HashBuilderWithExprOperator::~HashBuilderWithExprOperator()
{
    delete this->hashBuilderOperator;
}

int32_t HashBuilderWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectVectors(inputVecBatch, buildTypes, projectFuncs, buildHashCols);
    if (newInputVecBatch != nullptr) {
        hashBuilderOperator->AddInput(newInputVecBatch);
        inputVecBatches.push_back(newInputVecBatch);
    } else {
        hashBuilderOperator->AddInput(inputVecBatch);
    }
    return 0;
}

int32_t HashBuilderWithExprOperator::GetOutput(std::vector<VectorBatch *> &outputPages)
{
    hashBuilderOperator->GetOutput(outputPages);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus HashBuilderWithExprOperator::Close()
{
    VectorHelper::FreeVecBatches(inputVecBatches);
    return OMNI_STATUS_NORMAL;
}
}
}