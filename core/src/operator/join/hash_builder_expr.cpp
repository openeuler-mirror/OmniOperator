/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */

#include "hash_builder_expr.h"
#include <memory>
#include <utility>
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

HashBuilderWithExprOperatorFactory *HashBuilderWithExprOperatorFactory::CreateHashBuilderWithExprOperatorFactory(
    const type::DataTypes &buildTypes, const std::vector<omniruntime::expressions::Expr *> &buildHashKeys,
    int32_t buildHashKeysCount, int32_t hashTableCount, OverflowConfig *overflowConfig)
{
    return new HashBuilderWithExprOperatorFactory(buildTypes, buildHashKeys, buildHashKeysCount, hashTableCount,
        overflowConfig);
}

HashBuilderWithExprOperatorFactory::HashBuilderWithExprOperatorFactory(const type::DataTypes &buildTypes,
    const std::vector<omniruntime::expressions::Expr *> &buildHashKeys, int32_t buildHashKeysCount,
    int32_t hashTableCount, OverflowConfig *overflowConfig)
{
    std::vector<DataTypePtr> newBuildTypes;
    OperatorUtil::CreateProjectFuncs(buildTypes, buildHashKeys, buildHashKeysCount, newBuildTypes, this->projections,
        this->buildHashCols, this->projectFuncs, overflowConfig);
    this->buildTypes = std::make_unique<DataTypes>(newBuildTypes);
    this->operatorFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(*(this->buildTypes),
        this->buildHashCols.data(), buildHashKeysCount, hashTableCount);
}

HashBuilderWithExprOperatorFactory::~HashBuilderWithExprOperatorFactory()
{
    delete this->operatorFactory;
}

Operator *HashBuilderWithExprOperatorFactory::CreateOperator()
{
    auto hashBuilderOperator = static_cast<HashBuilderOperator *>(operatorFactory->CreateOperator());
    return new HashBuilderWithExprOperator(*buildTypes, buildHashCols, projectFuncs, hashBuilderOperator);
}

HashBuilderWithExprOperator::HashBuilderWithExprOperator(const DataTypes &buildTypes,
    const std::vector<int32_t> &buildHashCols, const std::vector<ProjFunc> &projectFuncs,
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

int32_t HashBuilderWithExprOperator::AddInput(VectorBatch *vecBatch)
{
    VectorBatch *newInputVecBatch = OperatorUtil::ProjectVectors(vecBatch, buildTypes, projectFuncs, buildHashCols);
    if (newInputVecBatch != nullptr) {
        hashBuilderOperator->AddInput(newInputVecBatch);
        VectorHelper::FreeVecBatch(vecBatch);
    } else {
        hashBuilderOperator->AddInput(vecBatch);
    }
    return 0;
}

int32_t HashBuilderWithExprOperator::GetOutput(VectorBatch **outputVecBatch)
{
    hashBuilderOperator->GetOutput(outputVecBatch);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus HashBuilderWithExprOperator::Close()
{
    hashBuilderOperator->Close();
    return OMNI_STATUS_NORMAL;
}
}
}