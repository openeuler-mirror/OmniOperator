/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */

#include "hash_builder_expr.h"
#include <memory>
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

HashBuilderWithExprOperatorFactory *HashBuilderWithExprOperatorFactory::CreateHashBuilderWithExprOperatorFactory(
    const DataTypes &buildTypes, const std::vector<omniruntime::expressions::Expr *> &buildHashKeys,
    int32_t buildHashKeysCount, std::string &filter, int32_t hashTableCount)
{
    return new HashBuilderWithExprOperatorFactory(buildTypes, buildHashKeys, buildHashKeysCount, filter,
        hashTableCount);
}


HashBuilderWithExprOperatorFactory::HashBuilderWithExprOperatorFactory(const DataTypes &buildTypes,
    const std::vector<omniruntime::expressions::Expr *> &buildHashKeys, int32_t buildHashKeysCount, std::string &filter,
    int32_t hashTableCount)
{
    std::vector<DataType> newBuildTypes;
    OperatorUtil::CreateProjectFuncs(buildTypes, buildHashKeys, buildHashKeysCount, newBuildTypes, this->rowProjections,
        this->buildHashCols, this->projectFuncs);
    this->buildTypes = std::make_unique<DataTypes>(newBuildTypes);
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
    return new HashBuilderWithExprOperator(*(buildTypes.get()), buildHashCols, projectFuncs, hashBuilderOperator);
}

HashBuilderWithExprOperator::HashBuilderWithExprOperator(const DataTypes &buildTypes,
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

int32_t HashBuilderWithExprOperator::AddInput(VectorBatch *vecBatch)
{
    VectorBatch *newInputVecBatch = OperatorUtil::ProjectVectors(vecBatch, buildTypes, projectFuncs, buildHashCols,
                                                                 vecAllocator);
    if (newInputVecBatch != nullptr) {
        hashBuilderOperator->AddInput(newInputVecBatch);
        VectorHelper::FreeVecBatch(vecBatch);
    } else {
        hashBuilderOperator->AddInput(vecBatch);
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
    hashBuilderOperator->Close();
    return OMNI_STATUS_NORMAL;
}
}
}