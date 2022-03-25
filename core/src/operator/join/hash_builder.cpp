/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash builder implementations
 */
#include "hash_builder.h"
#include <vector>
#include <memory>
#include "join_hash_table.h"
#include "operator/pages_hash_strategy.h"

namespace omniruntime {
namespace op {
HashBuilderOperatorFactory::HashBuilderOperatorFactory(const type::DataTypes &buildTypes, const int32_t *buildHashCols,
    int32_t buildHashColsCount, std::string &filterExpr, int32_t operatorCount)
    : hashTableCount(operatorCount), operatorIndex(0)
{
    this->buildTypes = std::make_unique<type::DataTypes>(buildTypes);
    this->buildHashCols.insert(this->buildHashCols.end(), buildHashCols, buildHashCols + buildHashColsCount);
    this->hashTables = new JoinHashTables(operatorCount);
    this->hashTables->SetBuildTypes(this->buildTypes.get());
    this->hashTables->SetFilterExpression(filterExpr);
}

HashBuilderOperatorFactory::~HashBuilderOperatorFactory()
{
    delete this->hashTables;
}

HashBuilderOperatorFactory *HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(
    const type::DataTypes &dataTypes, const int32_t *buildHashCols, int32_t buildHashColsCount, std::string &filterExpr,
    int32_t operatorCount)
{
    return new HashBuilderOperatorFactory(dataTypes, buildHashCols, buildHashColsCount, filterExpr, operatorCount);
}

Operator *HashBuilderOperatorFactory::CreateOperator()
{
    type::DataTypes &buildTypesRef = *(buildTypes.get());
    std::unique_ptr<PagesIndex> pagesIndex = std::make_unique<PagesIndex>(buildTypesRef);
    int32_t partitionIndex = operatorIndex++ % hashTables->GetHashTableCount();

    return new HashBuilderOperator(buildTypesRef, buildHashCols, hashTables, partitionIndex, pagesIndex);
}

HashBuilderOperator::HashBuilderOperator(const type::DataTypes &buildTypes, std::vector<int32_t> &buildHashCols,
    JoinHashTables *hashTables, int32_t partitionIndex, std::unique_ptr<PagesIndex> &pagesIndex)
    : buildTypes(buildTypes),
      buildHashCols(buildHashCols),
      hashTables(hashTables),
      partitionIndex(partitionIndex),
      pagesIndex(std::move(pagesIndex))
{}

HashBuilderOperator::~HashBuilderOperator()
{
    delete hashTables->GetHashTable(partitionIndex);
}

int32_t HashBuilderOperator::AddInput(omniruntime::vec::VectorBatch *vecBatch)
{
    inputVecBatches.push_back(vecBatch);
    return 0;
}

int32_t HashBuilderOperator::GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages)
{
    // add vecBatches into PagesIndex
    pagesIndex->AddVecBatches(inputVecBatches);

    // build JoinHashTable
    auto pagesHashStrategy = new PagesHashStrategy(pagesIndex->GetColumns(), buildTypes.GetIds(), buildTypes.GetSize(),
        buildHashCols.data(), buildHashCols.size());
    auto joinHashTable =
        new JoinHashTable(pagesHashStrategy, pagesIndex->GetValueAddresses(), pagesIndex->GetPositionCount());
    hashTables->AddHashTable(partitionIndex, joinHashTable);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus HashBuilderOperator::Close()
{
    VectorHelper::FreeVecBatches(inputVecBatches);
    return OMNI_STATUS_NORMAL;
}
} // end of op
} // end of omniruntime