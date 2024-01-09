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
HashBuilderOperatorFactory::HashBuilderOperatorFactory(const DataTypes &buildTypes, const int32_t *buildHashCols,
    int32_t buildHashColsCount, int32_t operatorCount)
    : buildTypes(buildTypes), hashTableCount(operatorCount), operatorIndex(0)
{
    if (operatorCount <= 0 || operatorCount > 10000) {
        throw OmniException("OPERATOR_RUNTIME_ERROR", "operatorCount is not in the acceptable range [1, 10000].");
    }
    this->buildHashCols.insert(this->buildHashCols.end(), buildHashCols, buildHashCols + buildHashColsCount);
    this->hashTables = new JoinHashTables(operatorCount);
    this->hashTables->SetBuildTypes(&(this->buildTypes));
}

HashBuilderOperatorFactory::~HashBuilderOperatorFactory()
{
    delete this->hashTables;
}

HashBuilderOperatorFactory *HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(const DataTypes &dataTypes,
    const int32_t *buildHashCols, int32_t buildHashColsCount, int32_t operatorCount)
{
    return new HashBuilderOperatorFactory(dataTypes, buildHashCols, buildHashColsCount, operatorCount);
}

Operator *HashBuilderOperatorFactory::CreateOperator()
{
    std::unique_ptr<PagesIndex> pagesIndex = std::make_unique<PagesIndex>(buildTypes);
    int32_t partitionIndex = operatorIndex++ % hashTables->GetHashTableCount();

    return new HashBuilderOperator(buildTypes, buildHashCols, hashTables, partitionIndex, pagesIndex);
}

HashBuilderOperator::HashBuilderOperator(const DataTypes &buildTypes, std::vector<int32_t> &buildHashCols,
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
    pagesIndex->AddVecBatch(vecBatch);
    return 0;
}

int32_t HashBuilderOperator::GetOutput(omniruntime::vec::VectorBatch **outputVecBatch)
{
    // add vecBatches into PagesIndex
    pagesIndex->Prepare();

    // build JoinHashTable
    auto pagesHashStrategy =
        new PagesHashStrategy(pagesIndex->GetColumns(), buildTypes, buildHashCols.data(), buildHashCols.size());
    auto joinHashTable =
        new JoinHashTable(pagesHashStrategy, pagesIndex->GetValueAddresses(), pagesIndex->GetRowCount());
    hashTables->AddHashTable(partitionIndex, joinHashTable);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus HashBuilderOperator::Close()
{
    pagesIndex->Clear();
    return OMNI_STATUS_NORMAL;
}
} // end of op
} // end of omniruntime