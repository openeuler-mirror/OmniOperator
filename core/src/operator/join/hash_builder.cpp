/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash builder implementations
 */
#include "hash_builder.h"
#include <vector>
#include <memory>
#include "join_hash_table.h"
#include "../pages_hash_strategy.h"

namespace omniruntime {
namespace op {
HashBuilderOperatorFactory::HashBuilderOperatorFactory(const vec::VecTypes &buildTypes, const int32_t *buildHashCols,
    int32_t buildHashColsCount, std::string &filterExpr, int32_t operatorCount)
    : hashTableCount(operatorCount), operatorIndex(0)
{
    this->buildTypes = std::make_unique<vec::VecTypes>(buildTypes);
    this->buildHashCols.insert(this->buildHashCols.end(), buildHashCols, buildHashCols + buildHashColsCount);
    this->hashTables = std::make_unique<JoinHashTables>(this->hashTableCount).release();
    this->hashTables->SetBuildTypes(this->buildTypes.get());
    this->hashTables->SetFilterExpression(filterExpr);
}

HashBuilderOperatorFactory::~HashBuilderOperatorFactory()
{
    delete this->hashTables;
}

HashBuilderOperatorFactory *HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(
    const vec::VecTypes &buildTypes, const int32_t *buildHashCols, int32_t buildHashColsCount, std::string &filterExpr,
    int32_t operatorCount)
{
    auto pOperatorFactory = std::make_unique<HashBuilderOperatorFactory>(buildTypes, buildHashCols, buildHashColsCount,
        filterExpr, operatorCount);
    return pOperatorFactory.release();
}

Operator *HashBuilderOperatorFactory::CreateOperator()
{
    vec::VecTypes &buildTypesRef = *(buildTypes.get());
    std::unique_ptr<PagesIndex> pagesIndex = std::make_unique<PagesIndex>(buildTypesRef);
    int32_t partitionIndex = operatorIndex++ % hashTables->GetHashTableCount();

    auto pHashBuilderOperator =
        std::make_unique<HashBuilderOperator>(buildTypesRef, buildHashCols, hashTables, partitionIndex, pagesIndex);
    return pHashBuilderOperator.release();
}

HashBuilderOperator::HashBuilderOperator(const vec::VecTypes &buildTypes, std::vector<int32_t> &buildHashCols,
    JoinHashTables *hashTables, int32_t partitionIndex, std::unique_ptr<PagesIndex> &pagesIndex)
    : buildTypes(buildTypes)
{
    this->buildHashCols = buildHashCols;
    this->hashTables = hashTables;
    this->partitionIndex = partitionIndex;
    this->pagesIndex = std::move(pagesIndex);
}

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
    PagesHashStrategy *pagesHashStrategy = std::make_unique<PagesHashStrategy>(pagesIndex->GetColumns(),
        buildTypes.GetIds(), buildTypes.GetSize(), &buildHashCols[0], buildHashCols.size()).release();
    JoinHashTable *joinHashTable = std::make_unique<JoinHashTable>(pagesHashStrategy, pagesIndex->GetValueAddresses(),
        pagesIndex->GetPositionCount()).release();
    hashTables->AddHashTable(partitionIndex, joinHashTable);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

const int32_t *HashBuilderOperator::GetSourceTypes()
{
    return buildTypes.GetIds();
}
} // end of op
} // end of omniruntime