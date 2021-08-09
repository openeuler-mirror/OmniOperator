/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#include "hash_builder.h"
#include <vector>
#include <memory>
#include "join_hash_table.h"
#include "../pages_hash_strategy.h"
#include "../util/operator_util.h"

namespace omniruntime {
namespace op {
HashBuilderOperatorFactory::HashBuilderOperatorFactory(const int32_t *buildTypes, int32_t buildTypesCount,
    const int32_t *buildOutputCols, int32_t buildOutputColsCount, const int32_t *buildHashCols,
    int32_t buildHashColsCount, int32_t operatorCount)
{
    this->buildTypes.insert(this->buildTypes.end(), buildTypes, buildTypes + buildTypesCount);
    this->buildOutputCols.insert(this->buildOutputCols.end(), buildOutputCols, buildOutputCols + buildOutputColsCount);
    this->buildHashCols.insert(this->buildHashCols.end(), buildHashCols, buildHashCols + buildHashColsCount);
    this->hashTables = nullptr;
    this->hashTableCount = operatorCount;
    this->operatorIndex = 0;
}

int32_t HashBuilderOperatorFactory::Init()
{
    this->hashTables = std::make_unique<JoinHashTables>(this->hashTableCount).release();
    return 0;
}

HashBuilderOperatorFactory::~HashBuilderOperatorFactory() {}

HashBuilderOperatorFactory *HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(const int32_t *buildTypes,
    int32_t buildTypesCount, const int32_t *buildOutputCols, int32_t buildOutputColsCount, const int32_t *buildHashCols,
    int32_t buildHashColsCount, int32_t operatorCount)
{
    auto pOperatorFactory = std::make_unique<HashBuilderOperatorFactory>(buildTypes, buildTypesCount, buildOutputCols,
        buildOutputColsCount, buildHashCols, buildHashColsCount, operatorCount);
    pOperatorFactory->Init();
    return pOperatorFactory.release();
}

Operator *HashBuilderOperatorFactory::CreateOperator()
{
    int32_t buildTypesCount = buildTypes.size();
    std::unique_ptr<PagesIndex> pagesIndex = std::make_unique<PagesIndex>(&buildTypes[0], buildTypesCount);
    int32_t partitionIndex = operatorIndex++ % hashTables->GetHashTableCount();

    auto pHashBuilderOperator = std::make_unique<HashBuilderOperator>(buildTypes, buildOutputCols, buildHashCols,
        hashTables, partitionIndex, pagesIndex);
    return pHashBuilderOperator.release();
}

HashBuilderOperator::HashBuilderOperator(std::vector<int32_t> &buildTypes, const std::vector<int32_t> &buildOutputCols,
    std::vector<int32_t> &buildHashCols, JoinHashTables *hashTables, int32_t partitionIndex,
    std::unique_ptr<PagesIndex> &pagesIndex)
{
    this->buildTypes = buildTypes;
    this->buildHashCols = buildHashCols;
    this->hashTables = hashTables;
    this->partitionIndex = partitionIndex;
    this->pagesIndex = std::move(pagesIndex);
}

HashBuilderOperator::~HashBuilderOperator() {}

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
    PagesHashStrategy *pagesHashStrategy = std::make_unique<PagesHashStrategy>(pagesIndex->GetColumns(), &buildTypes[0],
        buildTypes.size(), &buildHashCols[0], buildHashCols.size()).release();
    JoinHashTable *joinHashTable = std::make_unique<JoinHashTable>(pagesHashStrategy, pagesIndex->GetValueAddresses(),
        pagesIndex->GetPositionCount()).release();
    hashTables->AddHashTable(partitionIndex, joinHashTable);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

int32_t *HashBuilderOperator::GetSourceTypes()
{
    return buildTypes.data();
}

OmniStatus HashBuilderOperator::Close()
{
    hashTables->Clear(partitionIndex);
    return OMNI_STATUS_NORMAL;
}
} // end of op
} // end of omniruntime