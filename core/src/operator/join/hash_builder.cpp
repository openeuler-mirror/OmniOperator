#include "hash_builder.h"
#include "join_hash_table.h"
#include "../pages_hash_strategy.h"
#include <vector>
#include <cstring>

namespace omniruntime {
namespace op {

HashBuilderOperatorFactory::HashBuilderOperatorFactory(
        int32_t *buildTypes,
        int32_t buildTypesCount,
        int32_t *buildOutputCols,
        int32_t buildOutputColsCount,
        int32_t *buildHashCols,
        int32_t buildHashColsCount,
        int32_t operatorCount)
{
    int32_t intByteLen = sizeof(int32_t);

    this->buildTypes = new int32_t[buildTypesCount];
    memcpy(this->buildTypes, buildTypes, buildTypesCount * intByteLen);
    this->buildTypesCount = buildTypesCount;

    this->buildOutputCols = new int32_t[buildOutputColsCount];
    memcpy(this->buildOutputCols, buildOutputCols, buildOutputColsCount * intByteLen);
    this->buildOutputColsCount = buildOutputColsCount;

    this->buildHashCols = new int32_t[buildHashColsCount];
    memcpy(this->buildHashCols, buildHashCols, buildHashColsCount * intByteLen);
    this->buildHashColsCount = buildHashColsCount;

    this->hashTables = new JoinHashTables(operatorCount);
    this->operatorIndex = 0;
}

HashBuilderOperatorFactory::~HashBuilderOperatorFactory()
{
    delete[] buildTypes;
    delete[] buildOutputCols;
    delete[] buildHashCols;
}

HashBuilderOperatorFactory *HashBuilderOperatorFactory::createHashBuilderOperatorFactory(
        int32_t *buildTypes,
        int32_t buildTypesCount,
        int32_t *buildOutputCols,
        int32_t buildOutputColsCount,
        int32_t *buildHashCols,
        int32_t buildHashColsCount,
        int32_t operatorCount)
{
    HashBuilderOperatorFactory *operatorFactory = new HashBuilderOperatorFactory(
        buildTypes,
        buildTypesCount,
        buildOutputCols,
        buildOutputColsCount,
        buildHashCols,
        buildHashColsCount,
        operatorCount);
    return operatorFactory;
}

Operator *HashBuilderOperatorFactory::createOperator()
{
    int32_t partitionIndex = operatorIndex++;
    HashBuilderOperator *hashBuilderOperator = new HashBuilderOperator(
        buildTypes,
        buildTypesCount,
        buildOutputCols,
        buildOutputColsCount,
        buildHashCols,
        buildHashColsCount,
        hashTables,
        partitionIndex);
    return hashBuilderOperator;
}

HashBuilderOperator::HashBuilderOperator(
        int32_t *buildTypes,
        int32_t buildTypesCount,
        int32_t *buildOutputCols,
        int32_t buildOutputColsCount,
        int32_t *buildHashCols,
        int32_t buildHashColsCount,
        JoinHashTables *hashTables,
        int32_t partitionIndex)
{
    this->buildTypes = buildTypes;
    this->buildTypesCount = buildTypesCount;
    this->buildHashCols = buildHashCols;
    this->buildHashColsCount = buildHashColsCount;
    this->hashTables = hashTables;
    this->partitionIndex = partitionIndex;
    this->pagesIndex = new PagesIndex(buildTypes, buildTypesCount);
}

HashBuilderOperator::~HashBuilderOperator()
{

}

int32_t HashBuilderOperator::addInput(VectorBatch *vecBatch)
{
    inputVecBatches.push_back(vecBatch);
    return 0;
}

int32_t HashBuilderOperator::getOutput(std::vector<VectorBatch *>& outputPages)
{
    // add vecBatches into PagesIndex
    pagesIndex->addVecBatches(inputVecBatches);

    // build JoinHashTable
    PagesHashStrategy *pagesHashStrategy = new PagesHashStrategy(pagesIndex->getColumns(),
        buildTypes, buildTypesCount, buildHashCols, buildHashColsCount);
    JoinHashTable *joinHashTable = new JoinHashTable(pagesHashStrategy, pagesIndex->getValueAddresses(), pagesIndex->getPositionCount());

    hashTables->addHashTable(partitionIndex, joinHashTable);
    return 0;
}

int32_t *HashBuilderOperator::getSourceTypes()
{
    return buildTypes;
}
} // end of op
} // end of omniruntime