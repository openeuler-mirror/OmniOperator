#include "hash_builder.h"
#include "join_hash_table.h"
#include <vector>
#include <cstring>

NativeOmniHashBuilderOperatorFactory::NativeOmniHashBuilderOperatorFactory(
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

NativeOmniHashBuilderOperatorFactory::~NativeOmniHashBuilderOperatorFactory()
{
    delete[] buildTypes;
    delete[] buildOutputCols;
    delete[] buildHashCols;
}

NativeOmniHashBuilderOperatorFactory *NativeOmniHashBuilderOperatorFactory::createNativeOmniHashBuilderOperatorFactory(
        int32_t *buildTypes,
        int32_t buildTypesCount,
        int32_t *buildOutputCols,
        int32_t buildOutputColsCount,
        int32_t *buildHashCols,
        int32_t buildHashColsCount,
        int32_t operatorCount)
{
    NativeOmniHashBuilderOperatorFactory *operatorFactory = new NativeOmniHashBuilderOperatorFactory(
        buildTypes,
        buildTypesCount,
        buildOutputCols,
        buildOutputColsCount,
        buildHashCols,
        buildHashColsCount,
        operatorCount);
    return operatorFactory;
}

NativeOmniOperator *NativeOmniHashBuilderOperatorFactory::createOmniOperator()
{
    int32_t partitionIndex = operatorIndex++;
    NativeOmniHashBuilderOperator *hashBuilderOperator = new NativeOmniHashBuilderOperator(
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

NativeOmniHashBuilderOperator::NativeOmniHashBuilderOperator(
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
    this->buildOutputCols = buildOutputCols;
    this->buildOutputColsCount = buildOutputColsCount;
    this->buildHashCols = buildHashCols;
    this->buildHashColsCount = buildHashColsCount;
    this->hashTables = hashTables;
    this->partitionIndex = partitionIndex;
    this->pagesIndex = new PagesIndex(buildTypes, buildTypesCount);
}

NativeOmniHashBuilderOperator::~NativeOmniHashBuilderOperator()
{

}

int32_t NativeOmniHashBuilderOperator::addInput(Table **datas, int32_t *rowCounts, int32_t tableCount)
{
    if (tableCount <= 0) {
        return 0;
    }

    // add tables into PagesIndex
    pagesIndex->addTables(datas, rowCounts, tableCount);

    // build JoinHashTable
    PagesHashStrategy *pagesHashStrategy = new PagesHashStrategy(pagesIndex->getColumns(),
        tableCount, pagesIndex->getTypesCount(), buildHashCols, buildHashColsCount);
    JoinHashTable *table = new JoinHashTable(pagesHashStrategy, pagesIndex->getValueAddresses(), pagesIndex->getPositionCount());

    hashTables->addHashTable(partitionIndex, table);
    return 0;
}

int32_t NativeOmniHashBuilderOperator::getOutput(std::vector<Table *>& outputTables)
{
}

int32_t *NativeOmniHashBuilderOperator::getSourceTypes()
{}
