#include "lookup_join.h"
#include "hash_builder.h"
#include <vector>
#include <cstring>

namespace omniruntime {
namespace op {
LookupJoinOperatorFactory::LookupJoinOperatorFactory(
    int32_t *probeTypes,
    int32_t probeTypesCount,
    int32_t *probeOutputCols,
    int32_t probeOutputColsCount,
    int32_t *probeHashCols,
    int32_t probeHashColsCount,
    int32_t *buildOutputCols,
    int32_t *buildOutputTypes,
    int32_t buildOutputColsCount,
    JoinHashTables *hashTables)
{
    int32_t intByteLen = sizeof(int32_t);

    this->probeTypes = new int32_t[probeTypesCount];
    memcpy(this->probeTypes, probeTypes, probeTypesCount * intByteLen);
    this->probeTypesCount = probeTypesCount;

    this->probeOutputCols = new int32_t[probeOutputColsCount];
    memcpy(this->probeOutputCols, probeOutputCols, probeOutputColsCount * intByteLen);
    this->probeOutputColsCount = probeOutputColsCount;

    this->probeHashCols = new int32_t[probeHashColsCount];
    memcpy(this->probeHashCols, probeHashCols, probeHashColsCount * intByteLen);
    this->probeHashColsCount = probeHashColsCount;

    this->buildOutputCols = new int32_t[buildOutputColsCount];
    memcpy(this->buildOutputCols, buildOutputCols, buildOutputColsCount * intByteLen);
    this->buildOutputColsCount = buildOutputColsCount;

    this->buildOutputTypes = new int32_t[buildOutputColsCount];
    memcpy(this->buildOutputTypes, buildOutputTypes, buildOutputColsCount * intByteLen);

    this->hashTables = hashTables;

    // int32_t tablesCount = hashTables->getHashTableCount();
    // for (int32_t i = 0; i < tablesCount; i++) {
    //     JoinHashTable *hashTable = hashTables->getHashTable(i);
    //     int32_t keySize = hashTable->getPagesHash()->getKeySize();
    //     int32_t *key = hashTable->getPagesHash()->getKey();

    //     for (int32_t j = 0; j < keySize; j++) {
    //         std::cout << "partition=" << i << ", key[" << j << "]=" << key[j] << std::endl;
    //     }

    //     int32_t *links = hashTable->getPositionLinks()->getPositionLinks();
    //     int32_t linkSize = hashTable->getPositionLinks()->getPositionLinkSize();
    //     for (int32_t j = 0; j < linkSize; j++) {
    //         std::cout << "partition=" << i << ", links[" << j << "]=" << links[j] << std::endl;
    //     }
    // }
}

LookupJoinOperatorFactory::~LookupJoinOperatorFactory()
{
    delete[] probeTypes;
    delete[] probeOutputCols;
    delete[] probeHashCols;
    delete[] buildOutputCols;
    delete[] buildOutputTypes;
}

LookupJoinOperatorFactory *LookupJoinOperatorFactory::createLookupJoinOperatorFactory(
    int32_t *probeTypes,
    int32_t probeTypesCount,
    int32_t *probeOutputCols,
    int32_t probeOutputColsCount,
    int32_t *probeHashCols,
    int32_t probeHashColsCount,
    int32_t *buildOutputCols,
    int32_t *buildOutputTypes,
    int32_t buildOutputColsCount,
    int64_t hashBuilderFactoryAddr)
{
    HashBuilderOperatorFactory *hashBuilderFactory = (HashBuilderOperatorFactory *)hashBuilderFactoryAddr;
    JoinHashTables *hashTables = hashBuilderFactory->getHashTables();
    LookupJoinOperatorFactory *operatorFactory = new LookupJoinOperatorFactory(
        probeTypes,
        probeTypesCount,
        probeOutputCols,
        probeOutputColsCount,
        probeHashCols,
        probeHashColsCount,
        buildOutputCols,
        buildOutputTypes,
        buildOutputColsCount,
        hashTables);
    return operatorFactory;
}

Operator *LookupJoinOperatorFactory::createOperator()
{
    LookupJoinOperator *lookupJoinoperator = new LookupJoinOperator(
        probeTypes,
        probeTypesCount,
        probeOutputCols,
        probeOutputColsCount,
        probeHashCols,
        probeHashColsCount,
        buildOutputCols,
        buildOutputTypes,
        buildOutputColsCount,
        hashTables);
    return lookupJoinoperator;
}

LookupJoinOperator::LookupJoinOperator(
    int32_t *probeTypes,
    int32_t probeTypesCount,
    int32_t *probeOutputCols,
    int32_t probeOutputColsCount,
    int32_t *probeHashCols,
    int32_t probeHashColsCount,
    int32_t *buildOutputCols,
    int32_t *buildOutputTypes,
    int32_t buildOutputColsCount,
    JoinHashTables *hashTables)
{
    this->probeTypes = probeTypes;
    this->probeTypesCount = probeTypesCount;
    this->probeOutputCols = probeOutputCols;
    this->probeOutputColsCount = probeOutputColsCount;
    this->probeHashCols = probeHashCols;
    this->probeHashColsCount = probeHashColsCount;
    this->buildOutputCols = buildOutputCols;
    this->buildOutputTypes = buildOutputTypes;
    this->buildOutputColsCount = buildOutputColsCount;
    this->hashTables = hashTables;
    this->joinProbe = NULL;
    this->outputTable = NULL;
    this->partitionedJoinPosition = -1;
    this->outputBuilder = new LookupJoinOutputBuilder(probeOutputCols, probeOutputColsCount, buildOutputCols, buildOutputTypes, buildOutputColsCount);
}

LookupJoinOperator::~LookupJoinOperator()
{}

int32_t LookupJoinOperator::addInput(Table *data, int32_t rowCount)
{
    this->joinProbe = new JoinProbe(data, probeHashCols, probeHashColsCount, rowCount);
    this->partitionedJoinPosition = -1;

    // start probe
    processProbe();
    return 0;
}

int32_t LookupJoinOperator::addInput(Table **datas, int32_t *rowCounts, int32_t pageCount)
{
    return addInput(datas[0], rowCounts[0]);
}

int32_t LookupJoinOperator::getOutput(std::vector<Table *>& outputTables)
{
    if (outputTable != NULL) {
        Table *result = outputTable;
        outputTables.push_back(result);
        outputTable = NULL;
    }

    return 0;
}

int32_t *LookupJoinOperator::getSourceTypes()
{
    return probeTypes;
}

void LookupJoinOperator::processProbe()
{
    if (joinProbe == NULL) {
        return;
    }

    while (true) {
        if (joinProbe->getPosition() >= 0) {
            if (!joinCurrentPosition()) {
                break;
            }
        }

        // advance to next probe postition
        if (!advanceProbePosition()) {
            break;
        }
    }
}

bool LookupJoinOperator::joinCurrentPosition()
{
    while (partitionedJoinPosition >= 0) {
        // handle data of build
        outputBuilder->appendRow(joinProbe->getPosition(), partitionedJoinPosition);
        partitionedJoinPosition = getNextJoinPosition(partitionedJoinPosition, joinProbe->getPosition());
    }

    return true;
}

bool LookupJoinOperator::advanceProbePosition()
{
    if (!joinProbe->advanceNextPosition()) {
        // build output data
        outputTable = outputBuilder->buildOutput(joinProbe, hashTables);
        return false;
    }

    partitionedJoinPosition = joinProbe->getCurrentJoinPosition(hashTables);
    return true;
}

int64_t LookupJoinOperator::getNextJoinPosition(int64_t currentJoinPosition, int32_t probePosition)
{
    int64_t result = hashTables->getNextJoinPosition(currentJoinPosition, probePosition);
    return result;
}

JoinProbe::JoinProbe(Table *input, int32_t *hashCols, int32_t hashColsCount, int32_t positionCount)
{
    int32_t allColsCount = input->getColumnNumber();
    Column **allColumns = (Column **)malloc(allColsCount * sizeof(Column *));
    for (int32_t columnIdx = 0; columnIdx < allColsCount; columnIdx++) {
        allColumns[columnIdx] = input->getColumn(columnIdx);
    }

    this->probeAllColumns = allColumns;
    this->probeAllColsCount = allColsCount;
    this->positionCount = positionCount;
    this->probeHashColsCount = hashColsCount;
    this->probeHashColumns = (Column **)malloc(hashColsCount * sizeof(Column *));

    int32_t hashColumn;
    for (int32_t columnIdx = 0; columnIdx < hashColsCount; columnIdx++) {
        hashColumn = hashCols[columnIdx];
        probeHashColumns[columnIdx] = allColumns[hashColumn];
    }
    this->position = -1;
}

JoinProbe::~JoinProbe()
{}

bool JoinProbe::advanceNextPosition()
{
    position++;
    return position < positionCount;
}

int64_t JoinProbe::getCurrentJoinPosition(JoinHashTables *hashTables)
{
    if (currentRowContainsNull()) {
        return -1;
    }

    int64_t currentJoinPosition = hashTables->getJoinPosition(position, probeHashColumns, probeHashColsCount, probeAllColumns, probeAllColsCount);
    return currentJoinPosition;
}

bool JoinProbe::currentRowContainsNull()
{
    Column *column;
    for (int32_t columnIdx = 0; columnIdx < probeHashColsCount; columnIdx++) {
        column = probeHashColumns[columnIdx];
        if (column->isNull(position)) {
            return true;
        }
    }
    return false;
}

LookupJoinOutputBuilder::LookupJoinOutputBuilder(int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *buildOutputCols, int32_t *buildOutputTypes, int32_t buildOutputColsCount)
{
    this->probeOutputCols = probeOutputCols;
    this->probeOutputColsCount = probeOutputColsCount;
    this->buildOutputCols = buildOutputCols;
    this->buildOutputTypes = buildOutputTypes;
    this->buildOutputColsCount = buildOutputColsCount;
}

void LookupJoinOutputBuilder::appendRow(int32_t probePosition, int64_t partitionedJoinPosition)
{
    probeIndex.push_back(probePosition);
    buildIndex.push_back(partitionedJoinPosition);
}

Column *buildProbeINT32Column(int32_t *allData, int32_t *probeIndex, int32_t positionCount)
{
    int32_t *outputData = new int32_t[positionCount];
    int32_t index = 0;
    for (int32_t i = 0; i < positionCount; i++) {
        index = probeIndex[i];
        outputData[i] = allData[index];
    }

    Column *column = new Column(outputData, INT32, positionCount);
    return column;
}

Column *buildProbeINT64Column(int64_t *allData, int32_t *probeIndex, int32_t positionCount)
{
    int64_t *outputData = new int64_t[positionCount];
    int32_t index = 0;
    for (int32_t i = 0; i < positionCount; i++) {
        index = probeIndex[i];
        outputData[i] = allData[index];
    }

    Column *column = new Column(outputData, INT64, positionCount);
    return column;
}

Column *buildProbeDOUBLEColumn(double *allData, int32_t *probeIndex, int32_t positionCount)
{
    double *outputData = new double[positionCount];
    int32_t index = 0;
    for (int32_t i = 0; i < positionCount; i++) {
        index = probeIndex[i];
        outputData[i] = allData[index];
    }

    Column *column = new Column(outputData, DOUBLE, positionCount);
    return column;
}

Column *buildBuildINT32Column(JoinHashTables *hashTables, int32_t outputCol, int64_t *buildIndex, int32_t positionCount)
{
    int64_t partitionedJoinPosition;
    void *dataAddr = NULL;

    int32_t *outputData = new int32_t[positionCount];
    for (int32_t rowIdx = 0; rowIdx < positionCount; rowIdx++) {
        partitionedJoinPosition = buildIndex[rowIdx];
        dataAddr = hashTables->getBuildData(partitionedJoinPosition, outputCol);
        outputData[rowIdx] = *((int32_t *)dataAddr);
    }

    Column *column = new Column(outputData, INT32, positionCount);
    return column;
}

Column *buildBuildINT64Column(JoinHashTables *hashTables, int32_t outputCol, int64_t *buildIndex, int32_t positionCount)
{
    int64_t partitionedJoinPosition;
    void *dataAddr = NULL;

    int64_t *outputData = new int64_t[positionCount];
    for (int32_t rowIdx = 0; rowIdx < positionCount; rowIdx++) {
        partitionedJoinPosition = buildIndex[rowIdx];
        dataAddr = hashTables->getBuildData(partitionedJoinPosition, outputCol);
        outputData[rowIdx] = *((int64_t *)dataAddr);
    }

    Column *column = new Column(outputData, INT64, positionCount);
    return column;
}

Column *buildBuildDOUBLEColumn(JoinHashTables *hashTables, int32_t outputCol, int64_t *buildIndex, int32_t positionCount)
{
    int64_t partitionedJoinPosition;
    void *dataAddr = NULL;

    double *outputData = new double[positionCount];
    for (int32_t rowIdx = 0; rowIdx < positionCount; rowIdx++) {
        partitionedJoinPosition = buildIndex[rowIdx];
        dataAddr = hashTables->getBuildData(partitionedJoinPosition, outputCol);
        outputData[rowIdx] = *((double *)dataAddr);
    }

    Column *column = new Column(outputData, DOUBLE, positionCount);
    return column;
}

Table *LookupJoinOutputBuilder::buildOutput(JoinProbe *joinProbe, JoinHashTables *hashTables)
{
    int32_t columnCount = probeOutputColsCount + buildOutputColsCount;
    int32_t positionCount = probeIndex.size();
    Table *output = new Table(positionCount, columnCount);

    Column *probeColumn;
    Column *column;
    int32_t probeOutputCol;
    Column **probeAllColumns = joinProbe->getProbeAllColumns();
    ColumnType type;
    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        probeOutputCol = probeOutputCols[columnIdx];
        column = probeAllColumns[probeOutputCol];

        type = column->getType();
        switch (type)
        {
        case INT32:
            probeColumn = buildProbeINT32Column((int32_t *)(column->getData()), &probeIndex[0], positionCount);
            break;
        case INT64:
            probeColumn = buildProbeINT64Column((int64_t *)(column->getData()), &probeIndex[0], positionCount);
            break;
        case DOUBLE:
            probeColumn = buildProbeDOUBLEColumn((double *)(column->getData()), &probeIndex[0], positionCount);
            break;
        default:
            return NULL;
        }
        output->setColumn(probeColumn, type);
    }

    int32_t buildOutputCol;
    int32_t buildOutputColType;
    Column *buildColumn;

    for (int32_t columnIdx = 0; columnIdx < buildOutputColsCount; columnIdx++) {
        buildOutputCol = buildOutputCols[columnIdx];
        buildOutputColType = buildOutputTypes[columnIdx];

        switch (buildOutputColType)
        {
        case 1:
            buildColumn = buildBuildINT32Column(hashTables, buildOutputCol, &buildIndex[0], positionCount);
            output->setColumn(buildColumn, INT32);
            break;
        case 2:
            buildColumn = buildBuildINT64Column(hashTables, buildOutputCol, &buildIndex[0], positionCount);
            output->setColumn(buildColumn, INT64);
            break;
        case 3:
            buildColumn = buildBuildDOUBLEColumn(hashTables, buildOutputCol, &buildIndex[0], positionCount);
            output->setColumn(buildColumn, DOUBLE);
            break;
        default:
            return NULL;
        }
    }

    return output;
}
} // end of op
} // end of omniruntime
