#include "lookup_join.h"
#include "hash_builder.h"
#include "../../memory/memory_pool.h"
#include "../../vector/vector_common.h"

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
    this->joinProbe = nullptr;
    this->outputVecBatch = nullptr;
    this->partitionedJoinPosition = -1;
    this->outputBuilder = new LookupJoinOutputBuilder(probeOutputCols, probeOutputColsCount, buildOutputCols, buildOutputTypes, buildOutputColsCount);
}

LookupJoinOperator::~LookupJoinOperator()
{}

int32_t LookupJoinOperator::addInput(VectorBatch *vecBatch)
{
    this->joinProbe = new JoinProbe(vecBatch, probeHashCols, probeHashColsCount);
    this->partitionedJoinPosition = -1;

    // start probe
    processProbe();
    return 0;
}

int32_t LookupJoinOperator::getOutput(std::vector<VectorBatch *>& outputPages)
{
    if (outputVecBatch != nullptr) {
        VectorBatch *result = outputVecBatch;
        outputPages.push_back(result);
        outputVecBatch = nullptr;
    }

    return 0;
}

int32_t *LookupJoinOperator::getSourceTypes()
{
    return probeTypes;
}

void LookupJoinOperator::processProbe()
{
    if (joinProbe == nullptr) {
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
        outputVecBatch = outputBuilder->buildOutput(joinProbe, hashTables);
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

JoinProbe::JoinProbe(VectorBatch *input, int32_t *hashCols, int32_t hashColsCount)
{
    int32_t allColsCount = input->getVectorCount();
    Vector **allColumns = (Vector **)malloc(allColsCount * sizeof(Vector *));
    for (int32_t columnIdx = 0; columnIdx < allColsCount; columnIdx++) {
        allColumns[columnIdx] = input->getVector(columnIdx);
    }

    this->probeAllColumns = allColumns;
    this->probeAllColsCount = allColsCount;
    this->positionCount = input->getRowCount();
    this->probeHashColsCount = hashColsCount;
    this->probeHashColumns = (Vector **)malloc(hashColsCount * sizeof(Vector *));

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
    Vector *column;
    for (int32_t columnIdx = 0; columnIdx < probeHashColsCount; columnIdx++) {
        column = probeHashColumns[columnIdx];
        if (column->isValueNull(position)) {
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

Vector *buildProbeINT32Column(IntVector *allData, int32_t *probeIndex, int32_t positionCount)
{
    IntVector *column = new IntVector(nullptr, positionCount);
    int32_t index = 0;
    for (int32_t i = 0; i < positionCount; i++) {
        index = probeIndex[i];
        column->setValue(i, allData->getValue(index));
    }

    return column;
}

Vector *buildProbeINT64Column(LongVector *allData, int32_t *probeIndex, int32_t positionCount)
{
    LongVector *column = new LongVector(nullptr, positionCount);
    int32_t index = 0;
    for (int32_t i = 0; i < positionCount; i++) {
        index = probeIndex[i];
        column->setValue(i, allData->getValue(index));
    }

    return column;
}

Vector *buildProbeDOUBLEColumn(DoubleVector *allData, int32_t *probeIndex, int32_t positionCount)
{
    DoubleVector *column = new DoubleVector(nullptr, positionCount);
    int32_t index = 0;
    for (int32_t i = 0; i < positionCount; i++) {
        index = probeIndex[i];
        column->setValue(i, allData->getValue(index));
    }

    return column;
}

Vector *buildBuildINT32Column(JoinHashTables *hashTables, int32_t outputCol, int64_t *buildIndex, int32_t positionCount)
{
    int64_t partitionedJoinPosition;
    int32_t value;

    IntVector *column = new IntVector(nullptr, positionCount);
    for (int32_t rowIdx = 0; rowIdx < positionCount; rowIdx++) {
        partitionedJoinPosition = buildIndex[rowIdx];
        hashTables->getBuildValue(&value, partitionedJoinPosition, outputCol);
        column->setValue(rowIdx, value);
    }

    return column;
}

Vector *buildBuildINT64Column(JoinHashTables *hashTables, int32_t outputCol, int64_t *buildIndex, int32_t positionCount)
{
    int64_t partitionedJoinPosition;
    int64_t value;

    LongVector *column = new LongVector(nullptr, positionCount);
    for (int32_t rowIdx = 0; rowIdx < positionCount; rowIdx++) {
        partitionedJoinPosition = buildIndex[rowIdx];
        hashTables->getBuildValue(&value, partitionedJoinPosition, outputCol);
        column->setValue(rowIdx, value);
    }

    return column;
}

Vector *buildBuildDOUBLEColumn(JoinHashTables *hashTables, int32_t outputCol, int64_t *buildIndex, int32_t positionCount)
{
    int64_t partitionedJoinPosition;
    double value;

    DoubleVector *column = new DoubleVector(nullptr, positionCount);
    for (int32_t rowIdx = 0; rowIdx < positionCount; rowIdx++) {
        partitionedJoinPosition = buildIndex[rowIdx];
        hashTables->getBuildValue(&value, partitionedJoinPosition, outputCol);
        column->setValue(rowIdx, value);
    }

    return column;
}

VectorBatch *LookupJoinOutputBuilder::buildOutput(JoinProbe *joinProbe, JoinHashTables *hashTables)
{
    int32_t columnCount = probeOutputColsCount + buildOutputColsCount;
    int32_t positionCount = probeIndex.size();
    VectorBatch *output = new VectorBatch(columnCount);

    Vector *probeColumn;
    Vector *column;
    int32_t probeOutputCol;
    Vector **probeAllColumns = joinProbe->getProbeAllColumns();
    VecType type;
    int32_t outputColumnIndex = 0;
    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        probeOutputCol = probeOutputCols[columnIdx];
        column = probeAllColumns[probeOutputCol];

        type = column->getType();
        switch (type)
        {
        case OMNI_VEC_TYPE_INT:
            probeColumn = buildProbeINT32Column((IntVector *)(column), &probeIndex[0], positionCount);
            break;
        case OMNI_VEC_TYPE_LONG:
            probeColumn = buildProbeINT64Column((LongVector *)(column), &probeIndex[0], positionCount);
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            probeColumn = buildProbeDOUBLEColumn((DoubleVector *)(column), &probeIndex[0], positionCount);
            break;
        default:
            return nullptr;
        }
        output->setVector(outputColumnIndex++, probeColumn);
    }

    int32_t buildOutputCol;
    int32_t buildOutputColType;
    Vector *buildColumn;

    for (int32_t columnIdx = 0; columnIdx < buildOutputColsCount; columnIdx++) {
        buildOutputCol = buildOutputCols[columnIdx];
        buildOutputColType = buildOutputTypes[columnIdx];

        switch (buildOutputColType)
        {
        case 1:
            buildColumn = buildBuildINT32Column(hashTables, buildOutputCol, &buildIndex[0], positionCount);
            output->setVector(outputColumnIndex++, buildColumn);
            break;
        case 2:
            buildColumn = buildBuildINT64Column(hashTables, buildOutputCol, &buildIndex[0], positionCount);
            output->setVector(outputColumnIndex++, buildColumn);
            break;
        case 3:
            buildColumn = buildBuildDOUBLEColumn(hashTables, buildOutputCol, &buildIndex[0], positionCount);
            output->setVector(outputColumnIndex++, buildColumn);
            break;
        default:
            return nullptr;
        }
    }

    //output->printTable();
    return output;
}
} // end of op
} // end of omniruntime
