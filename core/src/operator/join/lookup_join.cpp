#include "lookup_join.h"
#include "hash_builder.h"
#include "../../vector/vector_common.h"
#include "../../vector/dictionary_vector.h"
#include "../optimization.h"
#include "../../jit/annotation.h"

#include <vector>
#include <cstring>
#include <algorithm>

int32_t getRowSizeFromAllTypes(int32_t *allTypes, int32_t *outputCols, int32_t outputColsCount)
{
    int32_t rowSize = 0;
    int32_t type;
    for (int32_t i = 0; i < outputColsCount; i++) {
        type = allTypes[outputCols[i]];
        switch (type)
        {
        case OMNI_VEC_TYPE_INT:
            rowSize += sizeof(int32_t);
            break;
        case OMNI_VEC_TYPE_LONG:
            rowSize += sizeof(int64_t);
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            rowSize += sizeof(double);
            break;
        default:
            break;
        }
    }
    return rowSize;
}

int32_t getRowSizeFromTypes(int32_t *types, int32_t *outputCols, int32_t outputColsCount)
{
    int32_t rowSize = 0;
    int32_t type;
    for (int32_t i = 0; i < outputColsCount; i++) {
        type = types[i];
        switch (type)
        {
        case OMNI_VEC_TYPE_INT:
            rowSize += sizeof(int32_t);
            break;
        case OMNI_VEC_TYPE_LONG:
            rowSize += sizeof(int64_t);
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            rowSize += sizeof(double);
            break;
        default:
            break;
        }
    }
    return rowSize;
}

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
    this->probeHashColTypes = new int32_t[probeHashColsCount];
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        this->probeHashColTypes[i] = probeTypes[probeHashCols[i]];
    }
    this->probeHashColsCount = probeHashColsCount;

    this->buildOutputCols = new int32_t[buildOutputColsCount];
    memcpy(this->buildOutputCols, buildOutputCols, buildOutputColsCount * intByteLen);
    this->buildOutputColsCount = buildOutputColsCount;

    this->buildOutputTypes = new int32_t[buildOutputColsCount];
    memcpy(this->buildOutputTypes, buildOutputTypes, buildOutputColsCount * intByteLen);

    this->hashTables = hashTables;
    this->rowSize = getRowSizeFromAllTypes(probeTypes, probeOutputCols, probeOutputColsCount);
    this->rowSize += getRowSizeFromTypes(buildOutputTypes, buildOutputCols, buildOutputColsCount);
}

LookupJoinOperatorFactory::~LookupJoinOperatorFactory()
{
    delete[] probeTypes;
    delete[] probeOutputCols;
    delete[] probeHashCols;
    delete[] probeHashColTypes;
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
        probeHashColTypes,
        probeHashColsCount,
        buildOutputCols,
        buildOutputTypes,
        buildOutputColsCount,
        hashTables,
        rowSize);
    return static_cast<Operator *>(lookupJoinoperator);
}

LookupJoinOperator::LookupJoinOperator(
    int32_t *probeTypes,
    int32_t probeTypesCount,
    int32_t *probeOutputCols,
    int32_t probeOutputColsCount,
    int32_t *probeHashCols,
    int32_t *probeHashColTypes,
    int32_t probeHashColsCount,
    int32_t *buildOutputCols,
    int32_t *buildOutputTypes,
    int32_t buildOutputColsCount,
    JoinHashTables *hashTables,
    int32_t outputRowSize)
{
    this->probeTypes = probeTypes;
    this->probeTypesCount = probeTypesCount;
    this->probeOutputCols = probeOutputCols;
    this->probeOutputColsCount = probeOutputColsCount;
    this->probeHashCols = probeHashCols;
    this->probeHashColTypes = probeHashColTypes;
    this->probeHashColsCount = probeHashColsCount;
    this->buildOutputCols = buildOutputCols;
    this->buildOutputTypes = buildOutputTypes;
    this->buildOutputColsCount = buildOutputColsCount;
    this->hashTables = hashTables;
    this->joinProbe = nullptr;
    this->partitionedJoinPosition = -1;
    this->outputBuilder = new LookupJoinOutputBuilder(probeTypes, probeOutputCols, probeOutputColsCount, buildOutputCols, buildOutputTypes, buildOutputColsCount, outputRowSize);
}

LookupJoinOperator::~LookupJoinOperator()
{}

int32_t LookupJoinOperator::AddInput(VectorBatch *vecBatch)
{
    this->joinProbe = new JoinProbe(vecBatch, probeTypesCount, probeHashCols, probeHashColTypes, probeHashColsCount);
    this->partitionedJoinPosition = -1;

    // start probe
    processProbe();
    return 0;
}

int32_t LookupJoinOperator::GetOutput(std::vector<VectorBatch *>& outputPages)
{
    // build output data
    outputBuilder->buildOutput(joinProbe, hashTables, outputPages);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

int32_t *LookupJoinOperator::GetSourceTypes()
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
        // finish probe
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

JoinProbe::JoinProbe(VectorBatch *input, int32_t allColsCount, int32_t *hashCols, int32_t *hashColTypes, int32_t hashColsCount)
{
    Vector **allColumns = (Vector **)malloc(allColsCount * sizeof(Vector *));
    for (int32_t columnIdx = 0; columnIdx < allColsCount; columnIdx++) {
        allColumns[columnIdx] = input->getVector(columnIdx);
    }

    this->probeAllColumns = allColumns;
    this->probeAllColsCount = allColsCount;
    this->positionCount = input->getRowCount();
    this->probeHashColTypes = hashColTypes;
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

    int64_t currentJoinPosition = hashTables->getJoinPosition(position, probeHashColumns, probeHashColTypes, probeHashColsCount, probeAllColumns, probeAllColsCount);
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

LookupJoinOutputBuilder::LookupJoinOutputBuilder(
    int32_t *probeTypes,
    int32_t *probeOutputCols,
    int32_t probeOutputColsCount,
    int32_t *buildOutputCols,
    int32_t *buildOutputTypes,
    int32_t buildOutputColsCount,
    int32_t outputRowSize)
{
    this->probeTypes = probeTypes;
    this->probeOutputCols = probeOutputCols;
    this->probeOutputColsCount = probeOutputColsCount;
    this->buildOutputCols = buildOutputCols;
    this->buildOutputTypes = buildOutputTypes;
    this->buildOutputColsCount = buildOutputColsCount;
    this->outputRowSize = outputRowSize;
    this->isSequentialProbeIndices = true;
}

void LookupJoinOutputBuilder::appendRow(int32_t probePosition, int64_t partitionedJoinPosition)
{
    int32_t previousPosition = probeIndex.size() == 0 ? -1 : probeIndex[probeIndex.size() - 1];
    isSequentialProbeIndices &= (probePosition == previousPosition + 1) || (previousPosition == -1);
    probeIndex.push_back(probePosition);
    buildIndex.push_back(partitionedJoinPosition);
}

Vector *buildProbeINT32Column(IntVector *allData, int32_t *probeIndex, int32_t offset, int32_t length)
{
    IntVector *column = new IntVector(nullptr, length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t index = 0;
    for (int32_t i = start; i < end; i++) {
        column->setValue(index++, allData->getValue(probeIndex[i]));
    }

    return column;
}

Vector *buildProbeINT64Column(LongVector *allData, int32_t *probeIndex, int32_t offset, int32_t length)
{
    LongVector *column = new LongVector(nullptr, length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t index = 0;
    for (int32_t i = start; i < end; i++) {
        column->setValue(index++, allData->getValue(probeIndex[i]));
    }

    return column;
}

Vector *buildProbeDOUBLEColumn(DoubleVector *allData, int32_t *probeIndex, int32_t offset, int32_t length)
{
    DoubleVector *column = new DoubleVector(nullptr, length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t index = 0;
    for (int32_t i = start; i < end; i++) {
        column->setValue(index++, allData->getValue(probeIndex[i]));
    }

    return column;
}

Vector *buildBuildINT32Column(JoinHashTables *hashTables, int32_t outputCol, int64_t *buildIndex, int32_t offset, int32_t length)
{
    int64_t partitionedJoinPosition;
    int32_t value;

    IntVector *column = new IntVector(nullptr, length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t index = 0;
    for (int32_t rowIdx = start; rowIdx < end; rowIdx++) {
        partitionedJoinPosition = buildIndex[rowIdx];
        hashTables->getBuildValue(&value, partitionedJoinPosition, outputCol);
        column->setValue(index++, value);
    }

    return column;
}

Vector *buildBuildINT64Column(JoinHashTables *hashTables, int32_t outputCol, int64_t *buildIndex, int32_t offset, int32_t length)
{
    int64_t partitionedJoinPosition;
    int64_t value;

    LongVector *column = new LongVector(nullptr, length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t index = 0;
    for (int32_t rowIdx = start; rowIdx < end; rowIdx++) {
        partitionedJoinPosition = buildIndex[rowIdx];
        hashTables->getBuildValue(&value, partitionedJoinPosition, outputCol);
        column->setValue(index++, value);
    }

    return column;
}

Vector *buildBuildDOUBLEColumn(JoinHashTables *hashTables, int32_t outputCol, int64_t *buildIndex, int32_t offset, int32_t length)
{
    int64_t partitionedJoinPosition;
    double value;

    DoubleVector *column = new DoubleVector(nullptr, length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t index = 0;
    for (int32_t rowIdx = start; rowIdx < end; rowIdx++) {
        partitionedJoinPosition = buildIndex[rowIdx];
        hashTables->getBuildValue(&value, partitionedJoinPosition, outputCol);
        column->setValue(index++, value);
    }

    return column;
}

void constructProbeColumnsFromSlice(VectorBatch *vectorBatch,
                                    Vector **probeAllColumns,
                                    int32_t *probeOutputCols,
                                    int32_t probeOutputColsCount,
                                    vector<int32_t> &probeIndex,
                                    int32_t position,
                                    int32_t rowCount)
{
    int32_t outputColumnIdx = 0;
    Vector *column;
    Vector *probeColumn;
    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        column = probeAllColumns[probeOutputCols[columnIdx]];
        probeColumn = column->slice(probeIndex[position], rowCount);
        vectorBatch->setVector(outputColumnIdx++, probeColumn);
    }
}

void constructProbeColumnsFromReuse(VectorBatch *vectorBatch,
                                    Vector **probeAllColumns,
                                    int32_t *probeOutputCols,
                                    int32_t probeOutputColsCount)
{
    int32_t outputColumnIdx = 0;
    Vector *column;
    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        column = probeAllColumns[probeOutputCols[columnIdx]];
        column->getReference()->incRef();
        vectorBatch->setVector(outputColumnIdx++, column);
    }
}

SPECIALIZE(OMNIJIT_CONSTRUCT_PROBE_COLUMNS_FROM_COPY)
void constructProbeColumnsFromCopy(VectorBatch *vectorBatch,
                                   Vector **probeAllColumns,
                                   int32_t *probeTypes,
                                   int32_t *probeOutputCols,
                                   int32_t probeOutputColsCount,
                                   vector<int32_t> &probeIndex,
                                   int32_t position,
                                   int32_t rowCount)
{
    int32_t outputColumnIdx = 0;
    Vector *column;
    Vector *probeColumn;

    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        column = probeAllColumns[probeOutputCols[columnIdx]];
        switch (probeTypes[columnIdx])
        {
            case OMNI_VEC_TYPE_INT:
                probeColumn = buildProbeINT32Column((IntVector *)column, &probeIndex[0], position, rowCount);
                break;
            case OMNI_VEC_TYPE_LONG:
                probeColumn = buildProbeINT64Column((LongVector *)column, &probeIndex[0], position, rowCount);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                probeColumn = buildProbeDOUBLEColumn((DoubleVector *)column, &probeIndex[0], position, rowCount);
            default:
                break;
        }
        vectorBatch->setVector(outputColumnIdx++, probeColumn);
    }
}

void constructProbeColumnsFromPositions(VectorBatch *vectorBatch,
                                        Vector **probeAllColumns,
                                        int32_t *probeTypes,
                                        int32_t *probeOutputCols,
                                        int32_t probeOutputColsCount,
                                        vector<int32_t> &probeIndex,
                                        int32_t position,
                                        int32_t rowCount)
{
    int32_t outputColumnIdx = 0;
    Vector *column;
    Vector *probeColumn;

    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        column = probeAllColumns[probeOutputCols[columnIdx]];
        probeColumn = new DictionaryVector(column, &probeIndex[position], rowCount);
        vectorBatch->setVector(outputColumnIdx++, probeColumn);
    }
}

void constructProbeColumns(VectorBatch *vectorBatch,
                       Vector **probeAllColumns,
                       int32_t *probeTypes,
                       int32_t *probeOutputCols,
                       int32_t probeOutputColsCount,
                       bool isSequentialProbeIndices,
                       std::vector<int32_t>& probeIndex,
                       int32_t position,
                       int32_t rowCount)
{
    int32_t probeLength = probeIndex.size();
    if (!isSequentialProbeIndices || probeLength == 0) {
        // probeIndices are discrete
        /*constructProbeColumnsFromCopy(vectorBatch, probeAllColumns, probeTypes, probeOutputCols, probeOutputColsCount,
                                      probeIndex, position, rowCount);*/
        constructProbeColumnsFromPositions(vectorBatch, probeAllColumns, probeTypes, probeOutputCols, probeOutputColsCount,
                                      probeIndex, position, rowCount);
    } else if (probeLength == probeAllColumns[probeOutputCols[0]]->getSize()) {
        // probeIndices are a simple covering of the vector
        constructProbeColumnsFromReuse(vectorBatch, probeAllColumns, probeOutputCols, probeOutputColsCount);
    } else {
        // probeIndices are sequential without holes
        constructProbeColumnsFromSlice(vectorBatch, probeAllColumns, probeOutputCols, probeOutputColsCount, probeIndex,
                                       position, rowCount);
    }
}

SPECIALIZE(OMNIJIT_CONSTRUCT_BUILD_COLUMNS)
void constructBuildColumns(VectorBatch *vectorBatch,
                       JoinHashTables *hashTables,
                       int32_t *buildOutputTypes,
                       int32_t *buildOutputCols,
                       int32_t buildOutputColsCount,
                       int32_t probeOutputColsCount,
                       std::vector<int64_t>& buildIndex,
                       int32_t position,
                       int32_t rowCount)
{
    Vector *buildColumn;
    int32_t buildOutputCol;
    int32_t outputColumnIndex = probeOutputColsCount;
    for (int32_t columnIdx = 0; columnIdx < buildOutputColsCount; columnIdx++) {
        buildOutputCol = buildOutputCols[columnIdx];
        switch (buildOutputTypes[columnIdx])
        {
            case OMNI_VEC_TYPE_INT:
                buildColumn = buildBuildINT32Column(hashTables, buildOutputCol, &buildIndex[0], position, rowCount);
                break;
            case OMNI_VEC_TYPE_LONG:
                buildColumn = buildBuildINT64Column(hashTables, buildOutputCol, &buildIndex[0], position, rowCount);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                buildColumn = buildBuildDOUBLEColumn(hashTables, buildOutputCol, &buildIndex[0], position, rowCount);
                break;
            default:
                break;
        }
        vectorBatch->setVector(outputColumnIndex++, buildColumn);
    }
}

void LookupJoinOutputBuilder::buildOutput(JoinProbe *joinProbe, JoinHashTables *hashTables, std::vector<VectorBatch *>& outputTables)
{
    int32_t maxRowCount = (MAX_VEC_BATCH_SIZE_IN_BYTES + outputRowSize - 1) / outputRowSize;
    int32_t positionCount = probeIndex.size();
    int32_t tableCount = ((positionCount + maxRowCount - 1) / maxRowCount);
    outputTables.reserve(tableCount);

    Vector **probeAllColumns = joinProbe->getProbeAllColumns();
    int32_t columnCount = probeOutputColsCount + buildOutputColsCount;

    VectorBatch *vectorBatch;
    int32_t position = 0;
    int32_t rowCount;
    for (int32_t tableIdx = 0; tableIdx < tableCount; tableIdx++) {
        rowCount = std::min(maxRowCount, positionCount - position);
        vectorBatch = new VectorBatch(columnCount);

        constructProbeColumns(vectorBatch, probeAllColumns, probeTypes, probeOutputCols, probeOutputColsCount, isSequentialProbeIndices, probeIndex, position, rowCount);
        constructBuildColumns(vectorBatch, hashTables, buildOutputTypes, buildOutputCols, buildOutputColsCount, probeOutputColsCount, buildIndex, position, rowCount);

        position += rowCount;
        outputTables.push_back(vectorBatch);
    }

    probeIndex.clear();
    buildIndex.clear();
}
} // end of op
} // end of omniruntime
