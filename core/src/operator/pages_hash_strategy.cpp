#include "pages_hash_strategy.h"
#include "hash_util.h"

PagesHashStrategy::PagesHashStrategy(Column ***columns, int32_t tableCount, int32_t columnCount, int32_t *hashCols, int32_t hashColsCount)
{
    this->buildColumns = columns;
    this->buildTableCount = tableCount;
    this->buildColumnCount = columnCount;
    this->buildHashColsCount = hashColsCount;

    this->buildHashColumns = (Column ***)malloc(hashColsCount * sizeof(Column **));
    int32_t hashColumn;
    for (int32_t i = 0; i < hashColsCount; i++) {
        hashColumn = hashCols[i];
        buildHashColumns[i] = columns[hashColumn];
    }
}

PagesHashStrategy::~PagesHashStrategy()
{
    delete[] buildHashColumns;
}

int64_t PagesHashStrategy::hashPosition(int32_t tableIndex, int32_t rowIndex)
{
    int64_t result = 0;
    Column *column;
    int64_t hash;
    void *valueAddr;

    for (int32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        column = buildHashColumns[columnIdx][tableIndex];
        if (column->isNull(rowIndex)) {
            continue;
        }
        valueAddr = column->getValue(rowIndex);
        switch (column->getType()) {
            case INT32: {
                int32_t intValue = *((int32_t *)valueAddr);
                hash = HashUtil::hashValue((int64_t)intValue);
                break;
            }
            case INT64: {
                int64_t int64Value = *((int64_t *)valueAddr);
                hash = HashUtil::hashValue(int64Value);
                break;
            }
            case DOUBLE: {
                double doubleValue = *((double *)valueAddr);
                hash = HashUtil::hashValue((int64_t)doubleValue);
                break;
            }
            default: {
                hash = 0;
                break;
            }
        }

        result = HashUtil::getHash(result, hash);
    }
    return result;
}

bool PagesHashStrategy::isPositionNull(int32_t tableIndex, int rowIndex)
{
    Column *column;
    for (int32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        column = buildHashColumns[columnIdx][tableIndex];
        if (column->isNull(rowIndex)) {
            return true;
        }
    }
    return false;
}

bool valueEqualsValueIgnoreNulls(ColumnType type, void *leftData, int32_t leftIndex, void *rightData, int32_t rightIndex);

bool PagesHashStrategy::positionEqualsPositionIgnoreNulls(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex, int32_t rightRowIndex)
{
    Column *leftColumn;
    Column *rightColumn;
    bool result = true;

    for (int32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        rightColumn = buildHashColumns[columnIdx][rightTableIndex];

        ColumnType type = leftColumn->getType();
        if (!valueEqualsValueIgnoreNulls(type, leftColumn->getData(), leftRowIndex, rightColumn->getData(), rightRowIndex))
        {
            return false;
        }
    }
    return true;
}

bool PagesHashStrategy::positionEqualsRowIgnoreNulls(int32_t buildTableIndex, int32_t buildRowIndex, int32_t probePosition, Column **probeJoinColumns, int32_t probeJoinColumnsCount)
{
    for (int32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        Column *buildColumn = buildHashColumns[columnIdx][buildTableIndex];
        Column *probeColumn = probeJoinColumns[columnIdx];

        if (!valueEqualsValueIgnoreNulls(buildColumn->getType(), buildColumn->getData(), buildRowIndex, probeColumn->getData(), probePosition)) {
            return false;
        }
    }

    return true;
}

bool valueEqualsValueIgnoreNulls(ColumnType type, void *leftData, int32_t leftIndex, void *rightData, int32_t rightIndex)
{
    bool result;
    switch (type)
    {
    case INT32:
        result = (((int32_t *)leftData)[leftIndex] == ((int32_t *)rightData)[rightIndex]);
        break;
    case INT64:
        result = (((int64_t *)leftData)[leftIndex] == ((int64_t *)rightData)[rightIndex]);
        break;
    case DOUBLE:
        double *leftValues = (double *)leftData;
        double *rightValues = (double *)rightData;
        if (std::abs(leftValues[leftIndex] - rightValues[rightIndex]) < __DBL_EPSILON__) {
            result = true;
        }
        else {
            result = false;
        }
        break;
    // default:
    //     result = false;
    //     break;
    }

    return result;
}
