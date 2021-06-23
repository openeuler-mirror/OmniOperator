#include "pages_hash_strategy.h"
#include "hash_util.h"
#include "optimization.h"
#include "../jit/annotation.h"

PagesHashStrategy::PagesHashStrategy(Column ***columns, int32_t tableCount, int32_t *columnTypes, int32_t columnCount, int32_t *hashCols, int32_t hashColsCount)
{
    this->buildColumns = columns;
    this->buildTableCount = tableCount;
    this->buildColumnCount = columnCount;
    this->buildHashColsCount = hashColsCount;
    this->buildHashColTypes = new int32_t[hashColsCount];

    if(hashColsCount==0){
        this->buildHashColumns= nullptr;
    }
    else{
        this->buildHashColumns = (Column ***)malloc(hashColsCount * sizeof(Column **));
        int32_t hashColumn;
        for (int32_t i = 0; i < hashColsCount; i++) {
            hashColumn = hashCols[i];
            buildHashColumns[i] = columns[hashColumn];
            buildHashColTypes[i] = columnTypes[hashColumn];
        }
    }
}

PagesHashStrategy::~PagesHashStrategy()
{
    delete[] buildHashColTypes;
    delete[] buildHashColumns;
}

int64_t PagesHashStrategy::hashPosition(int32_t tableIndex, int32_t rowIndex)
{
    return hashPosition(tableIndex, rowIndex, buildHashColTypes, buildHashColsCount);
}

//SPECIALIZE(OMNIJIT_HASH_STRATEGY_HASH_POSITION)
int64_t PagesHashStrategy::hashPosition(int32_t tableIndex, int32_t rowIndex, int32_t *hashColTypes, int32_t hashColCount)
{
    int64_t result = 0;
    Column *column;
    int64_t hash;
    void *valueAddr;
    int32_t columnType;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        column = buildHashColumns[columnIdx][tableIndex];
        if (column->isNull(rowIndex)) {
            continue;
        }
        valueAddr = column->getValue(rowIndex);
        columnType = hashColTypes[columnIdx];
        switch (columnType) {
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

bool intValueEqualsIgnoreNulls(int32_t *leftData, int32_t leftIndex, int32_t *rightData, int32_t rightIndex)
{
    bool result = (leftData[leftIndex] == rightData[rightIndex]);
    return result;
}

bool int64ValueEqualsIgnoreNulls(int64_t *leftData, int32_t leftIndex, int64_t *rightData, int32_t rightIndex)
{
    bool result = (leftData[leftIndex] == rightData[rightIndex]);
    return result;
}

bool doubleValueEqualsIgnoreNulls(double *leftData, int32_t leftIndex, double *rightData, int32_t rightIndex)
{
    if (std::abs(leftData[leftIndex] - rightData[rightIndex]) < __DBL_EPSILON__) {
        return true;
    }
    else {
        return false;
    }
}

bool PagesHashStrategy::positionEqualsPositionIgnoreNulls(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex, int32_t rightRowIndex)
{
    return positionEqualsPositionIgnoreNulls(leftTableIndex, leftRowIndex, rightTableIndex, rightRowIndex, buildHashColTypes, buildHashColsCount);
}

//SPECIALIZE(OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS)
bool PagesHashStrategy::positionEqualsPositionIgnoreNulls(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex, int32_t rightRowIndex, int32_t *hashColTypes, int32_t hashColCount)
{
    Column *leftColumn;
    Column *rightColumn;
    int32_t columnType;
    bool result;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        rightColumn = buildHashColumns[columnIdx][rightTableIndex];

        columnType = hashColTypes[columnIdx];
        switch (columnType)
        {
            case 1:
                result = intValueEqualsIgnoreNulls((int32_t *)(leftColumn->getData()), leftRowIndex, (int32_t *)(rightColumn->getData()), rightRowIndex);
                break;
            case 2:
                result = int64ValueEqualsIgnoreNulls((int64_t *)(leftColumn->getData()), leftRowIndex, (int64_t *)(rightColumn->getData()), rightRowIndex);
                break;
            case 3:
                result = doubleValueEqualsIgnoreNulls((double *)(leftColumn->getData()), leftRowIndex, (double *)(rightColumn->getData()), rightRowIndex);
                break;
            default:
                result = false;
                break;
        }
        if (!result) {
            return false;
        }
    }
    return true;
}

bool PagesHashStrategy::positionEqualsRowIgnoreNulls(int32_t buildTableIndex, int32_t buildRowIndex, int32_t probePosition, Column **probeJoinColumns)
{
    return positionEqualsRowIgnoreNulls(buildTableIndex, buildRowIndex, probePosition, (int64_t)probeJoinColumns, buildHashColTypes, buildHashColsCount);
}

//SPECIALIZE(OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS)
bool PagesHashStrategy::positionEqualsRowIgnoreNulls(int32_t buildTableIndex, int32_t buildRowIndex, int32_t probePosition, int64_t probeJoinColumnsAddr, int32_t *hashColTypes, int32_t hashColCount)
{
    bool result;
    int32_t columnType;
    Column **probeJoinColumns = (Column **)probeJoinColumnsAddr;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        Column *buildColumn = buildHashColumns[columnIdx][buildTableIndex];
        Column *probeColumn = probeJoinColumns[columnIdx];

        columnType = hashColTypes[columnIdx];
        switch (columnType) {
            case 1:
                result = intValueEqualsIgnoreNulls((int32_t *)(buildColumn->getData()), buildRowIndex, (int32_t *)(probeColumn->getData()), probePosition);
                break;
            case 2:
                result = int64ValueEqualsIgnoreNulls((int64_t *)(buildColumn->getData()), buildRowIndex, (int64_t *)(probeColumn->getData()), probePosition);
                break;
            case 3:
                result = doubleValueEqualsIgnoreNulls((double *)(buildColumn->getData()), buildRowIndex, (double *)(probeColumn->getData()), probePosition);
                break;
            default:
                result = false;
                break;
        }

        if (!result) {
            return false;
        }
    }

    return true;
}

bool PagesHashStrategy::positionEqualsPosition(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex, int32_t rightRowIndex)
{
    Column *leftColumn;
    Column *rightColumn;
    bool leftIsNull;
    bool rightIsNull;
    int32_t columnType;
    bool result;

    for (int32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        rightColumn = buildHashColumns[columnIdx][rightTableIndex];
        leftIsNull = leftColumn->isNull(leftRowIndex);
        rightIsNull = rightColumn->isNull(rightRowIndex);
        if (leftIsNull || rightIsNull) {
            return false;
        }

        columnType = buildHashColTypes[columnIdx];
        switch (columnType) {
            case 1:
                result = intValueEqualsIgnoreNulls((int32_t *)(leftColumn->getData()), leftRowIndex, (int32_t *)(rightColumn->getData()), rightRowIndex);
                break;
            case 2:
                result = int64ValueEqualsIgnoreNulls((int64_t *)(leftColumn->getData()), leftRowIndex, (int64_t *)(rightColumn->getData()), rightRowIndex);
                break;
            case 3:
                result = doubleValueEqualsIgnoreNulls((double *)(leftColumn->getData()), leftRowIndex, (double *)(rightColumn->getData()), rightRowIndex);
                break;
            default:
                result = false;
                break;
        }

        if (!result)
        {
            return false;
        }
    }
    return true;
}

bool PagesHashStrategy::valuePositionEqualsPosition(ColumnType type, Column *leftColumn, int32_t leftRowIndex, Column *rightColumn, int32_t rightRowIndex)
{
    bool leftIsNull = leftColumn->isNull(leftRowIndex);
    bool rightIsNull = rightColumn->isNull(rightRowIndex);
    if (leftIsNull || rightIsNull) {
        return leftIsNull && rightIsNull;
    }
    return valueEqualsValueIgnoreNulls(type, leftColumn->getData(), leftRowIndex, rightColumn->getData(), rightRowIndex);
}

bool PagesHashStrategy::valueEqualsValueIgnoreNulls(ColumnType type, void *leftData, int32_t leftIndex, void *rightData, int32_t rightIndex)
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
