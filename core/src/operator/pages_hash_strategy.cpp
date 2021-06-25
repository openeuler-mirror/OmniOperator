#include "pages_hash_strategy.h"
#include "hash_util.h"
#include "pages_index.h"
#include "../vector/vector_common.h"
#include "optimization.h"
#include "../jit/annotation.h"

PagesHashStrategy::PagesHashStrategy(Vector ***columns, int32_t *columnTypes, int32_t columnCount, int32_t *hashCols, int32_t hashColsCount)
{
    this->buildColumns = columns;
    this->buildColumnCount = columnCount;
    this->buildHashColsCount = hashColsCount;
    this->buildHashColTypes = new int32_t[hashColsCount];

    if(hashColsCount==0){
        this->buildHashColumns= nullptr;
    }
    else{
        this->buildHashColumns = new Vector**[hashColsCount];
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

int64_t PagesHashStrategy::hashPosition(int32_t pageIndex, int32_t rowIndex)
{
    return hashPosition(pageIndex, rowIndex, buildHashColTypes, buildHashColsCount);
}

//SPECIALIZE(OMNIJIT_HASH_STRATEGY_HASH_POSITION)
int64_t PagesHashStrategy::hashPosition(int32_t tableIndex, int32_t rowIndex, int32_t *hashColTypes, int32_t hashColCount)
{
    int64_t result = 0;
    Vector *column;
    int64_t hash;
    void *valueAddr;
    int32_t columnType;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        column = buildHashColumns[columnIdx][tableIndex];
        if (column->isValueNull(rowIndex)) {
            continue;
        }
        columnType = hashColTypes[columnIdx];
        switch (columnType) {
            case OMNI_VEC_TYPE_INT: {
                int32_t intValue = ((IntVector *)column)->getValue(rowIndex);
                hash = HashUtil::hashValue((int64_t)intValue);
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                int64_t int64Value = ((LongVector *)column)->getValue(rowIndex);
                hash = HashUtil::hashValue(int64Value);
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                double doubleValue = ((DoubleVector *)column)->getValue(rowIndex);
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

bool PagesHashStrategy::isPositionNull(int32_t pageIndex, int rowIndex)
{
    Vector *column;
    for (int32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        column = buildHashColumns[columnIdx][pageIndex];
        if (column->isValueNull(rowIndex)) {
            return true;
        }
    }
    return false;
}

bool valueEqualsValueIgnoreNulls(VecType type, void *leftData, int32_t leftIndex, void *rightData, int32_t rightIndex);

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
    Vector *leftColumn;
    Vector *rightColumn;
    int32_t columnType;
    bool result;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        rightColumn = buildHashColumns[columnIdx][rightTableIndex];

        columnType = hashColTypes[columnIdx];
        switch (columnType)
        {
            case 1:
                result = intValueEqualsIgnoreNulls((int32_t *)(leftColumn->getValues()), leftRowIndex, (int32_t *)(rightColumn->getValues()), rightRowIndex);
                break;
            case 2:
                result = int64ValueEqualsIgnoreNulls((int64_t *)(leftColumn->getValues()), leftRowIndex, (int64_t *)(rightColumn->getValues()), rightRowIndex);
                break;
            case 3:
                result = doubleValueEqualsIgnoreNulls((double *)(leftColumn->getValues()), leftRowIndex, (double *)(rightColumn->getValues()), rightRowIndex);
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

bool PagesHashStrategy::positionEqualsRowIgnoreNulls(int32_t buildTableIndex, int32_t buildRowIndex, int32_t probePosition, Vector **probeJoinColumns)
{
    return positionEqualsRowIgnoreNulls(buildTableIndex, buildRowIndex, probePosition, (int64_t)probeJoinColumns, buildHashColTypes, buildHashColsCount);
}

//SPECIALIZE(OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS)
bool PagesHashStrategy::positionEqualsRowIgnoreNulls(int32_t buildTableIndex, int32_t buildRowIndex, int32_t probePosition, int64_t probeJoinColumnsAddr, int32_t *hashColTypes, int32_t hashColCount)
{
    bool result;
    int32_t columnType;
    Vector **probeJoinColumns = (Vector **)probeJoinColumnsAddr;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        Vector *buildColumn = buildHashColumns[columnIdx][buildTableIndex];
        Vector *probeColumn = probeJoinColumns[columnIdx];

        columnType = hashColTypes[columnIdx];
        switch (columnType) {
            case 1:
                result = intValueEqualsIgnoreNulls((int32_t *)(buildColumn->getValues()), buildRowIndex, (int32_t *)(probeColumn->getValues()), probePosition);
                break;
            case 2:
                result = int64ValueEqualsIgnoreNulls((int64_t *)(buildColumn->getValues()), buildRowIndex, (int64_t *)(probeColumn->getValues()), probePosition);
                break;
            case 3:
                result = doubleValueEqualsIgnoreNulls((double *)(buildColumn->getValues()), buildRowIndex, (double *)(probeColumn->getValues()), probePosition);
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
    Vector *leftColumn;
    Vector *rightColumn;
    bool leftIsNull;
    bool rightIsNull;
    int32_t columnType;
    bool result;

    for (int32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        rightColumn = buildHashColumns[columnIdx][rightTableIndex];
        leftIsNull = leftColumn->isValueNull(leftRowIndex);
        rightIsNull = rightColumn->isValueNull(rightRowIndex);
        if (leftIsNull || rightIsNull) {
            return false;
        }

        columnType = buildHashColTypes[columnIdx];
        switch (columnType) {
            case 1:
                result = intValueEqualsIgnoreNulls((int32_t *)(leftColumn->getValues()), leftRowIndex, (int32_t *)(rightColumn->getValues()), rightRowIndex);
                break;
            case 2:
                result = int64ValueEqualsIgnoreNulls((int64_t *)(leftColumn->getValues()), leftRowIndex, (int64_t *)(rightColumn->getValues()), rightRowIndex);
                break;
            case 3:
                result = doubleValueEqualsIgnoreNulls((double *)(leftColumn->getValues()), leftRowIndex, (double *)(rightColumn->getValues()), rightRowIndex);
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

bool PagesHashStrategy::valuePositionEqualsPosition(VecType type, Vector *leftColumn, int32_t leftRowIndex, Vector *rightColumn, int32_t rightRowIndex)
{
    bool leftIsNull = leftColumn->isValueNull(leftRowIndex);
    bool rightIsNull = rightColumn->isValueNull(rightRowIndex);
    if (leftIsNull || rightIsNull) {
        return leftIsNull && rightIsNull;
    }
    return valueEqualsValueIgnoreNulls(type, leftColumn->getValues(), leftRowIndex, rightColumn->getValues(), rightRowIndex);
}

bool PagesHashStrategy::valueEqualsValueIgnoreNulls(VecType type, void *leftData, int32_t leftIndex, void *rightData, int32_t rightIndex)
{
    bool result;
    switch (type)
    {
    case OMNI_VEC_TYPE_INT:
        result = (((int32_t *)leftData)[leftIndex] == ((int32_t *)rightData)[rightIndex]);
        break;
    case OMNI_VEC_TYPE_LONG:
        result = (((int64_t *)leftData)[leftIndex] == ((int64_t *)rightData)[rightIndex]);
        break;
    case OMNI_VEC_TYPE_DOUBLE:
        if (std::abs(((double *)leftData)[leftIndex] - ((double *)rightData)[rightIndex]) < __DBL_EPSILON__) {
            result = true;
        }
        else {
            result = false;
        }
        break;
     default:
         result = false;
         break;
    }

    return result;
}
