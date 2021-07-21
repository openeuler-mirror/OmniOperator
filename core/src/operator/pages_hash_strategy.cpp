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

    if(hashColsCount == 0) {
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

SPECIALIZE(OMNIJIT_HASH_STRATEGY_HASH_POSITION)
int64_t hashPosition(int32_t vecBatchIdx, int32_t rowIndex, Vector ***buildHashColumns, int32_t *hashColTypes, int32_t hashColCount)
{
    int64_t result = 0;
    Vector *column;
    int64_t hash;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        column = buildHashColumns[columnIdx][vecBatchIdx];
        if (column->IsValueNull(rowIndex)) {
            continue;
        }

        switch (hashColTypes[columnIdx]) {
            case OMNI_VEC_TYPE_INT: {
                int32_t intValue = ((IntVector *)column)->GetValue(rowIndex);
                hash = HashUtil::hashValue((int64_t)intValue);
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                int64_t int64Value = ((LongVector *)column)->GetValue(rowIndex);
                hash = HashUtil::hashValue(int64Value);
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                double doubleValue = ((DoubleVector *)column)->GetValue(rowIndex);
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
        if (column->IsValueNull(rowIndex)) {
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

SPECIALIZE(OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS)
bool positionEqualsPositionIgnoreNulls(int32_t leftTableIndex,
                                       int32_t leftRowIndex,
                                       int32_t rightTableIndex,
                                       int32_t rightRowIndex,
                                       Vector ***buildHashColumns,
                                       int32_t *hashColTypes,
                                       int32_t hashColCount)
{
    Vector *leftColumn;
    Vector *rightColumn;
    bool result;
    bool isSame = leftTableIndex == rightTableIndex;
    void *leftValues;
    void *rightValues;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        leftValues = leftColumn->GetValues();
        if (isSame) {
            rightValues = leftValues;
        }
        else {
            rightColumn = buildHashColumns[columnIdx][rightTableIndex];
            rightValues = rightColumn->GetValues();
        }

        switch (hashColTypes[columnIdx])
        {
            case 1:
                result = intValueEqualsIgnoreNulls((int32_t *)leftValues, leftRowIndex, (int32_t *)rightValues, rightRowIndex);
                break;
            case 2:
                result = int64ValueEqualsIgnoreNulls((int64_t *)leftValues, leftRowIndex, (int64_t *)rightValues, rightRowIndex);
                break;
            case 3:
                result = doubleValueEqualsIgnoreNulls((double *)leftValues, leftRowIndex, (double *)rightValues, rightRowIndex);
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

SPECIALIZE(OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS)
bool positionEqualsRowIgnoreNulls(int32_t buildTableIndex,
                                  int32_t buildRowIndex,
                                  int32_t probePosition,
                                  Vector **probeJoinColumns,
                                  Vector ***buildHashColumns,
                                  int32_t *hashColTypes,
                                  int32_t hashColCount)
{
    bool result;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        Vector *buildColumn = buildHashColumns[columnIdx][buildTableIndex];
        Vector *probeColumn = probeJoinColumns[columnIdx];

        switch (hashColTypes[columnIdx]) {
            case 1:
                result = intValueEqualsIgnoreNulls((int32_t *)(buildColumn->GetValues()), buildRowIndex, (int32_t *)(probeColumn->GetValues()), probePosition);
                break;
            case 2:
                result = int64ValueEqualsIgnoreNulls((int64_t *)(buildColumn->GetValues()), buildRowIndex, (int64_t *)(probeColumn->GetValues()), probePosition);
                break;
            case 3:
                result = doubleValueEqualsIgnoreNulls((double *)(buildColumn->GetValues()), buildRowIndex, (double *)(probeColumn->GetValues()), probePosition);
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
        leftIsNull = leftColumn->IsValueNull(leftRowIndex);
        rightIsNull = rightColumn->IsValueNull(rightRowIndex);
        if (leftIsNull || rightIsNull) {
            return false;
        }

        columnType = buildHashColTypes[columnIdx];
        switch (columnType) {
            case 1:
                result = intValueEqualsIgnoreNulls((int32_t *)(leftColumn->GetValues()), leftRowIndex, (int32_t *)(rightColumn->GetValues()), rightRowIndex);
                break;
            case 2:
                result = int64ValueEqualsIgnoreNulls((int64_t *)(leftColumn->GetValues()), leftRowIndex, (int64_t *)(rightColumn->GetValues()), rightRowIndex);
                break;
            case 3:
                result = doubleValueEqualsIgnoreNulls((double *)(leftColumn->GetValues()), leftRowIndex, (double *)(rightColumn->GetValues()), rightRowIndex);
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
    bool leftIsNull = leftColumn->IsValueNull(leftRowIndex);
    bool rightIsNull = rightColumn->IsValueNull(rightRowIndex);
    if (leftIsNull || rightIsNull) {
        return leftIsNull && rightIsNull;
    }
    return valueEqualsValueIgnoreNulls(type, leftColumn->GetValues(), leftRowIndex, rightColumn->GetValues(), rightRowIndex);
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
