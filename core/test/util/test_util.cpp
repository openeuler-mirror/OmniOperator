#include "test_util.h"

bool typesMatch(ColumnType *actualTypes, ColumnType *expectTypes, int32_t columnNumber);
bool columnMatch(Column *actualColumn, Column *expectColumn);

bool tableMatch(Table *outputTables, Table *expectTable)
{
    if (outputTables->getPositionCount() != expectTable->getPositionCount()) {
        return false;
    }

    int32_t columnNumber = outputTables->getColumnNumber();
    if (columnNumber != expectTable->getColumnNumber()) {
        return false;
    }

    if (!typesMatch(outputTables->getColumnTypes(), expectTable->getColumnTypes(), columnNumber)) {
        return false;
    }

    for (int32_t i = 0; i < columnNumber; i++) {
        if (!columnMatch(outputTables->getColumn(i), expectTable->getColumn(i))) {
            return false;
        }
    }

    return true;
}

bool typesMatch(ColumnType *actualTypes, ColumnType *expectTypes, int32_t columnNumber)
{
    for (int32_t i = 0; i < columnNumber; i++) {
        if (actualTypes[i] != expectTypes[i]) {
            return false;
        }
    }

    return true;
}

bool columnMatch(Column *actualColumn, Column *expectColumn)
{
    if (actualColumn->getType() != expectColumn->getType()) {
        return false;
    }

    if (actualColumn->getSize() != expectColumn->getSize()) {
        return false;
    }

    bool result = true;
    for (int32_t i = 0; i < actualColumn->getSize(); i++) {
        void *actualValue = actualColumn->getValue(i);
        void *expectValue = expectColumn->getValue(i);
        switch (actualColumn->getType()) {
            case INT32: {
                int32_t actual = *((int32_t *)actualValue);
                int32_t expect = *((int32_t *)expectValue);
                result = (actual == expect) & result;
                break;
            }
            case INT64: {
                int64_t actual = *((int64_t *)actualValue);
                int64_t expect = *((int64_t *)expectValue);
                result = (actual == expect) & result;
                break;
            }
            case DOUBLE: {
                double actual = *((double *)actualValue);
                double expect = *((double *)expectValue);
                result = (actual == expect) & result;
                break;
            }
            default:
                result = false;
        }
        if (!result) {
            return false;
        }
    }

    return result;
}