/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#include "pages_hash_strategy.h"
#include "hash_util.h"
#include "pages_index.h"
#include "../vector/vector_common.h"
#include "optimization.h"
#include "../jit/annotation.h"

#include <memory>

using namespace omniruntime::vec;

PagesHashStrategy::PagesHashStrategy(Vector ***columns, int32_t *columnTypes, int32_t columnCount, int32_t *hashCols,
    int32_t hashColsCount)
{
    this->buildColumns = columns;
    this->buildColumnCount = columnCount;
    this->buildHashColsCount = hashColsCount;
    if (hashColsCount > 0) {
        this->buildHashColTypes = std::make_unique<int32_t[]>(hashColsCount).release();
        this->buildHashColumns = std::make_unique<Vector **[]>(hashColsCount).release();
        int32_t hashColumn = 0;
        for (int32_t i = 0; i < hashColsCount; i++) {
            hashColumn = hashCols[i];
            buildHashColumns[i] = columns[hashColumn];
            buildHashColTypes[i] = columnTypes[hashColumn];
        }
    } else {
        this->buildHashColTypes = nullptr;
        this->buildHashColumns = nullptr;
    }
}

PagesHashStrategy::~PagesHashStrategy()
{
    if (buildHashColTypes != nullptr) {
        delete[] buildHashColTypes;
    }
    if (buildHashColumns != nullptr) {
        delete[] buildHashColumns;
    }
}

SPECIALIZE(OMNIJIT_HASH_STRATEGY_HASH_POSITION)
int64_t HashPosition(int32_t vecBatchIdx, int32_t rowIndex, Vector ***buildHashColumns, const int32_t *hashColTypes,
    int32_t hashColCount)
{
    int64_t result = 0;
    Vector *column = nullptr;
    int64_t hash = 0;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        column = buildHashColumns[columnIdx][vecBatchIdx];
        if (column->IsValueNull(rowIndex)) {
            continue;
        }

        switch (hashColTypes[columnIdx]) {
            case OMNI_VEC_TYPE_INT: {
                int32_t intValue = (dynamic_cast<IntVector *>(column))->GetValue(rowIndex);
                hash = HashUtil::HashValue(static_cast<int64_t>(intValue));
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                int64_t int64Value = (dynamic_cast<LongVector *>(column))->GetValue(rowIndex);
                hash = HashUtil::HashValue(int64Value);
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                double doubleValue = (dynamic_cast<DoubleVector *>(column))->GetValue(rowIndex);
                hash = HashUtil::HashValue(static_cast<int64_t>(doubleValue));
                break;
            }
            default: {
                hash = 0;
                break;
            }
        }

        result = HashUtil::GetHash(result, hash);
    }
    return result;
}

bool PagesHashStrategy::IsPositionNull(int32_t pageIndex, int rowIndex) const
{
    Vector *column = nullptr;
    for (int32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        column = buildHashColumns[columnIdx][pageIndex];
        if (column->IsValueNull(rowIndex)) {
            return true;
        }
    }
    return false;
}

bool ValueEqualsValueIgnoreNulls(VecType type, void *leftData, int32_t leftIndex, void *rightData, int32_t rightIndex);

bool IntValueEqualsIgnoreNulls(const int32_t *leftData, int32_t leftIndex, const int32_t *rightData, int32_t rightIndex)
{
    bool result = (leftData[leftIndex] == rightData[rightIndex]);
    return result;
}

bool Int64ValueEqualsIgnoreNulls(const int64_t *leftData, int32_t leftIndex, const int64_t *rightData,
    int32_t rightIndex)
{
    bool result = (leftData[leftIndex] == rightData[rightIndex]);
    return result;
}

bool DoubleValueEqualsIgnoreNulls(const double *leftData, int32_t leftIndex, const double *rightData,
    int32_t rightIndex)
{
    if (std::abs(leftData[leftIndex] - rightData[rightIndex]) < __DBL_EPSILON__) {
        return true;
    } else {
        return false;
    }
}

SPECIALIZE(OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS)
bool PositionEqualsPositionIgnoreNulls(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex,
    int32_t rightRowIndex, Vector ***buildHashColumns, const int32_t *hashColTypes, int32_t hashColCount)
{
    Vector *leftColumn = nullptr;
    Vector *rightColumn = nullptr;
    bool result = true;
    bool isSame = leftTableIndex == rightTableIndex;
    void *leftValues = nullptr;
    void *rightValues = nullptr;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        leftValues = leftColumn->GetValues();
        if (isSame) {
            rightValues = leftValues;
        } else {
            rightColumn = buildHashColumns[columnIdx][rightTableIndex];
            rightValues = rightColumn->GetValues();
        }

        switch (hashColTypes[columnIdx]) {
            case OMNI_VEC_TYPE_INT:
                result = IntValueEqualsIgnoreNulls((int32_t *)leftValues, leftRowIndex, (int32_t *)rightValues,
                    rightRowIndex);
                break;
            case OMNI_VEC_TYPE_LONG:
                result = Int64ValueEqualsIgnoreNulls((int64_t *)leftValues, leftRowIndex, (int64_t *)rightValues,
                    rightRowIndex);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                result = DoubleValueEqualsIgnoreNulls((double *)leftValues, leftRowIndex, (double *)rightValues,
                    rightRowIndex);
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
bool PositionEqualsRowIgnoreNulls(int32_t buildTableIndex, int32_t buildRowIndex, int32_t probePosition,
    Vector **probeJoinColumns, Vector ***buildHashColumns, const int32_t *hashColTypes, int32_t hashColCount)
{
    bool result = true;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        Vector *buildColumn = buildHashColumns[columnIdx][buildTableIndex];
        Vector *probeColumn = probeJoinColumns[columnIdx];

        switch (hashColTypes[columnIdx]) {
            case OMNI_VEC_TYPE_INT:
                result = IntValueEqualsIgnoreNulls((int32_t *)(buildColumn->GetValues()), buildRowIndex,
                    (int32_t *)(probeColumn->GetValues()), probePosition);
                break;
            case OMNI_VEC_TYPE_LONG:
                result = Int64ValueEqualsIgnoreNulls((int64_t *)(buildColumn->GetValues()), buildRowIndex,
                    (int64_t *)(probeColumn->GetValues()), probePosition);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                result = DoubleValueEqualsIgnoreNulls((double *)(buildColumn->GetValues()), buildRowIndex,
                    (double *)(probeColumn->GetValues()), probePosition);
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

bool PagesHashStrategy::PositionEqualsPosition(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex,
    int32_t rightRowIndex) const
{
    Vector *leftColumn = nullptr;
    Vector *rightColumn = nullptr;
    bool leftIsNull = false;
    bool rightIsNull = false;
    int32_t columnType = 0;
    bool result = true;

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
            case OMNI_VEC_TYPE_INT:
                result = IntValueEqualsIgnoreNulls((int32_t *)(leftColumn->GetValues()), leftRowIndex,
                    (int32_t *)(rightColumn->GetValues()), rightRowIndex);
                break;
            case OMNI_VEC_TYPE_LONG:
                result = Int64ValueEqualsIgnoreNulls((int64_t *)(leftColumn->GetValues()), leftRowIndex,
                    (int64_t *)(rightColumn->GetValues()), rightRowIndex);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                result = DoubleValueEqualsIgnoreNulls((double *)(leftColumn->GetValues()), leftRowIndex,
                    (double *)(rightColumn->GetValues()), rightRowIndex);
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

bool PagesHashStrategy::ValuePositionEqualsPosition(VecType type, Vector *leftColumn, int32_t leftRowIndex,
    Vector *rightColumn, int32_t rightRowIndex) const
{
    bool leftIsNull = leftColumn->IsValueNull(leftRowIndex);
    bool rightIsNull = rightColumn->IsValueNull(rightRowIndex);
    if (leftIsNull || rightIsNull) {
        return leftIsNull && rightIsNull;
    }
    return ValueEqualsValueIgnoreNulls(type, leftColumn->GetValues(), leftRowIndex, rightColumn->GetValues(),
        rightRowIndex);
}

bool PagesHashStrategy::ValueEqualsValueIgnoreNulls(VecType type, void *leftData, int32_t leftIndex, void *rightData,
    int32_t rightIndex) const
{
    bool result = false;
    switch (type) {
        case OMNI_VEC_TYPE_INT:
            result = (static_cast<int32_t *>(leftData)[leftIndex] == (static_cast<int32_t *>(rightData))[rightIndex]);
            break;
        case OMNI_VEC_TYPE_LONG:
            result = (static_cast<int64_t *>(leftData)[leftIndex] == (static_cast<int64_t *>(rightData))[rightIndex]);
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            if (std::abs(static_cast<double *>(leftData)[leftIndex] - static_cast<double *>(rightData)[rightIndex]) <
                __DBL_EPSILON__) {
                result = true;
            } else {
                result = false;
            }
            break;
        default:
            result = false;
            break;
    }

    return result;
}
