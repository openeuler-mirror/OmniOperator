/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash strategy implementations
 */
#include "pages_hash_strategy.h"
#include "../vector/vector_common.h"
#include "optimization.h"
#include "../jit/annotation.h"

#include <memory>

using namespace omniruntime::vec;

PagesHashStrategy::PagesHashStrategy(Vector ***columns, const int32_t *columnTypes, int32_t columnCount,
    int32_t *hashCols, int32_t hashColsCount)
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

inline bool IntValueEqualsValueIgnoreNulls(const int32_t *leftData, int32_t leftIndex, const int32_t *rightData,
    int32_t rightIndex)
{
    bool result = (leftData[leftIndex] == rightData[rightIndex]);
    return result;
}

inline bool Int64ValueEqualsValueIgnoreNulls(const int64_t *leftData, int32_t leftIndex, const int64_t *rightData,
    int32_t rightIndex)
{
    bool result = (leftData[leftIndex] == rightData[rightIndex]);
    return result;
}

inline bool DoubleValueEqualsValueIgnoreNulls(const double *leftData, int32_t leftIndex, const double *rightData,
    int32_t rightIndex)
{
    if (std::abs(leftData[leftIndex] - rightData[rightIndex]) < __DBL_EPSILON__) {
        return true;
    } else {
        return false;
    }
}

bool VarcharValueEqualsValueIgnoreNulls(VarcharVector *leftVector, int32_t leftIndex, VarcharVector *rightVector,
    int32_t rightIndex)
{
    uint8_t *leftValue = nullptr;
    uint8_t *rightValue = nullptr;
    int32_t leftLength = 0;
    int32_t rightLength = 0;

    leftLength = leftVector->GetValue(leftIndex, &leftValue);
    rightLength = rightVector->GetValue(rightIndex, &rightValue);
    if (leftLength != rightLength) {
        return false;
    }
    if (memcmp(leftValue, rightValue, leftLength) == 0) {
        return true;
    } else {
        return false;
    }
}

inline bool ValueEqualsValueIgnoreNulls(int32_t vecType, Vector *leftVector, int32_t leftRowIndex, Vector *rightVector,
    int32_t rightRowIndex)
{
    switch (vecType) {
        case OMNI_VEC_TYPE_INT:
            return IntValueEqualsValueIgnoreNulls((int32_t *)leftVector->GetValues(), leftRowIndex,
                (int32_t *)rightVector->GetValues(), rightRowIndex);
        case OMNI_VEC_TYPE_LONG:
            return Int64ValueEqualsValueIgnoreNulls((int64_t *)leftVector->GetValues(), leftRowIndex,
                (int64_t *)rightVector->GetValues(), rightRowIndex);
        case OMNI_VEC_TYPE_DOUBLE:
            return DoubleValueEqualsValueIgnoreNulls((double *)leftVector->GetValues(), leftRowIndex,
                (double *)rightVector->GetValues(), rightRowIndex);
        case OMNI_VEC_TYPE_VARCHAR:
            return VarcharValueEqualsValueIgnoreNulls(static_cast<VarcharVector *>(leftVector), leftRowIndex,
                static_cast<VarcharVector *>(rightVector), rightRowIndex);
        default:
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

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        if (isSame) {
            rightColumn = leftColumn;
        } else {
            rightColumn = buildHashColumns[columnIdx][rightTableIndex];
        }

        result =
            ValueEqualsValueIgnoreNulls(hashColTypes[columnIdx], leftColumn, leftRowIndex, rightColumn, rightRowIndex);
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

        result = ValueEqualsValueIgnoreNulls(hashColTypes[columnIdx], buildColumn, buildRowIndex, probeColumn,
            probePosition);
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
    bool result = true;

    for (int32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        rightColumn = buildHashColumns[columnIdx][rightTableIndex];
        leftIsNull = leftColumn->IsValueNull(leftRowIndex);
        rightIsNull = rightColumn->IsValueNull(rightRowIndex);
        if (leftIsNull || rightIsNull) {
            return false;
        }

        result = ValueEqualsValueIgnoreNulls(buildHashColTypes[columnIdx], leftColumn, leftRowIndex, rightColumn,
            rightRowIndex);
        if (!result) {
            return false;
        }
    }
    return true;
}
