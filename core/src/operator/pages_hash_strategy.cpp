/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash strategy implementations
 */
#include "pages_hash_strategy.h"
#include "../vector/vector_common.h"
#include "optimization.h"
#include "../jit/annotation.h"
#include "util/operator_util.h"

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

template <typename V>
ALWAYS_INLINE bool ValueEqualsValueIgnoreNulls(Vector *leftVector, int32_t leftIndex, Vector *rightVector,
    int32_t rightIndex)
{
    return static_cast<V *>(leftVector)->GetValue(leftIndex) == static_cast<V *>(rightVector)->GetValue(rightIndex);
}

ALWAYS_INLINE bool DoubleValueEqualsValueIgnoreNulls(Vector *leftVector, int32_t leftIndex, Vector *rightVector,
    int32_t rightIndex)
{
    double leftValue = static_cast<DoubleVector *>(leftVector)->GetValue(leftIndex);
    double rightValue = static_cast<DoubleVector *>(rightVector)->GetValue(rightIndex);
    if (std::abs(leftValue - rightValue) < __DBL_EPSILON__) {
        return true;
    } else {
        return false;
    }
}

ALWAYS_INLINE bool VarcharValueEqualsValueIgnoreNulls(Vector *leftVector, int32_t leftIndex, Vector *rightVector,
    int32_t rightIndex)
{
    uint8_t *leftValue = nullptr;
    uint8_t *rightValue = nullptr;
    int32_t leftLength = 0;
    int32_t rightLength = 0;

    leftLength = static_cast<VarcharVector *>(leftVector)->GetValue(leftIndex, &leftValue);
    rightLength = static_cast<VarcharVector *>(rightVector)->GetValue(rightIndex, &rightValue);
    if (leftLength != rightLength) {
        return false;
    }
    if (memcmp(leftValue, rightValue, leftLength) == 0) {
        return true;
    } else {
        return false;
    }
}

ALWAYS_INLINE bool ValueEqualsValueIgnoreNulls(int32_t vecType, Vector *leftVector, int32_t leftRowIndex,
    Vector *rightVector, int32_t rightRowIndex)
{
    switch (vecType) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32:
            return ValueEqualsValueIgnoreNulls<IntVector>(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64:
            return ValueEqualsValueIgnoreNulls<LongVector>(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_VEC_TYPE_DOUBLE:
            return DoubleValueEqualsValueIgnoreNulls(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_VEC_TYPE_BOOLEAN:
            return ValueEqualsValueIgnoreNulls<BooleanVector>(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_VEC_TYPE_VARCHAR:
            return VarcharValueEqualsValueIgnoreNulls(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_VEC_TYPE_DECIMAL128:
            return ValueEqualsValueIgnoreNulls<Decimal128Vector>(leftVector, leftRowIndex, rightVector, rightRowIndex);
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

    int32_t originalLeftRowIndex, originalRightRowIndex;
    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        leftColumn = VectorHelper::ExpandVectorAndIndex(leftColumn, leftRowIndex, originalLeftRowIndex);
        rightColumn = buildHashColumns[columnIdx][rightTableIndex];
        rightColumn = VectorHelper::ExpandVectorAndIndex(rightColumn, rightRowIndex, originalRightRowIndex);

        result = ValueEqualsValueIgnoreNulls(hashColTypes[columnIdx], leftColumn, originalLeftRowIndex, rightColumn,
            originalRightRowIndex);
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
    int32_t originalBuildRowIndex, originalProbeRowIndex;
    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        Vector *buildColumn = buildHashColumns[columnIdx][buildTableIndex];
        Vector *probeColumn = probeJoinColumns[columnIdx];
        buildColumn = VectorHelper::ExpandVectorAndIndex(buildColumn, buildRowIndex, originalBuildRowIndex);
        probeColumn = VectorHelper::ExpandVectorAndIndex(probeColumn, probePosition, originalProbeRowIndex);
        result = ValueEqualsValueIgnoreNulls(hashColTypes[columnIdx], buildColumn, originalBuildRowIndex, probeColumn,
            originalProbeRowIndex);
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

    int32_t originalLeftRowIndex, originalRightRowIndex;
    for (int32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        rightColumn = buildHashColumns[columnIdx][rightTableIndex];
        leftColumn = VectorHelper::ExpandVectorAndIndex(leftColumn, leftRowIndex, originalLeftRowIndex);
        rightColumn = VectorHelper::ExpandVectorAndIndex(rightColumn, rightRowIndex, originalRightRowIndex);
        leftIsNull = leftColumn->IsValueNull(originalLeftRowIndex);
        rightIsNull = rightColumn->IsValueNull(originalRightRowIndex);
        if (leftIsNull || rightIsNull) {
            return false;
        }

        result = ValueEqualsValueIgnoreNulls(buildHashColTypes[columnIdx], leftColumn, originalLeftRowIndex,
            rightColumn, originalRightRowIndex);
        if (!result) {
            return false;
        }
    }
    return true;
}
