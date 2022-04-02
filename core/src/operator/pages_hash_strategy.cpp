/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash strategy implementations
 */
#include "pages_hash_strategy.h"
#include <memory>
#include "vector/vector_common.h"
#include "optimization.h"
#include "jit/annotation.h"
#include "util/operator_util.h"

using namespace omniruntime::vec;

namespace omniruntime {
namespace op {
PagesHashStrategy::PagesHashStrategy(Vector ***columns, const int32_t *columnTypes, int32_t columnCount,
    int32_t *hashCols, int32_t hashColsCount)
    : buildColumns(columns), buildColumnCount(columnCount), buildHashColsCount(hashColsCount)
{
    if (hashColsCount > 0) {
        this->buildHashColTypes = new int32_t[hashColsCount];
        this->buildHashColumns = new Vector **[hashColsCount];
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

static bool ValueEqualsValueIgnoreNulls(int32_t dataType, Vector *leftVector, int32_t leftRowIndex, Vector *rightVector,
    int32_t rightRowIndex)
{
    switch (dataType) {
        case OMNI_INT:
        case OMNI_DATE32:
            return ValueEqualsValueIgnoreNulls<IntVector>(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            return ValueEqualsValueIgnoreNulls<LongVector>(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_DOUBLE:
            return DoubleValueEqualsValueIgnoreNulls(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_BOOLEAN:
            return ValueEqualsValueIgnoreNulls<BooleanVector>(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            return VarcharValueEqualsValueIgnoreNulls(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_DECIMAL128:
            return ValueEqualsValueIgnoreNulls<Decimal128Vector>(leftVector, leftRowIndex, rightVector, rightRowIndex);
        default:
            return false;
    }
}

SPECIALIZE(OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS)
bool PositionEqualsPositionIgnoreNulls(int32_t leftTableIndex, int32_t leftRowIndex,
    int32_t rightTableIndex, int32_t rightRowIndex, Vector ***buildHashColumns, const int32_t *hashColTypes,
    int32_t hashColCount)
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
bool PositionEqualsRowIgnoreNulls(int32_t buildTableIndex, int32_t buildRowIndex,
    int32_t probePosition, Vector **probeJoinColumns, Vector ***buildHashColumns, const int32_t *hashColTypes,
    int32_t hashColCount)
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
        if (leftIsNull && rightIsNull) {
            continue;
        }
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
}
}