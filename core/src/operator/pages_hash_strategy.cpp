/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash strategy implementations
 */
#include "pages_hash_strategy.h"
#include <memory>
#include "vector/vector.h"
#include "util/operator_util.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace omniruntime {
namespace op {
PagesHashStrategy::PagesHashStrategy(BaseVector ***columns, const DataTypes &columnTypes, int32_t *hashCols,
    int32_t hashColsCount)
    : buildColumns(columns), buildColumnCount(columnTypes.GetSize()), buildHashColsCount(hashColsCount)
{
    if (hashColsCount > 0) {
        this->buildHashColTypes = new int32_t[hashColsCount];
        this->buildHashColumns = new BaseVector **[hashColsCount];
        int32_t hashColumn = 0;
        for (int32_t i = 0; i < hashColsCount; i++) {
            hashColumn = hashCols[i];
            buildHashColumns[i] = columns[hashColumn];
            buildHashColTypes[i] = (columnTypes.GetType(hashColumn))->GetId();
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

template <DataTypeId typeId>
static bool ValueEqualsValueIgnoreNulls(BaseVector *leftVector, uint32_t leftRowIndex, BaseVector *rightVector,
    uint32_t rightRowIndex)
{
    using T = typename NativeType<typeId>::type;
    if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, uint8_t>) {
        return VarcharValueEqualsValueIgnoreNulls(leftVector, leftRowIndex, rightVector, rightRowIndex);
    } else if constexpr (std::is_same_v<T, double>) {
        return DoubleValueEqualsValueIgnoreNulls(leftVector, leftRowIndex, rightVector, rightRowIndex);
    } else {
        return PrimitiveValueEqualsValueIgnoreNulls<T>(leftVector, leftRowIndex, rightVector, rightRowIndex);
    }
}

bool PagesHashStrategy::PositionEqualsPositionIgnoreNulls(uint32_t leftTableIndex, uint32_t leftRowIndex,
    uint32_t rightTableIndex, uint32_t rightRowIndex)
{
    BaseVector *leftColumn = nullptr;
    BaseVector *rightColumn = nullptr;
    bool result = true;

    for (uint32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        // todo:: handle dictionary
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        rightColumn = buildHashColumns[columnIdx][rightTableIndex];
        result = DYNAMIC_TYPE_DISPATCH(ValueEqualsValueIgnoreNulls, buildHashColTypes[columnIdx], leftColumn,
            leftRowIndex, rightColumn, rightRowIndex);
        if (!result) {
            return false;
        }
    }
    return true;
}

bool PagesHashStrategy::PositionEqualsRowIgnoreNulls(uint32_t buildTableIndex, uint32_t buildRowIndex,
    uint32_t probePosition, BaseVector **probeJoinColumns)
{
    bool result = true;
    for (uint32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        BaseVector *buildColumn = buildHashColumns[columnIdx][buildTableIndex];
        BaseVector *probeColumn = probeJoinColumns[columnIdx];
        // todo:: handle dictionary
        result = DYNAMIC_TYPE_DISPATCH(ValueEqualsValueIgnoreNulls, buildHashColTypes[columnIdx], buildColumn,
            buildRowIndex, probeColumn, probePosition);
        if (!result) {
            return false;
        }
    }

    return true;
}

bool PagesHashStrategy::PositionEqualsPosition(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex,
    int32_t rightRowIndex) const
{
    BaseVector *leftColumn = nullptr;
    BaseVector *rightColumn = nullptr;
    bool leftIsNull = false;
    bool rightIsNull = false;
    bool result = true;

    for (uint32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
        leftColumn = buildHashColumns[columnIdx][leftTableIndex];
        rightColumn = buildHashColumns[columnIdx][rightTableIndex];
        leftIsNull = leftColumn->IsNull(leftRowIndex);
        rightIsNull = rightColumn->IsNull(rightRowIndex);
        if (leftIsNull && rightIsNull) {
            continue;
        }
        if (leftIsNull || rightIsNull) {
            return false;
        }

        // todo:: handle dictionary
        result = DYNAMIC_TYPE_DISPATCH(ValueEqualsValueIgnoreNulls, buildHashColTypes[columnIdx], leftColumn,
            leftRowIndex, rightColumn, rightRowIndex);
        if (!result) {
            return false;
        }
    }
    return true;
}
}
}