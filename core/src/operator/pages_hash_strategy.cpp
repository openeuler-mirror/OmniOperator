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

template <typename T>
static ALWAYS_INLINE bool ValueEqualsValueIgnoreNulls(omniruntime::vec::BaseVector *leftVector, uint32_t leftIndex,
    omniruntime::vec::BaseVector *rightVector, uint32_t rightIndex)
{
    return static_cast<Vector<T> *>(leftVector)->GetValue(leftIndex) == static_cast<Vector<T> *>(rightVector)->GetValue(rightIndex);
}

static ALWAYS_INLINE bool DoubleValueEqualsValueIgnoreNulls(omniruntime::vec::BaseVector *leftVector, uint32_t leftIndex,
    omniruntime::vec::BaseVector *rightVector, uint32_t rightIndex)
{
    double leftValue = static_cast<Vector<double> *>(leftVector)->GetValue(leftIndex);
    double rightValue = static_cast<Vector<double> *>(rightVector)->GetValue(rightIndex);
    if (std::abs(leftValue - rightValue) < __DBL_EPSILON__) {
        return true;
    } else {
        return false;
    }
}

static ALWAYS_INLINE bool VarcharValueEqualsValueIgnoreNulls(omniruntime::vec::BaseVector *leftVector, uint32_t leftIndex,
    omniruntime::vec::BaseVector *rightVector, uint32_t rightIndex)
{
    auto leftValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(leftVector)->GetValue(leftIndex);
    auto rightValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(rightVector)->GetValue(rightIndex);
    auto leftLength = leftValue.length();
    if (leftLength != rightValue.length()) {
        return false;
    }
    if (memcmp(leftValue.data(), rightValue.data(), leftLength) == 0) {
        return true;
    } else {
        return false;
    }
}

bool ValueEqualsValueIgnoreNulls(int32_t dataType, BaseVector *leftVector, uint32_t leftRowIndex, BaseVector *rightVector,
    uint32_t rightRowIndex)
{
    switch (dataType) {
        case OMNI_INT:
        case OMNI_DATE32:
            return ValueEqualsValueIgnoreNulls<int32_t>(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_SHORT:
            return ValueEqualsValueIgnoreNulls<int16_t>(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            return ValueEqualsValueIgnoreNulls<int64_t>(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_DOUBLE:
            return DoubleValueEqualsValueIgnoreNulls(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_BOOLEAN:
            return ValueEqualsValueIgnoreNulls<bool>(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            return VarcharValueEqualsValueIgnoreNulls(leftVector, leftRowIndex, rightVector, rightRowIndex);
        case OMNI_DECIMAL128:
            return ValueEqualsValueIgnoreNulls<Decimal128>(leftVector, leftRowIndex, rightVector, rightRowIndex);
        default:
            return false;
    }
}

bool PagesHashStrategy::PositionEqualsPosition(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex,
    int32_t rightRowIndex) const
{
    BaseVector *leftColumn = nullptr;
    BaseVector *rightColumn = nullptr;
    bool leftIsNull = false;
    bool rightIsNull = false;
    bool result = true;

    int32_t originalLeftRowIndex, originalRightRowIndex;
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