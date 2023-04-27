/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: hash strategy implementations
 */
#ifndef __PAGES_HASH_STRATEGY_H__
#define __PAGES_HASH_STRATEGY_H__

#include <cstdint>
#include <cstring>
#include "vector/vector.h"
#include "pages_index.h"
#include "hash_util.h"
#include "util/operator_util.h"

namespace omniruntime {
namespace op {
template <typename T> ALWAYS_INLINE T GetValue(vec::BaseVector *vector, uint32_t rowIndex)
{
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        return static_cast<vec::Vector<T> *>(vector)->GetValue(rowIndex);
    } else {
        return reinterpret_cast<vec::Vector<vec::DictionaryContainer<T>> *>(vector)->GetValue(rowIndex);
    }
}

static ALWAYS_INLINE std::string_view GetVarcharValue(vec::BaseVector *vector, uint32_t rowIndex)
{
    using VarcharVector = vec::Vector<vec::LargeStringContainer<std::string_view>>;
    using DictionaryVector = vec::Vector<vec::DictionaryContainer<std::string_view, vec::LargeStringContainer>>;
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        return static_cast<VarcharVector *>(vector)->GetValue(rowIndex);
    } else {
        return reinterpret_cast<DictionaryVector *>(vector)->GetValue(rowIndex);
    }
}

template <typename T>
ALWAYS_INLINE bool PrimitiveValueEqualsValueIgnoreNulls(vec::BaseVector *leftVector, uint32_t leftIndex,
    vec::BaseVector *rightVector, uint32_t rightIndex)
{
    return GetValue<T>(leftVector, leftIndex) == GetValue<T>(rightVector, rightIndex);
}

static ALWAYS_INLINE bool DoubleValueEqualsValueIgnoreNulls(vec::BaseVector *leftVector, uint32_t leftIndex,
    vec::BaseVector *rightVector, uint32_t rightIndex)
{
    double leftValue = GetValue<double>(leftVector, leftIndex);
    double rightValue = GetValue<double>(rightVector, rightIndex);
    if (std::abs(leftValue - rightValue) < __DBL_EPSILON__) {
        return true;
    } else {
        return false;
    }
}

static ALWAYS_INLINE bool VarcharValueEqualsValueIgnoreNulls(vec::BaseVector *leftVector, uint32_t leftIndex,
    vec::BaseVector *rightVector, uint32_t rightIndex)
{
    std::string_view leftValue = GetVarcharValue(leftVector, leftIndex);
    std::string_view rightValue = GetVarcharValue(rightVector, rightIndex);
    if (leftValue.length() != rightValue.length()) {
        return false;
    }
    if (memcmp(leftValue.data(), rightValue.data(), leftValue.length()) == 0) {
        return true;
    } else {
        return false;
    }
}

/*
 * select * from t1 join t2 on t1.a1=t2.a1 and t1.b1=t2.b1
 * join columns for build vecBatch t2 is t2.a1 and t2.b1, so column count is 2.
 */
class PagesHashStrategy {
public:
    PagesHashStrategy(vec::BaseVector ***columns, const DataTypes &buildTypes, int32_t *hashCols,
        int32_t hashColsCount);
    ~PagesHashStrategy();

    bool IsPositionNull(int32_t pageIndex, int rowIndex) const
    {
        for (uint32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
            vec::BaseVector *vector = buildHashColumns[columnIdx][pageIndex];
            if (vector->IsNull(rowIndex)) {
                return true;
            }
        }
        return false;
    }

    bool PositionEqualsRowIgnoreNulls(uint32_t buildTableIndex, uint32_t buildRowIndex, uint32_t probePosition,
        BaseVector **probeJoinColumns);

    bool PositionEqualsPositionIgnoreNulls(uint32_t leftTableIndex, uint32_t leftRowIndex, uint32_t rightTableIndex,
        uint32_t rightRowIndex);

    bool PositionEqualsPosition(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex,
        int32_t rightRowIndex) const;

    int32_t *GetBuildHashColTypes() const
    {
        return buildHashColTypes;
    }
    uint32_t GetBuildHashColsCount() const
    {
        return buildHashColsCount;
    }
    vec::BaseVector ***GetBuildHashColumns() const
    {
        return buildHashColumns;
    }

    uint32_t GetBuildColsCount() const
    {
        return buildColumnCount;
    }
    vec::BaseVector ***GetBuildColumns() const
    {
        return buildColumns;
    }

private:
    vec::BaseVector ***buildColumns;     // Vector *[colCount][vecBatchCount]
    uint32_t buildColumnCount;           // column count
    int32_t *buildHashColTypes;          // build hash column types
    vec::BaseVector ***buildHashColumns; // Vector *[join colCount][vecBatchCount]
    uint32_t buildHashColsCount;         // join column count
};
}
}
#endif
