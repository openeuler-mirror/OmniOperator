/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * @Description: hash strategy implementations
 */
#ifndef __PAGES_HASH_STRATEGY_H__
#define __PAGES_HASH_STRATEGY_H__

#include <cstdint>
#include <cstring>
#include "vector/vector.h"
#include "vector/vector_common.h"
#include "vector/vector_helper.h"
#include "pages_index.h"
#include "hash_util.h"
#include "util/operator_util.h"

namespace omniruntime {
namespace op {
/*
 * select * from t1 join t2 on t1.a1=t2.a1 and t1.b1=t2.b1
 * join columns for build vecBatch t2 is t2.a1 and t2.b1, so column count is 2.
 */
class PagesHashStrategy {
public:
    PagesHashStrategy(omniruntime::vec::BaseVector ***columns, const DataTypes &buildTypes, int32_t *hashCols,
        int32_t hashColsCount);
    ~PagesHashStrategy();

    bool IsPositionNull(int32_t pageIndex, int rowIndex) const
    {
        for (uint32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
            omniruntime::vec::BaseVector *vector = buildHashColumns[columnIdx][pageIndex];
            if (vector->IsNull(rowIndex)) {
                return true;
            }
        }
        return false;
    }

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
    omniruntime::vec::BaseVector ***GetBuildHashColumns() const
    {
        return buildHashColumns;
    }

    uint32_t GetBuildColsCount() const
    {
        return buildColumnCount;
    }
    omniruntime::vec::BaseVector ***GetBuildColumns() const
    {
        return buildColumns;
    }

private:
    omniruntime::vec::BaseVector ***buildColumns;     // BaseVector *[colCount][vecBatchCount]
    uint32_t buildColumnCount;                        // column count
    int32_t *buildHashColTypes;                       // build hash column types
    omniruntime::vec::BaseVector ***buildHashColumns; // BaseVector *[join colCount][vecBatchCount]
    uint32_t buildHashColsCount;                      // join column count
};

bool ValueEqualsValueIgnoreNulls(int32_t dataType, BaseVector *leftBaseVector, uint32_t leftRowIndex,
    BaseVector *rightBaseVector, uint32_t rightRowIndex);
}
}
#endif
