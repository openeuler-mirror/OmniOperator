/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash strategy implementations
 */
#ifndef __PAGES_HASH_STRATETY_H__
#define __PAGES_HASH_STRATETY_H__

#include <stdint.h>
#include "../vector/vector.h"
#include "../vector/vector_type.h"
#include "../vector/vector_batch.h"
#include "../vector/int_vector.h"
#include "../vector/long_vector.h"
#include "../vector/double_vector.h"
#include "../vector/varchar_vector.h"
#include "pages_index.h"
#include "hash_util.h"
#include "util/operator_util.h"
#include "../vector/vector_helper.h"

/*
 * select * from t1 join t2 on t1.a1=t2.a1 and t1.b1=t2.b1
 * join columns for build vecBatch t2 is t2.a1 and t2.b1, so column count is 2.
 */
class PagesHashStrategy {
public:
    PagesHashStrategy(omniruntime::vec::Vector ***columns, const int32_t *columnTypes, int32_t columnCount,
        int32_t *joinCols, int32_t joinColsCount);
    ~PagesHashStrategy();

    bool IsPositionNull(int32_t pageIndex, int rowIndex) const
    {
        int32_t originalRowIndex;
        for (int32_t columnIdx = 0; columnIdx < buildHashColsCount; columnIdx++) {
            omniruntime::vec::Vector *vector = buildHashColumns[columnIdx][pageIndex];
            vector = omniruntime::vec::VectorHelper::ExpandVectorAndIndex(vector, rowIndex, originalRowIndex);
            if (vector->IsValueNull(originalRowIndex)) {
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
    int32_t GetBuildHashColsCount() const
    {
        return buildHashColsCount;
    }
    omniruntime::vec::Vector ***GetBuildHashColumns() const
    {
        return buildHashColumns;
    }
    omniruntime::vec::Vector ***GetBuildColumns() const
    {
        return buildColumns;
    }

private:
    omniruntime::vec::Vector ***buildColumns;     // Vector *[colCount][vecBatchCount]
    int32_t buildColumnCount;                     // column count
    int32_t *buildHashColTypes;                   // build hash column types
    omniruntime::vec::Vector ***buildHashColumns; // Vector *[join colCount][vecBatchCount]
    int32_t buildHashColsCount;                   // join column count
};

bool PositionEqualsPositionIgnoreNulls(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex,
    int32_t rightRowIndex, omniruntime::vec::Vector ***buildHashColumns, const int32_t *hashColTypes,
    int32_t hashColCount);
bool PositionEqualsRowIgnoreNulls(int32_t buildTableIndex, int32_t buildRowIndex, int32_t probePosition,
    omniruntime::vec::Vector **probeJoinColumns, omniruntime::vec::Vector ***buildHashColumns,
    const int32_t *hashColTypes, int32_t hashColCount);

#endif
