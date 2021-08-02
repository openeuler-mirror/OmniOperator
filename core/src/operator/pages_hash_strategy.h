/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#ifndef __PAGES_HASH_STRATETY_H__
#define __PAGES_HASH_STRATETY_H__

#include <stdint.h>
#include "../vector/vector.h"
#include "../vector/vector_type.h"
#include "../vector/vector_batch.h"
#include "pages_index.h"

/*
 * select * from t1 join t2 on t1.a1=t2.a1 and t1.b1=t2.b1
 * join columns for build vecBatch t2 is t2.a1 and t2.b1, so column count is 2.
 */
class PagesHashStrategy {
public:
    PagesHashStrategy(omniruntime::vec::Vector ***columns, int32_t *columnTypes, int32_t columnCount, int32_t *joinCols,
        int32_t joinColsCount);
    ~PagesHashStrategy();
    bool IsPositionNull(int32_t pageIndex, int32_t rowIndex) const;
    bool PositionEqualsPosition(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex,
        int32_t rightRowIndex) const;
    bool ValuePositionEqualsPosition(omniruntime::vec::VecType type, omniruntime::vec::Vector *leftColumn,
        int32_t leftRowIndex, omniruntime::vec::Vector *rightColumn, int32_t rightRowIndex) const;
    bool ValueEqualsValueIgnoreNulls(omniruntime::vec::VecType type, void *leftData, int32_t leftIndex, void *rightData,
        int32_t rightIndex) const;
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
    int32_t buildColumnCount;   // column count
    int32_t *buildHashColTypes; // build hash column types
    omniruntime::vec::Vector ***buildHashColumns; // Vector *[join colCount][vecBatchCount]
    int32_t buildHashColsCount; // join column count
};

int64_t HashPosition(int32_t vecBatchIdx, int32_t rowIndex, omniruntime::vec::Vector ***buildHashColumns,
    const int32_t *hashColTypes, int32_t hashColCount);
bool PositionEqualsPositionIgnoreNulls(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex,
    int32_t rightRowIndex, omniruntime::vec::Vector ***buildHashColumns,
    const int32_t *hashColTypes, int32_t hashColCount);
bool PositionEqualsRowIgnoreNulls(int32_t buildTableIndex, int32_t buildRowIndex, int32_t probePosition,
    omniruntime::vec::Vector **probeJoinColumns, omniruntime::vec::Vector ***buildHashColumns,
    const int32_t *hashColTypes, int32_t hashColCount);
#endif
