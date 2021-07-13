#ifndef __PAGES_HASH_STRATETY_H__
#define __PAGES_HASH_STRATETY_H__

#include <stdint.h>
#include <vector>
#include "../vector/vector.h"
#include "../vector/vector_type.h"
#include "../vector/vector_batch.h"
#include "pages_index.h"

/*
 * select * from t1 join t2 on t1.a1=t2.a1 and t1.b1=t2.b1
 * join columns for build vecBatch t2 is t2.a1 and t2.b1, so column count is 2.
 */
class PagesHashStrategy
{
public:
    PagesHashStrategy(Vector ***columns, int32_t *columnTypes, int32_t columnCount, int32_t *joinCols, int32_t joinColsCount);
    ~PagesHashStrategy();
    bool isPositionNull(int32_t pageIndex, int32_t rowIndex);
    bool positionEqualsPosition(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex, int32_t rightRowIndex);
    bool valuePositionEqualsPosition(VecType type, Vector *leftColumn, int32_t leftRowIndex, Vector *rightColumn, int32_t rightRowIndex);
    bool valueEqualsValueIgnoreNulls(VecType type, void *leftData, int32_t leftIndex, void *rightData, int32_t rightIndex);
    int32_t *getBuildHashColTypes()
    {
        return buildHashColTypes;
    }
    int32_t getBuildHashColsCount()
    {
        return buildHashColsCount;
    }
    Vector ***getBuildHashColumns()
    {
        return buildHashColumns;
    }
    Vector ***getBuildColumns()
    {
        return buildColumns;
    }

private:
    Vector ***buildColumns; // Vector *[colCount][vecBatchCount]
    int32_t buildColumnCount; // column count
    int32_t *buildHashColTypes; // build hash column types
    Vector ***buildHashColumns; // Vector *[join colCount][vecBatchCount]
    int32_t buildHashColsCount; // join column count
};

int64_t hashPosition(int32_t vecBatchIdx, int32_t rowIndex, Vector ***buildHashColumns, int32_t *hashColTypes, int32_t hashColCount);
bool positionEqualsPositionIgnoreNulls(int32_t leftTableIndex,
                                       int32_t leftRowIndex,
                                       int32_t rightTableIndex,
                                       int32_t rightRowIndex,
                                       Vector ***buildHashColumns,
                                       int32_t *hashColTypes,
                                       int32_t hashColCount);
bool positionEqualsRowIgnoreNulls(int32_t buildTableIndex,
                                  int32_t buildRowIndex,
                                  int32_t probePosition,
                                  Vector **probeJoinColumns,
                                  Vector ***buildHashColumns,
                                  int32_t *hashColTypes,
                                  int32_t hashColCount);
#endif
