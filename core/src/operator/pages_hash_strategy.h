#ifndef __PAGES_HASH_STRATETY_H__
#define __PAGES_HASH_STRATETY_H__

#include <stdint.h>
#include "../vector/table.h"

/*
 * select * from t1 join t2 on t1.a1=t2.a1 and t1.b1=t2.b1
 * join columns for build table t2 is t2.a1 and t2.b1, so column count is 2.
 */
class PagesHashStrategy
{
public:
    PagesHashStrategy(Column ***columns, int32_t tableCount, int32_t columnCount, int32_t *joinCols, int32_t joinColsCount);
    ~PagesHashStrategy();
    int64_t hashPosition(int32_t tableIndex, int32_t rowIndex);
    int64_t hashRow(int32_t rowIndex, Table *table);
    bool isPositionNull(int32_t tableIndex, int32_t rowIndex);
    bool positionEqualsPositionIgnoreNulls(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex, int32_t rightRowIndex);
    bool positionEqualsRowIgnoreNulls(int32_t buildTableIndex, int32_t buildRowIndex, int32_t probePosition, Column **joinColumns, int32_t joinColumnsCount);
    bool positionEqualsPosition(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex, int32_t rightRowIndex);
    bool valuePositionEqualsPosition(ColumnType type, Column *leftColumn, int32_t leftRowIndex, Column *rightColumn, int32_t rightRowIndex);
    bool valueEqualsValueIgnoreNulls(ColumnType type, void *leftData, int32_t leftIndex, void *rightData, int32_t rightIndex);

        Column ***getBuildColumns()
    {
        return buildColumns;
    }

private:
    Column ***buildColumns; // Column *[colCount][tableCount]
    int32_t buildTableCount;
    int32_t buildColumnCount; // column count
    //int32_t *joinCols;
    Column ***buildHashColumns; // Column *[join colCount][tableCount]
    int32_t buildHashColsCount; // join column count
};

#endif
