#ifndef __PAGES_INDEX_H__
#define __PAGES_INDEX_H__

#include "../vector/table.h"

#include <stdint.h>

class PagesIndex
{
public:
    PagesIndex(int32_t *types, int32_t typesCount);
    ~PagesIndex();
    int32_t addTables(Table **datas, int32_t *rowCounts, int32_t tableCount);
    void sort(
        int32_t *sortCols,
        int32_t *sortColTypes,
        int32_t *sortAscendings,
        int32_t *sortNullFirsts,
        int32_t sortColCount,
        int32_t from,
        int32_t to);
    void getOutput(int32_t *outputCols, int32_t outputColsCount, int64_t outputTableAddr, int32_t *sourceTypes, int32_t offset, int32_t length);

    int32_t *getTypes()
    {
        return types;
    };
    int32_t getTypesCount() {
        return typesCount;
    };
    int64_t *getValueAddresses()
    {
        return this->valueAddresses;
    }
    int32_t getPositionCount()
    {
        return this->positionCount;
    };
    Column ***getColumns()
    {
        return this->columns;
    }
    int32_t getTablesCount()
    {
        return this->tablesCount;
    }

private:
    int32_t *types;
    int32_t typesCount;
    Column ***columns; // Column *  [columnCount][tableCount]
    int32_t tablesCount;
    int64_t *valueAddresses;
    int32_t positionCount;
};

int64_t encodeSyntheticAddress(int32_t sliceIndex, int32_t sliceOffset);
int32_t decodeSliceIndex(int64_t sliceAddress);
int32_t decodePosition(int64_t sliceAddress);

#endif
