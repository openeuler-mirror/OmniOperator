#ifndef __SORT_API_H__
#define __SORT_API_H__

#include "../operator/sort.h"

// return the jit context address
int64_t sortPrepare(
    int32_t *sourceTypes,
    int32_t typeCount,
    int32_t *outputCols,
    int32_t outputColCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount);

// return the sort operator address
int64_t sortCreateOperator(
    int64_t contextAddress,
    int32_t *sourceTypes,
    int32_t typeCount,
    int32_t *outputCols,
    int32_t outputColCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount);

void sortAddInput(int64_t contextAddress, int64_t sortAddress, int64_t *datas, int64_t *nulls, int32_t pageCount, int32_t *rowCounts, int32_t totalRowCount);

void sortExecute(int64_t contextAddress, int64_t sortAddress);

Table **sortGetOutput(int64_t contextAddress, int64_t sortAddress, int32_t *tableCountAddr);

#endif