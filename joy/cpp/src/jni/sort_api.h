#ifndef __SORT_API_H__
#define __SORT_API_H__

#include "../operator/sort.h"

// return the jit context address
long sortPrepare(
    int *sourceTypes,
    int typeCount,
    int *outputCols,
    int outputColCount,
    int *sortCols,
    int *sortAscendings,
    int *sortNullFirsts,
    int sortColCount);

// return the sort operator address
long sortCreateOperator(
    long contextAddress,
    int *sourceTypes,
    int typeCount,
    int *outputCols,
    int outputColCount,
    int *sortCols,
    int *sortAscendings,
    int *sortNullFirsts,
    int sortColCount);

void sortAddInput(long contextAddress, long sortAddress, long *datas, long *nulls, int pageCount, long *rowCounts, long totalRowCount);

void sortExecute(long contextAddress, long sortAddress);

Table *sortGetOutput(long contextAddress, long sortAddress);

#endif