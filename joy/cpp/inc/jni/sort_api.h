#ifndef __SORT_API_H__
#define __SORT_API_H__

#include "../operator/sort.h"

long allocAndInitSort(int32_t sourceTypes[],
    int32_t typeCount,
    int32_t outputCols[],
    int32_t outputColCount, 
    int32_t sortCols[], 
    int32_t ascendings[], 
    int32_t nullFirsts[],
    int32_t sortColCount);

void addTable(long sortAddress, long *datas, int **nulls, uint32_t rowNum);

void sort(long sortAddress);

Table *getResult(long sortAddress);

#endif