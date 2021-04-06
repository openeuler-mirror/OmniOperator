#ifndef __SORT_API_H__
#define __SORT_API_H__

#include "../harden/Hammer.h"
#include "../operator/sort.h"
#include "../util/op_template_cache.h"

long allocAndInitSort(long stageId,
    int32_t sourceTypes[],
    int32_t typeCount,
    int32_t outputCols[],
    int32_t outputColCount, 
    int32_t sortCols[], 
    int32_t ascendings[], 
    int32_t nullFirsts[],
    int32_t sortColCount);

void addTable(long sortAddress, long *datas, long *nulls, uint32_t rowNum);

void sort(long sortAddress, long stageId);

Table *getResult(long sortAddress, long stageId);

#endif