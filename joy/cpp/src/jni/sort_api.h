#ifndef __SORT_API_H__
#define __SORT_API_H__

#include "../operator/sort.h"
#include "../harden/Hammer.h"
#include "../harden/HammerConfig.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"

typedef int64_t (*jit_createSort)(int32_t *, int32_t, int32_t *, int32_t, int32_t *, int32_t *, int32_t *, int32_t);
typedef void (*jit_quickSort)(int64_t, int32_t *, int32_t *, int32_t *, int32_t *, int32_t, int32_t, int32_t);
typedef void (*jit_allocColumns)(int64_t, int32_t *, int32_t *, int32_t, int32_t);
typedef void (*jit_getResult)(int64_t, int32_t *, int32_t, int64_t, int32_t *, int32_t, int32_t);

typedef struct JitSortContext
{
    LLJIT *jitter;
    jit_createSort createSortFunc;
    jit_quickSort sortFunc;
    jit_allocColumns allocColumnsFunc;
    jit_getResult getResultFunc;
} JitSortContext;

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

void freeOutputTable(Table **outputTable, int32_t tableCount);

#endif