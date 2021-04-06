#include "sort_api.h"
#include "../util/debug.h"
#include "../harden/HammerConfig.h"
#include <thread>

using namespace codegen;

typedef void (*jit_quickSort)(long, int *, int *, int *, int *, int, uint32_t, uint32_t);
typedef void (*jit_allocColumns)(long, int *, int *, int, uint32_t);
typedef void (*jit_getResult)(long, int *, int, long, int *, uint32_t);

typedef struct JitSortContext
{
    LLJIT *jitter;
    jit_quickSort sortFunc;
    jit_allocColumns allocColumnsFunc;
    jit_getResult getResultFunc;
} JitSortContext;

OpTemplateCache<JitSortContext *> g_jitSortContexts;

void createJitSortContext(long stageId, int *sourceTypes, int typeCount, int *outputCols, int outputColCount, 
        int *sortCols, int *sortAscendings, int *sortNullFirsts, int sortColCount)
{
    auto start = START();
    map<string, ParamValue *> testParam;
    list<Hammer *> deps = std::list<Hammer *>();
    int sortColTypes[sortColCount];
    
    for (int i = 0; i < sortColCount; ++i) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    ParamValue p_sortCols = ParamValue(sortCols, sortColCount);
    ParamValue p_sortColTypes = ParamValue(sortColTypes, sortColCount);
    ParamValue p_sortAscendings = ParamValue(sortAscendings, sortColCount);
    ParamValue p_sortNullFirsts = ParamValue(sortNullFirsts, sortColCount);
    ParamValue p_sortColCount = ParamValue(&sortColCount);

    testParam["_Z9compareTolPiS_S_S_ijj@1"] = &p_sortCols;
    testParam["_Z9compareTolPiS_S_S_ijj@2"] = &p_sortColTypes;
    testParam["_Z9compareTolPiS_S_S_ijj@3"] = &p_sortAscendings;
    testParam["_Z9compareTolPiS_S_S_ijj@4"] = &p_sortNullFirsts;
    testParam["_Z9compareTolPiS_S_S_ijj@5"] = &p_sortColCount;

    ParamValue p_sourceTypes = ParamValue(sourceTypes, typeCount);
    ParamValue p_outputCols = ParamValue(outputCols, outputColCount);
    ParamValue p_outputColCount = ParamValue(&outputColCount);
    
    testParam["_Z12allocColumnslPiS_ij@1"] = &p_sourceTypes;
    testParam["_Z12allocColumnslPiS_ij@2"] = &p_outputCols;
    testParam["_Z12allocColumnslPiS_ij@3"] = &p_outputColCount;

    testParam["_Z9getResultlPiilS_j@1"] = &p_outputCols;
    testParam["_Z9getResultlPiilS_j@2"] = &p_outputColCount;
    testParam["_Z9getResultlPiilS_j@4"] = &p_sourceTypes;

    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");
    
    Hammer hammer1("/home/joy/cpp/src/operator/ir/sort.ll", testParam);
    Hammer hammer2("/home/joy/cpp/src/operator/ir/memory_pool.ll", testParam);
  
    hammer1.harden();
   
    deps.push_back(&hammer2);

    HammerConfig hammerConfig;
    auto jitter = hammer1.create_jitter(deps, hammerConfig);
    auto sortFunc = (jit_quickSort)(jitter->lookup("_Z9quickSortlPiS_S_S_ijj")->getAddress());
    auto allocColumnsFunc = (jit_allocColumns)(jitter->lookup("_Z12allocColumnslPiS_ij")->getAddress());
    auto getResultFunc = (jit_getResult)(jitter->lookup("_Z9getResultlPiilS_j")->getAddress());
    
    JitSortContext *jitSortContext = new JitSortContext;
    jitSortContext->sortFunc = sortFunc;
    jitSortContext->allocColumnsFunc = allocColumnsFunc;
    jitSortContext->getResultFunc = getResultFunc;
    jitSortContext->jitter = jitter.release();

    g_jitSortContexts.put(stageId, jitSortContext);
    PRINT_API("create jit sort context elapsed time: %ld ms\n", END(start));
}

long allocAndInitSort(long stageId, 
    int32_t sourceTypes[],
    int32_t typeCount,
    int32_t outputCols[],
    int32_t outputColCount, 
    int32_t sortCols[], 
    int32_t ascendings[], 
    int32_t nullFirsts[],
    int32_t sortColCount)
{
    auto start = START();
#ifdef OPTIMIZE    
    if (!g_jitSortContexts.contains(stageId)) {
    #ifdef OPTIMIZE_BY_ASYNC
        thread threadObj(createJitSortContext, stageId, sourceTypes, typeCount, outputCols, outputColCount, sortCols, ascendings, nullFirsts, sortColCount);
        threadObj.detach();
    #else
        createJitSortContext(stageId, sourceTypes, typeCount, outputCols, outputColCount, sortCols, ascendings, nullFirsts, sortColCount);
    #endif
    }
#endif

    Sort *sort = new Sort(sourceTypes, typeCount, outputCols, outputColCount, sortCols, ascendings, nullFirsts, sortColCount);
    PRINT_API("create sort object elapsed time: %ld ms\n", END(start));
    return (long)sort;
}

void addTable(long sortAddress, long *datas, long *nulls, uint32_t rowNum)
{
    auto start = START();
    Sort *orderBy = (Sort *)sortAddress;
    PagesIndex *pagesIndex = orderBy->getPagesIndex();
    int columnNum = orderBy->getTypescount();
    int *sourceTypes = orderBy->getSourceTypes();

    Table *table = new Table(rowNum, columnNum);
    ColumnType columnType;
    Column *col;
    for (int i = 0; i < columnNum; i++) {
        columnType = getColumnType(sourceTypes[i]);
        col = new Column((void *)datas[i], columnType, rowNum, (int *)nulls[i]);
        table->setColumn(col, columnType);
    }

    PRINT_API("before pagesIndex addTable call elapsed time: %ld ms\n", END(start));
    pagesIndex->addTable(table, columnNum, rowNum);
    PRINT_API("after pagesIndex addTable call elapsed time: %ld ms\n", END(start));
}

void sort(long sortAddress, long stageId)
{
    auto start = START();
    Sort *orderBy = (Sort *)sortAddress;
    int *sourceTypes = orderBy->getSourceTypes();
    int *sortCols = orderBy->getSortCols();
    int sortColCount = orderBy->getSortColCount();
    PagesIndex *pagesIndex = orderBy->getPagesIndex();
    
    uint32_t  positionCount = pagesIndex->getPositionCount();
    uint32_t from = 0;
    int sortColTypes[sortColCount];

    for (int i = 0; i < sortColCount; i++) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    jit_quickSort sortFunc = NULL;
#ifdef OPTIMIZE
    auto jitSortContext = g_jitSortContexts.get(stageId);
    if (jitSortContext) {
        sortFunc = g_jitSortContexts.get(stageId)->sortFunc;
    }
#endif

    if (sortFunc != NULL)
    {
        PRINT_API("before JIT quicksort call elapsed time: %ld ms\n", END(start));
        sortFunc((long)pagesIndex, 
            sortCols, 
            sortColTypes, 
            orderBy->getSortAscendings(), 
            orderBy->getSortNullFirsts(), 
            sortColCount,
            from,
            positionCount);
        PRINT_API("after JIT quicksort call elapsed time: %ld ms\n", END(start));   
    }
    else {
        PRINT_API("before original quicksort call elapsed time: %ld ms\n", END(start));
        quickSort((long)pagesIndex,
            sortCols, 
            sortColTypes,
            orderBy->getSortAscendings(), 
            orderBy->getSortNullFirsts(), 
            sortColCount, 
            from, 
            positionCount);
        PRINT_API("after original quicksort call elapsed time: %ld ms\n", END(start));
    }
}

Table *getResult(long sortAddress, long stageId)
{
    auto start = START();
    Sort *orderBy = (Sort *)sortAddress; 
    PagesIndex *pagesIndex = orderBy->getPagesIndex();
    uint32_t positionCount = pagesIndex->getPositionCount();
    int outputColsCount = orderBy->getOutputColsCount();
    int *outputCols = orderBy->getOutputCols();
    int *sourceTypes = orderBy->getSourceTypes();

    Table *outputTable = new Table(positionCount, outputColsCount);
    jit_allocColumns allocColumnsFunc = NULL;
    jit_getResult getResultFunc = NULL;
#ifdef OPTIMIZE
    auto jitSortContext = g_jitSortContexts.get(stageId);
    if (jitSortContext) {
        allocColumnsFunc = g_jitSortContexts.get(stageId)->allocColumnsFunc;
        getResultFunc = g_jitSortContexts.get(stageId)->getResultFunc;
    }
#endif
    PRINT_API("before allocColumns call elapsed time: %ld ms\n", END(start));
    if (allocColumnsFunc != NULL) {
        allocColumnsFunc((long)outputTable, sourceTypes, outputCols, outputColsCount, positionCount);
    }
    else {
        allocColumns((long)outputTable, sourceTypes, outputCols, outputColsCount, positionCount);
    }

    PRINT_API("before getResult call elapsed time: %ld ms\n", END(start));
    if (getResultFunc != NULL) {
        getResultFunc((long)pagesIndex, outputCols, outputColsCount, (long)outputTable, sourceTypes, positionCount);
    }
    else {
        getResult((long)pagesIndex, outputCols, outputColsCount, (long)outputTable, sourceTypes, positionCount);
    }
    PRINT_API("after getResult call elapsed time: %ld ms\n", END(start));

    delete orderBy;

    return outputTable;
}