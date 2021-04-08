#include "sort_api.h"
#include "../util/debug.h"
#include "../harden/Hammer.h"
#include "../harden/HammerConfig.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include <thread>

using namespace codegen;

typedef void (*jit_quickSort)(long, int *, int *, int *, int *, int, uint32_t, uint32_t);
typedef void (*jit_allocColumns)(long, int *, int *, int, uint32_t);
typedef void (*jit_getResult)(long, int *, int, long, int *, uint32_t);
typedef long (*jit_createSort)(int *, int, int *, int, int *, int *, int *, int);

typedef struct JitSortContext
{
    LLJIT *jitter;
    jit_createSort createSortFunc;
    jit_quickSort sortFunc;
    jit_allocColumns allocColumnsFunc;
    jit_getResult getResultFunc;
} JitSortContext;

long sortPrepare(
    int *sourceTypes,
    int typeCount,
    int *outputCols,
    int outputColCount,
    int *sortCols,
    int *sortAscendings,
    int *sortNullFirsts,
    int sortColCount)
{
    auto start = START();
    map<string, ParamValue *> testParam;
    list<Hammer *> deps = std::list<Hammer *>();
    int sortColTypes[sortColCount];

    for (int i = 0; i < sortColCount; ++i) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    ParamValue p_sourceTypes = ParamValue(sourceTypes, typeCount);
    ParamValue p_typeCount = ParamValue(&typeCount);
    ParamValue p_outputCols = ParamValue(outputCols, outputColCount);
    ParamValue p_outputColCount = ParamValue(&outputColCount);
    ParamValue p_sortCols = ParamValue(sortCols, sortColCount);
    ParamValue p_sortColTypes = ParamValue(sortColTypes, sortColCount);
    ParamValue p_sortAscendings = ParamValue(sortAscendings, sortColCount);
    ParamValue p_sortNullFirsts = ParamValue(sortNullFirsts, sortColCount);
    ParamValue p_sortColCount = ParamValue(&sortColCount);

    testParam["_Z10createSortPiiS_iS_S_S_i@0"] = &p_sourceTypes;
    testParam["_Z10createSortPiiS_iS_S_S_i@1"] = &p_typeCount;
    testParam["_Z10createSortPiiS_iS_S_S_i@2"] = &p_outputCols;
    testParam["_Z10createSortPiiS_iS_S_S_i@3"] = &p_outputColCount;
    testParam["_Z10createSortPiiS_iS_S_S_i@4"] = &p_sortCols;
    testParam["_Z10createSortPiiS_iS_S_S_i@5"] = &p_sortAscendings;
    testParam["_Z10createSortPiiS_iS_S_S_i@6"] = &p_sortNullFirsts;
    testParam["_Z10createSortPiiS_iS_S_S_i@7"] = &p_sortColCount;


    testParam["_Z9compareTolPiS_S_S_ijj@1"] = &p_sortCols;
    testParam["_Z9compareTolPiS_S_S_ijj@2"] = &p_sortColTypes;
    testParam["_Z9compareTolPiS_S_S_ijj@3"] = &p_sortAscendings;
    testParam["_Z9compareTolPiS_S_S_ijj@4"] = &p_sortNullFirsts;
    testParam["_Z9compareTolPiS_S_S_ijj@5"] = &p_sortColCount;

    testParam["_Z12allocColumnslPiS_ij@1"] = &p_sourceTypes;
    testParam["_Z12allocColumnslPiS_ij@2"] = &p_outputCols;
    testParam["_Z12allocColumnslPiS_ij@3"] = &p_outputColCount;

    testParam["_Z9getResultlPiilS_j@1"] = &p_outputCols;
    testParam["_Z9getResultlPiilS_j@2"] = &p_outputColCount;
    testParam["_Z9getResultlPiilS_j@4"] = &p_sourceTypes;



    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");

    Hammer hammer1("/opt/lib/ir/sort.ll", testParam);
    Hammer hammer2("/opt/lib/ir/memory_pool.ll", testParam);
    hammer1.harden();
    hammer2.harden();
    deps.push_back(&hammer2);


    HammerConfig hammerConfig;
    auto jitter = hammer1.create_jitter(deps, hammerConfig);
    auto createSortFunc = (jit_createSort)(jitter->lookup("_Z10createSortPiiS_iS_S_S_i")->getAddress());
    auto sortFunc = (jit_quickSort)(jitter->lookup("_Z9quickSortlPiS_S_S_ijj")->getAddress());
    auto allocColumnsFunc = (jit_allocColumns)(jitter->lookup("_Z12allocColumnslPiS_ij")->getAddress());
    auto getResultFunc = (jit_getResult)(jitter->lookup("_Z9getResultlPiilS_j")->getAddress());

    JitSortContext *jitSortContext = new JitSortContext;
    jitSortContext->createSortFunc = createSortFunc;
    jitSortContext->sortFunc = sortFunc;
    jitSortContext->allocColumnsFunc = allocColumnsFunc;
    jitSortContext->getResultFunc = getResultFunc;
    jitSortContext->jitter = jitter.release();

    PRINT_API("create jit sort context elapsed time: %ld ms\n", END(start));
    return (long)jitSortContext;
}

long sortCreateOperator(
    long contextAddress,
    int *sourceTypes,
    int typeCount,
    int *outputCols,
    int outputColCount,
    int *sortCols,
    int *sortAscendings,
    int *sortNullFirsts,
    int sortColCount)
{
    auto start = START();
    JitSortContext *jitSortContext = (JitSortContext *)contextAddress;
    long sortAddress;
    if (jitSortContext != NULL) {
        auto createSortFunc = jitSortContext->createSortFunc;
        sortAddress = createSortFunc(sourceTypes, typeCount, outputCols, outputColCount, sortCols, sortAscendings, sortNullFirsts, sortColCount);
    }
    else {
        sortAddress = createSort(sourceTypes, typeCount, outputCols, outputColCount, sortCols, sortAscendings, sortNullFirsts, sortColCount);
    }
    PRINT_API("create sort operator elapsed time: %ld ms\n", END(start));
    return sortAddress;
}

void sortAddInput(long contextAddress, long sortAddress, long *datas, long *nulls, int pageCount, long *rowCounts, long totalRowCount)
{
    auto start = START();
    Sort *sort = (Sort *)sortAddress;
    PagesIndex *pagesIndex = sort->getPagesIndex();

    PRINT_API("before pagesIndex addTable call elapsed time: %ld ms\n", END(start));
    pagesIndex->addTables(datas, nulls, pageCount, rowCounts, totalRowCount);
    PRINT_API("after pagesIndex addTable call elapsed time: %ld ms\n", END(start));
}

void sortExecute(long contextAddress, long sortAddress)
{
    Sort *sort = (Sort *)sortAddress;
    int *sourceTypes = sort->getSourceTypes();
    int *sortCols = sort->getSortCols();
    int sortColCount = sort->getSortColCount();
    PagesIndex *pagesIndex = sort->getPagesIndex();

    uint32_t  positionCount = pagesIndex->getPositionCount();
    uint32_t from = 0;
    int sortColTypes[sortColCount];

    for (int i = 0; i < sortColCount; i++) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    JitSortContext *jitSortContext = (JitSortContext *)contextAddress;
    if (jitSortContext != NULL) {
        auto start = START();
        auto sortFunc = jitSortContext->sortFunc;
        sortFunc((long)pagesIndex,
            sortCols,
            sortColTypes,
            sort->getSortAscendings(),
            sort->getSortNullFirsts(),
            sortColCount,
            from,
            positionCount);
        PRINT_API("JIT quicksort call elapsed time: %ld ms\n", END(start));
    }
    else {
        auto start = START();
        quickSort((long)pagesIndex,
            sortCols,
            sortColTypes,
            sort->getSortAscendings(),
            sort->getSortNullFirsts(),
            sortColCount,
            from,
            positionCount);
        PRINT_API("ORIGINAL quicksort call elapsed time: %ld ms\n", END(start));
    }
}

Table *sortGetOutput(long contextAddress, long sortAddress)
{
    auto start = START();
    Sort *sort = (Sort *)sortAddress;
    PagesIndex *pagesIndex = sort->getPagesIndex();
    uint32_t positionCount = pagesIndex->getPositionCount();
    int outputColsCount = sort->getOutputColsCount();
    int *outputCols = sort->getOutputCols();
    int *sourceTypes = sort->getSourceTypes();

    Table *outputTable = new Table(positionCount, outputColsCount);

    JitSortContext *jitSortContext = (JitSortContext *)contextAddress;
    if (jitSortContext != NULL) {
        auto start = START();
        auto allocColumnsFunc = jitSortContext->allocColumnsFunc;
        allocColumnsFunc((long)outputTable, sourceTypes, outputCols, outputColsCount, positionCount);
        auto end = START();
        PRINT_API("JIT allocColumns call elapsed time: %ld ms\n", (end - start));

        auto getResultFunc = jitSortContext->getResultFunc;
        getResultFunc((long)pagesIndex, outputCols, outputColsCount, (long)outputTable, sourceTypes, positionCount);
        PRINT_API("JIT getResult call elapsed time: %ld ms\n", END(end));
    }
    else {
        auto start = START();
        allocColumns((long)outputTable, sourceTypes, outputCols, outputColsCount, positionCount);
        auto end = START();
        PRINT_API("ORIGINAL allocColumns call elapsed time: %ld ms\n", (end - start));

        getResult((long)pagesIndex, outputCols, outputColsCount, (long)outputTable, sourceTypes, positionCount);
        PRINT_API("ORIGINAL getResult call elapsed time: %ld ms\n", END(end));
    }

    //outputTable->printTable();

    delete sort;

    return outputTable;
}
