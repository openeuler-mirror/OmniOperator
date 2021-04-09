#include "sort_api.h"
#include "../util/debug.h"
#include "../harden/Hammer.h"
#include "../harden/HammerConfig.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include <thread>

using namespace codegen;

typedef int64_t (*jit_createSort)(int32_t *, int32_t, int32_t *, int32_t, int32_t *, int32_t *, int32_t *, int32_t);
typedef void (*jit_quickSort)(int64_t, int32_t *, int32_t *, int32_t *, int32_t *, int32_t, int32_t, int32_t);
typedef void (*jit_allocColumns)(int64_t, int32_t *, int32_t *, int32_t, int32_t);
typedef void (*jit_getResult)(int64_t, int32_t *, int32_t, int64_t, int32_t *, int32_t);

typedef struct JitSortContext
{
    LLJIT *jitter;
    jit_createSort createSortFunc;
    jit_quickSort sortFunc;
    jit_allocColumns allocColumnsFunc;
    jit_getResult getResultFunc;
} JitSortContext;

int64_t sortPrepare(
    int32_t *sourceTypes,
    int32_t typeCount,
    int32_t *outputCols,
    int32_t outputColCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount)
{
    auto start = START();
    map<string, ParamValue *> testParam;
    list<Hammer *> deps = std::list<Hammer *>();
    int sortColTypes[sortColCount];

    for (int32_t i = 0; i < sortColCount; ++i) {
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


    testParam["_Z9compareTolPiS_S_S_iii@1"] = &p_sortCols;
    testParam["_Z9compareTolPiS_S_S_iii@2"] = &p_sortColTypes;
    testParam["_Z9compareTolPiS_S_S_iii@3"] = &p_sortAscendings;
    testParam["_Z9compareTolPiS_S_S_iii@4"] = &p_sortNullFirsts;
    testParam["_Z9compareTolPiS_S_S_iii@5"] = &p_sortColCount;

    testParam["_Z12allocColumnslPiS_ii@1"] = &p_sourceTypes;
    testParam["_Z12allocColumnslPiS_ii@2"] = &p_outputCols;
    testParam["_Z12allocColumnslPiS_ii@3"] = &p_outputColCount;

    testParam["_Z9getResultlPiilS_i@1"] = &p_outputCols;
    testParam["_Z9getResultlPiilS_i@2"] = &p_outputColCount;
    testParam["_Z9getResultlPiilS_i@4"] = &p_sourceTypes;



    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");

    Hammer hammer1("/opt/lib/ir/sort.ll", testParam);
    Hammer hammer2("/opt/lib/ir/memory_pool.ll", testParam);
    hammer1.harden();
    hammer2.harden();
    deps.push_back(&hammer2);


    HammerConfig hammerConfig;
    auto jitter = hammer1.create_jitter(deps, hammerConfig);
    auto createSortFunc = (jit_createSort)(jitter->lookup("_Z10createSortPiiS_iS_S_S_i")->getAddress());
    auto sortFunc = (jit_quickSort)(jitter->lookup("_Z9quickSortlPiS_S_S_iii")->getAddress());
    auto allocColumnsFunc = (jit_allocColumns)(jitter->lookup("_Z12allocColumnslPiS_ii")->getAddress());
    auto getResultFunc = (jit_getResult)(jitter->lookup("_Z9getResultlPiilS_i")->getAddress());

    JitSortContext *jitSortContext = new JitSortContext;
    jitSortContext->createSortFunc = createSortFunc;
    jitSortContext->sortFunc = sortFunc;
    jitSortContext->allocColumnsFunc = allocColumnsFunc;
    jitSortContext->getResultFunc = getResultFunc;
    jitSortContext->jitter = jitter.release();

    PRINT_API("create jit sort context elapsed time: %ld ms\n", END(start));
    return (int64_t)jitSortContext;
}

int64_t sortCreateOperator(
    int64_t contextAddress,
    int32_t *sourceTypes,
    int32_t typeCount,
    int32_t *outputCols,
    int32_t outputColCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount)
{
    auto start = START();
    JitSortContext *jitSortContext = (JitSortContext *)contextAddress;
    int64_t sortAddress;
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

void sortAddInput(int64_t contextAddress, int64_t sortAddress, int64_t *datas, int64_t *nulls, int32_t pageCount, int32_t *rowCounts, int32_t totalRowCount)
{
    auto start = START();
    Sort *sort = (Sort *)sortAddress;
    PagesIndex *pagesIndex = sort->getPagesIndex();

    PRINT_API("before pagesIndex addTable call elapsed time: %ld ms\n", END(start));
    pagesIndex->addTables(datas, nulls, pageCount, rowCounts, totalRowCount);
    PRINT_API("after pagesIndex addTable call elapsed time: %ld ms\n", END(start));
}

void sortExecute(int64_t contextAddress, int64_t sortAddress)
{
    Sort *sort = (Sort *)sortAddress;
    int32_t *sourceTypes = sort->getSourceTypes();
    int32_t *sortCols = sort->getSortCols();
    int32_t sortColCount = sort->getSortColCount();
    PagesIndex *pagesIndex = sort->getPagesIndex();

    int32_t positionCount = pagesIndex->getPositionCount();
    int32_t from = 0;
    int32_t sortColTypes[sortColCount];

    for (int32_t i = 0; i < sortColCount; i++) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    JitSortContext *jitSortContext = (JitSortContext *)contextAddress;
    if (jitSortContext != NULL) {
        auto start = START();
        auto sortFunc = jitSortContext->sortFunc;
        sortFunc((int64_t)pagesIndex,
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
        quickSort((int64_t)pagesIndex,
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

Table *sortGetOutput(int64_t contextAddress, int64_t sortAddress)
{
    auto start = START();
    Sort *sort = (Sort *)sortAddress;
    PagesIndex *pagesIndex = sort->getPagesIndex();
    int32_t positionCount = pagesIndex->getPositionCount();
    int32_t outputColsCount = sort->getOutputColsCount();
    int32_t *outputCols = sort->getOutputCols();
    int32_t *sourceTypes = sort->getSourceTypes();

    Table *outputTable = new Table(positionCount, outputColsCount);

    JitSortContext *jitSortContext = (JitSortContext *)contextAddress;
    if (jitSortContext != NULL) {
        auto start = START();
        auto allocColumnsFunc = jitSortContext->allocColumnsFunc;
        allocColumnsFunc((int64_t)outputTable, sourceTypes, outputCols, outputColsCount, positionCount);
        auto end = START();
        PRINT_API("JIT allocColumns call elapsed time: %ld ms\n", (end - start));

        auto getResultFunc = jitSortContext->getResultFunc;
        getResultFunc((int64_t)pagesIndex, outputCols, outputColsCount, (int64_t)outputTable, sourceTypes, positionCount);
        PRINT_API("JIT getResult call elapsed time: %ld ms\n", END(end));
    }
    else {
        auto start = START();
        allocColumns((int64_t)outputTable, sourceTypes, outputCols, outputColsCount, positionCount);
        auto end = START();
        PRINT_API("ORIGINAL allocColumns call elapsed time: %ld ms\n", (end - start));

        getResult((int64_t)pagesIndex, outputCols, outputColsCount, (int64_t)outputTable, sourceTypes, positionCount);
        PRINT_API("ORIGINAL getResult call elapsed time: %ld ms\n", END(end));
    }

    //outputTable->printTable();

    delete sort;

    return outputTable;
}
