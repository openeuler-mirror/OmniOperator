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
typedef void (*jit_getResult)(int64_t, int32_t *, int32_t, int64_t, int32_t *, int32_t, int32_t);

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

    testParam["_Z9getResultlPiilS_ii@1"] = &p_outputCols;
    testParam["_Z9getResultlPiilS_ii@2"] = &p_outputColCount;
    testParam["_Z9getResultlPiilS_ii@4"] = &p_sourceTypes;



    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");
    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/local/lib/libjemalloc.so.2");

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
    auto getResultFunc = (jit_getResult)(jitter->lookup("_Z9getResultlPiilS_ii")->getAddress());

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

    if (pagesIndex->getTableCount() == 0) {
        return;
    }

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

int32_t DEFAULT_MAX_PAGE_SIZE_IN_BYTES = 1 * 1024 * 1024;

int32_t getMaxRowCount(int32_t *sourceTypes, int32_t *outputCols, int32_t outputColsCount) 
{
    int32_t rowSize = 0;
    int type;
    for (int32_t i = 0; i < outputColsCount; i++) {
        type = sourceTypes[outputCols[i]];
        switch (type)
        {
        case 1:
            rowSize = rowSize + 1 + sizeof(int32_t);
            break;
        case 2:
            rowSize = rowSize + 1 + sizeof(int64_t);
            break;
        case 3:
            rowSize = rowSize + 1 + sizeof(double);
            break;    
        default:
            break;
        }
    }
    
    int32_t maxRowCount = (DEFAULT_MAX_PAGE_SIZE_IN_BYTES + rowSize - 1) / rowSize;
    return maxRowCount;
}

int32_t getTableCount(int32_t positionCount, int32_t maxRowCount)
{
    return ((positionCount + maxRowCount - 1) / maxRowCount);
}

Table **sortGetOutput(int64_t contextAddress, int64_t sortAddress, int32_t *tableCountAddr)
{
    JitSortContext *jitSortContext = (JitSortContext *)contextAddress;
    Sort *sort = (Sort *)sortAddress;
    PagesIndex *pagesIndex = sort->getPagesIndex();
    int32_t positionCount = pagesIndex->getPositionCount();
    int32_t outputColsCount = sort->getOutputColsCount();
    int32_t *outputCols = sort->getOutputCols();
    int32_t *sourceTypes = sort->getSourceTypes();

    if (positionCount == 0) {
        return NULL;
    }

    int32_t maxRowCount = getMaxRowCount(sourceTypes, outputCols, outputColsCount);
    int32_t tableCount = getTableCount(positionCount, maxRowCount);
    *tableCountAddr = tableCount;

    Table **result = (Table **)malloc(sizeof(Table *) * tableCount);
    Table *outputTable = NULL;
    int32_t position = 0;
    int32_t rowCount = 0;
    for (int32_t i = 0; i < tableCount; i++) {
        rowCount = min(maxRowCount, positionCount - position);
        outputTable = new Table(rowCount, outputColsCount);
        if (jitSortContext != NULL) {
            auto start = START();
            auto allocColumnsFunc = jitSortContext->allocColumnsFunc;
            auto getResultFunc = jitSortContext->getResultFunc;
            allocColumnsFunc((int64_t)outputTable, sourceTypes, outputCols, outputColsCount, rowCount);
            PRINT_API("JIT allocColumns call elapsed time: %ld ms\n", END(start));
            getResultFunc((int64_t)pagesIndex, outputCols, outputColsCount, (int64_t)outputTable, sourceTypes, position, rowCount);
            PRINT_API("JIT getResult call elapsed time: %ld ms\n", END(start));
        }
        else {
            auto start = START();
            allocColumns((int64_t)outputTable, sourceTypes, outputCols, outputColsCount, rowCount);
            PRINT_API("ORIGINAL allocColumns call elapsed time: %ld ms\n", END(start));
            getResult((int64_t)pagesIndex, outputCols, outputColsCount, (int64_t)outputTable, sourceTypes, position, rowCount);
            PRINT_API("ORIGINAL getResult call elapsed time: %ld ms\n", END(start));
        }
        position += rowCount;   
        result[i] = outputTable;
    }
    return result;
}
