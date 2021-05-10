#ifndef __SORT_H__
#define __SORT_H__

#include "../native_base.h"
#include "../../vector/table.h"
#include "../../vector/type.h"
#include "../../jit/hammer.h"
#include "../../jit/hammer_config.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include <vector>

using namespace std;

typedef int64_t (*jit_createSort)(int32_t *, int32_t, int32_t *, int32_t, int32_t *, int32_t *, int32_t *, int32_t);

typedef struct JitSortContext
{
    LLJIT *jitter;
    jit_createSort createSortFunc;
} JitSortContext;

class PagesIndex
{
public:
    PagesIndex(int32_t *types, int32_t typesCount);
    ~PagesIndex();
    int32_t addTables(Table **datas, int32_t *rowCounts, int32_t tableCount);
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

class NativeOmniSortOperatorFactory : public NativeOmniOperatorFactory
{
public:
    NativeOmniSortOperatorFactory(
        int32_t *sourceTypes,
        int32_t sourceTypeCount,
        int32_t *outputCols,
        int32_t outputColCount,
        int32_t *sortCols,
        int32_t *sortAscendings,
        int32_t *sortNullFirsts,
        int32_t sortColCount);
    ~NativeOmniSortOperatorFactory();
    static NativeOmniSortOperatorFactory *createNativeOmniSortOperatorFactory(
        int32_t *sourceTypes,
        int32_t sourceTypeCount,
        int32_t *outputCols,
        int32_t outputColCount,
        int32_t *sortCols,
        int32_t *sortAscendings,
        int32_t *sortNullFirsts,
        int32_t sortColCount);
    NativeOmniOperator *createOmniOperator();
    int32_t *getSourceTypes() { return sourceTypes; }
    int32_t getSourceTypeCount() { return sourceTypeCount; }
    int32_t *getOutputCols() { return outputCols; }
    int32_t getOutputColCount() { return outputColCount; }
    int32_t *getSortCols() { return sortCols; }
    int32_t *getSortAscendings() { return sortAscendings; }
    int32_t *getSortNullFirsts() { return sortNullFirsts; }
    int32_t getSortColCount() { return sortColCount; }

private:
    int32_t *sourceTypes;
    int32_t sourceTypeCount;
    int32_t *outputCols;
    int32_t outputColCount;
    int32_t *sortCols;
    int32_t *sortAscendings;
    int32_t *sortNullFirsts;
    int32_t sortColCount;
};

class NativeOmniSortOperator : public NativeOmniOperator
{
public:
    NativeOmniSortOperator(
        int32_t *sourceTypes,
        int32_t typesCount,
        int32_t *outputCols,
        int32_t outputColsCount,
        int32_t *sortCols,
        int32_t *sortAscendings,
        int32_t *sortNullFirsts,
        int32_t sortColCount);
    ~NativeOmniSortOperator();
    int32_t addInput(Table* data, int32_t rowCount) override
    {
        return 0;
    }
    int32_t addInput(Table **datas, int32_t *rowCounts, int32_t pageCount) override;
    int32_t getOutput(vector<Table *>& outputTables) override;

    int32_t *getSourceTypes() { return sourceTypes; }
    int32_t getTypescount() { return typesCount; }
    int32_t *getOutputCols() { return outputCols; }
    int32_t getOutputColsCount() { return outputColsCount; }
    int32_t *getSortCols() { return sortCols; }
    int32_t *getSortAscendings() { return sortAscendings; }
    int32_t *getSortNullFirsts() { return sortNullFirsts; }
    int32_t getSortColCount() { return sortColCount; }
    PagesIndex *getPagesIndex() { return pagesIndex; }

private:
    int32_t *sourceTypes;
    int32_t typesCount;
    int32_t *outputCols;
    int32_t outputColsCount;
    int32_t *sortCols;
    int32_t *sortAscendings;
    int32_t *sortNullFirsts;
    int32_t sortColCount;
    PagesIndex *pagesIndex;
};

typedef NativeOmniOperator * (*sort_module) (NativeOmniSortOperatorFactory *);

int64_t createSort(
    int32_t *sourceTypes,
    int32_t typeCount,
    int32_t *outputCols,
    int32_t outputColCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount);

void quickSort(int64_t pagesIndexAddr, int32_t *sortCols, int32_t *sortColTypes, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount, int32_t from, int32_t to);
ColumnType getColumnType(int32_t colTypeIdx);
void allocColumns(int64_t outputTableAddr, int32_t *sourceTypes, int32_t *outputCols, int32_t outputColCount, int32_t positionCount);
void getResult(int64_t pagesIndexAddr, int32_t *outputCols, int32_t outputColsCount, int64_t outputTableAddr, int32_t *sourceTypes, int32_t offset, int32_t length);

void freeInputTable(Table **inputTables, int32_t inputTableCount);
void freeOutputTable(vector<Table *>& outputTables);
void freeDataInColumn(Table **tables, int32_t tableCount);

#ifdef DEBUG_JNI
#define PRINT_JNI(format, ...) printf("[%s][%s][%d]:" format, __FILE__, __FUNCTION__, __LINE__, __VA_ARGS__)
#else
#define PRINT_JNI(format, ...)
#endif

#ifdef DEBUG_API
#define PRINT_API(format, ...) printf("[%s][%s][%d]:" format, __FILE__, __FUNCTION__, __LINE__, __VA_ARGS__)
#else
#define PRINT_API(format, ...)
#endif

#ifdef DEBUG_IMPL
#define PRINT_IMPL(format, ...) printf("[%s][%s][%d]:" format, __FILE__, __FUNCTION__, __LINE__, __VA_ARGS__)
#else
#define PRINT_IMPL(format, ...)
#endif

#endif