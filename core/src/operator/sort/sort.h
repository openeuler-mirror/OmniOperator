#ifndef __SORT_H__
#define __SORT_H__

#include "../../vector/table.h"
#include "../../vector/type.h"
#include "../op_template.h"
#include <vector>

using namespace std;

class SortOrder
{
public:
    SortOrder(){}
    SortOrder(int32_t ascending, int32_t nullsFirst) : ascending(ascending), nullsFirst(nullsFirst) {}
    ~SortOrder() {}
    int32_t isAscending() { return this->ascending; }
    int32_t isNullsFirst() { return this->nullsFirst; }
    void setAscending(int32_t ascending) { this->ascending = ascending; }
    void setNullsFirst(int32_t nullsFirst) { this->nullsFirst = nullsFirst; }
    int32_t compareValue(Column *leftColumn, int32_t leftPosition, Column *rightColumn, int32_t rightPosition);

private:
    int32_t ascending;
    int32_t nullsFirst;
};

class PagesIndex
{
public:
    PagesIndex(int32_t *sourceTypes, int32_t typesCount);
    ~PagesIndex();
    void addTables(int64_t *datas, int64_t *nulls, int32_t pageCount, int32_t *rowCounts, int32_t totalRowCount);

    int32_t *getColumnTypes()
    {
        return types;
    };
    int32_t getTypeCount() {
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
    int32_t getTableCount()
    {
        return this->tableCount;
    }

private:
    int32_t *types;
    int32_t typesCount;
    int64_t *valueAddresses;
    int32_t positionCount;
    Column ***columns; // columns *[colCount][tableCount]
    int32_t tableCount;
};

class SimplePagesIndexComparator
{
public:
    SimplePagesIndexComparator(int32_t *sortCols, SortOrder *sortOrder, ColumnType *sortTypes)
        : sortCols(sortCols), sortOrder(sortOrder), sortTypes(sortTypes) {}
    ~SimplePagesIndexComparator() {}
    int32_t compareTo(PagesIndex *pagesIndex, int32_t leftPosition, int32_t rightPosition);

private:
    int32_t *sortCols;
    SortOrder *sortOrder;
    ColumnType *sortTypes;
};

class Sort : public OpTemplate
{
public:
    Sort(int32_t *sourceTypes,
        int32_t typesCount,
        int32_t *outputCols,
        int32_t outputColsCount,
        int32_t *sortCols,
        int32_t *sortAscendings,
        int32_t *sortNullFirsts,
        int32_t sortColCount);
    ~Sort();
    void preloop(Table *table);
    void inloop(Table *table, uint32_t rowIdx);
    void postloop(Table *table);
    void process(Table *table, uint32_t rowIdx);
    Table *getResult() {};
    void createPagesIndex(int32_t *sourceTypes, int32_t typesCount, int64_t *datas, int64_t *nulls, int32_t pageCount, int64_t *rowCounts, int64_t totalRowCount);

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