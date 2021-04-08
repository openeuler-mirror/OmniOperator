#ifndef __SORT_H__
#define __SORT_H__

#include "../data/table.h"
#include "../data/type.h"
#include "op_template.h"
#include <vector>

using namespace std;

class SortOrder
{
public:
    SortOrder(){}
    SortOrder(int ascending, int nullsFirst) : ascending(ascending), nullsFirst(nullsFirst) {}
    ~SortOrder() {}
    int isAscending() { return this->ascending; }
    int isNullsFirst() { return this->nullsFirst; }
    void setAscending(int ascending) { this->ascending = ascending; }
    void setNullsFirst(int nullsFirst) { this->nullsFirst = nullsFirst; }
    int compareValue(Column *leftColumn, uint32_t leftPosition, Column *rightColumn, uint32_t rightPosition);

private:
    int ascending;
    int nullsFirst;
};

class PagesIndex
{
public:
    PagesIndex(int *sourceTypes, int typesCount);
    ~PagesIndex();
    void addTables(long *datas, long *nulls, int pageCount, long *rowCounts, long totalRowCount);

    int *getColumnTypes()
    {
        return types;
    };
    int getTypeCount() {
        return typesCount;
    };
    long *getValueAddresses()
    {
        return this->valueAddresses;
    }
    long getPositionCount()
    {
        return this->positionCount;
    };
    Column ***getColumns()
    {
        return this->columns;
    }
    int getTableCount()
    {
        return this->tableCount;
    }

private:
    int *types;
    int typesCount;
    long *valueAddresses;
    long positionCount;
    Column ***columns; // columns *[colCount][tableCount]
    int tableCount;
};

class SimplePagesIndexComparator
{
public:
    SimplePagesIndexComparator(int *sortCols, SortOrder *sortOrder, ColumnType *sortTypes)
        : sortCols(sortCols), sortOrder(sortOrder), sortTypes(sortTypes) {}
    ~SimplePagesIndexComparator() {}
    int compareTo(PagesIndex *pagesIndex, uint32_t leftPosition, uint32_t rightPosition);

private:
    int *sortCols;
    SortOrder *sortOrder;
    ColumnType *sortTypes;
};

class Sort : public OpTemplate
{
public:
    Sort(int *sourceTypes,
        int typesCount,
        int *outputCols,
        int outputColsCount,
        int *sortCols,
        int *sortAscendings,
        int *sortNullFirsts,
        int sortColCount);
    ~Sort();
    void preloop(Table *table);
    void inloop(Table *table, uint32_t rowIdx);
    void postloop(Table *table);
    void process(Table *table, uint32_t rowIdx);
    Table *getResult() {};
    void createPagesIndex(int *sourceTypes, int typesCount, long *datas, long *nulls, int pageCount, long *rowCounts, long totalRowCount);

    int *getSourceTypes() { return sourceTypes; }
    int getTypescount() { return typesCount; }
    int *getOutputCols() { return outputCols; }
    int getOutputColsCount() { return outputColsCount; }
    int *getSortCols() { return sortCols; }
    int *getSortAscendings() { return sortAscendings; }
    int *getSortNullFirsts() { return sortNullFirsts; }
    int getSortColCount() { return sortColCount; }
    PagesIndex *getPagesIndex() { return pagesIndex; }

private:
    int *sourceTypes;
    int typesCount;
    int *outputCols;
    int outputColsCount;
    int *sortCols;
    int *sortAscendings;
    int *sortNullFirsts;
    int sortColCount;
    PagesIndex *pagesIndex;
};

long createSort(
    int *sourceTypes,
    int typeCount,
    int *outputCols,
    int outputColCount,
    int *sortCols,
    int *sortAscendings,
    int *sortNullFirsts,
    int sortColCount);

void quickSort(long pagesIndexAddr, int *sortCols, int *sortColTypes, int *sortAscendings, int *sortNullFirsts, int sortColCount, uint32_t from, uint32_t to);
ColumnType getColumnType(int colTypeIdx);
void allocColumns(long outputTableAddr, int *sourceTypes, int *outputCols, int outputColCount, uint32_t positionCount);
void getResult(long pagesIndexAddr, int *outputCols, int outputColsCount, long outputTableAddr, int *sourceTypes, uint32_t positionCount);

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