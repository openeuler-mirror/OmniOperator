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
    PagesIndex(int *types, int typeCount);
    ~PagesIndex() {}
    void addTable(Table *table, int colNum, uint32_t positionCount);
    void sort(int *sortCols, int *sortAscendings, int *sortNullFirsts, int sortColCount);
    int *getColumnType()
    {
        return types;
    };
    int getTypeCount() {
        return typeCount;
    };
    vector<long>& getValueAddresses()
    {
        return this->valueAddresses;
    };
    vector<vector<Column *>>& getColumns()
    {
        return this->columns;
    };

    uint32_t getPositionCount()
    {
        return this->positionCount;
    };

    void getResult(int *outputCols, int outputColsCount, Table *outputTable, uint32_t positionCount);

private:
    int *types;
    int typeCount;
    vector<long> valueAddresses;
    vector<vector<Column *>> columns;
    uint32_t positionCount;
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

    int *getSourceTypes() { return sourceTypes; }
    int getTypescount() { return typesCount; }
    int *getOutputCols() { return outputCols; }
    int getOutputColsCount() { return outputColsCount; }
    int *getSortCols() { return sortCols; }
    int *getSortAscendings() { return sortAscendings; }
    int *getSortNullFirsts() { return sortNullFirsts; }
    int getSortColCount() { return sortColCount; }

private:
    int *sourceTypes;
    int typesCount;
    int *outputCols;
    int outputColsCount;
    int *sortCols;
    int *sortAscendings;
    int *sortNullFirsts;
    int sortColCount;
};

PagesIndex *getPagesIndex(long sortAddress);
void putPagesIndex(long sortAddress, PagesIndex *pagesIndex);
void removePagesIndex(long sortAddress);
void allocColumn(Table *outputTable, int columnTypeIdx, uint32_t positionCount);
void quickSort(long pagesIndexAddr, int *sortCols, int *sortColTypes, int *sortAscendings, int *sortNullFirsts, int sortColCount, uint32_t from, uint32_t to);
ColumnType getColumnType(int colTypeIdx);

#define CLOCKS_PER_MILLISECOND 1000

//#define JNI
//#define API
//#define IMP
//#define TIME

#ifdef JNI
#define LOG_JNI(format, ...) printf(format, __VA_ARGS__)
#else
#define LOG_JNI(format, ...)
#endif

#ifdef API
#define LOG_API(format, ...) printf(format, __VA_ARGS__)
#else
#define LOG_API(format, ...)
#endif

#ifdef IMP
#define LOG_IMP(format, ...) printf(format, __VA_ARGS__)
#else
#define LOG_IMP(format, ...)
#endif


#ifdef TIME 
#define START() clock()
#else
#define START() 0
#endif

#endif