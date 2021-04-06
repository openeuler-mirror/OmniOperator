#include "gtest/gtest.h"
#include "../src/jni/sort_api.h"
#include <time.h>
#include <vector>
#include <iostream>

bool tableMatch(Table *actualTable, Table *expectTable);
bool typesMatch(ColumnType *actualTypes, ColumnType *expectTypes, uint32_t columnNumber);
bool columnMatch(Column *actualColumn, Column *expectColumn);

TEST (OrderByTest, TestSortByPerformance)
{
    // construct input data
    const int32_t DATA_SIZE = 100000;
    int sourceTypes[2] = {1, 1};
    int outputCols[2] = {0, 1};
    int sortCols[2] = {0, 1};
    int ascendings[2] = {true, true};
    int nullFirsts[2] = {true, true};
    long stageId = 1011;

    long sortAddress = allocAndInitSort(stageId, sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    int32_t *data1 = new int32_t[DATA_SIZE];
    // for (int32_t i = DATA_SIZE - 1; i >= 0; --i) {
    //     data1[DATA_SIZE - i] = i % (DATA_SIZE/100);
    // }
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data1[i] = i;
    }
    int *nulls1 = new int[DATA_SIZE];
    memset(nulls1, false, DATA_SIZE);

    int32_t *data2 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data2[i] = i;
    }
    int *nulls2 = new int[DATA_SIZE];
    memset(nulls2, false, DATA_SIZE);
    
    long datas[2] = {(long)data1, (long)data2};
    long nulls[2] = {(long)nulls1, (long)nulls2};

    // order by
    addTable(sortAddress, datas, nulls, DATA_SIZE);
    //cout<<"ADD TABLE FINISH:"<<endl;
    clock_t start = clock();
    sort(sortAddress, stageId);
    std::cout << "sort elapsed end time: " << (double)(std::clock() - start) / 1000 << " ms" << std::endl;

    // construct output
    Table *output = getResult(sortAddress, stageId);
    

    // free memory
    delete output;
    delete []nulls2;
    delete []nulls1;
    delete []data2;
    delete []data1;
}

// TEST(SortTest, testOrderByOneColumn)
// {
//     //construct input data
//     const int32_t DATA_SIZE = 5;

//     int sourceTypes[2] = {1, 2};
//     int outputCols[2] = {0, 1};
//     int sortCols[1] = {1};
//     int ascendings[1] = {false};
//     int nullFirsts[1] = {true};

//     long sortAddress = allocAndInitSort(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 1);

//     int32_t data1[DATA_SIZE] = {4, 3, 2, 1, 0};
//     int nulls1[DATA_SIZE] = {false, false, false, false, false};

//     int64_t data2[DATA_SIZE] = {0, 1, 2, 3, 4};
//     int nulls2[DATA_SIZE] = {false, false, false, false, false};

//     long datas[2] = {(long)data1, (long)data2};
//     long nulls[2] = {(long)nulls1, (long)nulls2};

//     // order by
//     addTable(sortAddress, datas, nulls, DATA_SIZE);
//     sort(sortAddress);

//     // construct output
//     Table *actualTable = getResult(sortAddress);

//     int32_t expectData1[DATA_SIZE] = {0, 1, 2, 3, 4};
//     Column expectCol1(expectData1, INT32, DATA_SIZE);
//     int64_t expectData2[DATA_SIZE] = {4, 3, 2, 1, 0};
//     Column expectCol2(expectData2, INT64, DATA_SIZE);
//     Table* expectTable = new Table(DATA_SIZE, 2);
//     expectTable->setColumn(&expectCol1, INT32);
//     expectTable->setColumn(&expectCol2, INT64);

//     EXPECT_TRUE(tableMatch(actualTable, expectTable));

//     //free memory
//     delete expectTable;
//     delete actualTable;
// }

TEST(SortTest, testOrderByDoubleColumn)
{
    // construct input data
    const int32_t DATA_SIZE = 6;
    int sourceTypes[3] = {1, 2, 3};
    int outputCols[2] = {1, 2};
    int sortCols[2] = {0, 2};
    int ascendings[2] = {false, true};
    int nullFirsts[2] = {true, true};
    long stageId = 1012;

    long sortAddress = allocAndInitSort(stageId, sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int nulls0[DATA_SIZE] = {false, false, false, false, false, false};

    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    int nulls1[DATA_SIZE] = {false, false, false, false, false, false};

    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int nulls2[DATA_SIZE] = {false, false, false, false, false, false};

    long datas[3] = {(long)data0, (long)data1, (long)data2};
    long nulls[3] = {(long)nulls0, (long)nulls1, (long)nulls2};

    // order by
    addTable(sortAddress, datas, nulls, DATA_SIZE);
    sort(sortAddress, stageId);

    // construct output
    Table *actualTable = getResult(sortAddress, stageId);

    int64_t expectData1[DATA_SIZE] = {5, 2, 4, 1, 3, 0};
    Column expectCol1(expectData1, INT64, DATA_SIZE);
    double expectData2[DATA_SIZE] = {1.1, 4.4, 2.2, 5.5, 3.3, 6.6};
    Column expectCol2(expectData2, DOUBLE, DATA_SIZE);
    Table* expectTable = new Table(DATA_SIZE, 2);
    expectTable->setColumn(&expectCol1, INT64);
    expectTable->setColumn(&expectCol2, DOUBLE);
    
    EXPECT_TRUE(tableMatch(actualTable, expectTable));

    // free memory
    delete expectTable;
    delete actualTable;
}

void buildSortTestData(int tableCount, int distinctValueCount, int repeatCount, long **datas, long **nulls) 
{
    uint32_t positionCount = distinctValueCount * repeatCount;
    int *data1;
    int *data2;
    int *null1;
    int *null2;
    uint32_t size = positionCount * sizeof(int);
    uint32_t idx = 0;

    for (int i = 0; i < tableCount; i++) {
        data1 = (int *)malloc(size);
        null1 = (int *)malloc(size);
        data2 = (int *)malloc(size);
        null2 = (int *)malloc(size);      

        idx = 0;
        for (int j = 0; j < distinctValueCount; j++) {
            for (int k = 0; k < repeatCount; k++) {
                data1[idx] = j;
                data2[idx] = j;
                null1[idx] = 0;
                null2[idx] = 0;
                idx++;
            }
        }

        datas[i][0] = (long)data1;
        datas[i][1] = (long)data2;
        nulls[i][0] = (long)null1;
        nulls[i][1] = (long)null2;  
    }
}

TEST(SortTest, testOrderByTwoColumnPerf)
{
    printf("test_sort_one called\n");

    int tableCount = 10;
    int distinctValue = 4;
    int repeatCount = 250000;
    uint32_t rowNum = distinctValue * repeatCount;
    long *datas[tableCount];
    long *nulls[tableCount];

    for (int i = 0; i < tableCount; i++) {
        datas[i] = (long *)malloc(2 * sizeof(long));
        nulls[i] = (long *)malloc(2 * sizeof(long));
    }

    buildSortTestData(tableCount, distinctValue, repeatCount, datas, nulls);
    std::cout<<"finish build sort data" << endl;
    
    int sourceTypes[] = {1, 1};
    int outputCols[] = {0, 1};
    int sortCols[] = {0, 1};
    int ascendings[] = {1, 1};
    int nullFirsts[] = {0, 0};
    long stageId = 1013;

    long sortAddress = allocAndInitSort(stageId, sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    for (int i = 0; i < tableCount; i++) {
        addTable(sortAddress, datas[i], nulls[i], rowNum);
    }

    clock_t start = clock();
    sort(sortAddress, stageId);
    std::cout << "sort elapsed end time: " << (double) (std::clock() - start) / 1000 << " ms" << std::endl;

    Table * output = getResult(sortAddress, stageId);
}

bool tableMatch(Table *actualTable, Table *expectTable)
{
    if (actualTable->getPositionCount() != expectTable->getPositionCount()) {
        return false;
    }

    uint32_t columnNumber = actualTable->getColumnNumber();
    if (columnNumber != expectTable->getColumnNumber()) {
        return false;
    }

    if (!typesMatch(actualTable->getColumnTypes(), expectTable->getColumnTypes(), columnNumber)) {
        return false;
    }

    for (uint32_t i = 0; i < columnNumber; i++) {
        if (!columnMatch(actualTable->getColumn(i), expectTable->getColumn(i))) {
            return false;
        }
    }

    return true;
}

bool typesMatch(ColumnType *actualTypes, ColumnType *expectTypes, uint32_t columnNumber)
{
    for (uint32_t i = 0; i < columnNumber; i++) {
        if (actualTypes[i] != expectTypes[i]) {
            return false;
        }
    }

    return true;
}

bool columnMatch(Column *actualColumn, Column *expectColumn)
{
    if (actualColumn->getType() != expectColumn->getType()) {
        return false;
    }

    if (actualColumn->getSize() != expectColumn->getSize()) {
        return false;
    }

    bool result = true;
    for (uint32_t i = 0; i < actualColumn->getSize(); i++) {
        void *actualValue = actualColumn->getValue(i);
        void *expectValue = expectColumn->getValue(i);
        switch (actualColumn->getType()) {
            case INT32: {
                int32_t actual = *((int32_t *)actualValue);
                int32_t expect = *((int32_t *)expectValue);
                result = (actual == expect);
                break; 
            }
            case INT64: {
                int64_t actual = *((int64_t *)actualValue);
                int64_t expect = *((int64_t *)expectValue);
                result = (actual == expect);
                break;
            }
            case DOUBLE: {
                double actual = *((double *)actualValue);
                double expect = *((double *)expectValue);
                result = (actual == expect);
                break;
            }
            default:
                result = false;
        }
    }

    return result;
}