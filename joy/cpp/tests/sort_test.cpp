#include "gtest/gtest.h"
#include "../src/jni/sort_api.h"
#include <time.h>
#include <vector>
#include <iostream>
#include <chrono>

bool tableMatch(Table *actualTable, Table *expectTable);
bool typesMatch(ColumnType *actualTypes, ColumnType *expectTypes, int32_t columnNumber);
bool columnMatch(Column *actualColumn, Column *expectColumn);

TEST (OrderByTest, TestSortByPerformance)
{
    // construct input data
    const int32_t DATA_SIZE = 100000;
    int32_t sourceTypes[2] = {1, 1};
    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {true, true};

    int64_t contextAddress = sortPrepare(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    int64_t sortAddress = sortCreateOperator(contextAddress, sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    int32_t *data1 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data1[i] = i;
    }
    int32_t *nulls1 = new int32_t[DATA_SIZE];
    memset(nulls1, false, DATA_SIZE);

    int32_t *data2 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data2[i] = i;
    }
    int32_t *nulls2 = new int32_t[DATA_SIZE];
    memset(nulls2, false, DATA_SIZE);

    int64_t datas[2] = {(int64_t)data1, (int64_t)data2};
    int64_t nulls[2] = {(int64_t)nulls1, (int64_t)nulls2};

    // order by
    int32_t rowCounts[1] = {DATA_SIZE};
    sortAddInput(contextAddress, sortAddress, datas, nulls, 1, rowCounts, DATA_SIZE);
    //cout<<"ADD TABLE FINISH:"<<endl;
    clock_t start = clock();
    sortExecute(contextAddress, sortAddress);
    std::cout << "sort elapsed end time: " << (double)(std::clock() - start) / 1000 << " ms" << std::endl;

    // construct output
    int32_t outputTableCount = 0;
    Table **output = sortGetOutput(contextAddress, sortAddress, &outputTableCount);

    // free memory
    delete []nulls2;
    delete []nulls1;
    delete []data2;
    delete []data1;
    delete (JitSortContext *)contextAddress;
    freeOutputTable(output, outputTableCount);
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

//     int64_t contextAddress = sortPrepare(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
//     int64_t sortAddress = sortCreateOperator(contextAddress, sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 1);

//     int32_t data1[DATA_SIZE] = {4, 3, 2, 1, 0};
//     int nulls1[DATA_SIZE] = {false, false, false, false, false};

//     int64_t data2[DATA_SIZE] = {0, 1, 2, 3, 4};
//     int nulls2[DATA_SIZE] = {false, false, false, false, false};

//     int64_t datas[2] = {(int64_t)data1, (int64_t)data2};
//     int64_t nulls[2] = {(int64_t)nulls1, (int64_t)nulls2};

//     // order by
//     int32_t rowCounts[1] = {DATA_SIZE};
//     sortAddInput(contextAddress, sortAddress, datas, nulls, 1, rowCounts, DATA_SIZE);
//     //cout<<"ADD TABLE FINISH:"<<endl;
//     clock_t start = clock();
//     sortExecute(contextAddress, sortAddress);
//     int32_t outputTableCount = 0;
//     Table **outputTable = sortGetOutput(contextAddress, sortAddress, &outputTableCount);

//     int32_t expectData1[DATA_SIZE] = {0, 1, 2, 3, 4};
//     Column expectCol1(expectData1, INT32, DATA_SIZE);
//     int64_t expectData2[DATA_SIZE] = {4, 3, 2, 1, 0};
//     Column expectCol2(expectData2, INT64, DATA_SIZE);
//     Table* expectTable = new Table(DATA_SIZE, 2);
//     expectTable->setColumn(&expectCol1, INT32);
//     expectTable->setColumn(&expectCol2, INT64);

//     EXPECT_TRUE(tableMatch(outputTable[0], expectTable));

//     //free memory
//     delete expectTable;
//     freeOutputTable(outputTable, outputTableCount);
//     delete (JitSortContext *)contextAddress;
// }

TEST(SortTest, testOrderByDoubleColumn)
{
    // construct input data
    const int32_t DATA_SIZE = 6;
    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int32_t nulls0[DATA_SIZE] = {false, false, false, false, false, false};

    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    int32_t nulls1[DATA_SIZE] = {false, false, false, false, false, false};

    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t nulls2[DATA_SIZE] = {false, false, false, false, false, false};

    int64_t datas[3] = {(int64_t)data0, (int64_t)data1, (int64_t)data2};
    int64_t nulls[3] = {(int64_t)nulls0, (int64_t)nulls1, (int64_t)nulls2};
    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int64_t contextAddress = 0;
    int64_t sortAddress = sortCreateOperator(contextAddress, sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    sortAddInput(contextAddress, sortAddress, datas, nulls, 1, rowCounts, DATA_SIZE);
    sortExecute(contextAddress, sortAddress);
    int32_t outputTableCount = 0;
    Table **actualTable = sortGetOutput(contextAddress, sortAddress, &outputTableCount);

    int64_t expectData1[DATA_SIZE] = {5, 2, 4, 1, 3, 0};
    Column *expectCol1 = new Column(expectData1, INT64, DATA_SIZE);
    double expectData2[DATA_SIZE] = {1.1, 4.4, 2.2, 5.5, 3.3, 6.6};
    Column *expectCol2 = new Column(expectData2, DOUBLE, DATA_SIZE);
    Table* expectTable = new Table(DATA_SIZE, 2);
    expectTable->setColumn(expectCol1, INT64);
    expectTable->setColumn(expectCol2, DOUBLE);

    actualTable[0]->printTable();
    expectTable->printTable();
    EXPECT_TRUE(tableMatch(actualTable[0], expectTable));
    freeOutputTable(actualTable, outputTableCount);

    contextAddress = sortPrepare(sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    sortAddress = sortCreateOperator(contextAddress, sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    sortAddInput(contextAddress, sortAddress, datas, nulls, 1, rowCounts, DATA_SIZE);
    sortExecute(contextAddress, sortAddress);
    actualTable = sortGetOutput(contextAddress, sortAddress, &outputTableCount);
    EXPECT_TRUE(tableMatch(actualTable[0], expectTable));

    freeOutputTable(actualTable, outputTableCount);
    delete (JitSortContext *)contextAddress;
    delete expectTable;
}

TEST(SortTest, testOrderByDoubleColumnV2)
{
    // construct input data
    const int32_t DATA_SIZE = 6;
    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int32_t nulls0[DATA_SIZE] = {false, false, false, false, false, false};

    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    int32_t nulls1[DATA_SIZE] = {false, false, false, false, false, false};

    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t nulls2[DATA_SIZE] = {false, false, false, false, false, false};

    int64_t datas[3] = {(int64_t)data0, (int64_t)data1, (int64_t)data2};
    int64_t nulls[3] = {(int64_t)nulls0, (int64_t)nulls1, (int64_t)nulls2};
    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int64_t contextAddress = 0;
    int64_t sortAddress = sortCreateOperator(contextAddress, sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    sortAddInput(contextAddress, sortAddress, datas, nulls, 1, rowCounts, DATA_SIZE);
    sortExecute(contextAddress, sortAddress);
    int32_t outputTableCount; 
    Table **actualTable = sortGetOutput(contextAddress, sortAddress, &outputTableCount);

    int64_t expectData1[DATA_SIZE] = {5, 2, 4, 1, 3, 0};
    Column *expectCol1 = new Column(expectData1, INT64, DATA_SIZE);
    double expectData2[DATA_SIZE] = {1.1, 4.4, 2.2, 5.5, 3.3, 6.6};
    Column *expectCol2 = new Column(expectData2, DOUBLE, DATA_SIZE);
    Table *expectTable = new Table(DATA_SIZE, 2);
    expectTable->setColumn(expectCol1, INT64);
    expectTable->setColumn(expectCol2, DOUBLE);

    actualTable[0]->printTable();
    expectTable->printTable();
    EXPECT_TRUE(tableMatch(actualTable[0], expectTable));
    freeOutputTable(actualTable, outputTableCount);

    contextAddress = sortPrepare(sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    sortAddress = sortCreateOperator(contextAddress, sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    sortAddInput(contextAddress, sortAddress, datas, nulls, 1, rowCounts, DATA_SIZE);
    sortExecute(contextAddress, sortAddress);
    actualTable = sortGetOutput(contextAddress, sortAddress, &outputTableCount);
    EXPECT_TRUE(tableMatch(actualTable[0], expectTable));
    freeOutputTable(actualTable, outputTableCount);
    delete (JitSortContext *)contextAddress;
    delete expectTable;
}

void buildSortTestData(int32_t tableCount, int32_t distinctValueCount, int32_t repeatCount, int64_t *datas, int64_t *nulls)
{
    uint32_t positionCount = distinctValueCount * repeatCount;
    int64_t *data1;
    int64_t *data2;
    int32_t *null1;
    int32_t *null2;
    uint32_t size = positionCount * sizeof(int64_t);
    uint32_t idx = 0;

    for (int32_t i = 0; i < tableCount; i++) {
        data1 = (int64_t *)malloc(size);
        null1 = (int32_t *)malloc(size);
        data2 = (int64_t *)malloc(size);
        null2 = (int32_t *)malloc(size);

        idx = 0;
        for (int32_t j = 0; j < distinctValueCount; j++) {
            for (int32_t k = 0; k < repeatCount; k++) {
                data1[idx] = j;
                data2[idx] = j;
                null1[idx] = 0;
                null2[idx] = 0;
                idx++;
            }
        }

        datas[i * 2 + 0] = (int64_t)data1;
        datas[i * 2 + 1] = (int64_t)data2;
        nulls[i * 2 + 0] = (int64_t)null1;
        nulls[i * 2 + 1] = (int64_t)null2;
    }
}

TEST(SortTest, testOrderByTwoColumnPerf)
{
    printf("test_sort_one called\n");

    int32_t tableCount = 10;
    int32_t distinctValue = 4;
    int32_t repeatCount = 25000;
    int32_t rowNum = distinctValue * repeatCount;
    int64_t datas[tableCount * 2];
    int64_t nulls[tableCount * 2];

    buildSortTestData(tableCount, distinctValue, repeatCount, datas, nulls);
    std::cout<<"finish build sort data" << endl;
    int32_t rowCounts[1] = {rowNum};
    int32_t sourceTypes[] = {1, 1};
    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {0, 1};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};

    int64_t contextAddress = sortPrepare(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    int64_t sortAddress = sortCreateOperator(contextAddress, sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    sortAddInput(contextAddress, sortAddress, datas, nulls, tableCount, rowCounts, rowNum);

    typedef std::chrono::high_resolution_clock Time;
    typedef std::chrono::milliseconds ms;
    typedef std::chrono::duration<float> fsec;
    auto t0 = Time::now();
    sortExecute(contextAddress, sortAddress);
    auto t1 = Time::now();
    fsec fs = t1 - t0;
    ms d = std::chrono::duration_cast<ms>(fs);
    std::cout << "sortExecute elapsed time: " << (double)d.count() << "ms" << std::endl;
    int32_t outputTableCount = 0;
    Table **actualTable = sortGetOutput(contextAddress, sortAddress, &outputTableCount);

    for (int32_t i = 0; i < tableCount * 2; i++) {
        delete[] (int64_t *)datas[i];
        delete[] (int32_t *)nulls[i];
    }

    freeOutputTable(actualTable, outputTableCount);
    delete (JitSortContext *)contextAddress;
}

bool tableMatch(Table *actualTable, Table *expectTable)
{
    if (actualTable->getPositionCount() != expectTable->getPositionCount()) {
        return false;
    }

    int32_t columnNumber = actualTable->getColumnNumber();
    if (columnNumber != expectTable->getColumnNumber()) {
        return false;
    }

    if (!typesMatch(actualTable->getColumnTypes(), expectTable->getColumnTypes(), columnNumber)) {
        return false;
    }

    for (int32_t i = 0; i < columnNumber; i++) {
        if (!columnMatch(actualTable->getColumn(i), expectTable->getColumn(i))) {
            return false;
        }
    }

    return true;
}

bool typesMatch(ColumnType *actualTypes, ColumnType *expectTypes, int32_t columnNumber)
{
    for (int32_t i = 0; i < columnNumber; i++) {
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
    for (int32_t i = 0; i < actualColumn->getSize(); i++) {
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