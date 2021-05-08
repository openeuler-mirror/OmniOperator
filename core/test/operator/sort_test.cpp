#include "gtest/gtest.h"
#include "../../src/operator/sort/sort.h"
#include <time.h>
#include <vector>
#include <iostream>
#include <chrono>

bool tableMatch(Table *outputTables, Table *expectTable);
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

    NativeOmniSortOperatorFactory *operatorFactory = NativeOmniSortOperatorFactory::createNativeOmniSortOperatorFactory(
        sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    NativeOmniSortOperator *sortOperator = (NativeOmniSortOperator *)operatorFactory->createOmniOperator();

    int32_t *data1 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data1[i] = i;
    }

    int32_t *data2 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data2[i] = i;
    }

    // add input
    int32_t rowCounts[1] = {DATA_SIZE};

    Table **tables = (Table **)malloc(1 * sizeof(Table *));
    tables[0] = new Table(DATA_SIZE, 2);
    Column *column1 = new Column(data1, INT32, DATA_SIZE);
    Column *column2 = new Column(data2, INT32, DATA_SIZE);
    tables[0]->setColumn(column1, INT32);
    tables[0]->setColumn(column2, INT32);
    sortOperator->addInput(tables, rowCounts, 1);

    clock_t start = clock();
    vector<Table *> outputTables;
    sortOperator->getOutput(outputTables);
    std::cout << "sort and get output elapsed end time: " << (double)(std::clock() - start) / 1000 << " ms" << std::endl;

    // free memory
    delete []data2;
    delete []data1;
    delete sortOperator;
    delete operatorFactory;
    freeInputTable(tables, 1);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
}

TEST(SortTest, testOrderByOneColumn)
{
    //construct input data
    const int32_t DATA_SIZE = 5;

    int sourceTypes[2] = {1, 2};
    int outputCols[2] = {0, 1};
    int sortCols[1] = {1};
    int ascendings[1] = {false};
    int nullFirsts[1] = {true};

    NativeOmniSortOperatorFactory *operatorFactory = NativeOmniSortOperatorFactory::createNativeOmniSortOperatorFactory(
        sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
    NativeOmniSortOperator *sortOperator = (NativeOmniSortOperator *)operatorFactory->createOmniOperator();

    int32_t data1[DATA_SIZE] = {4, 3, 2, 1, 0};
    int64_t data2[DATA_SIZE] = {0, 1, 2, 3, 4};
    int32_t rowCounts[1] = {DATA_SIZE};

    Table **tables = (Table **)malloc(1 * sizeof(Table *));
    tables[0] = new Table(DATA_SIZE, 2);
    Column *column1 = new Column(data1, INT32, DATA_SIZE);
    Column *column2 = new Column(data2, INT64, DATA_SIZE);
    tables[0]->setColumn(column1, INT32);
    tables[0]->setColumn(column2, INT64);

    sortOperator->addInput(tables, rowCounts, 1);
    vector<Table *> outputTables;
    sortOperator->getOutput(outputTables);

    int32_t expectData1[DATA_SIZE] = {0, 1, 2, 3, 4};
    Column expectCol1(expectData1, INT32, DATA_SIZE);
    int64_t expectData2[DATA_SIZE] = {4, 3, 2, 1, 0};
    Column expectCol2(expectData2, INT64, DATA_SIZE);
    Table* expectTable = new Table(DATA_SIZE, 2);
    expectTable->setColumn(&expectCol1, INT32);
    expectTable->setColumn(&expectCol2, INT64);

    EXPECT_TRUE(tableMatch(outputTables[0], expectTable));

    //free memory
    delete sortOperator;
    delete operatorFactory;
    freeInputTable(tables, 1);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
}

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
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    Table **tables = (Table **)malloc(1 * sizeof(Table *));
    tables[0] = new Table(DATA_SIZE, 3);
    Column *column0 = new Column(data0, INT32, DATA_SIZE);
    Column *column1 = new Column(data1, INT64, DATA_SIZE);
    Column *column2 = new Column(data2, DOUBLE, DATA_SIZE);
    tables[0]->setColumn(column0, INT32);
    tables[0]->setColumn(column1, INT64);
    tables[0]->setColumn(column2, DOUBLE);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    NativeOmniSortOperatorFactory *operatorFactory = NativeOmniSortOperatorFactory::createNativeOmniSortOperatorFactory(
        sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    NativeOmniSortOperator *sortOperator = (NativeOmniSortOperator *)operatorFactory->createOmniOperator();

    sortOperator->addInput(tables, rowCounts, 1);
    vector<Table *> outputTables;
    sortOperator->getOutput(outputTables);

    int64_t expectData1[DATA_SIZE] = {5, 2, 4, 1, 3, 0};
    Column *expectCol1 = new Column(expectData1, INT64, DATA_SIZE);
    double expectData2[DATA_SIZE] = {1.1, 4.4, 2.2, 5.5, 3.3, 6.6};
    Column *expectCol2 = new Column(expectData2, DOUBLE, DATA_SIZE);
    Table* expectTable = new Table(DATA_SIZE, 2);
    expectTable->setColumn(expectCol1, INT64);
    expectTable->setColumn(expectCol2, DOUBLE);

    EXPECT_TRUE(tableMatch(outputTables[0], expectTable));

    delete sortOperator;
    delete operatorFactory;
    freeInputTable(tables, 1);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
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
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    Table **tables = (Table **)malloc(1 * sizeof(Table *));
    tables[0] = new Table(DATA_SIZE, 3);
    Column *column0 = new Column(data0, INT32, DATA_SIZE);
    Column *column1 = new Column(data1, INT64, DATA_SIZE);
    Column *column2 = new Column(data2, DOUBLE, DATA_SIZE);
    tables[0]->setColumn(column0, INT32);
    tables[0]->setColumn(column1, INT64);
    tables[0]->setColumn(column2, DOUBLE);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    NativeOmniSortOperatorFactory *operatorFactory = NativeOmniSortOperatorFactory::createNativeOmniSortOperatorFactory(
        sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    NativeOmniSortOperator *sortOperator = (NativeOmniSortOperator *)operatorFactory->createOmniOperator();

    sortOperator->addInput(tables, rowCounts, 1);
    vector<Table *> outputTables;
    sortOperator->getOutput(outputTables);

    int64_t expectData1[DATA_SIZE] = {5, 2, 4, 1, 3, 0};
    Column *expectCol1 = new Column(expectData1, INT64, DATA_SIZE);
    double expectData2[DATA_SIZE] = {1.1, 4.4, 2.2, 5.5, 3.3, 6.6};
    Column *expectCol2 = new Column(expectData2, DOUBLE, DATA_SIZE);
    Table *expectTable = new Table(DATA_SIZE, 2);
    expectTable->setColumn(expectCol1, INT64);
    expectTable->setColumn(expectCol2, DOUBLE);

    EXPECT_TRUE(tableMatch(outputTables[0], expectTable));

    delete sortOperator;
    delete operatorFactory;
    freeInputTable(tables, 1);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
    delete expectTable;
}

void buildSortTestData(int32_t tableCount, int32_t distinctValueCount, int32_t repeatCount, Table **tables)
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
        data2 = (int64_t *)malloc(size);

        idx = 0;
        for (int32_t j = 0; j < distinctValueCount; j++) {
            for (int32_t k = 0; k < repeatCount; k++) {
                data1[idx] = j;
                data2[idx] = j;
                idx++;
            }
        }

        Table *table = new Table(positionCount, 2);
        Column *column1 = new Column(data1, INT64, positionCount);
        Column *column2 = new Column(data2, INT64, positionCount);
        table->setColumn(column1, INT64);
        table->setColumn(column2, INT64);
        tables[i] = table;
    }
}

TEST(SortTest, testOrderByTwoColumnPerf)
{
    printf("test_sort_one called\n");

    int32_t tableCount = 1;
    int32_t distinctValue = 40;
    int32_t repeatCount = 25000;
    int32_t rowNum = distinctValue * repeatCount;
    Table **tables = (Table **)malloc(tableCount * sizeof(Table *));

    buildSortTestData(tableCount, distinctValue, repeatCount, tables);
    std::cout<<"finish build sort data" << endl;

    int32_t rowCounts[tableCount];
    for (int32_t i = 0; i < tableCount; i++) {
        rowCounts[i] = rowNum;
    }
    int32_t sourceTypes[] = {1, 1};
    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {0, 1};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};

    NativeOmniSortOperatorFactory *operatorFactory = NativeOmniSortOperatorFactory::createNativeOmniSortOperatorFactory(
        sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    NativeOmniSortOperator *sortOperator = (NativeOmniSortOperator *)operatorFactory->createOmniOperator();

    sortOperator->addInput(tables, rowCounts, tableCount);
    typedef std::chrono::high_resolution_clock Time;
    typedef std::chrono::milliseconds ms;
    typedef std::chrono::duration<float> fsec;
    auto t0 = Time::now();
    vector<Table *> outputTables;
    sortOperator->getOutput(outputTables);
    auto t1 = Time::now();
    fsec fs = t1 - t0;
    ms d = std::chrono::duration_cast<ms>(fs);
    std::cout << "sort elapsed time: " << (double)d.count() << "ms" << std::endl;

    delete sortOperator;
    delete operatorFactory;
    freeDataInColumn(tables, tableCount);
    freeInputTable(tables, tableCount);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
}

bool tableMatch(Table *outputTables, Table *expectTable)
{
    if (outputTables->getPositionCount() != expectTable->getPositionCount()) {
        return false;
    }

    int32_t columnNumber = outputTables->getColumnNumber();
    if (columnNumber != expectTable->getColumnNumber()) {
        return false;
    }

    if (!typesMatch(outputTables->getColumnTypes(), expectTable->getColumnTypes(), columnNumber)) {
        return false;
    }

    for (int32_t i = 0; i < columnNumber; i++) {
        if (!columnMatch(outputTables->getColumn(i), expectTable->getColumn(i))) {
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
