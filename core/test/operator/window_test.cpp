#include "gtest/gtest.h"
#include "../../src/operator/sort/sort.h"
#include "../../src/operator/window/window.h"
#include "../util/test_util.h"
#include <time.h>
#include <vector>
#include <iostream>
#include <chrono>

TEST(NativeOmniWindowOperatorTest, testRowNumberPartition)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t DATA_SIZE = 6;
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

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {0};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    int32_t allTypes[4] = {1, 2, 3, 2};
    int32_t argumentChannels[0] = {};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::createWindowOperatorFactory(sourceTypes, 3,
        outputCols, 3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, 4, argumentChannels, 0);
    JitContext *jitContext = NULL;
    operatorFactory->setJitContext(jitContext);
    WindowOperator *windowOperator;
    if (jitContext == NULL) {
        windowOperator = (WindowOperator *)operatorFactory->createOperator();
    } else {
        opt_module windowModule = (opt_module)(jitContext->func);
        windowOperator = (WindowOperator *)windowModule(operatorFactory);
    }

    windowOperator->addInput(tables, rowCounts, 1);
    vector<Table *> outputTables;
    windowOperator->getOutput(outputTables);

    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    Column *expectCol1 = new Column(expectData1, INT32, DATA_SIZE);
    int64_t expectData2[DATA_SIZE] = {3, 0, 4, 1, 5, 2};
    Column *expectCol2 = new Column(expectData2, INT64, DATA_SIZE);
    double expectData3[DATA_SIZE] = {3.3, 6.6, 2.2, 5.5, 1.1, 4.4};
    Column *expectCol3 = new Column(expectData3, DOUBLE, DATA_SIZE);
    int64_t expectData4[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    Column *expectCol4 = new Column(expectData4, INT64, DATA_SIZE);
    Table *expectTable = new Table(DATA_SIZE, 4);
    expectTable->setColumn(expectCol1, INT32);
    expectTable->setColumn(expectCol2, INT64);
    expectTable->setColumn(expectCol3, DOUBLE);
    expectTable->setColumn(expectCol4, INT64);
    EXPECT_TRUE(tableMatch(outputTables[0], expectTable));

    delete windowOperator;
    delete operatorFactory;
    freeInputTable(tables, 1);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
    delete expectTable;
}

TEST(NativeOmniWindowOperatorTest, testRowNumber)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t DATA_SIZE = 6;
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

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[2] = {2, 1};
    int32_t sortCols[0] = {};
    int32_t ascendings[0] = {};
    int32_t nullFirsts[0] = {};
    int32_t windowFunctionTypes[1] = {0};
    int32_t partitionCols[1] = {2};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    int32_t allTypes[4] = {1, 2, 3, 2};
    int32_t argumentChannels[0] = {};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::createWindowOperatorFactory(sourceTypes, 3,
        outputCols, 2, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 0,
        preSortedChannelPrefix, expectedPositions, allTypes, 4, argumentChannels, 0);
    JitContext *jitContext = NULL;
    operatorFactory->setJitContext(jitContext);
    WindowOperator *windowOperator;
    if (jitContext == NULL) {
        windowOperator = (WindowOperator *)operatorFactory->createOperator();
    } else {
        opt_module windowModule = (opt_module)(jitContext->func);
        windowOperator = (WindowOperator *)windowModule(operatorFactory);
    }

    windowOperator->addInput(tables, rowCounts, 1);
    vector<Table *> outputTables;
    windowOperator->getOutput(outputTables);

    double expectData1[DATA_SIZE] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    Column *expectCol1 = new Column(expectData1, DOUBLE, DATA_SIZE);
    int64_t expectData2[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    Column *expectCol2 = new Column(expectData2, INT64, DATA_SIZE);
    int64_t expectData3[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    Column *expectCol3 = new Column(expectData3, INT64, DATA_SIZE);
    Table *expectTable = new Table(DATA_SIZE, 3);
    expectTable->setColumn(expectCol1, DOUBLE);
    expectTable->setColumn(expectCol2, INT64);
    expectTable->setColumn(expectCol3, INT64);
    EXPECT_TRUE(tableMatch(outputTables[0], expectTable));

    delete windowOperator;
    delete operatorFactory;
    freeInputTable(tables, 1);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
    delete expectTable;
}

TEST(NativeOmniWindowOperatorTest, testRankPartition)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t DATA_SIZE = 6;
    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
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

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    int32_t allTypes[4] = {1, 2, 3, 2};
    int32_t argumentChannels[0] = {};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::createWindowOperatorFactory(sourceTypes, 3,
        outputCols, 3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, 4, argumentChannels, 0);
    JitContext *jitContext = NULL;
    operatorFactory->setJitContext(jitContext);
    WindowOperator *windowOperator;
    if (jitContext == NULL) {
        windowOperator = (WindowOperator *)operatorFactory->createOperator();
    } else {
        opt_module windowModule = (opt_module)(jitContext->func);
        windowOperator = (WindowOperator *)windowModule(operatorFactory);
    }

    windowOperator->addInput(tables, rowCounts, 1);
    vector<Table *> outputTables;
    windowOperator->getOutput(outputTables);

    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    Column *expectCol1 = new Column(expectData1, INT32, DATA_SIZE);
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 1, 5, 2};
    Column *expectCol2 = new Column(expectData2, INT64, DATA_SIZE);
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 5.5, 1.1, 4.4};
    Column *expectCol3 = new Column(expectData3, DOUBLE, DATA_SIZE);
    int64_t expectData4[DATA_SIZE] = {1, 1, 1, 2, 1, 2};
    Column *expectCol4 = new Column(expectData4, INT64, DATA_SIZE);
    Table *expectTable = new Table(DATA_SIZE, 4);
    expectTable->setColumn(expectCol1, INT32);
    expectTable->setColumn(expectCol2, INT64);
    expectTable->setColumn(expectCol3, DOUBLE);
    expectTable->setColumn(expectCol4, INT64);
    EXPECT_TRUE(tableMatch(outputTables[0], expectTable));

    delete windowOperator;
    delete operatorFactory;
    freeInputTable(tables, 1);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
    delete expectTable;
}

TEST(NativeOmniWindowOperatorTest, testRank)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t DATA_SIZE = 6;
    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
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

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[3] = {1, 2, 0};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    int32_t allTypes[4] = {1, 2, 3, 2};
    int32_t argumentChannels[0] = {};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::createWindowOperatorFactory(sourceTypes, 3,
        outputCols, 3, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, 4, argumentChannels, 0);
    JitContext *jitContext = NULL;
    operatorFactory->setJitContext(jitContext);
    WindowOperator *windowOperator;
    if (jitContext == NULL) {
        windowOperator = (WindowOperator *)operatorFactory->createOperator();
    } else {
        opt_module windowModule = (opt_module)(jitContext->func);
        windowOperator = (WindowOperator *)windowModule(operatorFactory);
    }

    windowOperator->addInput(tables, rowCounts, 1);
    vector<Table *> outputTables;
    windowOperator->getOutput(outputTables);

    int64_t expectData1[DATA_SIZE] = {8, 8, 5, 4, 2, 1};
    Column *expectCol1 = new Column(expectData1, INT64, DATA_SIZE);
    double expectData2[DATA_SIZE] = {6.6, 3.3, 1.1, 2.2, 4.4, 5.5};
    Column *expectCol2 = new Column(expectData2, DOUBLE, DATA_SIZE);
    int32_t expectData3[DATA_SIZE] = {0, 0, 2, 1, 2, 1};
    Column *expectCol3 = new Column(expectData3, INT32, DATA_SIZE);
    int64_t expectData4[DATA_SIZE] = {1, 1, 3, 4, 5, 6};
    Column *expectCol4 = new Column(expectData4, INT64, DATA_SIZE);
    Table *expectTable = new Table(DATA_SIZE, 4);
    expectTable->setColumn(expectCol1, INT64);
    expectTable->setColumn(expectCol2, DOUBLE);
    expectTable->setColumn(expectCol3, INT32);
    expectTable->setColumn(expectCol4, INT64);
    EXPECT_TRUE(tableMatch(outputTables[0], expectTable));

    delete windowOperator;
    delete operatorFactory;
    freeInputTable(tables, 1);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
    delete expectTable;
}