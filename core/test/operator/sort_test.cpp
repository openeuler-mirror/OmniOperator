#include "gtest/gtest.h"
#include "../../src/operator/sort/sort.h"
#include "../../src/jit/jit.h"
#include "../../src/jit/specialization.h"
#include "../../src/operator/optimization.h"
#include "../util/test_util.h"
#include <thread>
#include <time.h>
#include <vector>
#include <iostream>
#include <chrono>

using namespace std;
JitContext *createTestSortJitContext(
  int32_t *sourceTypes,
  int32_t typesCount,
  int32_t *outputCols,
  int32_t outputColsCount,
  int32_t *sortCols,
  int32_t *sortAscendings,
  int32_t *sortNullFirsts,
  int32_t sortColsCount)
{
    using namespace omniruntime::jit;
    int sortColTypes[sortColsCount];

    for (int32_t i = 0; i < sortColsCount; ++i) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    ParamValue p_sourceTypes = ParamValue(sourceTypes, typesCount);
    ParamValue p_typeCount = ParamValue(&typesCount);
    ParamValue p_outputCols = ParamValue(outputCols, outputColsCount);
    ParamValue p_outputColCount = ParamValue(&outputColsCount);
    ParamValue p_sortCols = ParamValue(sortCols, sortColsCount);
    ParamValue p_sortColTypes = ParamValue(sortColTypes, sortColsCount);
    ParamValue p_sortAscendings = ParamValue(sortAscendings, sortColsCount);
    ParamValue p_sortNullFirsts = ParamValue(sortNullFirsts, sortColsCount);
    ParamValue p_sortColCount = ParamValue(&sortColsCount);

    Specialization *compareToSp = new Specialization();
    compareToSp->addSpecializedParam(1, &p_sortCols);
    compareToSp->addSpecializedParam(2, &p_sortColTypes);
    compareToSp->addSpecializedParam(3, &p_sortAscendings);
    compareToSp->addSpecializedParam(4, &p_sortNullFirsts);
    compareToSp->addSpecializedParam(5, &p_sortColCount);

    Specialization *allocColumnsSp = new Specialization();
    allocColumnsSp->addSpecializedParam(1, &p_sourceTypes);
    allocColumnsSp->addSpecializedParam(2, &p_outputCols);
    allocColumnsSp->addSpecializedParam(3, &p_outputColCount);

    Specialization *getOutputSp = new Specialization();
    getOutputSp->addSpecializedParam(1, &p_outputCols);
    getOutputSp->addSpecializedParam(2, &p_outputColCount);
    getOutputSp->addSpecializedParam(4, &p_sourceTypes);

    std::map<std::string, Specialization> sortSps = {
        {OMNIJIT_SORT_ALLOC_COLUMNS, *allocColumnsSp}
    };

    std::map<std::string, Specialization> pagesIndexSps = {
        {OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp},
        {OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp}
    };

    omniruntime::jit::Context *sortContext = new omniruntime::jit::Context("sort", sortSps, std::vector<std::string>(), std::vector<std::string>(), true);
    omniruntime::jit::Context *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    omniruntime::jit::Context *pagesIndexContext = new omniruntime::jit::Context("pages_index", pagesIndexSps, std::vector<std::string>(), std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*sortContext, *memoryPoolContext, *pagesIndexContext});
    auto createOperatorFunc = jit->specialize();

    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    // jitContext->jitter = reinterpret_cast<uintptr_t>(jitter.release());

    return jitContext;
}

const int32_t TABLE_COUNT = 10;
const int32_t DISTINCT_VALUE_COUNT = 4;
const int32_t REPEAT_COUNT = 250000;
const int32_t COLUMN_COUNT_2 = 2;
const int32_t COLUMN_COUNT_4 = 4;
void buildSortTestData(Table **tables, int32_t columnCount)
{
    uint32_t positionCount = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int64_t *data;
    uint32_t size = positionCount * sizeof(int64_t);
    uint32_t idx = 0;

    for (int32_t i = 0; i < TABLE_COUNT; i++) {
        Table *table = new Table(positionCount, columnCount);
        for (int32_t colIdx = 0; colIdx < columnCount; colIdx++) {
            data = (int64_t *)malloc(size);
            idx = 0;
            for (int32_t j = 0; j < DISTINCT_VALUE_COUNT; j++) {
                for (int32_t k = 0; k < REPEAT_COUNT; k++) {
                    data[idx] = j;
                    idx++;
                }
            }
            Column *column = new Column(data, INT64, positionCount);
            table->setColumn(column, INT64);
        }
        tables[i] = table;
    }
}

TEST (NativeOmniSortOperatorTest, TestSortPerformance)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t DATA_SIZE = 100000;
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

    int32_t sourceTypes[2] = {1, 1};
    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {true, true};

    SortOperatorFactory *operatorFactory = SortOperatorFactory::createSortOperatorFactory(
        sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = createTestSortJitContext(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->setJitContext(jitContext);

    clock_t start = clock();
    SortOperator *sortOperator = (SortOperator *)createTestOperator(operatorFactory);
    sortOperator->addInput(tables, rowCounts, 1);
    vector<Table *> outputTables;
    sortOperator->getOutput(outputTables);
    std::cout << "sort and get output elapsed end time: " << (double)(std::clock() - start) / 1000 << " ms" << std::endl;

    // free memory
    delete []data2;
    delete []data1;
    delete sortOperator;
    delete operatorFactory;
    delete jitContext;
    freeInputTable(tables, 1);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
}

 TEST(NativeOmniSortOperatorTest, testOrderByOneColumn)
 {
     //construct input data
     const int32_t DATA_SIZE = 5;
     int32_t data1[DATA_SIZE] = {4, 3, 2, 1, 0};
     int64_t data2[DATA_SIZE] = {0, 1, 2, 3, 4};
     int32_t rowCounts[1] = {DATA_SIZE};

     Table **tables = (Table **)malloc(1 * sizeof(Table *));
     tables[0] = new Table(DATA_SIZE, 2);
     Column *column1 = new Column(data1, INT32, DATA_SIZE);
     Column *column2 = new Column(data2, INT64, DATA_SIZE);
     tables[0]->setColumn(column1, INT32);
     tables[0]->setColumn(column2, INT64);

     int sourceTypes[2] = {1, 2};
     int outputCols[2] = {0, 1};
     int sortCols[1] = {1};
     int ascendings[1] = {false};
     int nullFirsts[1] = {true};

     using namespace omniruntime::op;
     SortOperatorFactory *operatorFactory = SortOperatorFactory::createSortOperatorFactory(
         sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
     //JitContext *jitContext = createTestSortJitContext(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
     JitContext *jitContext = nullptr;
     operatorFactory->setJitContext(jitContext);

     SortOperator *sortOperator = (SortOperator *)createTestOperator(operatorFactory);
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
     delete jitContext;
     freeInputTable(tables, 1);
     freeDataInColumn(&outputTables[0], outputTables.size());
     freeOutputTable(outputTables);
 }

TEST(NativeOmniSortOperatorTest, testOrderByDoubleColumn)
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
    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    SortOperatorFactory *operatorFactory = SortOperatorFactory::createSortOperatorFactory(
        sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = createTestSortJitContext(sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->setJitContext(jitContext);

    SortOperator *sortOperator = (SortOperator *)createTestOperator(operatorFactory);
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
    delete jitContext;
    freeInputTable(tables, 1);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
    delete expectTable;
}

TEST(NativeOmniSortOperatorTest, testOrderByDoubleColumnV2)
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
    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    SortOperatorFactory *operatorFactory = SortOperatorFactory::createSortOperatorFactory(
        sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = createTestSortJitContext(sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    //JitContext *jitContext = nullptr;
    operatorFactory->setJitContext(jitContext);

    SortOperator *sortOperator = (SortOperator *)createTestOperator(operatorFactory);
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
    delete jitContext;
    freeInputTable(tables, 1);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
    delete expectTable;
}

TEST(NativeOmniSortOperatorTest, testOrderByTwoColumnPerf)
{
    using namespace omniruntime::op;

    int32_t rowNum = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    Table **tables = (Table **)malloc(TABLE_COUNT * sizeof(Table *));

    buildSortTestData(tables, COLUMN_COUNT_2);
    std::cout<<"finish build sort data" << endl;

    int32_t rowCounts[TABLE_COUNT];
    for (int32_t i = 0; i < TABLE_COUNT; i++) {
        rowCounts[i] = rowNum;
    }
    int32_t sourceTypes[] = {2, 2};
    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {0, 1};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};

    SortOperatorFactory *operatorFactory = SortOperatorFactory::createSortOperatorFactory(
        sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = createTestSortJitContext(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->setJitContext(jitContext);

    Timer timer;
    timer.setStart();
    SortOperator *sortOperator = (SortOperator *)createTestOperator(operatorFactory);
    sortOperator->addInput(tables, rowCounts, TABLE_COUNT);
    vector<Table *> outputTables;
    sortOperator->getOutput(outputTables);
    timer.calculateElapse();
    double wall_elapsed = timer.getWallElapse();
    double cpu_elapsed = timer.getCpuElapse();
    std::cout << "testOrderByTwoColumnPerf wall_elapsed time: " << wall_elapsed << "s" << std::endl;
    std::cout << "testOrderByTwoColumnPerf cpu_elapsed time: " << cpu_elapsed << "s" << std::endl;

    delete sortOperator;
    if (jitContext != nullptr) {
        delete jitContext;
    }
    delete operatorFactory;
    freeDataInColumn(tables, TABLE_COUNT);
    freeInputTable(tables, TABLE_COUNT);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
}

struct SortThreadArgs
{
    int64_t operatorFactoryAddr;
    bool isOriginal;
    Table **tables;
    int32_t *rowCounts;
    int32_t tableCount;
};

void setSortThreadArgs(struct SortThreadArgs *sortThreadArgs, int64_t operatorFactoryAddr, bool isOriginal, Table **tables, int32_t *rowCounts, int32_t tableCount)
{
    sortThreadArgs->operatorFactoryAddr = operatorFactoryAddr;
    sortThreadArgs->isOriginal = isOriginal;
    sortThreadArgs->tables = tables;
    sortThreadArgs->rowCounts = rowCounts;
    sortThreadArgs->tableCount = tableCount;
}

void testOrderBy(struct SortThreadArgs *threadArgs)
{
    using namespace omniruntime::op;

    // create operator
    SortOperatorFactory *operatorFactory = (SortOperatorFactory *)threadArgs->operatorFactoryAddr;
    SortOperator *sortOperator;
    if (threadArgs->isOriginal) {
        sortOperator = dynamic_cast<SortOperator *>(operatorFactory->createOperator());
    }
    else {
        opt_module sortModule = (opt_module)(operatorFactory->getJitContext()->func);
        sortOperator = (SortOperator *)sortModule(operatorFactory);
    }

    sortOperator->addInput(threadArgs->tables, threadArgs->rowCounts, TABLE_COUNT);
    std::vector<Table *> outputTables;
    sortOperator->getOutput(outputTables);

    delete sortOperator;
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
}

TEST(NativeOmniSortOperatorTest, testOrderByOriginalMultiThreads)
{
    using namespace omniruntime::op;

    Table **tables = (Table **)malloc(TABLE_COUNT * sizeof(Table *));
    buildSortTestData(tables, COLUMN_COUNT_4);

    int32_t rowNum = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int32_t rowCounts[TABLE_COUNT];
    for (int32_t i = 0; i < TABLE_COUNT; i++) {
        rowCounts[i] = rowNum;
    }
    std::cout<<"finish build sort data" << endl;

    int32_t sourceTypes[] = {2, 2, 2, 2};
    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {2, 3};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};

    SortOperatorFactory *operatorFactory = SortOperatorFactory::createSortOperatorFactory(
            sourceTypes, 4, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = createTestSortJitContext(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->setJitContext(jitContext);

    struct SortThreadArgs threadArgs;
    setSortThreadArgs(&threadArgs, (int64_t)operatorFactory, true, tables, rowCounts, TABLE_COUNT);

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
            std::thread t(testOrderBy, &threadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << "testOrderByOriginalMultiThreads " << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << "testOrderByOriginalMultiThreads " << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    delete operatorFactory;
    if (jitContext != nullptr) {
        delete jitContext;
    }
    freeDataInColumn(tables, TABLE_COUNT);
    freeInputTable(tables, TABLE_COUNT);
}

TEST(NativeOmniSortOperatorTest, testOrderByJITMultiThreads)
{
    using namespace omniruntime::op;

    Table **tables = (Table **)malloc(TABLE_COUNT * sizeof(Table *));
    buildSortTestData(tables, COLUMN_COUNT_4);

    int32_t rowNum = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int32_t rowCounts[TABLE_COUNT];
    for (int32_t i = 0; i < TABLE_COUNT; i++) {
        rowCounts[i] = rowNum;
    }
    std::cout<<"finish build sort data" << endl;

    int32_t sourceTypes[] = {2, 2, 2, 2};
    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {2, 3};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};

    SortOperatorFactory *operatorFactory = SortOperatorFactory::createSortOperatorFactory(
            sourceTypes, 4, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = createTestSortJitContext(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->setJitContext(jitContext);

    struct SortThreadArgs threadArgs;
    setSortThreadArgs(&threadArgs, (int64_t)operatorFactory, false, tables, rowCounts, TABLE_COUNT);
   
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
            std::thread t(testOrderBy, &threadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << "testOrderByJITMultiThreads " << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << "testOrderByJITMultiThreads " << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    delete operatorFactory;
    if (jitContext != nullptr) {
        delete jitContext;
    }
    freeDataInColumn(tables, TABLE_COUNT);
    freeInputTable(tables, TABLE_COUNT);
}
