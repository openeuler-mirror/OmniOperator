#include "gtest/gtest.h"
#include "../../src/operator/sort/sort.h"
#include "../../src/jit/hammer.h"
#include "../util/test_util.h"
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
    using namespace omniruntime::codegen;
    std::map<std::string, ParamValue *> testParam;
    std::list<Hammer *> deps = std::list<Hammer *>();
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

    testParam["_Z9compareTolPiS_S_S_iii@1"] = &p_sortCols;
    testParam["_Z9compareTolPiS_S_S_iii@2"] = &p_sortColTypes;
    testParam["_Z9compareTolPiS_S_S_iii@3"] = &p_sortAscendings;
    testParam["_Z9compareTolPiS_S_S_iii@4"] = &p_sortNullFirsts;
    testParam["_Z9compareTolPiS_S_S_iii@5"] = &p_sortColCount;

    testParam["_ZN11omniruntime2op12allocColumnsElPiS1_ii@1"] = &p_sourceTypes;
    testParam["_ZN11omniruntime2op12allocColumnsElPiS1_ii@2"] = &p_outputCols;
    testParam["_ZN11omniruntime2op12allocColumnsElPiS1_ii@3"] = &p_outputColCount;

    testParam["_ZN10PagesIndex9getOutputEPiilS0_ii@1"] = &p_outputCols;
    testParam["_ZN10PagesIndex9getOutputEPiilS0_ii@2"] = &p_outputColCount;
    testParam["_ZN10PagesIndex9getOutputEPiilS0_ii@4"] = &p_sourceTypes;

    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");
    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/local/lib/libjemalloc.so.2");

    Hammer hammer1("/opt/lib/ir/sort.ll", testParam);
    Hammer hammer2("/opt/lib/ir/pages_index.ll", testParam);
    Hammer hammer3("/opt/lib/ir/memory_pool.ll", testParam);
    hammer1.harden();
    hammer2.harden();
    hammer3.harden();
    deps.push_back(&hammer2);
    deps.push_back(&hammer3);

    HammerConfig hammerConfig;
    auto jitter = hammer1.create_jitter(deps, hammerConfig);
    auto func = (opt_module)(jitter->lookup("_ZN11omniruntime2op19SortOperatorFactory14createOperatorEv")->getAddress());

    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(func);;
    jitContext->jitter = reinterpret_cast<uintptr_t>(jitter.release());

    return jitContext;
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
    //JitContext *jitContext = NULL;
    operatorFactory->setJitContext(jitContext);    
    SortOperator *sortOperator;
    if (jitContext == NULL) {   
        sortOperator = (SortOperator *)operatorFactory->createOperator();
    }
    else {
        opt_module sortModule = (opt_module)(jitContext->func);
        sortOperator = (SortOperator *)sortModule(operatorFactory);
    }

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
    delete jitContext;
    freeInputTable(tables, 1);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
}

// TEST(NativeOmniSortOperatorTest, testOrderByOneColumn)
// {
//     //construct input data
//     const int32_t DATA_SIZE = 5;
//     int32_t data1[DATA_SIZE] = {4, 3, 2, 1, 0};
//     int64_t data2[DATA_SIZE] = {0, 1, 2, 3, 4};
//     int32_t rowCounts[1] = {DATA_SIZE};

//     Table **tables = (Table **)malloc(1 * sizeof(Table *));
//     tables[0] = new Table(DATA_SIZE, 2);
//     Column *column1 = new Column(data1, INT32, DATA_SIZE);
//     Column *column2 = new Column(data2, INT64, DATA_SIZE);
//     tables[0]->setColumn(column1, INT32);
//     tables[0]->setColumn(column2, INT64);

//     int sourceTypes[2] = {1, 2};
//     int outputCols[2] = {0, 1};
//     int sortCols[1] = {1};
//     int ascendings[1] = {false};
//     int nullFirsts[1] = {true};

//     SortOperatorFactory *operatorFactory = SortOperatorFactory::createSortOperatorFactory(
//         sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
//     JitContext *jitContext = createTestSortJitContext(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
//     operatorFactory->setJitContext(jitContext);     
//     SortOperator *sortOperator;
//     if (jitContext == NULL) {   
//         sortOperator = (SortOperator *)operatorFactory->createOperator();
//     }
//     else {
//         opt_module sortModule = (opt_module)(jitContext->func);
//         sortOperator = (SortOperator *)sortModule(operatorFactory);
//     }

//     sortOperator->addInput(tables, rowCounts, 1);
//     vector<Table *> outputTables;
//     sortOperator->getOutput(outputTables);

//     int32_t expectData1[DATA_SIZE] = {0, 1, 2, 3, 4};
//     Column expectCol1(expectData1, INT32, DATA_SIZE);
//     int64_t expectData2[DATA_SIZE] = {4, 3, 2, 1, 0};
//     Column expectCol2(expectData2, INT64, DATA_SIZE);
//     Table* expectTable = new Table(DATA_SIZE, 2);
//     expectTable->setColumn(&expectCol1, INT32);
//     expectTable->setColumn(&expectCol2, INT64);

//     EXPECT_TRUE(tableMatch(outputTables[0], expectTable));

//     //free memory
//     delete sortOperator;
//     delete operatorFactory;
//     delete jitContext;
//     freeInputTable(tables, 1);
//     freeDataInColumn(&outputTables[0], outputTables.size());
//     freeOutputTable(outputTables);
// }

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
    //JitContext *jitContext = NULL;
    operatorFactory->setJitContext(jitContext);
    SortOperator *sortOperator;
    if (jitContext == NULL) {   
        sortOperator = (SortOperator *)operatorFactory->createOperator();
    }
    else {
        opt_module sortModule = (opt_module)(jitContext->func);
        sortOperator = (SortOperator *)sortModule(operatorFactory);
    }

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
    //JitContext *jitContext = NULL;
    operatorFactory->setJitContext(jitContext);    
    SortOperator *sortOperator;
    if (jitContext == NULL) {   
        sortOperator = (SortOperator *)operatorFactory->createOperator();
    }
    else {
        opt_module sortModule = (opt_module)(jitContext->func);
        sortOperator = (SortOperator *)sortModule(operatorFactory);
    }

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

TEST(NativeOmniSortOperatorTest, testOrderByTwoColumnPerf)
{
    using namespace omniruntime::op;
    printf("testOrderByTwoColumnPerf called\n");

    int32_t tableCount = 1;
    int32_t distinctValue = 400;
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

    SortOperatorFactory *operatorFactory = SortOperatorFactory::createSortOperatorFactory(
        sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = createTestSortJitContext(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    //JitContext *jitContext = NULL;
    operatorFactory->setJitContext(jitContext);

    SortOperator *sortOperator;
    if (jitContext == NULL) {   
        sortOperator = (SortOperator *)operatorFactory->createOperator();
    }
    else {
        opt_module sortModule = (opt_module)(jitContext->func);
        sortOperator = (SortOperator *)sortModule(operatorFactory);
    }

    typedef std::chrono::high_resolution_clock Time;
    typedef std::chrono::milliseconds ms;
    typedef std::chrono::duration<float> fsec;
    auto t0 = Time::now();
    sortOperator->addInput(tables, rowCounts, tableCount);
    vector<Table *> outputTables;
    sortOperator->getOutput(outputTables);
    auto t1 = Time::now();
    fsec fs = t1 - t0;
    ms d = std::chrono::duration_cast<ms>(fs);
    std::cout << "testOrderByTwoColumnPerf sort elapsed time: " << (double)d.count() << "ms" << std::endl;

    delete sortOperator;
    delete operatorFactory;
    if (jitContext != NULL) {
        delete jitContext;
    }
    freeDataInColumn(tables, tableCount);
    freeInputTable(tables, tableCount);
    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
}
