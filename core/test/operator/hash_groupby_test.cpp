#include <gtest/gtest.h>
#include "../src/operator/aggregator/hash_groupby.h"
#include <time.h>
#include <vector>
#include <iostream>
#include "../../src/jit/hammer.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/SourceMgr.h"
#include <thread>
#include <cstdlib>

long lrand()
{
    if (sizeof(int) < sizeof(long)) {
        return (static_cast<long>(rand())) << (sizeof(int) * 8) | rand();
    }
    return rand();
}

Table** buildInput(int32_t pageNum, int32_t rowPerpage, int32_t cardinality)
{
    Table** input = new Table*[pageNum];
    for (int32_t i = 0; i < pageNum; ++i) {
        Table* table = new Table(rowPerpage, 4);
        int64_t* data1 = new int64_t[rowPerpage];
        for (int32_t i = 0; i < rowPerpage; ++i) {
            data1[i] = i % cardinality;
        }
        Column* col1 = new Column(data1, INT64, rowPerpage);

        int64_t* data2 = new int64_t[rowPerpage];
        for (int32_t i = 0; i < rowPerpage; ++i) {
            data2[i] = i % cardinality;
        }
        Column* col2 = new Column(data2, INT64, rowPerpage);

        int64_t* data3 = new int64_t[rowPerpage];
        for (int32_t i = 0; i < rowPerpage; ++i) {
            data3[i] = 1;
        }
        Column* col3 = new Column(data3, INT64, rowPerpage);

        int64_t* data4 = new int64_t[rowPerpage];
        for (int32_t i = 0; i < rowPerpage; ++i) {
            data4[i] = 1;
        }
        Column* col4 = new Column(data4, INT64, rowPerpage);

        table->setColumn(col1, INT64);
        table->setColumn(col2, INT64);
        table->setColumn(col3, INT64);
        table->setColumn(col4, INT64);
        input[i] = table;
    }
    return input;
}

void destroyInput(Table** input, int32_t pageNum)
{
    for (int32_t i = 0; i < pageNum; ++i) {
        delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(0)->getData());
        delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(1)->getData());
        delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(2)->getData());
        delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(3)->getData());
        delete input[i];
    }
}

TEST(NativeOmniHashAggregationOperatorTest, VerfifyCorrectness)
{
    // create 10 pages
    const int PAGE_NUM = 10;
    const int DATA_SIZE = 100;
    const int CARDINALITY = 4;
    Table** input = new Table*[PAGE_NUM];
    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        Table* table = new Table(DATA_SIZE, 4);
        int32_t* data1 = new int32_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data1[i] = i % CARDINALITY;
        }
        Column* col1 = new Column(data1, INT32, DATA_SIZE);

        int64_t* data2 = new int64_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data2[i] = i % CARDINALITY;
        }
        Column* col2 = new Column(data2, INT64, DATA_SIZE);

        int64_t* data3 = new int64_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data3[i] = 1;
        }
        Column* col3 = new Column(data3, INT64, DATA_SIZE);

        int64_t* data4 = new int64_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data4[i] = 1;
        }
        Column* col4 = new Column(data4, INT64, DATA_SIZE);

        table->setColumn(col1, INT32);
        table->setColumn(col2, INT64);
        table->setColumn(col3, INT64);
        table->setColumn(col4, INT64);
        input[i] = table;
    }
    
    ColumnIndex c0 = {0, INT32};
    ColumnIndex c1 = {1, INT64};
    ColumnIndex c2 = {2, INT64};
    ColumnIndex c3 = {3, INT64};
    std::vector<ColumnIndex> v1 = {c0, c1};
    std::vector<ColumnIndex> v2 = {c2, c3};
    std::vector<Aggregator*> aggs;
    SumAggregator* sum1 = new SumAggregator(3);
    SumAggregator* sum2 = new SumAggregator(3);
    aggs.push_back(sum1);
    aggs.push_back(sum2);
    NativeOmniHashAggregationOperator* groupBy = new NativeOmniHashAggregationOperator(v1, v2, aggs);

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        groupBy->addInput(input[i], DATA_SIZE);
    }
    
    std::vector<Table*> result;
    int32_t tableCount = groupBy->getOutput(result);

    EXPECT_EQ(result[0]->getColumnNumber(), 4);
    EXPECT_EQ(result[0]->getPositionCount(), 4);

    for (int32_t i = 0; i < result[0]->getColumnNumber(); ++i) {
        Column* col = result[0]->getColumn(i);
        col->printColumn();
    }
    
    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(0)->getData());
        delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(1)->getData());
        delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(2)->getData());
        delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(3)->getData());
        delete input[i];
    }
    
    for (int32_t i = 0; i < result[0]->getColumnNumber(); ++i) {
        Column* col = result[0]->getColumn(i);
        delete[] reinterpret_cast<int64_t*>(col->getData());
        delete col;
    }
}

TEST(NativeOmniHashAggregationOperatorTest, VerfifyCorrectness_GroupByAggSameCols)
{
    //FIXME INT32+INT64
    // create 10 pages
    const int PAGE_NUM = 10;
    Table** input = new Table*[PAGE_NUM];
    const int DATA_SIZE = 10;
    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        Table* table = new Table(DATA_SIZE, 2);
        int64_t* data1 = new int64_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data1[i] = i % 3;
        }
        Column* col1 = new Column(data1, INT64, DATA_SIZE);

        int64_t* data2 = new int64_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data2[i] = i % 3;
        }
        Column* col2 = new Column(data2, INT64, DATA_SIZE);
        table->setColumn(col1, INT64);
        table->setColumn(col2, INT64);
        input[i] = table;
    }
    ColumnIndex c0 = {0, INT64};
    ColumnIndex c1 = {1, INT64};
    std::vector<ColumnIndex> v1 = {c0, c1};
    std::vector<ColumnIndex> v2 = {c0, c1};
    std::vector<Aggregator*> aggs;
    SumAggregator* sum1 = new SumAggregator(1);
    SumAggregator* sum2 = new SumAggregator(2);
    aggs.push_back(sum1);
    aggs.push_back(sum2);
    NativeOmniHashAggregationOperator* groupBy = new NativeOmniHashAggregationOperator(v1, v2, aggs);

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        groupBy->addInput(input[i], DATA_SIZE);
    }
    
    std::vector<Table*> result;
    int32_t tableCount = groupBy->getOutput(result);

    EXPECT_EQ(result[0]->getColumnNumber(), 4);

    for (int32_t i = 0; i < result[0]->getColumnNumber(); ++i) {
        Column* col = result[0]->getColumn(i);
        col->printColumn();
    }
    
    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        delete[] reinterpret_cast<int32_t*>(input[i]->getColumn(0)->getData());
        delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(1)->getData());
        delete input[i];
    }
    
    for (int32_t i = 0; i < result[0]->getColumnNumber(); ++i) {
        Column* col = result[0]->getColumn(i);
        delete[] reinterpret_cast<int64_t*>(col->getData());
        delete col;
    }
}

#include <time.h>
#include <mutex>
double total_cpu_time;
double total_wall_time;
void perfTest(int64_t moduleAddr, Table** input, int32_t pageNum, int32_t* rowCount)
{
    uint32_t* columnTypes1 = new uint32_t[input[0]->getColumnNumber()];
    for (int32_t i = 0; i < input[0]->getColumnNumber(); ++i) {
        columnTypes1[i] = (int32_t)input[0]->getColumnTypes()[i];
    }
    // create operatory
    NativeOmniHashAggregationOperatorFactory* nativeOperatorFactory  = reinterpret_cast<NativeOmniHashAggregationOperatorFactory*>(moduleAddr);
    auto groupBy = reinterpret_cast<opt_module>(nativeOperatorFactory->getJitContext()->func)(nativeOperatorFactory);
 
    // execution
    auto errNo = groupBy->addInput(input, rowCount, pageNum);
    std::vector<Table*> result;
    int32_t tableCount = groupBy->getOutput(result); 
    EXPECT_EQ(result[0]->getColumnNumber(), 4);
    EXPECT_EQ(result[0]->getPositionCount(), 4);
    delete[] columnTypes1;
}

uint64_t prepare()
{
    uint32_t* groupCols = new uint32_t[2];
    groupCols[0] = 0;
    groupCols[1] = 1;
    uint32_t* groupTypes = new uint32_t[2];
    groupTypes[0] = 2;
    groupTypes[1] = 2;
    uint32_t* aggCols = new uint32_t[2];
    aggCols[0] = 2;
    aggCols[1] = 3;
    uint32_t* aggTypes = new uint32_t[2];
    aggTypes[0] = 2;
    aggTypes[1] = 2;
    // uint32_t aggFunType[] = {0, 0};
    uint32_t* aggFunType = new uint32_t[2];
    aggFunType[0] = 0;
    aggFunType[1] = 0;
    uint32_t retTypes[] = {1,1,1,1};
    PrepareContext groupByColContext = {groupCols, 2};
    PrepareContext groupByTypeContext = {groupTypes, 2};
    PrepareContext aggColContext = {aggCols, 2};
    PrepareContext aggTypeContext = {aggTypes, 2};
    PrepareContext aggFuncTypeContext = {aggFunType, 2};
    PrepareContext retTypesContext = {retTypes, 4};

    using namespace codegen;
    std::map<std::string, ParamValue *> testParam;
    std::list<Hammer *> deps = std::list<Hammer *>();
    int32_t groupColNum = groupByColContext.len;
    int32_t aggColNum = aggColContext.len;
    int32_t colNum = groupByColContext.len + aggColContext.len;
    int32_t* colTypes = new int32_t[colNum];
    
    for (int i = 0; i < groupColNum; ++i) {
        colTypes[groupByColContext.context[i]] = groupByTypeContext.context[i];
    }
    for (int i = 0; i < aggColNum; ++i) {
        colTypes[aggColContext.context[i]] = aggTypeContext.context[i];
    }

    ParamValue p_col_type = ParamValue(colTypes, colNum);
    ParamValue p_col_count = ParamValue(&colNum);
    ParamValue p_groupByColIdx = ParamValue((int32_t*)groupByColContext.context, groupColNum);
    ParamValue p_group_num = ParamValue(&groupColNum);
    ParamValue p_aggColIdx = ParamValue((int32_t*)aggColContext.context, aggColNum);
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_data_type = ParamValue((int32_t*)aggTypeContext.context, aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t*)aggFuncTypeContext.context, aggColNum);

    testParam["_ZN33NativeOmniHashAggregationOperator6inloopEPPcjPiiS2_iS2_iS2_@3"] = &p_col_type;
    testParam["_ZN33NativeOmniHashAggregationOperator6inloopEPPcjPiiS2_iS2_iS2_@4"] = &p_col_count;
    testParam["_ZN33NativeOmniHashAggregationOperator6inloopEPPcjPiiS2_iS2_iS2_@6"] = &p_group_num;
    testParam["_ZN33NativeOmniHashAggregationOperator6inloopEPPcjPiiS2_iS2_iS2_@8"] = &p_agg_num;
    testParam["_ZN33NativeOmniHashAggregationOperator6inloopEPPcjPiiS2_iS2_iS2_@9"] = &p_agg_types;
    
    testParam["processAgg@2"] =  &p_agg_types;
    testParam["processAgg@3"] =  &p_agg_num;
    testParam["processAgg@4"] =  &p_col_type;

    testParam["_ZN33NativeOmniHashAggregationOperator15constructColumnEP5TablePijjiR8Iterator@2"] = &p_col_type;
    testParam["_ZN33NativeOmniHashAggregationOperator15constructColumnEP5TablePijjiR8Iterator@3"] = &p_group_num;
    testParam["_ZN33NativeOmniHashAggregationOperator15constructColumnEP5TablePijjiR8Iterator@4"] = &p_agg_num;

    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");
    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/local/lib/libjemalloc.so.2");

    Hammer hammer1("/opt/lib/ir/memory_pool.ll", testParam);
    Hammer hammer2("/opt/lib/ir/hash_groupby.ll", testParam);
    Hammer hammer3("/opt/lib/ir/aggregator.ll", testParam);
    
    hammer1.harden();
    hammer2.harden();
    hammer3.harden();

    deps.push_back(&hammer3);
    deps.push_back(&hammer2);
    HammerConfig hammerConfig;
    auto jitter = hammer1.create_jitter(deps, hammerConfig);
    auto func = (opt_module)(jitter->lookup("_ZN40NativeOmniHashAggregationOperatorFactory18createOmniOperatorEv")->getAddress());
    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(func);
    jitContext->jitter = reinterpret_cast<uintptr_t>(jitter.release());
    std::cout << "after jit" << std::endl;
    NativeOmniHashAggregationOperatorFactory* nativeOperatorFactory = new NativeOmniHashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext);
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->setJitContext(jitContext); 
    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

TEST(NativeOmniHashAggregationOperatorTest, PerfViaAPI_Multiple_Threads)
{
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;

    int32_t pageNum = 20;
    int32_t rowPerPage = 100000;
    int32_t cardinality = 4;
    
    Table** input = buildInput(pageNum, rowPerPage, cardinality);
    int32_t* rowCount = new int32_t[pageNum];
    for (int32_t i = 0; i < pageNum; i++) {
        rowCount[i] = rowPerPage;
    }
    uint64_t factoryObjAddr = prepare();
    std::cout << "after prepare" << std::endl;
    int threadNums[] = {1, 8, 16, 32, 64};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

       	struct timespec cpu_start, wall_start, cpu_end, wall_end;
        clock_gettime(CLOCK_REALTIME, &wall_start);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_start);
        double wall_elapsed = 0;
        double cpu_elapsed = 0;
        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        for (int32_t i = 0; i < threadNum; ++i) {
            // same stage Id
            std::thread t(perfTest, factoryObjAddr, input, pageNum, rowCount);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        clock_gettime(CLOCK_REALTIME, &wall_end);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_end);
        long seconds_wall = wall_end.tv_sec - wall_start.tv_sec;
        long seconds_cpu = cpu_end.tv_sec - cpu_start.tv_sec;
        long ns_wall = wall_end.tv_nsec - wall_start.tv_nsec;
        long ns_cpu = cpu_end.tv_nsec - cpu_start.tv_nsec;
        wall_elapsed = seconds_wall + ns_wall*1e-9;
        cpu_elapsed = seconds_cpu + ns_cpu*1e-9;
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    delete[] rowCount;
    destroyInput(input, pageNum);
}

void perfTestOriginal(int64_t moduleAddr, Table** input)
{
    int32_t pageNum = 20;
    uint32_t* columnTypes1 = new uint32_t[input[0]->getColumnNumber()];
    for (int32_t i = 0; i < input[0]->getColumnNumber(); ++i) {
        columnTypes1[i] = (int32_t)input[0]->getColumnTypes()[i];
    }
    // create operatory
    NativeOmniHashAggregationOperatorFactory* nativeOperatorFactory  = reinterpret_cast<NativeOmniHashAggregationOperatorFactory*>(moduleAddr);
    auto groupBy = nativeOperatorFactory->createOmniOperator();
 
    // execution
    for (int32_t i = 0; i < pageNum; ++i) {
        groupBy->addInput(input[i], input[i]->getPositionCount());
    }
    std::vector<Table*> result;
    int32_t tableCount = groupBy->getOutput(result); 
    EXPECT_EQ(result[0]->getColumnNumber(), 4);
    EXPECT_EQ(result[0]->getPositionCount(), 4);
    delete[] columnTypes1;
}

TEST(NativeOmniHashAggregationOperatorTest, Original_Multiple_Threads)
{
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;

    int32_t pageNum = 20;
    int32_t rowPerPage = 100000;
    int32_t cardinality = 4;
    
    Table** input = buildInput(pageNum, rowPerPage, cardinality);
    
    uint32_t* groupCols = new uint32_t[2];
    groupCols[0] = 0;
    groupCols[1] = 1;
    uint32_t* groupTypes = new uint32_t[2];
    groupTypes[0] = 2;
    groupTypes[1] = 2;
    uint32_t* aggCols = new uint32_t[2];
    aggCols[0] = 2;
    aggCols[1] = 3;
    uint32_t* aggTypes = new uint32_t[2];
    aggTypes[0] = 2;
    aggTypes[1] = 2;
    // uint32_t aggFunType[] = {0, 0};
    uint32_t* aggFunType = new uint32_t[2];
    aggFunType[0] = 0;
    aggFunType[1] = 0;
    uint32_t retTypes[] = {1,1,1,1};
    PrepareContext groupByColContext = {groupCols, 2};
    PrepareContext groupByTypeContext = {groupTypes, 2};
    PrepareContext aggColContext = {aggCols, 2};
    PrepareContext aggTypeContext = {aggTypes, 2};
    PrepareContext aggFuncTypeContext = {aggFunType, 2};
    PrepareContext retTypesContext = {retTypes, 4};
    NativeOmniHashAggregationOperatorFactory* nativeOperatorFactory = new NativeOmniHashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext);
    uint64_t factoryObjAddr = reinterpret_cast<uint64_t>(nativeOperatorFactory);
    
    int threadNums[] = {1, 8, 16, 32, 64};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

       	struct timespec cpu_start, wall_start, cpu_end, wall_end;
        clock_gettime(CLOCK_REALTIME, &wall_start);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_start);
        double wall_elapsed = 0;
        double cpu_elapsed = 0;
        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        for (int32_t i = 0; i < threadNum; ++i) {
            // same stage Id
            std::thread t(perfTestOriginal, factoryObjAddr, input);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        clock_gettime(CLOCK_REALTIME, &wall_end);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_end);
        long seconds_wall = wall_end.tv_sec - wall_start.tv_sec;
        long seconds_cpu = cpu_end.tv_sec - cpu_start.tv_sec;
        long ns_wall = wall_end.tv_nsec - wall_start.tv_nsec;
        long ns_cpu = cpu_end.tv_nsec - cpu_start.tv_nsec;
        wall_elapsed = seconds_wall + ns_wall*1e-9;
        cpu_elapsed = seconds_cpu + ns_cpu*1e-9;
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }

    destroyInput(input, pageNum);
}
