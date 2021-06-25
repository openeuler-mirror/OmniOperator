#include <gtest/gtest.h>
#include "../../src/operator/aggregation/group_aggregation.h"
#include "../../src/operator/aggregation/non_group_aggregation.h"
#include "../util/test_util.h"
#include <time.h>
#include <vector>
#include <iostream>
#include "../../src/jit/jit.h"
#include "../../src/jit/specialization.h"
#include "../../src/operator/optimization.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/SourceMgr.h"
#include <thread>
#include <cstdlib>

using namespace std;

const int32_t PAGE_NUM = 10;
const int32_t ROW_PER_PAGE = 10000000;
const int32_t CARDINALITY = 4;
const int32_t COLUMN_NUM = 4;
const bool INPUT_MODE = true;
const bool OUTPUT_MODE = false;

long lrand()
{
    if (sizeof(int) < sizeof(long)) {
        return (static_cast<long>(rand())) << (sizeof(int) * 8) | rand();
    }
    return rand();
}

Table** buildInput(int32_t pageNum, int32_t colNum, int32_t rowPerpage, int32_t cardinality)
{
    Table** input = new Table*[pageNum];
    for (int32_t i = 0; i < pageNum; ++i) {
        Table* table = new Table(rowPerpage, colNum);
        for (int32_t c = 0; c < colNum; ++ c) {
            int64_t* data = new int64_t[rowPerpage];
            for (int32_t i = 0; i < rowPerpage; ++i) {
                data[i] = i % cardinality;
            }
            Column* col = new Column(data, INT64, rowPerpage);
            table->setColumn(col, INT64);
        }
        input[i] = table;
    }
    return input;
}

void destroyInput(Table** input, int32_t pageNum, int32_t colNum)
{
    for (int32_t i = 0; i < pageNum; ++i) {
        for (int32_t c =0; c < colNum; ++c) {
            delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(c)->getData());
        }
        delete input[i];
    }
}

TEST(HashAggregationOperatorTest, VerifyCorrectness)
{
    using namespace omniruntime::op;
    // create 10 pages
    const int PAGE_NUM = 10;
    const int ROW_SIZE = 100;
    const int CARDINALITY = 4;
    const int COLUMN_COUNT = 7; // groupby*2 + sum + avg + count + min + max
    string aggNames[] = {"group", "group", "sum", "avg", "count", "min", "max"};

    Table** input = buildInput(PAGE_NUM, COLUMN_COUNT, ROW_SIZE, CARDINALITY);

    // First stage
    ColumnIndex c0 = {0, INT64};
    ColumnIndex c1 = {1, INT64};
    ColumnIndex c2 = {2, INT64};
    ColumnIndex c3 = {3, INT64};
    ColumnIndex c4 = {4, INT64};
    ColumnIndex c5 = {5, INT64};
    ColumnIndex c6 = {6, INT64};
    std::vector<ColumnIndex> groupByColumns1 = {c0, c1};
    std::vector<ColumnIndex> aggregateColumns1 = {c2, c3, c4, c5, c6};
    std::vector<Aggregator*> aggs1;
    SumAggregator* sumAgg1 = new SumAggregator(2, INPUT_MODE, OUTPUT_MODE);
    AverageAggregator* avgAgg1 = new AverageAggregator(2, INPUT_MODE, OUTPUT_MODE);
    CountAggregator* countAgg1 = new CountAggregator(2, INPUT_MODE, OUTPUT_MODE);
    MinAggregator* minAgg1 = new MinAggregator(2, INPUT_MODE, OUTPUT_MODE);
    MaxAggregator* maxAgg1 = new MaxAggregator(2, INPUT_MODE, OUTPUT_MODE);
    aggs1.push_back(sumAgg1);
    aggs1.push_back(avgAgg1);
    aggs1.push_back(countAgg1);
    aggs1.push_back(minAgg1);
    aggs1.push_back(maxAgg1);
    HashAggregationOperator* groupBy1 = new HashAggregationOperator(groupByColumns1, aggregateColumns1, aggs1);

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        groupBy1->addInput(input[i], ROW_SIZE);
    }

    std::vector<Table*> result1;
    groupBy1->getOutput(result1);
    delete groupBy1;

    ColumnIndex c7 = {0, INT64};
    ColumnIndex c8 = {1, INT64};
    ColumnIndex c9 = {2, INT64};
    ColumnIndex c10 = {3, INT64};
    ColumnIndex c11 = {4, INT64};
    ColumnIndex c12 = {5, INT64};
    ColumnIndex c13 = {6, INT64};
    groupByColumns1 = {c7, c8};
    aggregateColumns1 = {c9, c10, c11, c12, c13};
    sumAgg1 = new SumAggregator(2, INPUT_MODE, OUTPUT_MODE);
    avgAgg1 = new AverageAggregator(2, INPUT_MODE, OUTPUT_MODE);
    countAgg1 = new CountAggregator(2, INPUT_MODE, OUTPUT_MODE);
    minAgg1 = new MinAggregator(2, INPUT_MODE, OUTPUT_MODE);
    maxAgg1 = new MaxAggregator(2, INPUT_MODE, OUTPUT_MODE);
    aggs1.clear();
    aggs1.push_back(sumAgg1);
    aggs1.push_back(avgAgg1);
    aggs1.push_back(countAgg1);
    aggs1.push_back(minAgg1);
    aggs1.push_back(maxAgg1);
    HashAggregationOperator* groupBy2 = new HashAggregationOperator(groupByColumns1, aggregateColumns1, aggs1);

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        groupBy2->addInput(input[i], ROW_SIZE);
    }

    int32_t tableCount2 = groupBy2->getOutput(result1);
    delete groupBy2;
    for (int32_t tIdx = 0; tIdx < result1.size(); ++tIdx) {
        for (int32_t i = 0; i < result1[tIdx]->getColumnNumber(); ++i) {
            Column *col = result1[tIdx]->getColumn(i);
            std::cout << aggNames[i] << " ";
            col->printColumn();
        }
    }

    // Second stage
    ColumnIndex c14 = {0, INT64};
    ColumnIndex c15 = {1, INT64};
    ColumnIndex c16 = {2, INT64};
    ColumnIndex c17 = {3, INT64};
    ColumnIndex c18 = {4, INT64};
    ColumnIndex c19 = {5, INT64};
    ColumnIndex c20 = {6, INT64};
    std::vector<ColumnIndex> groupByColumns2 = {c14, c15};
    std::vector<ColumnIndex> aggregateColumns2 = {c16, c17, c18, c19, c20};
    std::vector<Aggregator*> aggs2;
    SumAggregator* sumAgg2 = new SumAggregator(2, false, false);
    AverageAggregator* avgAgg2 = new AverageAggregator(2, false, false);
    CountAggregator* countAgg2 = new CountAggregator(2, false, false);
    MinAggregator* minAgg2 = new MinAggregator(2, false, false);
    MaxAggregator* maxAgg2 = new MaxAggregator(2, false, false);
    aggs2.push_back(sumAgg2);
    aggs2.push_back(avgAgg2);
    aggs2.push_back(countAgg2);
    aggs2.push_back(minAgg2);
    aggs2.push_back(maxAgg2);
    HashAggregationOperator* groupBy3 = new HashAggregationOperator(groupByColumns2, aggregateColumns2, aggs2);

    for (int32_t i = 0; i < result1.size(); ++i) {
        groupBy3->addInput(result1[i], result1[i]->getPositionCount());
    }

    for (int32_t i = 0; i < result1[0]->getColumnNumber(); ++i) {
        Column* col = result1[0]->getColumn(i);
        delete[] reinterpret_cast<int64_t*>(col->getData());
        delete col;
    }

    std::vector<Table*> result2;
    groupBy3->getOutput(result2);
    delete groupBy3;

    EXPECT_EQ(result2[0]->getColumnNumber(), 7);
    EXPECT_EQ(result2[0]->getPositionCount(), 4);

    for (int32_t i = 0; i < result2[0]->getColumnNumber(); ++i) {
        Column* col = result2[0]->getColumn(i);
        std::cout << aggNames[i] << " ";
        col->printColumn();
    }
    
    destroyInput(input, PAGE_NUM, COLUMN_COUNT);
    
    for (int32_t i = 0; i < result2[0]->getColumnNumber(); ++i) {
        Column* col = result2[0]->getColumn(i);
        delete[] reinterpret_cast<int64_t*>(col->getData());
        delete col;
    }
}

TEST(HashAggregationOperatorTest, VerfifyCorrectness_GroupByAggSameCols)
{
    using namespace omniruntime::op;
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
    SumAggregator* sum1 = new SumAggregator(1, INPUT_MODE, OUTPUT_MODE);
    SumAggregator* sum2 = new SumAggregator(2, INPUT_MODE, OUTPUT_MODE);
    aggs.push_back(sum1);
    aggs.push_back(sum2);
    HashAggregationOperator* groupBy = new HashAggregationOperator(v1, v2, aggs);

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

void perfTestOriginal(int64_t moduleAddr, Table** input)
{
    using namespace omniruntime::op;
    uint32_t* columnTypes1 = new uint32_t[input[0]->getColumnNumber()];
    for (int32_t i = 0; i < input[0]->getColumnNumber(); ++i) {
        columnTypes1[i] = (int32_t)input[0]->getColumnTypes()[i];
    }
    // create operator
    HashAggregationOperatorFactory* nativeOperatorFactory  = reinterpret_cast<HashAggregationOperatorFactory*>(moduleAddr);
    auto groupBy = nativeOperatorFactory->createOperator();
 
    // execution
    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        groupBy->addInput(input[i], input[i]->getPositionCount());
    }
    std::vector<Table*> result;
    int32_t tableCount = groupBy->getOutput(result); 
    EXPECT_EQ(result[0]->getColumnNumber(), 4);
    EXPECT_EQ(result[0]->getPositionCount(), 4);
    delete[] columnTypes1;
}

TEST(HashAggregationOperatorTest, Original_Multiple_Threads)
{
    using namespace omniruntime::op;
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;

    
    Table** input = buildInput(PAGE_NUM, COLUMN_NUM, ROW_PER_PAGE, CARDINALITY);
    
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
    HashAggregationOperatorFactory* nativeOperatorFactory = new HashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext, true, false);
    uint64_t factoryObjAddr = reinterpret_cast<uint64_t>(nativeOperatorFactory);
    
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
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
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }

    destroyInput(input, PAGE_NUM, COLUMN_NUM);
}

void perfTest(int64_t moduleAddr, Table** input, int32_t pageNum, int32_t* rowCount)
{
    using namespace omniruntime::op;

    // create operatory
    HashAggregationOperatorFactory* nativeOperatorFactory  = reinterpret_cast<HashAggregationOperatorFactory*>(moduleAddr);
    auto groupBy = reinterpret_cast<hashagg_module>(nativeOperatorFactory->getJitContext()->func)(nativeOperatorFactory);
 
    // execution
    auto errNo = groupBy->addInput(input, rowCount, pageNum);
    std::vector<Table*> result;
    int32_t tableCount = groupBy->getOutput(result); 
    EXPECT_EQ(result[0]->getColumnNumber(), 4);
    EXPECT_EQ(result[0]->getPositionCount(), 4);
}

uint64_t prepare_group()
{
    using namespace omniruntime::jit;
    using namespace omniruntime::op;
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

    Specialization *inloopSp = new Specialization();
    inloopSp->addSpecializedParam(3, &p_col_type);
    inloopSp->addSpecializedParam(4, &p_col_count);
    inloopSp->addSpecializedParam(5, &p_groupByColIdx);
    inloopSp->addSpecializedParam(6, &p_group_num);
    inloopSp->addSpecializedParam(7, &p_aggColIdx);
    inloopSp->addSpecializedParam(8, &p_agg_num);
    inloopSp->addSpecializedParam(9, &p_agg_types);

    Specialization *processAggSp = new Specialization();
    processAggSp->addSpecializedParam(2, &p_agg_num);
    processAggSp->addSpecializedParam(3, &p_col_type);
    processAggSp->addSpecializedParam(4, &p_aggColIdx);

    Specialization *hashColumnSp = new Specialization();
    hashColumnSp->addSpecializedParam(2, &p_col_type);
    hashColumnSp->addSpecializedParam(3, &p_group_num);

    Specialization *aggColumnSp = new Specialization();
    aggColumnSp->addSpecializedParam(2, &p_col_type);
    aggColumnSp->addSpecializedParam(3, &p_agg_num);

    std::map<std::string, Specialization> hashGroupbySps = {
        {OMNIJIT_HASH_GROUPBY_INLOOP, *inloopSp},
        {OMNIJIT_HASH_GROUPBY_HASH_COLUMN, *hashColumnSp},
        {OMNIJIT_HASH_GROUPBY_AGG_COLUMN, *aggColumnSp},
        {OMNIJIT_HASH_GROUPBY_PROCESS_AGG, *processAggSp}
    };

    omniruntime::jit::Context *groupAggregationContext = new omniruntime::jit::Context("group_aggregation", hashGroupbySps, std::vector<std::string>(), std::vector<std::string>(), true);
    omniruntime::jit::Context *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    omniruntime::jit::Context *aggregatorContext = new omniruntime::jit::Context("aggregator", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*groupAggregationContext, *memoryPoolContext, *aggregatorContext});
    auto createOperatorFunc = jit->specialize();

    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    // jitContext->jitter = reinterpret_cast<uintptr_t>(jitter.release());
    std::cout << "after jit" << std::endl;
    omniruntime::op::HashAggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext, true, false);
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->setJitContext(jitContext); 
    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

TEST(HashAggregationOperatorTest, PerfViaAPI_Multiple_Threads)
{
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    
    Table** input = buildInput(PAGE_NUM, COLUMN_NUM, ROW_PER_PAGE, CARDINALITY);
    int32_t* rowCount = new int32_t[PAGE_NUM];
    for (int32_t i = 0; i < PAGE_NUM; i++) {
        rowCount[i] = ROW_PER_PAGE;
    }
    uint64_t factoryObjAddr = prepare_group();
    std::cout << "after prepare" << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
            // same stage Id
            std::thread t(perfTest, factoryObjAddr, input, PAGE_NUM, rowCount);
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
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    delete[] rowCount;
    destroyInput(input, PAGE_NUM, COLUMN_NUM);
}

TEST(AggregationOperatorTest, VerifyCorrectness)
{
    using namespace omniruntime::op;
    // create 10 pages
    const int PAGE_NUM = 10;
    const int ROW_SIZE = 100;
    const int CARDINALITY = 4;
    const int COLUMN_COUNT = 5; // groupby*2 + sum + avg + count + min + max
    string aggNames[] = {"sum", "avg", "count", "min", "max"};
    Table** input = buildInput(PAGE_NUM, COLUMN_COUNT, ROW_SIZE, CARDINALITY);
    
    ColumnIndex c0 = {0, INT64};
    ColumnIndex c1 = {1, INT64};
    ColumnIndex c2 = {2, INT64};
    ColumnIndex c3 = {3, INT64};
    ColumnIndex c4 = {4, INT64};
    std::vector<ColumnIndex> aggregateColumns = {c0, c1, c2, c3, c4};
    std::vector<Aggregator*> aggs;
    SumAggregator* sumAgg = new SumAggregator(2, INPUT_MODE, OUTPUT_MODE);
    AverageAggregator* avgAgg = new AverageAggregator(2, INPUT_MODE, OUTPUT_MODE);
    CountAggregator* countAgg = new CountAggregator(2, INPUT_MODE, OUTPUT_MODE);
    MinAggregator* minAgg = new MinAggregator(2, INPUT_MODE, OUTPUT_MODE);
    MaxAggregator* maxAgg = new MaxAggregator(2, INPUT_MODE, OUTPUT_MODE);
    aggs.push_back(sumAgg);
    aggs.push_back(avgAgg);
    aggs.push_back(countAgg);
    aggs.push_back(minAgg);
    aggs.push_back(maxAgg);
    AggregationOperator* aggregate1 = new AggregationOperator(aggregateColumns, aggs);

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        aggregate1->addInput(input[i], ROW_SIZE);
    }
    
    std::vector<Table*> result;
    int32_t tableCount1 = aggregate1->getOutput(result);
    delete aggregate1;

    ColumnIndex c5 = {0, INT64};
    ColumnIndex c6 = {1, INT64};
    ColumnIndex c7 = {2, INT64};
    ColumnIndex c8 = {3, INT64};
    ColumnIndex c9 = {4, INT64};
    aggregateColumns = {c5, c6, c7, c8, c9};
    aggs.clear();
    sumAgg = new SumAggregator(2, INPUT_MODE, OUTPUT_MODE);
    avgAgg = new AverageAggregator(2, INPUT_MODE, OUTPUT_MODE);
    countAgg = new CountAggregator(2, INPUT_MODE, OUTPUT_MODE);
    minAgg = new MinAggregator(2, INPUT_MODE, OUTPUT_MODE);
    maxAgg = new MaxAggregator(2, INPUT_MODE, OUTPUT_MODE);
    aggs.push_back(sumAgg);
    aggs.push_back(avgAgg);
    aggs.push_back(countAgg);
    aggs.push_back(minAgg);
    aggs.push_back(maxAgg);
    AggregationOperator* aggregate2 = new AggregationOperator(aggregateColumns, aggs);

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        aggregate2->addInput(input[i], ROW_SIZE);
    }
    int32_t tableCount2 = aggregate1->getOutput(result);
    delete aggregate2;

    // Second stage
    ColumnIndex c10 = {0, INT64};
    ColumnIndex c11 = {1, INT64};
    ColumnIndex c12 = {2, INT64};
    ColumnIndex c13 = {3, INT64};
    ColumnIndex c14 = {4, INT64};
    aggregateColumns = {c10, c11, c12, c13, c14};
    std::vector<Aggregator*> aggs1;
    sumAgg = new SumAggregator(2, false, false);
    avgAgg = new AverageAggregator(2, false, false);
    countAgg = new CountAggregator(2, false, false);
    minAgg = new MinAggregator(2, false, false);
    maxAgg = new MaxAggregator(2, false, false);
    aggs1.push_back(sumAgg);
    aggs1.push_back(avgAgg);
    aggs1.push_back(countAgg);
    aggs1.push_back(minAgg);
    aggs1.push_back(maxAgg);
    AggregationOperator* aggregate3 = new AggregationOperator(aggregateColumns, aggs1);

    for (int32_t i = 0; i < result.size(); ++i) {
        aggregate3->addInput(result[i], result[i]->getPositionCount());
    }

    for (int32_t tIdx = 0; tIdx < tableCount1 + tableCount2; ++tIdx) {
        for (int32_t i = 0; i < result[tIdx]->getColumnNumber(); ++i) {
            Column *col = result[tIdx]->getColumn(i);
            delete[] reinterpret_cast<int64_t *>(col->getData());
            delete col;
        }
    }

    std::vector<Table*> result1;
    int32_t tableCount3 = aggregate1->getOutput(result1);
    delete aggregate3;
    EXPECT_EQ(result1[0]->getPositionCount(), 1);
    EXPECT_EQ(result1[0]->getColumnNumber(), 5);

    for (int32_t i = 0; i < result1[0]->getColumnNumber(); ++i) {
        Column* col = result1[0]->getColumn(i);
        std::cout << aggNames[i] << " ";
        col->printColumn();
    }
    
    destroyInput(input, PAGE_NUM, COLUMN_COUNT);
    
    for (int32_t i = 0; i < result1[0]->getColumnNumber(); ++i) {
        Column* col = result1[0]->getColumn(i);
        delete[] reinterpret_cast<int64_t*>(col->getData());
        delete col;
    }
}

TEST(AggregatorTest, avg_correctness_test)
{
    using namespace omniruntime::op;
    // create 10 pages
    const int PAGE_NUM = 10;
    const int ROW_SIZE = 100;
    const int CARDINALITY = 100;
    const int COLUMN_COUNT = 1;
    Table** input = buildInput(PAGE_NUM, COLUMN_COUNT, ROW_SIZE, CARDINALITY);

    ColumnIndex c0 = {0, INT64};
    std::vector<ColumnIndex> aggregateColumns = {c0};
    std::vector<Aggregator*> aggs;
    AverageAggregator* avgAgg = new AverageAggregator(2, INPUT_MODE, OUTPUT_MODE);
    aggs.push_back(avgAgg);
    AggregationOperator* aggregate = new AggregationOperator(aggregateColumns, aggs);

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        aggregate->addInput(input[i], ROW_SIZE);
    }
    
    std::vector<Table*> result;
    int32_t tableCount = aggregate->getOutput(result);

    EXPECT_EQ(result[0]->getColumnNumber(), 1);
    EXPECT_EQ(result[0]->getPositionCount(), 1);

    string aggNames[] = {"avg"};

    for (int32_t i = 0; i < result[0]->getColumnNumber(); ++i) {
        Column* col = result[0]->getColumn(i);
        std::cout << aggNames[i] << " ";
        col->printColumn();
    }
    
    destroyInput(input, PAGE_NUM, COLUMN_COUNT);
    
    for (int32_t i = 0; i < result[0]->getColumnNumber(); ++i) {
        Column* col = result[0]->getColumn(i);
        delete[] reinterpret_cast<int64_t*>(col->getData());
        delete col;
    }
}

void perfTestNonGroup(int64_t moduleAddr, bool codegenMode, Table** input, int32_t pageNum, int32_t* rowCount)
{
    using namespace omniruntime::op;
    uint32_t* columnTypes1 = new uint32_t[input[0]->getColumnNumber()];
    for (int32_t i = 0; i < input[0]->getColumnNumber(); ++i) {
        columnTypes1[i] = (int32_t)input[0]->getColumnTypes()[i];
    }
    // create operatory
    AggregationOperatorFactory* nativeOperatorFactory  = reinterpret_cast<AggregationOperatorFactory*>(moduleAddr);
    Operator* aggregation;
    if (codegenMode) {
        aggregation = reinterpret_cast<opt_module>(nativeOperatorFactory->getJitContext()->func)(nativeOperatorFactory);
    }else {
        aggregation = nativeOperatorFactory->createOperator();
    }
 
    // execution
    auto errNo = aggregation->addInput(input, rowCount, pageNum);
    std::vector<Table*> result;
    int32_t tableCount = aggregation->getOutput(result); 
    EXPECT_EQ(result[0]->getColumnNumber(), 4);
    EXPECT_EQ(result[0]->getPositionCount(), 1);
    delete[] columnTypes1;
}

TEST(AggregationOperatorTest, Perf_Original)
{
    uint32_t* aggTypes = new uint32_t[4];
    aggTypes[0] = 2;
    aggTypes[1] = 2;
    aggTypes[2] = 2;
    aggTypes[3] = 2;
    // uint32_t aggFunType[] = {0, 0};
    uint32_t* aggFunType = new uint32_t[4];
    aggFunType[0] = 0;
    aggFunType[1] = 0;
    aggFunType[2] = 0;
    aggFunType[3] = 0;
    PrepareContext aggTypeContext = {aggTypes, 4};
    PrepareContext aggFuncTypeContext = {aggFunType, 4};

    int32_t aggColNum = aggTypeContext.len;

    omniruntime::op::AggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::AggregationOperatorFactory(aggTypeContext, aggFuncTypeContext, true, false);
    int64_t factoryAddr = reinterpret_cast<int64_t>(nativeOperatorFactory);
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    
    Table** input = buildInput(PAGE_NUM, COLUMN_NUM, ROW_PER_PAGE, CARDINALITY);
    int32_t* rowCount = new int32_t[PAGE_NUM];
    for (int32_t i = 0; i < PAGE_NUM; i++) {
        rowCount[i] = ROW_PER_PAGE;
    }
    std::cout << "after prepare" << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
            // same stage Id
            std::thread t(perfTestNonGroup, factoryAddr, false, input, PAGE_NUM, rowCount);
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
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    delete[] rowCount;
    destroyInput(input, PAGE_NUM, COLUMN_NUM);
}

uint64_t prepare_nogroup()
{
    using namespace omniruntime::jit;
    uint32_t* aggTypes = new uint32_t[4];
    aggTypes[0] = 2;
    aggTypes[1] = 2;
    aggTypes[2] = 2;
    aggTypes[3] = 2;
    // uint32_t aggFunType[] = {0, 0};
    uint32_t* aggFunType = new uint32_t[4];
    aggFunType[0] = 0;
    aggFunType[1] = 0;
    aggFunType[2] = 0;
    aggFunType[3] = 0;
    PrepareContext aggTypeContext = {aggTypes, 4};
    PrepareContext aggFuncTypeContext = {aggFunType, 4};

    int32_t aggColNum = aggTypeContext.len;
    int32_t colNum = aggTypeContext.len;
    
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_data_type = ParamValue((int32_t*)aggTypeContext.context, aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t*)aggFuncTypeContext.context, aggColNum);

    auto *inloopSp = new Specialization();
    inloopSp->addSpecializedParam(3, &p_agg_num);
    inloopSp->addSpecializedParam(4, &p_agg_data_type);
    inloopSp->addSpecializedParam(5, &p_agg_types);

    std::map<std::string, Specialization> nonGroupSps = {
        {OMNIJIT_NON_GROUP_INLOOP, *inloopSp}
    };

    auto *groupAggregationContext = new omniruntime::jit::Context("non_group_aggregation", nonGroupSps, std::vector<std::string>(), std::vector<std::string>(), true);
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *aggregatorContext = new omniruntime::jit::Context("aggregator", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*groupAggregationContext, *memoryPoolContext, *aggregatorContext});
    auto createOperatorFunc = jit->specialize();

    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    // jitContext->jitter = reinterpret_cast<uintptr_t>(jitter.release());
    std::cout << "after jit" << std::endl;
    omniruntime::op::AggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::AggregationOperatorFactory(aggTypeContext, aggFuncTypeContext, true, false);
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->setJitContext(jitContext); 
    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

TEST(AggregationOperatorTest, Perf_Codegen)
{
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    
    Table** input = buildInput(PAGE_NUM, COLUMN_NUM, ROW_PER_PAGE, CARDINALITY);
    int32_t* rowCount = new int32_t[PAGE_NUM];
    for (int32_t i = 0; i < PAGE_NUM; i++) {
        rowCount[i] = ROW_PER_PAGE;
    }
    uint64_t factoryObjAddr = prepare_nogroup();
    std::cout << "after prepare" << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
            // same stage Id
            std::thread t(perfTestNonGroup, factoryObjAddr, true, input, PAGE_NUM, rowCount);
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
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    delete[] rowCount;
    destroyInput(input, PAGE_NUM, COLUMN_NUM);
}

TEST(HashAggregationOperatorTest, compare_perf)
{
    using namespace omniruntime::jit;
    using namespace omniruntime::op;
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
    // ------------------------------------------JIT Optimization --------------------------------------------
    ParamValue p_col_type = ParamValue(colTypes, colNum);
    ParamValue p_col_count = ParamValue(&colNum);
    ParamValue p_groupByColIdx = ParamValue((int32_t*)groupByColContext.context, groupColNum);
    ParamValue p_group_num = ParamValue(&groupColNum);
    ParamValue p_aggColIdx = ParamValue((int32_t*)aggColContext.context, aggColNum);
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_data_type = ParamValue((int32_t*)aggTypeContext.context, aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t*)aggFuncTypeContext.context, aggColNum);

    auto *inloopSp = new Specialization();
    inloopSp->addSpecializedParam(3, &p_col_type);
    inloopSp->addSpecializedParam(4, &p_col_count);
    inloopSp->addSpecializedParam(5, &p_groupByColIdx);
    inloopSp->addSpecializedParam(6, &p_group_num);
    inloopSp->addSpecializedParam(7, &p_aggColIdx);
    inloopSp->addSpecializedParam(8, &p_agg_num);
    inloopSp->addSpecializedParam(9, &p_agg_types);

    auto *processAggSp = new Specialization();
    processAggSp->addSpecializedParam(2, &p_agg_num);
    processAggSp->addSpecializedParam(3, &p_col_type);
    processAggSp->addSpecializedParam(4, &p_aggColIdx);

    std::map<std::string, Specialization> hashGroupbySps = {
        {OMNIJIT_HASH_GROUPBY_INLOOP, *inloopSp},
        {OMNIJIT_HASH_GROUPBY_PROCESS_AGG, *processAggSp}
    };

    auto *groupAggregationContext = new omniruntime::jit::Context("group_aggregation", hashGroupbySps, std::vector<std::string>(), std::vector<std::string>(), true);
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *aggregatorContext = new omniruntime::jit::Context("aggregator", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*groupAggregationContext, *memoryPoolContext, *aggregatorContext});
    auto createOperatorFunc = jit->specialize();

     // ------------------------------------------Create operator--------------------------------------------
    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    std::cout << "after jit" << std::endl;
    omniruntime::op::HashAggregationOperatorFactory* nativeOperatorFactory = new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext,true, false);
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->setJitContext(jitContext); 
     // create operator
    auto jitGroupBy = reinterpret_cast<hashagg_module>(nativeOperatorFactory->getJitContext()->func)(nativeOperatorFactory);
 
    // ------------------------------------------Process Input--------------------------------------------
    Table** input = buildInput(PAGE_NUM, COLUMN_NUM, ROW_PER_PAGE, CARDINALITY);
    int32_t* rowCount = new int32_t[PAGE_NUM];
    for (int32_t i = 0; i < PAGE_NUM; i++) {
        rowCount[i] = ROW_PER_PAGE;
    }

    // TODO insert timer
    Timer timer;
    timer.setStart();
    auto errNo = jitGroupBy->addInput(input, rowCount, PAGE_NUM);
    timer.calculateElapse();
    double wall_elapsed = timer.getWallElapse();
    double cpu_elapsed = timer.getCpuElapse();
    std::cout << "wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;
    // TODO insert timer

    HashAggregationOperatorFactory* nativeOperatorFactory2 = new HashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext,true, false);
    auto groupBy = nativeOperatorFactory2->createOperator();

    timer.reset();
    groupBy->addInput(input, rowCount, PAGE_NUM);
    timer.calculateElapse();
    wall_elapsed = timer.getWallElapse();
    cpu_elapsed = timer.getCpuElapse();

    std::cout << "wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;
}