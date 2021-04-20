#include <gtest/gtest.h>
#include "../src/operator/hash_groupby.h"
#include "../src/jni/api.h"
#include <time.h>
#include <vector>
#include <iostream>
#include "../src/harden/Hammer.h"
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

Table** buildInput(int32_t pageNum, int32_t rowPerpage)
{
    Table** input = new Table*[pageNum];
    for (int32_t i = 0; i < pageNum; ++i) {
        Table* table = new Table(rowPerpage, 4);
        int64_t* data1 = new int64_t[rowPerpage];
        for (int32_t i = 0; i < rowPerpage; ++i) {
            data1[i] = i % 4;
        }
        Column* col1 = new Column(data1, INT64, rowPerpage);

        int64_t* data2 = new int64_t[rowPerpage];
        for (int32_t i = 0; i < rowPerpage; ++i) {
            data2[i] = i % 4;
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

TEST(HashGroupByTest, VerfifyCorrectness)
{
    // create 10 pages
    const int PAGE_NUM = 10;
    Table* input[PAGE_NUM];
    const int DATA_SIZE = 10;
    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        Table* table = new Table(DATA_SIZE, 4);
        double* data1 = new double[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data1[i] = 0.1;
        }
        Column* col1 = new Column(data1, DOUBLE, DATA_SIZE);

        int64_t* data2 = new int64_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data2[i] = i % 3;
        }
        Column* col2 = new Column(data2, INT64, DATA_SIZE);
        
        int32_t* data3 = new int32_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data3[i] = i % 3;
        }
        Column* col3 = new Column(data3, INT32, DATA_SIZE);

        double* data4 = new double[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data4[i] = 0.1;
        }
        Column* col4 = new Column(data4, DOUBLE, DATA_SIZE);
        table->setColumn(col1, DOUBLE);
        table->setColumn(col2, INT64);
        table->setColumn(col3, INT32);
        table->setColumn(col4, DOUBLE);
        input[i] = table;
    }
    
    ColumnIndex c0 = {2, INT32};
    ColumnIndex c1 = {1, INT64};
    ColumnIndex c2 = {0, DOUBLE};
    ColumnIndex c3 = {3, DOUBLE};
    std::vector<ColumnIndex> v1 = {c1, c0};
    std::vector<ColumnIndex> v2 = {c2, c3};
    std::vector<Aggregator*> aggs;
    SumAggregator* sum1 = new SumAggregator(3);
    SumAggregator* sum2 = new SumAggregator(3);
    aggs.push_back(sum1);
    aggs.push_back(sum2);
    HashGroupBy* groupBy = new HashGroupBy(v1, v2, aggs);

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        groupBy->process(input[i], DATA_SIZE);
    }
    
    std::vector<Table*> result;
    int32_t tableCount = groupBy->getResult(result);

    EXPECT_EQ(result[0]->getColumnNumber(), 4);
    EXPECT_EQ(result[0]->getPositionCount(), 3);

    for (int32_t i = 0; i < result[0]->getColumnNumber(); ++i) {
        Column* col = result[0]->getColumn(i);
        col->printColumn();
    }
    
    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        delete[] reinterpret_cast<double*>(input[i]->getColumn(0)->getData());
        delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(1)->getData());
        delete[] reinterpret_cast<int32_t*>(input[i]->getColumn(2)->getData());
        delete[] reinterpret_cast<double*>(input[i]->getColumn(3)->getData());
        delete input[i];
    }
    
    for (int32_t i = 0; i < result[0]->getColumnNumber(); ++i) {
        Column* col = result[0]->getColumn(i);
        delete col->getData();
        delete col;
    }
    delete result[0];
}

TEST(HashGroupByTest, VerfifyCorrectness_GroupByAggSameCols)
{
    // create 10 pages
    const int PAGE_NUM = 10;
    Table* input[PAGE_NUM];
    const int DATA_SIZE = 10;
    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        Table* table = new Table(DATA_SIZE, 2);
         int32_t* data1 = new int32_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data1[i] = i % 3;
        }
        Column* col1 = new Column(data1, INT32, DATA_SIZE);
        int64_t* data2 = new int64_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data2[i] = i % 3;
        }
        Column* col2 = new Column(data2, INT64, DATA_SIZE);
        table->setColumn(col1, INT32);
        table->setColumn(col2, INT64);
        input[i] = table;
    }
    
    ColumnIndex c0 = {0, INT32};
    ColumnIndex c1 = {1, INT64};
    std::vector<ColumnIndex> v1 = {c0, c1};
    std::vector<ColumnIndex> v2 = {c0, c1};
    std::vector<Aggregator*> aggs;
    SumAggregator* sum1 = new SumAggregator(1);
    SumAggregator* sum2 = new SumAggregator(2);
    aggs.push_back(sum1);
    aggs.push_back(sum2);
    HashGroupBy* groupBy = new HashGroupBy(v1, v2, aggs);

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        groupBy->process(input[i], DATA_SIZE);
    }
    
    std::vector<Table*> result;
    int32_t tableCount = groupBy->getResult(result);

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
        delete col->getData();
        delete col;
    }
    delete result[0];
}

#include <time.h>
#include <mutex>
double total_cpu_time;
double total_wall_time;
void perfTest(int64_t moduleAddr)
{
    uint32_t groupCols[] = {0, 1};
    uint32_t groupTypes[] = {2, 2};
    uint32_t aggCols[] = {2, 3};
    uint32_t aggTypes[] = {2, 2};
    uint32_t aggFunType[] = {0, 0};
    uint32_t retTypes[] = {1,1,1,1};
    int64_t opId = lrand();
    int32_t pageNum = 20;
    int32_t rowPerPage = 100000;
    
    PrepareContext groupColsContext = {groupCols, 2};
    PrepareContext groupTypesContext = {groupTypes, 2};
    PrepareContext aggColsContext = {aggCols, 2};
    PrepareContext aggTypesContext = {aggTypes, 2};
    PrepareContext aggFunTypeContext = {aggFunType, 2};
    PrepareContext retTypesContext = {retTypes, 4};
    Table** input = buildInput(pageNum, rowPerPage);
    uint32_t* columnTypes1 = new uint32_t[input[0]->getColumnNumber()];
    for (int32_t i = 0; i < input[0]->getColumnNumber(); ++i) {
        columnTypes1[i] = (int32_t)input[0]->getColumnTypes()[i];
    }
    
    uint64_t opAddr = createOperator(moduleAddr, groupColsContext, groupTypesContext, aggColsContext, aggTypesContext, aggFunTypeContext, retTypesContext);

    for (int32_t i = 0; i < pageNum; ++i) {
        opAddr = executeHashGroupByLlvm(opAddr, columnTypes1, input[i]->getColumnNumber(), (void**)input[i]->getHeads(), input[i]->getColumnNumber(), input[i]->getPositionCount());
    }
    std::vector<Table*> result;
    int32_t tableCount = executeAggFinal(opAddr, result); 
    EXPECT_EQ(result[0]->getColumnNumber(), 4);
    EXPECT_EQ(result[0]->getPositionCount(), 4);

    destroyInput(input, pageNum);
    delete[] columnTypes1;
}

TEST(HashGroupByTest, PerfViaAPI_Multiple_Threads)
{
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    uint32_t groupCols[] = {0, 1};
    uint32_t groupTypes[] = {2, 2};
    uint32_t aggCols[] = {2, 3};
    uint32_t aggTypes[] = {2, 2};
    uint32_t aggFunType[] = {0, 0};
    uint32_t retTypes[] = {1,1,1,1};
    PrepareContext groupColsContext = {groupCols, 2};
    PrepareContext groupTypesContext = {groupTypes, 2};
    PrepareContext aggColsContext = {aggCols, 2};
    PrepareContext aggTypesContext = {aggTypes, 2};
    PrepareContext aggFunTypeContext = {aggFunType, 2};
    PrepareContext retTypesContext = {retTypes, 4};

    uint64_t moduleAddr = prepareHashGroupBy(groupColsContext, groupTypesContext, aggColsContext, aggTypesContext, aggFunTypeContext, retTypesContext);
    
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
            std::thread t(perfTest, moduleAddr);
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
}
