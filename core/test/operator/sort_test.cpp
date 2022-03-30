/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort operator test implementations
 */
#include <thread>
#include <time.h>
#include <vector>
#include <iostream>
#include <chrono>
#include <memory>

#include "gtest/gtest.h"
#include "operator/sort/sort.h"
#include "jit_context/jit_context.h"
#include "vector/vector_helper.h"
#include "../util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;

const int32_t VEC_BATCH_COUNT = 10;
const int32_t DISTINCT_VALUE_COUNT = 4;
const int32_t REPEAT_COUNT = 250000;
const int32_t COLUMN_COUNT_2 = 2;
const int32_t COLUMN_COUNT_4 = 4;

void BuildVectorValues(LongVector *vector)
{
    int32_t idx = 0;
    for (int32_t j = 0; j < DISTINCT_VALUE_COUNT; j++) {
        for (int32_t k = 0; k < REPEAT_COUNT; k++) {
            vector->SetValue(idx++, j);
        }
    }
}

void BuildSortTestData(VectorBatch **vecBatches, int32_t columnCount)
{
    uint32_t positionCount = DISTINCT_VALUE_COUNT * REPEAT_COUNT;

    for (int32_t i = 0; i < VEC_BATCH_COUNT; i++) {
        VectorBatch *vecBatch = std::make_unique<VectorBatch>(columnCount).release();
        for (int32_t colIdx = 0; colIdx < columnCount; colIdx++) {
            LongVector *vector =
                std::make_unique<LongVector>(VectorAllocatorFactory::GetGlobalAllocator(), positionCount).release();
            BuildVectorValues(vector);
            vecBatch->SetVector(colIdx, vector);
        }
        vecBatches[i] = vecBatch;
    }
}

TEST(NativeOmniSortTest, TestSortPerformance)
{
    // construct input data
    const int32_t dataSize = 10000000;
    const int32_t vecSize = 4;
    int32_t *data1 = new int32_t[dataSize];
    int64_t *data2 = new int64_t[dataSize];
    double *data3 = new double[dataSize];
    std::string *data4 = new std::string[dataSize];

    for (int32_t i = 0; i < dataSize; ++i) {
        data1[i] = i % vecSize;
        data2[i] = i % vecSize;
        data3[i] = i % vecSize;
        data4[i] = to_string(i % vecSize);
    }

    DataTypes sourceTypes(
        std::vector<DataType>({ IntDataType(), LongDataType(), DoubleDataType(), VarcharDataType(9) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    int32_t outputCols[vecSize] = {0, 1, 2 ,3};
    int32_t sortCols[vecSize] = {0, 1, 2, 3};
    int32_t ascendings[vecSize] = {true, true, true, true};
    int32_t nullFirsts[vecSize] = {true, true, true, true};

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, vecSize, sortCols,
        ascendings, nullFirsts, vecSize);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, vecSize, sortCols, ascendings, nullFirsts, vecSize);
    operatorFactory->SetJitContext(jitContext);

    clock_t start = clock();
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    std::cout << "sort and get output elapsed end time: " << static_cast<double>(std::clock() - start) / 1000 <<
        " ms" << std::endl;

    // free memory
    delete[] data4;
    delete[] data3;
    delete[] data2;
    delete[] data1;
    VectorHelper::FreeVecBatches(outputVecBatches);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortLongColumn)
{
    // construct input data
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4};

    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int outputCols[2] = {0, 1};
    int sortCols[1] = {1};
    int ascendings[1] = {false};
    int nullFirsts[1] = {true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
    operatorFactory->SetJitContext(jitContext);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortWithNullFirst)
{
    // construct input data
    const int32_t dataSize = 6;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0, -1};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4, -1};
    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    vecBatch->GetVector(0)->SetValueNull(dataSize - 1);
    vecBatch->GetVector(1)->SetValueNull(dataSize - 1);

    int outputCols[2] = {0, 1};
    int sortCols[1] = {1};
    int ascendings[1] = {false};
    int nullFirsts[1] = {true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
    operatorFactory->SetJitContext(jitContext);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int32_t expectData1[dataSize] = {-1, 0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {-1, 4, 3, 2, 1, 0};
    AssertVecBatchEquals(outputVecBatches[0], 2, dataSize, expectData1, expectData2);

    // free memory
    VectorHelper::FreeVecBatches(outputVecBatches);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortWithNullLast)
{
    // construct input data
    const int32_t dataSize = 6;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0, -1};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4, -1};
    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    vecBatch->GetVector(0)->SetValueNull(dataSize - 1);
    vecBatch->GetVector(1)->SetValueNull(dataSize - 1);

    int outputCols[2] = {0, 1};
    int sortCols[1] = {1};
    int ascendings[1] = {false};
    int nullFirsts[1] = {false};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
    operatorFactory->SetJitContext(jitContext);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4, -1};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0, -1};
    AssertVecBatchEquals(outputVecBatches[0], 2, dataSize, expectData1, expectData2);

    // free memory
    VectorHelper::FreeVecBatches(outputVecBatches);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortWithMultiNulls)
{
    // construct input data
    const int32_t dataSize = 6;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0, -1};
    int64_t data2[dataSize] = {0, 1, -1, -1, -1, -1};
    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    vecBatch->GetVector(0)->SetValueNull(dataSize - 1);
    for (int32_t i = dataSize - 1; i > 1; i--) {
        vecBatch->GetVector(1)->SetValueNull(i);
    }

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[2] = {1, 0};
    int32_t ascendings[2] = {false, false};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int32_t expectData1[dataSize] = {-1, 2, 1, 0, 3, 4};
    int64_t expectData2[dataSize] = {-1, -1, -1, -1, 1, 0};
    AssertVecBatchEquals(outputVecBatches[0], 2, dataSize, expectData1, expectData2);

    // free memory
    VectorHelper::FreeVecBatches(outputVecBatches);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortDoubleColumn)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType(), DoubleDataType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    double expectData2[dataSize] = {1.1, 4.4, 2.2, 5.5, 3.3, 6.6};
    DataTypes expectedTypes(std::vector<DataType> { LongDataType(), DoubleDataType() });
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoColumnsPerf)
{
    VectorBatch *vecBatches[VEC_BATCH_COUNT];
    BuildSortTestData(vecBatches, COLUMN_COUNT_2);
    std::cout << "finish build sort data" << endl;

    DataTypes sourceTypes(std::vector<DataType> { LongDataType(), LongDataType() });
    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {0, 1};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);

    Timer timer;
    timer.setStart();
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    for (int i = 0; i < VEC_BATCH_COUNT; ++i) {
        sortOperator->AddInput(vecBatches[i]);
    }
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    timer.calculateElapse();
    double wallElapsed = timer.getWallElapse();
    double cpuElapsed = timer.getCpuElapse();
    std::cout << "testOrderByTwoColumnPerf wall_elapsed time: " << wallElapsed << "s" << std::endl;
    std::cout << "testOrderByTwoColumnPerf cpu_elapsed time: " << cpuElapsed << "s" << std::endl;

    VectorHelper::FreeVecBatches(outputVecBatches);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

struct SortThreadArgs {
    SortOperatorFactory *operatorFactory;
    bool isOriginal;
    VectorBatch **vecBatches;
    int32_t *rowCounts;
    int32_t tableCount;
};

void SetSortThreadArgs(struct SortThreadArgs *sortThreadArgs, SortOperatorFactory *operatorFactory, bool isOriginal,
    VectorBatch **vecBatches, int32_t *rowCounts, int32_t tableCount)
{
    sortThreadArgs->operatorFactory = operatorFactory;
    sortThreadArgs->isOriginal = isOriginal;
    sortThreadArgs->vecBatches = vecBatches;
    sortThreadArgs->rowCounts = rowCounts;
    sortThreadArgs->tableCount = tableCount;
}

SortOperatorFactory *PrepareOrderBy(bool isOriginal)
{
    DataTypes sourceTypes(std::vector<DataType> { LongDataType(), LongDataType(), LongDataType(), LongDataType() });
    int32_t outputCols[] = {0, 1};
    int32_t outputColsCount = 2;
    int32_t sortCols[] = {2, 3};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};
    int32_t sortColsCount = 2;

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, outputColsCount,
        sortCols, ascendings, nullFirsts, sortColsCount);
    JitContext *jitContext = nullptr;
    if (!isOriginal) {
        jitContext = CreateSortJitContext(sourceTypes, outputCols, outputColsCount, sortCols, ascendings, nullFirsts,
            sortColsCount);
    }
    operatorFactory->SetJitContext(jitContext);
    return operatorFactory;
}

void TestOrderBy(struct SortThreadArgs *threadArgs)
{
    // create operator
    SortOperatorFactory *operatorFactory = threadArgs->operatorFactory;
    SortOperator *sortOperator;
    if (threadArgs->isOriginal) {
        sortOperator = dynamic_cast<SortOperator *>(operatorFactory->CreateOperator());
    } else {
        sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    }

    for (int i = 0; i < threadArgs->tableCount; ++i) {
        sortOperator->AddInput(DuplicateVectorBatch(threadArgs->vecBatches[i]));
    }
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    VectorHelper::FreeVecBatches(outputVecBatches);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
}

TEST(NativeOmniSortTest, TestSortOriginalMultiThreads)
{
    VectorBatch **vecBatches = new VectorBatch *[VEC_BATCH_COUNT];
    BuildSortTestData(vecBatches, COLUMN_COUNT_4);

    int32_t rowNum = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int32_t rowCounts[VEC_BATCH_COUNT];
    for (int32_t i = 0; i < VEC_BATCH_COUNT; i++) {
        rowCounts[i] = rowNum;
    }

    SortOperatorFactory *operatorFactory = PrepareOrderBy(true);
    struct SortThreadArgs threadArgs;
    SetSortThreadArgs(&threadArgs, operatorFactory, true, vecBatches, rowCounts, VEC_BATCH_COUNT);

    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;
    uint32_t threadNums[] = {1};
    for (uint32_t i : threadNums) {
        auto t = i < processorCount ? processorCount / i : 1;

        uint32_t threadNum = i;
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (uint32_t i = 0; i < threadNum; ++i) {
            std::thread t(TestOrderBy, &threadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wallElapsed = timer.getWallElapse();
        double cpuElapsed = timer.getCpuElapse();
        std::cout << "testOrderByOriginalMultiThreads " << threadNum << " wall_elapsed time: " << wallElapsed << "s" <<
            std::endl;
        std::cout << "testOrderByOriginalMultiThreads " << threadNum << " cpu_elapsed time: " <<
            cpuElapsed / processorCount * t << "s" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    VectorHelper::FreeVecBatches(vecBatches, VEC_BATCH_COUNT);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortJITMultiThreads)
{
    VectorBatch **vecBatches = new VectorBatch *[VEC_BATCH_COUNT];
    BuildSortTestData(vecBatches, COLUMN_COUNT_4);

    int32_t rowNum = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int32_t rowCounts[VEC_BATCH_COUNT];
    for (int32_t i = 0; i < VEC_BATCH_COUNT; i++) {
        rowCounts[i] = rowNum;
    }

    SortOperatorFactory *operatorFactory = PrepareOrderBy(false);
    struct SortThreadArgs threadArgs;
    SetSortThreadArgs(&threadArgs, operatorFactory, false, vecBatches, rowCounts, VEC_BATCH_COUNT);

    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;
    uint32_t threadNums[] = {1};
    for (auto i : threadNums) {
        auto t = i < processorCount ? processorCount / i : 1;

        uint32_t threadNum = i;
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (uint32_t i = 0; i < threadNum; ++i) {
            std::thread t(TestOrderBy, &threadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wallElapsed = timer.getWallElapse();
        double cpuElapsed = timer.getCpuElapse();
        std::cout << "testOrderByJITMultiThreads " << threadNum << " wall_elapsed time: " << wallElapsed << "s" <<
            std::endl;
        std::cout << "testOrderByJITMultiThreads " << threadNum << " cpu_elapsed time: " <<
            cpuElapsed / processorCount * t << "s" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    VectorHelper::FreeVecBatches(vecBatches, VEC_BATCH_COUNT);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoVarcharColumn)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    std::string data0[dataSize] = {"0", "1", "2", "0", "1", "2"};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    std::string data2[dataSize] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1"};
    DataTypes sourceTypes(std::vector<DataType>({ VarcharDataType(3), LongDataType(), VarcharDataType(3) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    std::string expectData2[dataSize] = {"1.1", "4.4", "2.2", "5.5", "3.3", "6.6"};
    DataTypes expectedTypes(std::vector<DataType>({ LongDataType(), VarcharDataType(3) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoCharColumn)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    std::string data0[dataSize] = {"0", "1", "2", "0", "1", "2"};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    std::string data2[dataSize] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1"};
    DataTypes sourceTypes(std::vector<DataType>({ CharDataType(3), LongDataType(), CharDataType(3) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    std::string expectData2[dataSize] = {"1.1", "4.4", "2.2", "5.5", "3.3", "6.6"};
    DataTypes expectedTypes(std::vector<DataType>({ LongDataType(), CharDataType(3) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoDate32Column)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int32_t data2[dataSize] = {66, 55, 44, 33, 22, 11};
    DataTypes sourceTypes(std::vector<DataType>({ Date32DataType(DAY), LongDataType(), Date32DataType(MILLI) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int32_t expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataType>({ LongDataType(), Date32DataType(MILLI) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoDecimal64Column)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int64_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int64_t data2[dataSize] = {66, 55, 44, 33, 22, 11};
    DataTypes sourceTypes(std::vector<DataType>({ Decimal64DataType(2, 0), LongDataType(), Decimal64DataType(2, 0) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int64_t expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataType>({ LongDataType(), Decimal64DataType(2, 0) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoDecimal128Column)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    Decimal128 data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    Decimal128 data2[dataSize] = {66, 55, 44, 33, 22, 11};
    DataTypes sourceTypes(
        std::vector<DataType>({ Decimal128DataType(2, 0), LongDataType(), Decimal128DataType(2, 0) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    Decimal128 expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataType>({ LongDataType(), Decimal128DataType(2, 0) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoDictionaryColumn)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int64_t data2[dataSize] = {66, 55, 44, 33, 22, 11};
    void *datas[3] = {data0, data1, data2};
    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType(), LongDataType() }));
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vecBatch = new VectorBatch(3, dataSize);
    for (int32_t i = 0; i < 3; i++) {
        DataType dataType = sourceTypes.Get()[i];
        vecBatch->SetVector(i, CreateDictionaryVector(dataType, dataSize, ids, dataSize, datas[i]));
    }

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int64_t expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataType> { LongDataType(), LongDataType() });
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

VectorBatch *CreateSortInputForAllTypes(DataTypes &sourceTypes, void **sortDatas, int32_t dataSize, int32_t loopCount,
    VectorAllocator *vectorAllocator, bool isDictionary, bool hasNull)
{
    int32_t sourceTypesSize = sourceTypes.GetSize();
    std::vector<DataType> sourceTypesVec = sourceTypes.Get();
    int32_t *sourceTypeIds = const_cast<int32_t *>(sourceTypes.GetIds());
    int32_t totalDataSize = dataSize * loopCount;

    Vector *sourceVectors[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        sourceVectors[i] = VectorHelper::CreateVector(vectorAllocator, OMNI_VEC_ENCODING_FLAT, sourceTypeIds[i],
            sourceTypesVec[i].GetWidth() * totalDataSize, totalDataSize);
        VectorHelper::SetValue(sourceVectors[i], 0, sortDatas[i]);
    }
    for (int32_t i = 1; i < totalDataSize; i++) {
        for (int32_t j = 0; j < sourceTypesSize; j++) {
            if ((i == j + 1) && hasNull && (sourceTypeIds[j] == OMNI_VARCHAR || sourceTypeIds[j] == OMNI_CHAR)) {
                static_cast<VarcharVector *>(sourceVectors[j])->SetValueNull(i);
            } else if ((i == j + 1) && hasNull) {
                sourceVectors[j]->SetValueNull(i);
            } else {
                VectorHelper::SetValue(sourceVectors[j], i, sortDatas[j]);
            }
        }
    }

    if (isDictionary) {
        int32_t ids[totalDataSize];
        for (int32_t i = 0; i < totalDataSize; i++) {
            ids[i] = i;
        }
        for (int32_t i = 0; i < sourceTypesSize; i++) {
            auto sortVector = sourceVectors[i];
            sourceVectors[i] = new DictionaryVector(sortVector, ids, totalDataSize);
            delete sortVector;
        }
    }

    auto sortVecBatch = new VectorBatch(sourceTypesSize, totalDataSize);
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        sortVecBatch->SetVector(i, sourceVectors[i]);
    }
    return sortVecBatch;
}

VectorBatch *CreateSortExpectForAllTypes(DataTypes &sourceTypes, void **sortDatas, int32_t dataSize, int32_t loopCount,
    VectorAllocator *vectorAllocator, bool hasNull)
{
    int32_t sourceTypesSize = sourceTypes.GetSize();
    std::vector<DataType> sourceTypesVec = sourceTypes.Get();
    int32_t *sourceTypeIds = const_cast<int32_t *>(sourceTypes.GetIds());
    int32_t totalDataSize = dataSize * loopCount;

    Vector *expectVectors[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        expectVectors[i] = VectorHelper::CreateVector(vectorAllocator, OMNI_VEC_ENCODING_FLAT, sourceTypeIds[i],
            sourceTypesVec[i].GetWidth() * totalDataSize, totalDataSize);
        VectorHelper::SetValue(expectVectors[i], 0, sortDatas[i]);
    }
    for (int32_t i = 1; i < totalDataSize; i++) {
        for (int32_t j = sourceTypesSize - 1; j >= 0; j--) {
            if ((i + j == sourceTypesSize) && hasNull) {
                expectVectors[j]->SetValueNull(i);
                continue;
            }
            VectorHelper::SetValue(expectVectors[j], i, sortDatas[j]);
        }
    }

    auto expectVecBatch = new VectorBatch(sourceTypesSize, totalDataSize);
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        expectVecBatch->SetVector(i, expectVectors[i]);
    }
    return expectVecBatch;
}

// sort keys are all types ascending
TEST(NativeOmniSortTest, TestSortAllTypesAsc)
{
    // all types: int, long, boolean, double, date32, decimal, decimal128, varchar, char
    int32_t intValue = 20;
    int64_t longValue = 20;
    bool boolValue = true;
    double doubleValue = 20.0;
    Decimal128 decimal128(20, 0);
    std::string stringValue("20");
    const int32_t DATA_SIZE = 10;
    void *sortDatas[DATA_SIZE] = {&intValue, &longValue, &boolValue, &doubleValue, &intValue, &longValue, &decimal128, &stringValue, &stringValue};
    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType(), BooleanDataType(), DoubleDataType(),
        Date32DataType(DAY), Decimal64DataType(2, 0), Decimal128DataType(2, 0), VarcharDataType(2), CharDataType(2) }));

    int32_t sourceTypesSize = sourceTypes.GetSize();
    int32_t outputCols[sourceTypesSize];
    int32_t sortCols[sourceTypesSize];
    int32_t ascendings[sourceTypesSize];
    int32_t nullFirsts[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        outputCols[i] = i;
        sortCols[i] = i;
        ascendings[i] = 1;
        nullFirsts[i] = 0;
    }
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    auto sourceVecBatch = CreateSortInputForAllTypes(sourceTypes, sortDatas, DATA_SIZE, 10, vecAllocator, false, false);

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, sourceTypesSize, sortCols, ascendings, nullFirsts,
        sourceTypesSize);
    operatorFactory->SetJitContext(jitContext);
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(sourceVecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, DATA_SIZE, 10, vecAllocator, false);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

// sort keys are all types with nulls
TEST(NativeOmniSortTest, TestSortAllTypesWithNulls)
{
    // all types: int, long, boolean, double, date32, decimal, decimal128, varchar, char
    int32_t intValue = 20;
    int64_t longValue = 20;
    bool boolValue = true;
    double doubleValue = 20.0;
    Decimal128 decimal128(20, 0);
    std::string stringValue("20");
    const int32_t DATA_SIZE = 10;
    void *sortDatas[DATA_SIZE] = {&intValue, &longValue, &boolValue, &doubleValue, &intValue, &longValue, &decimal128, &stringValue, &stringValue};
    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType(), BooleanDataType(), DoubleDataType(),
        Date32DataType(DAY), Decimal64DataType(2, 0), Decimal128DataType(2, 0), VarcharDataType(2), CharDataType(2) }));

    int32_t sourceTypesSize = sourceTypes.GetSize();
    int32_t outputCols[sourceTypesSize];
    int32_t sortCols[sourceTypesSize];
    int32_t ascendings[sourceTypesSize];
    int32_t nullFirsts[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        outputCols[i] = i;
        sortCols[i] = i;
        ascendings[i] = 1;
        nullFirsts[i] = 0;
    }
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    auto sourceVecBatch = CreateSortInputForAllTypes(sourceTypes, sortDatas, DATA_SIZE, 1, vecAllocator, false, true);

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, sourceTypesSize, sortCols, ascendings, nullFirsts,
        sourceTypesSize);
    operatorFactory->SetJitContext(jitContext);
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(sourceVecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, DATA_SIZE, 1, vecAllocator, true);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

// sort keys are dictionary vector with all types and nulls
TEST(NativeOmniSortTest, TestSortAllTypesWithDictionaryAndNulls)
{
    // all types: int, long, boolean, double, date32, decimal, decimal128, varchar, char
    int32_t intValue = 20;
    int64_t longValue = 20;
    bool boolValue = true;
    double doubleValue = 20.0;
    Decimal128 decimal128(20, 0);
    std::string stringValue("20");
    const int32_t DATA_SIZE = 10;
    void *sortDatas[DATA_SIZE] = {&intValue, &longValue, &boolValue, &doubleValue, &intValue, &longValue, &decimal128, &stringValue, &stringValue};
    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType(), BooleanDataType(), DoubleDataType(),
        Date32DataType(DAY), Decimal64DataType(2, 0), Decimal128DataType(2, 0), VarcharDataType(2), CharDataType(2) }));

    int32_t sourceTypesSize = sourceTypes.GetSize();
    int32_t outputCols[sourceTypesSize];
    int32_t sortCols[sourceTypesSize];
    int32_t ascendings[sourceTypesSize];
    int32_t nullFirsts[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        outputCols[i] = i;
        sortCols[i] = i;
        ascendings[i] = 1;
        nullFirsts[i] = 0;
    }
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    auto sourceVecBatch = CreateSortInputForAllTypes(sourceTypes, sortDatas, DATA_SIZE, 1, vecAllocator, true, true);

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize);
    auto jitContext = CreateSortJitContext(sourceTypes, outputCols, sourceTypesSize, sortCols, ascendings, nullFirsts,
        sourceTypesSize);
    operatorFactory->SetJitContext(jitContext);
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(sourceVecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, DATA_SIZE, 1, vecAllocator, true);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}
