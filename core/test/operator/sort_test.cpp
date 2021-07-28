/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#include "gtest/gtest.h"
#include "../../src/operator/sort/sort.h"
#include "../../src/jit/jit.h"
#include "../../src/jit/specialization.h"
#include "../../src/operator/optimization.h"
#include "../../src/vector/vector_helper.h"
#include "../util/test_util.h"
#include <thread>
#include <time.h>
#include <vector>
#include <iostream>
#include <chrono>
#include <memory>

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;

JitContext *CreateTestSortJitContext(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColsCount)
{
    using namespace omniruntime::jit;
    int sortColTypes[sortColsCount];

    for (int32_t i = 0; i < sortColsCount; ++i) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    ParamValue pSourceTypes = ParamValue(sourceTypes, typesCount);
    ParamValue pTypeCount = ParamValue(&typesCount);
    ParamValue pOutputCols = ParamValue(outputCols, outputColsCount);
    ParamValue pOutputColCount = ParamValue(&outputColsCount);
    ParamValue pSortCols = ParamValue(sortCols, sortColsCount);
    ParamValue pSortColTypes = ParamValue(sortColTypes, sortColsCount);
    ParamValue pSortAscendings = ParamValue(sortAscendings, sortColsCount);
    ParamValue pSortNullFirsts = ParamValue(sortNullFirsts, sortColsCount);
    ParamValue pSortColCount = ParamValue(&sortColsCount);

    Specialization *compareToSp = std::make_unique<Specialization>().release();
    compareToSp->AddSpecializedParam(PARAM_OFFSET_0, &pSortCols);
    compareToSp->AddSpecializedParam(PARAM_OFFSET_1, &pSortColTypes);
    compareToSp->AddSpecializedParam(PARAM_OFFSET_2, &pSortAscendings);
    compareToSp->AddSpecializedParam(PARAM_OFFSET_3, &pSortNullFirsts);
    compareToSp->AddSpecializedParam(PARAM_OFFSET_4, &pSortColCount);

    Specialization *allocColumnsSp = std::make_unique<Specialization>().release();
    allocColumnsSp->AddSpecializedParam(PARAM_OFFSET_1, &pSourceTypes);
    allocColumnsSp->AddSpecializedParam(PARAM_OFFSET_2, &pOutputCols);
    allocColumnsSp->AddSpecializedParam(PARAM_OFFSET_3, &pOutputColCount);

    Specialization *getOutputSp = std::make_unique<Specialization>().release();
    getOutputSp->AddSpecializedParam(PARAM_OFFSET_1, &pOutputCols);
    getOutputSp->AddSpecializedParam(PARAM_OFFSET_2, &pOutputColCount);
    getOutputSp->AddSpecializedParam(PARAM_OFFSET_4, &pSourceTypes);

    std::map<std::string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp } };

    omniruntime::jit::Context *sortContext = new omniruntime::jit::Context("sort",
        std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>(), true);
    omniruntime::jit::Context *memoryPoolContext = new omniruntime::jit::Context("memory_pool",
        std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    omniruntime::jit::Context *pagesIndexContext = new omniruntime::jit::Context("pages_index", pagesIndexSps,
        std::vector<std::string>(), std::vector<std::string>());

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *sortContext, *memoryPoolContext, *pagesIndexContext });
    auto createOperatorFunc = jit->Specialize();

    JitContext *jitContext = new JitContext;
    jitContext->func = static_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
}

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
            LongVector *vector = std::make_unique<LongVector>(nullptr, positionCount).release();
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
    int64_t *data1 = new int64_t[dataSize];
    for (int32_t i = 0; i < dataSize; ++i) {
        data1[i] = i;
    }

    int64_t *data2 = new int64_t[dataSize];
    for (int32_t i = 0; i < dataSize; ++i) {
        data2[i] = i;
    }

    // add input
    VectorBatch *vecBatch = new VectorBatch(2);
    LongVector *column1 = new LongVector(nullptr, dataSize);
    column1->SetValues(0, data1, dataSize);
    LongVector *column2 = new LongVector(nullptr, dataSize);
    column2->SetValues(0, data2, dataSize);
    vecBatch->SetVector(0, column1);
    vecBatch->SetVector(1, column2);

    int32_t sourceTypes[2] = {1, 1};
    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {true, true};

    SortOperatorFactory *operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, 2, outputCols, 2,
        sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext =
        CreateTestSortJitContext(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);

    clock_t start = clock();
    SortOperator *sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    std::cout << "sort and get output elapsed end time: " << static_cast<double>(std::clock() - start) / 1000 <<
        " ms" << std::endl;

    // free memory
    delete[] data2;
    delete[] data1;
    delete sortOperator;
    DeleteOperatorFactory(operatorFactory);
    vecBatch->FreeAllVectors();
    delete vecBatch;
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniSortTest, TestOrderByOneColumn)
{
    // construct input data
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4};

    VectorBatch *vecBatch = new VectorBatch(2);
    IntVector *column1 = new IntVector(nullptr, dataSize);
    column1->SetValues(0, data1, dataSize);
    LongVector *column2 = new LongVector(nullptr, dataSize);
    column2->SetValues(0, data2, dataSize);
    vecBatch->SetVector(0, column1);
    vecBatch->SetVector(1, column2);

    int sourceTypes[2] = {1, 2};
    int outputCols[2] = {0, 1};
    int sortCols[1] = {1};
    int ascendings[1] = {false};
    int nullFirsts[1] = {true};

    SortOperatorFactory *operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, 2, outputCols, 2,
        sortCols, ascendings, nullFirsts, 1);
    JitContext *jitContext =
        CreateTestSortJitContext(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 1);
    operatorFactory->SetJitContext(jitContext);

    SortOperator *sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    IntVector expectCol1(nullptr, dataSize);
    expectCol1.SetValues(0, expectData1, dataSize);
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    LongVector expectCol2(nullptr, dataSize);
    expectCol2.SetValues(0, expectData2, dataSize);
    VectorBatch expectVecBatch(2);
    expectVecBatch.SetVector(0, &expectCol1);
    expectVecBatch.SetVector(1, &expectCol2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], &expectVecBatch));

    // free memory
    delete sortOperator;
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniSortTest, TestOrderByDoubleColumn)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorBatch *vecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, dataSize);
    column0->SetValues(0, data0, dataSize);
    LongVector *column1 = new LongVector(nullptr, dataSize);
    column1->SetValues(0, data1, dataSize);
    DoubleVector *column2 = new DoubleVector(nullptr, dataSize);
    column2->SetValues(0, data2, dataSize);
    vecBatch->SetVector(0, column0);
    vecBatch->SetVector(1, column1);
    vecBatch->SetVector(2, column2);

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    SortOperatorFactory *operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, 3, outputCols, 2,
        sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext =
        CreateTestSortJitContext(sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);

    SortOperator *sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    LongVector *expectCol1 = new LongVector(nullptr, dataSize);
    expectCol1->SetValues(0, expectData1, dataSize);
    double expectData2[dataSize] = {1.1, 4.4, 2.2, 5.5, 3.3, 6.6};
    DoubleVector *expectCol2 = new DoubleVector(nullptr, dataSize);
    expectCol2->SetValues(0, expectData2, dataSize);
    VectorBatch *expectPage = new VectorBatch(2);
    expectPage->SetVector(0, expectCol1);
    expectPage->SetVector(1, expectCol2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectPage));

    delete sortOperator;
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
    delete expectPage;
}

TEST(NativeOmniSortTest, TestOrderByDoubleColumnV2)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorBatch *vecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, dataSize);
    column0->SetValues(0, data0, dataSize);
    LongVector *column1 = new LongVector(nullptr, dataSize);
    column1->SetValues(0, data1, dataSize);
    DoubleVector *column2 = new DoubleVector(nullptr, dataSize);
    column2->SetValues(0, data2, dataSize);
    vecBatch->SetVector(0, column0);
    vecBatch->SetVector(1, column1);
    vecBatch->SetVector(2, column2);

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    SortOperatorFactory *operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, 3, outputCols, 2,
        sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext =
        CreateTestSortJitContext(sourceTypes, 3, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);

    SortOperator *sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    LongVector *expectCol1 = new LongVector(nullptr, dataSize);
    expectCol1->SetValues(0, expectData1, dataSize);
    double expectData2[dataSize] = {1.1, 4.4, 2.2, 5.5, 3.3, 6.6};
    DoubleVector *expectCol2 = new DoubleVector(nullptr, dataSize);
    expectCol2->SetValues(0, expectData2, dataSize);
    VectorBatch *expectVecBatch = new VectorBatch(2);
    expectVecBatch->SetVector(0, expectCol1);
    expectVecBatch->SetVector(1, expectCol2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete sortOperator;
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniSortTest, TestOrderByTwoColumnPerf)
{
    int32_t rowNum = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    VectorBatch **vecBatches = new VectorBatch *[VEC_BATCH_COUNT];
    BuildSortTestData(vecBatches, COLUMN_COUNT_2);
    std::cout << "finish build sort data" << endl;

    int32_t rowCounts[VEC_BATCH_COUNT];
    for (int32_t i = 0; i < VEC_BATCH_COUNT; i++) {
        rowCounts[i] = rowNum;
    }
    int32_t sourceTypes[] = {2, 2};
    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {0, 1};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};

    SortOperatorFactory *operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, 2, outputCols, 2,
        sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext =
        CreateTestSortJitContext(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);

    Timer timer;
    timer.setStart();
    SortOperator *sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
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

    delete sortOperator;
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatches(vecBatches, VEC_BATCH_COUNT);
    VectorHelper::FreeVecBatches(outputVecBatches);
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
    int32_t sourceTypes[] = {2, 2, 2, 2};
    int32_t sourceTypesCount = 4;
    int32_t outputCols[] = {0, 1};
    int32_t outputColsCount = 2;
    int32_t sortCols[] = {2, 3};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};
    int32_t sortColsCount = 2;

    SortOperatorFactory *operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, sourceTypesCount,
        outputCols, outputColsCount, sortCols, ascendings, nullFirsts, sortColsCount);
    JitContext *jitContext = nullptr;
    if (!isOriginal) {
        jitContext = CreateTestSortJitContext(sourceTypes, sourceTypesCount, outputCols, outputColsCount, sortCols,
            ascendings, nullFirsts, sortColsCount);
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
        opt_module sortModule = reinterpret_cast<opt_module>(operatorFactory->GetJitContext()->func);
        sortOperator = dynamic_cast<SortOperator *>(sortModule(operatorFactory));
    }

    for (int i = 0; i < threadArgs->tableCount; ++i) {
        sortOperator->AddInput(threadArgs->vecBatches[i]);
    }
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    delete sortOperator;
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniSortTest, testOrderByOriginalMultiThreads)
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
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto t = threadNums[i] < processorCount ? processorCount / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
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

    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatches(vecBatches, VEC_BATCH_COUNT);
}

TEST(NativeOmniSortTest, testOrderByJITMultiThreads)
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
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto t = threadNums[i] < processorCount ? processorCount / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
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

    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatches(vecBatches, VEC_BATCH_COUNT);
}
