/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * @Description: sort operator test implementations
 */
#include <thread>
#include <ctime>
#include <vector>
#include <iostream>
#include <chrono>
#include <memory>

#include "gtest/gtest.h"
#include "operator/sort/sort.h"
#include "vector/vector_helper.h"
#include "util/test_util.h"
#include "operator/omni_id_type_vector_traits.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;
using namespace TestUtil;

namespace SortTest {
const int32_t VEC_BATCH_COUNT = 10;
const int32_t DISTINCT_VALUE_COUNT = 4;
const int32_t REPEAT_COUNT = 25000;
const int32_t COLUMN_COUNT_2 = 2;
const int32_t COLUMN_COUNT_4 = 4;
const uint64_t MAX_SPILL_BYTES = (1L << 20);

using IntVector = NativeAndVectorType<DataTypeId::OMNI_INT>::vector;
using LongVector = NativeAndVectorType<DataTypeId::OMNI_LONG>::vector;
using DoubleVector = NativeAndVectorType<DataTypeId::OMNI_DOUBLE>::vector;
using ShortVector = NativeAndVectorType<DataTypeId::OMNI_SHORT>::vector;
using CharVector = NativeAndVectorType<DataTypeId::OMNI_CHAR>::vector;
using VarcharVector = NativeAndVectorType<DataTypeId::OMNI_VARCHAR>::vector;

void DeleteSortOperatorFactory(SortOperatorFactory *sortOperatorFactory)
{
    if (sortOperatorFactory != nullptr) {
        delete sortOperatorFactory;
    }
}

void BuildVectorValues(Vector<int64_t> *vector)
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
        VectorBatch *vecBatch = new VectorBatch(columnCount);
        for (int32_t colIdx = 0; colIdx < columnCount; colIdx++) {
            Vector<int64_t> *vector = new Vector<int64_t>(positionCount);
            BuildVectorValues(vector);
            vecBatch->Append(std::unique_ptr<BaseVector>(vector));
        }
        vecBatches[i] = vecBatch;
    }
}

TEST(NativeOmniSortTest, TestSortPerformance)
{
    // construct input data
    const int32_t dataSize = 1000000;
    const int32_t vecSize = 5;
    int32_t *data1 = new int32_t[dataSize];
    int64_t *data2 = new int64_t[dataSize];
    double *data3 = new double[dataSize];
    std::string *data4 = new std::string[dataSize];
    int16_t *data5 = new int16_t[dataSize];

    for (int32_t i = 0; i < dataSize; ++i) {
        data1[i] = i % vecSize;
        data2[i] = i % vecSize;
        data3[i] = i % vecSize;
        data4[i] = to_string(i % vecSize);
        data5[i] = i % vecSize;
    }

    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), VarcharType(9), ShortType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4, data5);

    int32_t outputCols[vecSize] = {0, 1, 2, 3, 4};
    int32_t sortCols[vecSize] = {0, 1, 2, 3, 4};
    int32_t ascendings[vecSize] = {true, true, true, true, true};
    int32_t nullFirsts[vecSize] = {true, true, true, true, true};

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, vecSize, sortCols,
        ascendings, nullFirsts, vecSize);

    clock_t start = clock();
    auto sortOperator = CreateTestOperator(operatorFactory);
    sortOperator->AddInput(vecBatch);
    while (sortOperator->GetStatus() != OMNI_STATUS_FINISHED) {
        VectorBatch *outputVecBatch = nullptr;
        sortOperator->GetOutput(&outputVecBatch);
        VectorHelper::FreeVecBatch(outputVecBatch);
    }
    std::cout << "sort and get output elapsed end time: " << static_cast<double>(std::clock() - start) / 1000 <<
        " ms" << std::endl;

    // free memory
    delete[] data5;
    delete[] data4;
    delete[] data3;
    delete[] data2;
    delete[] data1;
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortLongColumn)
{
    // construct input data
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4};

    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    std::vector<DataTypePtr> typess = { IntType(), LongType() };
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int outputCols[2] = {0, 1};
    int sortCols[1] = {1};
    int ascendings[1] = {false};
    int nullFirsts[1] = {true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 1);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    // vector::VectorHelper::PrintVecBatch(outputVecBatches[0],sourceTypes);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, sourceTypes.Get()));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortWithNullFirst)
{
    // construct input data
    const int32_t dataSize = 6;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0, -1};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4, -1};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    vecBatch->Get(0)->SetNull(dataSize - 1);
    vecBatch->Get(1)->SetNull(dataSize - 1);

    int outputCols[2] = {0, 1};
    int sortCols[1] = {1};
    int ascendings[1] = {false};
    int nullFirsts[1] = {true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 1);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch,
                                     (std::vector<omniruntime::type::DataTypePtr> &)sourceTypes.Get());

    int32_t expectData1[dataSize] = {-1, 0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {-1, 4, 3, 2, 1, 0};

    AssertVecBatchEquals(outputVecBatch, 2, dataSize,
        (std::vector<omniruntime::type::DataTypePtr> &)sourceTypes.Get(), expectData1, expectData2);

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortWithNullLast)
{
    // construct input data
    const int32_t dataSize = 6;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0, -1};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4, -1};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    vecBatch->Get(0)->SetNull(dataSize - 1);
    vecBatch->Get(1)->SetNull(dataSize - 1);

    int outputCols[2] = {0, 1};
    int sortCols[1] = {1};
    int ascendings[1] = {false};
    int nullFirsts[1] = {false};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 1);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch,
                                     (std::vector<omniruntime::type::DataTypePtr> &)sourceTypes.Get());

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4, -1};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0, -1};
    AssertVecBatchEquals(outputVecBatch, 2, dataSize,
        (std::vector<omniruntime::type::DataTypePtr> &)sourceTypes.Get(), expectData1, expectData2);

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortWithMultiNulls)
{
    // construct input data
    const int32_t dataSize = 6;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0, -1};
    int64_t data2[dataSize] = {0, 1, -1, -1, -1, -1};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    vecBatch->Get(0)->SetNull(dataSize - 1);
    for (int32_t i = dataSize - 1; i > 1; i--) {
        vecBatch->Get(1)->SetNull(i);
    }

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[2] = {1, 0};
    int32_t ascendings[2] = {false, false};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch,
                                     (std::vector<omniruntime::type::DataTypePtr> &)sourceTypes.Get());

    int32_t expectData1[dataSize] = {-1, 2, 1, 0, 3, 4};
    int64_t expectData2[dataSize] = {-1, -1, -1, -1, 1, 0};
    AssertVecBatchEquals(outputVecBatch, 2, dataSize,
        (std::vector<omniruntime::type::DataTypePtr> &)sourceTypes.Get(), expectData1, expectData2);

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortDoubleColumn)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    double expectData2[dataSize] = {1.1, 4.4, 2.2, 5.5, 3.3, 6.6};
    DataTypes expectedTypes(std::vector<DataTypePtr> { LongType(), DoubleType() });
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch,
        (std::vector<omniruntime::type::DataTypePtr> &)sourceTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortShortColumn)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int16_t data2[dataSize] = {6, 5, 4, 3, 2, 1};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), ShortType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int16_t expectData2[dataSize] = {1, 4, 2, 5, 3, 6};
    DataTypes expectedTypes(std::vector<DataTypePtr> { LongType(), ShortType() });
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);


    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch,
        (std::vector<omniruntime::type::DataTypePtr> &)expectedTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoColumnsPerf)
{
    VectorBatch *vecBatches[VEC_BATCH_COUNT];

    BuildSortTestData(vecBatches, COLUMN_COUNT_2);
    std::cout << "finish build sort data" << endl;

    DataTypes sourceTypes(std::vector<DataTypePtr> { LongType(), LongType() });
    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {0, 1};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    Timer timer;
    timer.SetStart();
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    for (int i = 0; i < VEC_BATCH_COUNT; ++i) {
        sortOperator->AddInput(vecBatches[i]);
    }
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse();
    double cpuElapsed = timer.GetCpuElapse();
    std::cout << "testOrderByTwoColumnPerf wallElapsed time: " << wallElapsed << "s" << std::endl;
    std::cout << "testOrderByTwoColumnPerf cpuElapsed time: " << cpuElapsed << "s" << std::endl;

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
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
    DataTypes sourceTypes(std::vector<DataTypePtr> { LongType(), LongType(), LongType(), LongType() });
    int32_t outputCols[] = {0, 1};
    int32_t outputColsCount = 2;
    int32_t sortCols[] = {2, 3};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};
    int32_t sortColsCount = 2;

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, outputColsCount,
        sortCols, ascendings, nullFirsts, sortColsCount);
    return operatorFactory;
}

void TestOrderBy(struct SortThreadArgs *threadArgs)
{
    std::vector<DataTypePtr> allTypes{ LongType(), LongType(), LongType(), LongType() };
    // create operator
    SortOperatorFactory *operatorFactory = threadArgs->operatorFactory;
    SortOperator *sortOperator;
    if (threadArgs->isOriginal) {
        sortOperator = dynamic_cast<SortOperator *>(operatorFactory->CreateOperator());
    } else {
        sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    }

    for (int i = 0; i < threadArgs->tableCount; ++i) {
        sortOperator->AddInput(DuplicateVectorBatch(threadArgs->vecBatches[i], allTypes));
    }
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
}

TEST(NativeOmniSortTest, TestSortOriginalMultiThreads)
{
    std::vector<DataTypePtr> allTypes{ LongType(), LongType(), LongType(), LongType() };
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
        timer.SetStart();
        for (uint32_t j = 0; j < threadNum; ++j) {
            std::thread t(TestOrderBy, &threadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.CalculateElapse();
        double wallElapsed = timer.GetWallElapse();
        double cpuElapsed = timer.GetCpuElapse();
        std::cout << "testOrderByOriginalMultiThreads " << threadNum << " wallElapsed time: " << wallElapsed << "s" <<
            std::endl;
        std::cout << "testOrderByOriginalMultiThreads " << threadNum << " cpuElapsed time: " <<
            cpuElapsed / processorCount * t << "s" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    for (int i = 0; i < VEC_BATCH_COUNT; ++i) {
        VectorHelper::FreeVecBatch(vecBatches[i]);
    }
    delete[] vecBatches;

    DeleteSortOperatorFactory(operatorFactory);
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
        timer.SetStart();
        for (uint32_t j = 0; j < threadNum; ++j) {
            std::thread t(TestOrderBy, &threadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.CalculateElapse();
        double wallElapsed = timer.GetWallElapse();
        double cpuElapsed = timer.GetCpuElapse();
        std::cout << "testOrderByJITMultiThreads " << threadNum << " wallElapsed time: " << wallElapsed << "s" <<
            std::endl;
        std::cout << "testOrderByJITMultiThreads " << threadNum << " cpuElapsed time: " <<
            cpuElapsed / processorCount * t << "s" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    for (int i = 0; i < VEC_BATCH_COUNT; ++i) {
        VectorHelper::FreeVecBatch(vecBatches[i]);
    }
    delete[] vecBatches;
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoVarcharColumn)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    std::string data0[dataSize] = {"0", "1", "2", "0", "1", "2"};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    std::string data2[dataSize] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1"};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(3), LongType(), VarcharType(3) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    // vector::VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    std::string expectData2[dataSize] = {"1.1", "4.4", "2.2", "5.5", "3.3", "6.6"};
    DataTypes expectedTypes(std::vector<DataTypePtr>({ LongType(), VarcharType(3) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, expectedTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoCharColumn)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    std::string data0[dataSize] = {"0", "1", "2", "0", "1", "2"};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    std::string data2[dataSize] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1"};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ CharType(3), LongType(), CharType(3) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    // vector::VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    std::string expectData2[dataSize] = {"1.1", "4.4", "2.2", "5.5", "3.3", "6.6"};
    DataTypes expectedTypes(std::vector<DataTypePtr>({ LongType(), CharType(3) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, expectedTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoDate32Column)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int32_t data2[dataSize] = {66, 55, 44, 33, 22, 11};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ Date32Type(DAY), LongType(), Date32Type(MILLI) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    // vector::VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int32_t expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataTypePtr>({ LongType(), Date32Type(MILLI) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, expectedTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoDecimal64Column)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int64_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int64_t data2[dataSize] = {66, 55, 44, 33, 22, 11};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ Decimal64Type(2, 0), LongType(), Decimal64Type(2, 0) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    // vector::VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int64_t expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataTypePtr>({ LongType(), Decimal64Type(2, 0) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, expectedTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortTwoDecimal128Column)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    Decimal128 data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    Decimal128 data2[dataSize] = {66, 55, 44, 33, 22, 11};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ Decimal128Type(2, 0), LongType(), Decimal128Type(2, 0) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    // vector::VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    Decimal128 expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataTypePtr>({ LongType(), Decimal128Type(2, 0) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, expectedTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
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
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), LongType() }));
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vecBatch = new VectorBatch(dataSize);
    for (int32_t i = 0; i < 3; i++) {
        DataTypePtr dataType = sourceTypes.GetType(i);
        vecBatch->Append(CreateDictionaryVector(*dataType, dataSize, ids, dataSize, datas[i]));
    }

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int64_t expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataTypePtr> { LongType(), LongType() });
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, expectedTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

VectorBatch *CreateSortInputForAllTypes(DataTypes &sourceTypes, void **sortDatas, int32_t dataSize,
                                             int32_t loopCount, bool isDictionary, bool hasNull)
{
    int32_t sourceTypesSize = sourceTypes.GetSize();
    int32_t *sourceTypeIds = const_cast<int32_t *>(sourceTypes.GetIds());
    int32_t totalDataSize = dataSize * loopCount;

    std::unique_ptr<BaseVector> sourceVectors[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        sourceVectors[i] = VectorHelper::CreateVector(OMNI_FLAT, sourceTypeIds[i], totalDataSize);
        SetValue(sourceVectors[i].get(), 0, sortDatas[i], sourceTypeIds[i]);
    }
    for (int32_t i = 1; i < totalDataSize; i++) {
        for (int32_t j = 0; j < sourceTypesSize; j++) {
            if (((i % sourceTypesSize) == j + 1) && hasNull &&
                (sourceTypeIds[j] == OMNI_VARCHAR || sourceTypeIds[j] == OMNI_CHAR)) {
                sourceVectors[j]->SetNull(i);
            } else if ((i == j + 1) && hasNull) {
                sourceVectors[j]->SetNull(i);
            } else {
                SetValue(sourceVectors[j].get(), i, sortDatas[j], sourceTypeIds[j]);
            }
        }
    }

    if (isDictionary) {
        int32_t ids[totalDataSize];
        for (int32_t i = 0; i < totalDataSize; i++) {
            ids[i] = i;
        }
        for (int32_t i = 0; i < sourceTypesSize; i++) {
            DataTypePtr dataType = sourceTypes.GetType(i);
            sourceVectors[i] =
                DYNAMIC_TYPE_DISPATCH(CreateDictionary, dataType->GetId(), sourceVectors[i].get(), ids, totalDataSize);
        }
    }

    auto sortVecBatch = new VectorBatch(totalDataSize);
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        sortVecBatch->Append(move(sourceVectors[i]));
    }
    return sortVecBatch;
}

VectorBatch *CreateSortExpectForAllTypes(DataTypes &sourceTypes, void **sortDatas, int32_t dataSize,
                                              int32_t loopCount, bool hasNull)
{
    int32_t sourceTypesSize = sourceTypes.GetSize();
    int32_t *sourceTypeIds = const_cast<int32_t *>(sourceTypes.GetIds());
    int32_t totalDataSize = dataSize * loopCount;

    std::unique_ptr<BaseVector> expectVectors[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        expectVectors[i] = VectorHelper::CreateVector(OMNI_FLAT, sourceTypeIds[i], totalDataSize);
    }

    for (int32_t i = 0; i < dataSize; i++) {
        int32_t index = i * loopCount;
        for (int32_t loopIdx = 0; loopIdx < loopCount; loopIdx++) {
            for (int32_t colIdx = sourceTypesSize - 1; colIdx >= 0; colIdx--) {
                ((i + colIdx == sourceTypesSize) && hasNull) ?
                    expectVectors[colIdx]->SetNull(index + loopIdx) :
                    SetValue(expectVectors[colIdx].get(), index + loopIdx, sortDatas[colIdx], sourceTypeIds[colIdx]);
            }
        }
    }

    auto expectVecBatch = new VectorBatch(totalDataSize);
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        expectVecBatch->Append(move(expectVectors[i]));
    }
    return expectVecBatch;
}

// sort keys are all types ascending
TEST(NativeOmniSortTestV2, TestSortAllTypesAsc)
{
    // all types: int, long, boolean, double, date32, decimal, decimal128, varchar, char
    int32_t intValue = 20;
    int64_t longValue = 20;
    bool boolValue = true;
    double doubleValue = 20.0;
    Decimal128 decimal128(20, 0);
    std::string stringValue("20");
    int16_t shortValue = 20;
    const int32_t dataSize = 10;
    void *sortDatas[dataSize] = {&intValue, &longValue, &boolValue, &doubleValue, &intValue, &longValue, &decimal128,
                                 &stringValue, &stringValue, &shortValue};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), BooleanType(), DoubleType(),
        Date32Type(DAY), Decimal64Type(2, 0), Decimal128Type(2, 0), VarcharType(2), CharType(2), ShortType() }));

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

    auto sourceVecBatch = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 10, false, false);

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(sourceVecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, dataSize, 10, false);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, sourceTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

// sort keys are all types with nulls
TEST(NativeOmniSortTestV2, TestSortAllTypesWithNulls)
{
    // all types: int, long, boolean, double, date32, decimal, decimal128, varchar, char, short
    int32_t intValue = 20;
    int64_t longValue = 20;
    bool boolValue = true;
    double doubleValue = 20.0;
    Decimal128 decimal128(20, 0);
    std::string stringValue("20");
    int16_t shortValue = 20;
    const int32_t dataSize = 11;
    void *sortDatas[dataSize] = {&intValue, &longValue, &boolValue, &doubleValue, &intValue, &longValue, &decimal128,
        &stringValue, &stringValue, &shortValue};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), BooleanType(), DoubleType(),
        Date32Type(DAY), Decimal64Type(2, 0), Decimal128Type(2, 0), VarcharType(2), CharType(2), ShortType() }));

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

    auto sourceVecBatch = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 1, false, true);

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize);
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(sourceVecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, dataSize, 1, true);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, sourceTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

// sort keys are dictionary vector with all types and nulls
TEST(NativeOmniSortTestV2, TestSortAllTypesWithDictionaryAndNulls)
{
    // all types: int, long, boolean, double, date32, decimal, decimal128, varchar, char, short
    int32_t intValue = 20;
    int64_t longValue = 20;
    bool boolValue = true;
    double doubleValue = 20.0;
    Decimal128 decimal128(20, 0);
    std::string stringValue("20");
    int16_t shortValue = 20;
    const int32_t dataSize = 11;
    void *sortDatas[dataSize] = {&intValue, &longValue, &boolValue, &doubleValue, &intValue, &longValue, &decimal128,
        &stringValue, &stringValue, &shortValue};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), BooleanType(), DoubleType(),
        Date32Type(DAY), Decimal64Type(2, 0), Decimal128Type(2, 0), VarcharType(2), CharType(2), ShortType() }));

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

    auto sourceVecBatch = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 1, true, true);

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize);
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(sourceVecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, dataSize, 1, true);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, sourceTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTestV2, TestSortZeroRowCountInMemory)
{
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), BooleanType(), DoubleType(),
        Date32Type(DAY), Decimal64Type(2, 0), Decimal128Type(2, 0), VarcharType(2), CharType(2), ShortType() }));
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
    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));

    int32_t *sourceTypeIds = const_cast<int32_t *>(sourceTypes.GetIds());
    auto sourceVecBatch = new VectorBatch(0);
    std::unique_ptr<BaseVector> sourceVectors[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        sourceVectors[i] = VectorHelper::CreateVector(OMNI_FLAT, sourceTypeIds[i], 0);
        sourceVecBatch->Append(move(sourceVectors[i]));
    }

    sortOperator->AddInput(sourceVecBatch);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    EXPECT_EQ(outputVecBatch, nullptr);

    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTestV2, TestSortSpillWithInvalidConfig)
{
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    auto sourceTypesSize = sourceTypes.GetSize();
    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {0, 1};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};

    SpillConfig spillConfig1(SPILL_CONFIG_NONE, true, "", 5);
    OperatorConfig operatorConfig1(spillConfig1);
    EXPECT_THROW(SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize, sortCols,
        ascendings, nullFirsts, sourceTypesSize, operatorConfig1),
        omniruntime::exception::OmniException);

    SpillConfig spillConfig2(SPILL_CONFIG_NONE, true, "/", 5);
    OperatorConfig operatorConfig2(spillConfig2);
    EXPECT_THROW(SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize, sortCols,
        ascendings, nullFirsts, sourceTypesSize, operatorConfig2),
        omniruntime::exception::OmniException);

    SpillConfig spillConfig4(SPILL_CONFIG_NONE, true, "+-ab23", 5);
    OperatorConfig operatorConfig4(spillConfig4);
    EXPECT_THROW(SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize, sortCols,
        ascendings, nullFirsts, sourceTypesSize, operatorConfig4),
        omniruntime::exception::OmniException);
}

TEST(NativeOmniSortTestV2, TestSortSpillWithDictionaryAndNulls)
{
    // all types: int, long, boolean, double, date32, decimal, decimal128, varchar, char
    int32_t intValue = 20;
    int64_t longValue = 20;
    bool boolValue = true;
    double doubleValue = 20.0;
    Decimal128 decimal128(20, 0);
    std::string stringValue("20");
    int16_t shortValue = 20;
    const int32_t dataSize = 11;
    void *sortDatas[dataSize] = {&intValue, &longValue, &boolValue, &doubleValue, &intValue, &longValue, &decimal128,
        &stringValue, &stringValue, &shortValue};

    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), BooleanType(), DoubleType(),
        Date32Type(DAY), Decimal64Type(2, 0), Decimal128Type(2, 0), VarcharType(2), CharType(2), ShortType() }));
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

    auto sourceVecBatch1 = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 1, true, true);
    auto sourceVecBatch2 = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 1, true, true);
    auto sourceVecBatch3 = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 1, true, true);

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize, operatorConfig);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));

    sortOperator->AddInput(sourceVecBatch1);
    sortOperator->AddInput(sourceVecBatch2);
    sortOperator->AddInput(sourceVecBatch3);

    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, dataSize, 3, true);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, sourceTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTestV2, TestSortZeroRowCountInMemoryWithSpill)
{
    // all types: int, long, boolean, double, date32, decimal, decimal128, varchar, char
    int32_t intValue = 20;
    int64_t longValue = 20;
    bool boolValue = true;
    double doubleValue = 20.0;
    Decimal128 decimal128(20, 0);
    std::string stringValue("20");
    int16_t shortValue = 20;
    const int32_t dataSize = 11;
    void *sortDatas[dataSize] = {&intValue, &longValue, &boolValue, &doubleValue, &intValue, &longValue, &decimal128,
        &stringValue, &stringValue, &shortValue};

    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), BooleanType(), DoubleType(),
        Date32Type(DAY), Decimal64Type(2, 0), Decimal128Type(2, 0), VarcharType(2), CharType(2), ShortType() }));
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

    auto sourceVecBatch1 = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 1, true, true);
    auto sourceVecBatch2 = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 1, true, true);
    auto sourceVecBatch3 = new VectorBatch(0);
    std::unique_ptr<BaseVector> sourceVectors[sourceTypesSize];
    int32_t *sourceTypeIds = const_cast<int32_t *>(sourceTypes.GetIds());
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        sourceVectors[i] = VectorHelper::CreateVector(OMNI_FLAT, sourceTypeIds[i], 0);
        sourceVecBatch3->Append(move(sourceVectors[i]));
    }

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize, operatorConfig);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));

    sortOperator->AddInput(sourceVecBatch1);
    sortOperator->AddInput(sourceVecBatch2);
    sortOperator->AddInput(sourceVecBatch3);

    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, dataSize, 2, true);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, sourceTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortAscendingWithSpill)
{
    const int32_t dataSize = 10;
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    int32_t sourceData10[dataSize] = {23, 23, 23, 23, 23, 23, 23, 23, 23, 23};
    int32_t sourceData11[dataSize] = {1, 1, 1, 2, 1, 1, 1, 1, 2, 2};
    int32_t sourceData12[dataSize] = {12, 12, 12, 12, 12, 12, 12, 12, 12, 12};
    auto sourceVecBatch1 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData10, sourceData11, sourceData12);

    int32_t sourceData20[dataSize] = {45, 45, 45, 45, 45, 45, 45, 45, 45, 45};
    int32_t sourceData21[dataSize] = {1, 1, 1, 2, 1, 1, 1, 1, 2, 2};
    int32_t sourceData22[dataSize] = {24, 24, 24, 24, 24, 24, 24, 24, 24, 24};
    auto sourceVecBatch2 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData20, sourceData21, sourceData22);

    int32_t sourceData30[dataSize] = {67, 67, 67, 67, 67, 67, 67, 67, 67, 67};
    int32_t sourceData31[dataSize] = {1, 1, 1, 2, 1, 1, 1, 1, 2, 2};
    int32_t sourceData32[dataSize] = {36, 36, 36, 36, 36, 36, 36, 36, 36, 36};
    auto sourceVecBatch3 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData30, sourceData31, sourceData32);

    int32_t sourceData40[dataSize] = {89, 89, 89, 89, 89, 89, 89, 89, 89, 89};
    int32_t sourceData41[dataSize] = {1, 1, 1, 2, 1, 1, 1, 1, 2, 2};
    int32_t sourceData42[dataSize] = {48, 48, 48, 48, 48, 48, 48, 48, 48, 48};
    auto sourceVecBatch4 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData40, sourceData41, sourceData42);

    int32_t outputCols[] = {0, 1, 2};
    int32_t sortCols[] = {1};
    int32_t ascendings[] = {1};
    int32_t nullFirsts[] = {0};
    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 1);
    OperatorConfig operatorConfig(spillConfig);
    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 3, sortCols,
        ascendings, nullFirsts, 1, operatorConfig);
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));

    sortOperator->AddInput(sourceVecBatch1);
    sortOperator->AddInput(sourceVecBatch2);
    sortOperator->AddInput(sourceVecBatch3);
    sortOperator->AddInput(sourceVecBatch4);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData0[] = {23, 67, 89, 23, 67, 89, 23, 67, 89, 23, 67, 89, 23, 67, 89, 23, 67, 89, 23, 67, 89, 45, 45,
                             45, 45, 45, 45, 45, 23, 89, 45, 23, 89, 45, 23, 89, 67, 45, 67, 67};
    int32_t expectData1[] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2};
    int32_t expectData2[] = {12, 36, 48, 12, 36, 48, 12, 36, 48, 12, 36, 48, 12, 36, 48, 12, 36, 48, 12, 36, 48, 24, 24,
                             24, 24, 24, 24, 24, 12, 48, 24, 12, 48, 24, 12, 48, 36, 24, 36, 36};
    auto expectVecBatch = TestUtil::CreateVectorBatch(sourceTypes, 40, expectData0, expectData1, expectData2);
    ASSERT_TRUE(TestUtil::VecBatchMatch(outputVecBatch, expectVecBatch, sourceTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortDescendingWithSpill)
{
    const int32_t dataSize = 10;
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    int32_t sourceData10[dataSize] = {23, 23, 23, 23, 23, 23, 23, 23, 23, 23};
    int32_t sourceData11[dataSize] = {1, 1, 1, 2, 1, 1, 1, 1, 2, 2};
    int32_t sourceData12[dataSize] = {12, 12, 12, 12, 12, 12, 12, 12, 12, 12};
    auto sourceVecBatch1 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData10, sourceData11, sourceData12);

    int32_t sourceData20[dataSize] = {45, 45, 45, 45, 45, 45, 45, 45, 45, 45};
    int32_t sourceData21[dataSize] = {1, 1, 1, 2, 1, 1, 1, 1, 2, 2};
    int32_t sourceData22[dataSize] = {24, 24, 24, 24, 24, 24, 24, 24, 24, 24};
    auto sourceVecBatch2 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData20, sourceData21, sourceData22);

    int32_t sourceData30[dataSize] = {67, 67, 67, 67, 67, 67, 67, 67, 67, 67};
    int32_t sourceData31[dataSize] = {1, 1, 1, 2, 1, 1, 1, 1, 2, 2};
    int32_t sourceData32[dataSize] = {36, 36, 36, 36, 36, 36, 36, 36, 36, 36};
    auto sourceVecBatch3 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData30, sourceData31, sourceData32);

    int32_t sourceData40[dataSize] = {89, 89, 89, 89, 89, 89, 89, 89, 89, 89};
    int32_t sourceData41[dataSize] = {1, 1, 1, 2, 1, 1, 1, 1, 2, 2};
    int32_t sourceData42[dataSize] = {48, 48, 48, 48, 48, 48, 48, 48, 48, 48};
    auto sourceVecBatch4 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData40, sourceData41, sourceData42);

    int32_t outputCols[] = {0, 1, 2};
    int32_t sortCols[] = {1};
    int32_t ascendings[] = {0};
    int32_t nullFirsts[] = {0};
    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 1);
    OperatorConfig operatorConfig(spillConfig);
    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 3, sortCols,
        ascendings, nullFirsts, 1, operatorConfig);
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));

    sortOperator->AddInput(sourceVecBatch1);
    sortOperator->AddInput(sourceVecBatch2);
    sortOperator->AddInput(sourceVecBatch3);
    sortOperator->AddInput(sourceVecBatch4);
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData0[] = {23, 67, 89, 23, 67, 89, 23, 67, 89, 45, 45, 45, 67, 23, 45, 67, 23, 45, 67, 23, 45, 67, 23,
                             45, 67, 23, 45, 67, 23, 45, 67, 23, 89, 45, 89, 89, 89, 89, 89, 89};
    int32_t expectData1[] = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                             1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    int32_t expectData2[] = {12, 36, 48, 12, 36, 48, 12, 36, 48, 24, 24, 24, 36, 12, 24, 36, 12, 24, 36, 12, 24, 36, 12,
                             24, 36, 12, 24, 36, 12, 24, 36, 12, 48, 24, 48, 48, 48, 48, 48, 48};
    auto expectVecBatch = TestUtil::CreateVectorBatch(sourceTypes, 40, expectData0, expectData1, expectData2);
    ASSERT_TRUE(TestUtil::VecBatchMatch(outputVecBatch, expectVecBatch, sourceTypes.Get()));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}
}