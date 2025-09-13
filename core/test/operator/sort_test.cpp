/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * @Description: sort operator test implementations
 */
#include <thread>
#include <ctime>
#include <vector>
#include <iostream>
#include <chrono>
#include <memory>
#include <tuple>
#include <algorithm>
#include <random>
#include "gtest/gtest.h"
#include "operator/sort/sort.h"
#include "vector/vector_helper.h"
#include "util/test_util.h"
#include "operator/omni_id_type_vector_traits.h"
#include "simd/func/quick_sort_simd.h"
#include "simd/func/small_case_sort.h"
#include "operator/pages_index.h"
#include "random"

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
const uint64_t MAX_SPILL_BYTES = (5L << 20);

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
            vecBatch->Append(vector);
        }
        vecBatches[i] = vecBatch;
    }
}

TEST(NativeOmniSortTest, TestSortPerformance)
{
    // construct input data
    const int32_t dataSize = 1000;
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
    sortOperator->noMoreInput();
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
    constexpr int32_t dataSize = 5;
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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortWithNullFirst)
{
    // construct input data
    constexpr int32_t dataSize = 6;
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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData1[dataSize] = {-1, 0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {-1, 4, 3, 2, 1, 0};

    AssertVecBatchEquals(outputVecBatch, 2, dataSize, expectData1, expectData2);

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestQuickSortInternalSIMDAsc)
{
    constexpr int32_t dataSize = 18;
    uint64_t data0[dataSize] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
    int64_t data1[dataSize] = {12546, 12558, 12557, 12556, 12558, 12557, 12556, 12550, 12550, 12565, 12549, 12556,
        12556, 12546, 12549, 12549, 12557, 12565};

    int64_t valueBuf[50];
    uint64_t addrBuf[50];
    const SortTag<int64_t> d;
    const simd::MakeTraits<int64_t, simd::SortAscending> st;
    int64_t avg = 12562;
    QuickSortInternalSIMD(d, st, data1, data0, 0, dataSize, valueBuf, addrBuf, true, avg);
    int64_t expectData[dataSize] = {12546, 12546, 12549, 12549, 12549, 12550, 12550, 12556, 12556, 12556, 12556, 12557,
        12557, 12557, 12558, 12558, 12565, 12565};
    for (int32_t i = 0; i < dataSize; i++) {
        EXPECT_EQ(data1[i], expectData[i]);
    }
}

TEST(NativeOmniSortTest, TestQuickSortInternalSIMDDes)
{
    constexpr int32_t dataSize = 18;
    uint64_t data0[dataSize] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
    int64_t data1[dataSize] = {18555, 18555, 18555, 18555, 18556, 18555, 18555, 18556, 18555, 18555, 18555, 18555,
        18555, 18556, 18555, 18555, 18555, 18555};

    int64_t valueBuf[50];
    uint64_t addrBuf[50];
    const SortTag<int64_t> d;
    const simd::MakeTraits<int64_t, simd::SortDescending> st;
    QuickSortInternalSIMD(d, st, data1, data0, 0, dataSize, valueBuf, addrBuf);
    int64_t expectData[dataSize] = {18556, 18556, 18556, 18555, 18555, 18555, 18555, 18555, 18555, 18555, 18555, 18555,
        18555, 18555, 18555, 18555, 18555, 18555};
    for (int32_t i = 0; i < dataSize; i++) {
        EXPECT_EQ(data1[i], expectData[i]);
    }
}

TEST(NativeOmniSortTest, TestSortWithNullLast)
{
    // construct input data
    constexpr int32_t dataSize = 6;
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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4, -1};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0, -1};
    AssertVecBatchEquals(outputVecBatch, 2, dataSize, expectData1, expectData2);

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortWithMultiNulls)
{
    // construct input data
    constexpr int32_t dataSize = 6;
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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData1[dataSize] = {-1, 2, 1, 0, 3, 4};
    int64_t expectData2[dataSize] = {-1, -1, -1, -1, 1, 0};
    AssertVecBatchEquals(outputVecBatch, 2, dataSize, expectData1, expectData2);

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortIntColumnAscSIMD)
{
    constexpr int32_t dataSize = 24;
    int32_t data0[] = {38, 26, 97, 19, 66, 1, 5, 49, 38, 26, 97, 19, 66, 1, 5, 49, 38, 26, 97, 19, 66, 1, 5, 49};
    int32_t data1[] = {33, 24, 96, 16, 64, 2, 6, 47, 34, 25, 97, 17, 65, 3, 7, 48, 35, 26, 98, 18, 66, 4, 8, 49};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), IntType() }));
    auto vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {0, 1};
    int32_t ascendings[] = {true, true};
    int32_t nullFirsts[] = {true, true};
    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    auto sortOperator = CreateTestOperator(operatorFactory);
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData0[] = {1, 1, 1, 5, 5, 5, 19, 19, 19, 26, 26, 26, 38, 38, 38, 49, 49, 49, 66, 66, 66, 97, 97, 97};
    int32_t expectData1[] = {2, 3, 4, 6, 7, 8, 16, 17, 18, 24, 25, 26, 33, 34, 35, 47, 48, 49, 64, 65, 66, 96, 97, 98};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortIntColumnDescSIMD)
{
    constexpr int32_t dataSize = 24;
    int32_t data0[] = {38, 26, 97, 19, 66, 1, 5, 49, 38, 26, 97, 19, 66, 1, 5, 49, 38, 26, 97, 19, 66, 1, 5, 49};
    int32_t data1[] = {33, 24, 96, 16, 64, 2, 6, 47, 34, 25, 97, 17, 65, 3, 7, 48, 35, 26, 98, 18, 66, 4, 8, 49};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), IntType() }));
    auto vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {0, 1};
    int32_t ascendings[] = {false, false};
    int32_t nullFirsts[] = {false, false};
    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    auto sortOperator = CreateTestOperator(operatorFactory);
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData0[] = {97, 97, 97, 66, 66, 66, 49, 49, 49, 38, 38, 38, 26, 26, 26, 19, 19, 19, 5, 5, 5, 1, 1, 1};
    int32_t expectData1[] = {98, 97, 96, 66, 65, 64, 49, 48, 47, 35, 34, 33, 26, 25, 24, 18, 17, 16, 8, 7, 6, 4, 3, 2};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortLongColumnAscSIMD)
{
    // construct input data
    constexpr int32_t dataSize = 37;
    int32_t data1[dataSize];
    int64_t data2[dataSize];
    const int32_t lastData = dataSize - 1;
    for (int32_t i = 0; i < dataSize; i++) {
        data1[i] = i;
        data2[i] = lastData - i;
    }

    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    std::vector<DataTypePtr> typess = { IntType(), LongType() };
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int outputCols[2] = {0, 1};
    int sortCols[1] = {1};
    int ascendings[1] = {true};
    int nullFirsts[1] = {false};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 1);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData1[dataSize];
    int64_t expectData2[dataSize];
    for (int32_t i = 0; i < dataSize; i++) {
        expectData1[i] = lastData - i;
        expectData2[i] = i;
    }
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortLongColumnDescSIMD)
{
    // construct input data
    constexpr int32_t dataSize = 37;
    int32_t data1[dataSize];
    int64_t data2[dataSize];
    const int32_t lastData = dataSize - 1;
    for (int32_t i = 0; i < dataSize; i++) {
        data1[i] = lastData - i;
        data2[i] = i;
    }

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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData1[dataSize];
    int64_t expectData2[dataSize];
    for (int32_t i = 0; i < dataSize; i++) {
        expectData1[i] = i;
        expectData2[i] = lastData - i;
    }
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortDuplicateLongColumnAscSIMDPerformance2)
{
    // construct input data
    const int64_t dataSize = 1000;
    auto prepareStart = std::chrono::high_resolution_clock::now();
    auto *data1 = new int64_t[dataSize];
    auto *data2 = new uint64_t[dataSize];
    for (int64_t i = 0; i < dataSize; i++) {
        data1[i] = i % 50;
        data2[i] = i;
    }
    auto prepareEnd = std::chrono::high_resolution_clock::now();
    auto preDuration = std::chrono::duration_cast<std::chrono::milliseconds>(prepareEnd - prepareStart);
    std::cout << "TestSortDuplicateLongColumnAscSIMDPerformance2 prepare data cost: " << preDuration.count() << " ms\n";

    auto start2 = std::chrono::high_resolution_clock::now();
    QuickSortAscSIMD(data1, data2, 0, dataSize);
    auto end2 = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(end2 - start2);
    std::cout << "TestSortDuplicateLongColumnAscSIMDPerformance2 simd sort long cost: " << duration2.count() << " ms\n";

    delete[] data1;
    delete[] data2;
}

TEST(NativeOmniSortTest, TestSortDuplicateLongColumnAscSIMDPerformance1)
{
    // construct input data
    const int64_t dataSize = 1000;
    auto prepareStart = std::chrono::high_resolution_clock::now();
    auto *data1 = new int64_t[dataSize];
    auto *data2 = new uint64_t[dataSize];
    for (int64_t i = 0; i < dataSize; i++) {
        data1[i] = i % 20;
        data2[i] = i;
    }
    auto prepareEnd = std::chrono::high_resolution_clock::now();
    auto preDuration = std::chrono::duration_cast<std::chrono::milliseconds>(prepareEnd - prepareStart);
    std::cout << "TestSortDuplicateLongColumnAscSIMDPerformance1 prepare data cost: " << preDuration.count() << " ms\n";

    auto start2 = std::chrono::high_resolution_clock::now();
    QuickSortAscSIMD(data1, data2, 0, dataSize);
    auto end2 = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(end2 - start2);
    std::cout << "TestSortDuplicateLongColumnAscSIMDPerformance1 simd sort long cost: " << duration2.count() << " ms\n";

    delete[] data1;
    delete[] data2;
}

TEST(NativeOmniSortTest, TestSortOrderlyLongColumnAscSIMDPerformance)
{
    // construct input data
    const int64_t dataSize = 1000;
    auto prepareStart = std::chrono::high_resolution_clock::now();
    auto *data1 = new int64_t[dataSize];
    auto *data2 = new uint64_t[dataSize];
    for (int64_t i = 0; i < dataSize; i++) {
        data1[i] = i;
        data2[i] = i;
    }
    auto prepareEnd = std::chrono::high_resolution_clock::now();
    auto preDuration = std::chrono::duration_cast<std::chrono::milliseconds>(prepareEnd - prepareStart);
    std::cout << "TestSortOrderlyLongColumnAscSIMDPerformance prepare data cost: " << preDuration.count() << " ms\n";

    auto start2 = std::chrono::high_resolution_clock::now();
    QuickSortAscSIMD(data1, data2, 0, dataSize);
    auto end2 = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(end2 - start2);
    std::cout << "TestSortOrderlyLongColumnAscSIMDPerformance simd sort long cost: " << duration2.count() << " ms\n";

    delete[] data1;
    delete[] data2;
}

TEST(NativeOmniSortTest, TestSortRandomLongColumnAscSIMDPerformance)
{
    // construct input data
    const int64_t dataSize = 1000;
    auto prepareStart = std::chrono::high_resolution_clock::now();
    auto *data1 = new int64_t[dataSize];
    auto *data2 = new uint64_t[dataSize];
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 100);
    for (int64_t i = 0; i < dataSize; i++) {
        data1[i] = dis(gen);
        ;
        data2[i] = i;
    }
    auto prepareEnd = std::chrono::high_resolution_clock::now();
    auto preDuration = std::chrono::duration_cast<std::chrono::milliseconds>(prepareEnd - prepareStart);
    std::cout << "TestSortRandomLongColumnAscSIMDPerformance prepare data cost: " << preDuration.count() << " ms\n";

    auto start2 = std::chrono::high_resolution_clock::now();
    QuickSortAscSIMD(data1, data2, 0, dataSize);
    auto end2 = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(end2 - start2);
    std::cout << "TestSortRandomLongColumnAscSIMDPerformance simd sort long cost: " << duration2.count() << " ms\n";

    delete[] data1;
    delete[] data2;
}

TEST(NativeOmniSortTest, TestSortLongColumnAscSIMDPerformance)
{
    // construct input data
    const int64_t dataSize = 1000;
    auto prepareStart = std::chrono::high_resolution_clock::now();
    auto *data1 = new int64_t[dataSize];
    auto *data2 = new uint64_t[dataSize];
    for (int64_t i = 0; i < dataSize; i++) {
        data1[i] = dataSize - i;
        data2[i] = i;
    }
    auto prepareEnd = std::chrono::high_resolution_clock::now();
    auto preDuration = std::chrono::duration_cast<std::chrono::milliseconds>(prepareEnd - prepareStart);
    std::cout << "prepare data cost: " << preDuration.count() << " ms\n";

    auto start2 = std::chrono::high_resolution_clock::now();
    QuickSortAscSIMD(data1, data2, 0, dataSize);
    auto end2 = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(end2 - start2);
    std::cout << "simd sort long cost: " << duration2.count() << " ms\n";

    delete[] data1;
    delete[] data2;
}

TEST(NativeOmniSortTest, TestSortDoubleColumnAscSIMDPerformance)
{
    // construct input data
    const int64_t dataSize = 1000;
    double *data1 = new double[dataSize];
    double baseNumber = 1.11111;
    for (int64_t i = 0; i < dataSize; i++) {
        data1[i] = baseNumber * static_cast<double>(i);
    }

    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType() }));
    std::vector<DataTypePtr> typess = { DoubleType() };
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1);

    int outputCols[1] = {0};
    int sortCols[1] = {0};
    int ascendings[1] = {true};
    int nullFirsts[1] = {false};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 1, sortCols, ascendings, nullFirsts, 1);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    auto start = std::chrono::high_resolution_clock::now();
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration1 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "simd sort long cost: " << duration1.count() << " ms\n";
    // free memory
    delete[] data1;
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortDoubleColumnAscSIMD)
{
    // construct input data
    const int32_t dataSize = 37;
    // prepare data
    int32_t data0[dataSize];
    int64_t data1[dataSize];
    double data2[dataSize];
    for (int32_t i = 0; i < dataSize; i++) {
        data0[i] = i;
        data1[i] = i;
        data2[i] = 38.8 - 1.1 * i;
    }
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {2, 0};
    int32_t ascendings[2] = {true, false};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize];
    double expectData2[dataSize];
    const int32_t lastData = dataSize - 1;
    for (int32_t i = 0; i < dataSize; i++) {
        expectData1[i] = data1[lastData - i];
        expectData2[i] = data2[lastData - i];
    }
    DataTypes expectedTypes(std::vector<DataTypePtr> { LongType(), DoubleType() });
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortDoubleColumnDescSIMD)
{
    // construct input data
    const int32_t dataSize = 37;
    // prepare data
    int32_t data0[dataSize];
    int64_t data1[dataSize];
    double data2[dataSize];
    for (int32_t i = 0; i < dataSize; i++) {
        data0[i] = i;
        data1[i] = i;
        data2[i] = -0.8 + 1.1 * i;
    }
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t outputCols[2] = {1, 2};
    int32_t sortCols[2] = {2, 0};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize];
    double expectData2[dataSize];
    const int32_t lastData = dataSize - 1;
    for (int32_t i = 0; i < dataSize; i++) {
        expectData1[i] = data1[lastData - i];
        expectData2[i] = data2[lastData - i];
    }
    DataTypes expectedTypes(std::vector<DataTypePtr> { LongType(), DoubleType() });
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    double expectData2[dataSize] = {1.1, 4.4, 2.2, 5.5, 3.3, 6.6};
    DataTypes expectedTypes(std::vector<DataTypePtr> { LongType(), DoubleType() });
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortDuplicatedLongColumn)
{
    constexpr int dataSize = 32;
    int64_t data0[dataSize];
    int64_t data1[dataSize];
    for (int i = 0; i < dataSize; i++) {
        if (i == 6 || i == 7 || i == 14) {
            data0[i] = 0;
        } else {
            data0[i] = 2;
        }
        data1[i] = i;
    }

    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[] = {0};
    int32_t ascendings[] = {true};
    int32_t nullFirsts[] = {true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 1);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    VectorHelper::PrintVecBatch(outputVecBatch);

    VectorHelper::FreeVecBatch(outputVecBatch);
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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int16_t expectData2[dataSize] = {1, 4, 2, 5, 3, 6};
    DataTypes expectedTypes(std::vector<DataTypePtr> { LongType(), ShortType() });
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);


    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

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
    sortOperator->noMoreInput();
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
    std::vector<DataTypePtr> allTypes { LongType(), LongType(), LongType(), LongType() };
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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
}

TEST(NativeOmniSortTest, TestSortOriginalMultiThreads)
{
    std::vector<DataTypePtr> allTypes { LongType(), LongType(), LongType(), LongType() };
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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    std::string expectData2[dataSize] = {"1.1", "4.4", "2.2", "5.5", "3.3", "6.6"};
    DataTypes expectedTypes(std::vector<DataTypePtr>({ LongType(), VarcharType(3) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    std::string expectData2[dataSize] = {"1.1", "4.4", "2.2", "5.5", "3.3", "6.6"};
    DataTypes expectedTypes(std::vector<DataTypePtr>({ LongType(), CharType(3) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int32_t expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataTypePtr>({ LongType(), Date32Type(MILLI) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int64_t expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataTypePtr>({ LongType(), Decimal64Type(2, 0) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    Decimal128 expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataTypePtr>({ LongType(), Decimal128Type(2, 0) }));
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

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
        auto &dataType = sourceTypes.GetType(i);
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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int64_t expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    DataTypes expectedTypes(std::vector<DataTypePtr> { LongType(), LongType() });
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

VectorBatch *CreateSortInputForAllTypes(DataTypes &sourceTypes, void **sortDatas, int32_t dataSize, int32_t loopCount,
    bool isDictionary, bool hasNull)
{
    int32_t sourceTypesSize = sourceTypes.GetSize();
    int32_t *sourceTypeIds = const_cast<int32_t *>(sourceTypes.GetIds());
    int32_t totalDataSize = dataSize * loopCount;

    BaseVector *sourceVectors[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        sourceVectors[i] = VectorHelper::CreateVector(OMNI_FLAT, sourceTypeIds[i], totalDataSize);
        SetValue(sourceVectors[i], 0, sortDatas[i]);
    }
    for (int32_t i = 1; i < totalDataSize; i++) {
        for (int32_t j = 0; j < sourceTypesSize; j++) {
            if (((i % sourceTypesSize) == j + 1) && hasNull &&
                (sourceTypeIds[j] == OMNI_VARCHAR || sourceTypeIds[j] == OMNI_CHAR)) {
                sourceVectors[j]->SetNull(i);
            } else if ((i == j + 1) && hasNull) {
                sourceVectors[j]->SetNull(i);
            } else {
                SetValue(sourceVectors[j], i, sortDatas[j]);
            }
        }
    }

    if (isDictionary) {
        int32_t ids[totalDataSize];
        for (int32_t i = 0; i < totalDataSize; i++) {
            ids[i] = i;
        }
        for (int32_t i = 0; i < sourceTypesSize; i++) {
            auto &dataType = sourceTypes.GetType(i);
            BaseVector *sourceVector = sourceVectors[i];
            sourceVectors[i] =
                DYNAMIC_TYPE_DISPATCH(CreateDictionary, dataType->GetId(), sourceVector, ids, totalDataSize);
            delete sourceVector;
        }
    }

    auto sortVecBatch = new VectorBatch(totalDataSize);
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        sortVecBatch->Append(sourceVectors[i]);
    }
    return sortVecBatch;
}

VectorBatch *CreateSortExpectForAllTypes(DataTypes &sourceTypes, void **sortDatas, int32_t dataSize, int32_t loopCount,
    bool hasNull)
{
    int32_t sourceTypesSize = sourceTypes.GetSize();
    int32_t *sourceTypeIds = const_cast<int32_t *>(sourceTypes.GetIds());
    int32_t totalDataSize = dataSize * loopCount;

    BaseVector *expectVectors[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        expectVectors[i] = VectorHelper::CreateVector(OMNI_FLAT, sourceTypeIds[i], totalDataSize);
    }

    for (int32_t i = 0; i < dataSize; i++) {
        int32_t index = i * loopCount;
        for (int32_t loopIdx = 0; loopIdx < loopCount; loopIdx++) {
            for (int32_t colIdx = sourceTypesSize - 1; colIdx >= 0; colIdx--) {
                ((i + colIdx == sourceTypesSize) && hasNull) ?
                    expectVectors[colIdx]->SetNull(index + loopIdx) :
                    SetValue(expectVectors[colIdx], index + loopIdx, sortDatas[colIdx]);
            }
        }
    }

    auto expectVecBatch = new VectorBatch(totalDataSize);
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        expectVecBatch->Append(expectVectors[i]);
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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, dataSize, 10, false);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

// sort keys are all types with nulls
TEST(NativeOmniSortTest, TestSortAllTypesWithNulls)
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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, dataSize, 1, true);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

// sort keys are dictionary vector with all types and nulls
TEST(NativeOmniSortTest, TestSortAllTypesWithDictionaryAndNulls)
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
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, dataSize, 1, true);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortZeroRowCountInMemory)
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
    BaseVector *sourceVectors[sourceTypesSize];
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        sourceVectors[i] = VectorHelper::CreateVector(OMNI_FLAT, sourceTypeIds[i], 0);
        sourceVecBatch->Append(sourceVectors[i]);
    }

    sortOperator->AddInput(sourceVecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    EXPECT_EQ(outputVecBatch, nullptr);

    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortSpillWithInvalidConfig)
{
    const int32_t dataSize = 6;
    int32_t data1[dataSize] = {5, 2, 4, 1, 3, 0};
    int64_t data2[dataSize] = {11, 44, 22, 55, 33, 66};

    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    auto sourceTypesSize = sourceTypes.GetSize();
    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {0, 1};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};

    SparkSpillConfig spillConfig1("", UINT64_MAX, 5);
    OperatorConfig operatorConfig1(spillConfig1);
    auto operatorFactory1 = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize, operatorConfig1);
    auto operator1 = operatorFactory1->CreateOperator();
    auto vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    EXPECT_THROW(operator1->AddInput(vecBatch1), omniruntime::exception::OmniException);
    omniruntime::op::Operator::DeleteOperator(operator1);
    DeleteSortOperatorFactory(operatorFactory1);

    SparkSpillConfig spillConfig2("/", UINT64_MAX, 5);
    OperatorConfig operatorConfig2(spillConfig2);
    auto operatorFactory2 = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize, operatorConfig2);
    auto operator2 = operatorFactory2->CreateOperator();
    auto vecBatch2 = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    EXPECT_THROW(operator2->AddInput(vecBatch2), omniruntime::exception::OmniException);
    omniruntime::op::Operator::DeleteOperator(operator2);
    DeleteSortOperatorFactory(operatorFactory2);

    SparkSpillConfig spillConfig3("/opt/+-ab23", UINT64_MAX, 5);
    OperatorConfig operatorConfig3(spillConfig3);
    auto operatorFactory3 = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize, operatorConfig3);
    auto operator3 = operatorFactory3->CreateOperator();
    auto vecBatch3 = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    EXPECT_THROW(operator3->AddInput(vecBatch3), omniruntime::exception::OmniException);
    omniruntime::op::Operator::DeleteOperator(operator3);
    DeleteSortOperatorFactory(operatorFactory3);
    rmdir("/opt/+-ab23");
}

TEST(NativeOmniSortTest, TestSortSpillWithDictionaryAndNulls)
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
    ASSERT_EQ(sortOperator->GetPagesIndex()->GetRowCount(), 0); // page index cleared after spill
    sortOperator->AddInput(sourceVecBatch2);
    ASSERT_EQ(sortOperator->GetPagesIndex()->GetRowCount(), 0);
    sortOperator->AddInput(sourceVecBatch3);
    ASSERT_EQ(sortOperator->GetPagesIndex()->GetRowCount(), 0);
    sortOperator->noMoreInput();

    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, dataSize, 3, true);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortZeroRowCountInMemoryWithSpill)
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
    BaseVector *sourceVectors[sourceTypesSize];
    int32_t *sourceTypeIds = const_cast<int32_t *>(sourceTypes.GetIds());
    for (int32_t i = 0; i < sourceTypesSize; i++) {
        sourceVectors[i] = VectorHelper::CreateVector(OMNI_FLAT, sourceTypeIds[i], 0);
        sourceVecBatch3->Append(sourceVectors[i]);
    }

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize, operatorConfig);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));

    sortOperator->AddInput(sourceVecBatch1);
    sortOperator->AddInput(sourceVecBatch2);
    sortOperator->AddInput(sourceVecBatch3);
    sortOperator->noMoreInput();

    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, dataSize, 2, true);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortSpillWithMemoryThreshold)
{
    // all types: int, long, boolean, double, date32, decimal, decimal128, varchar, char
    int32_t intValue = 20;
    int64_t longValue = 20;
    bool boolValue = true;
    double doubleValue = 20.0;
    Decimal128 decimal128(20, 0);
    std::string stringValue("20-20-20-20-20-20-20-20-20-20-20-20-20-20-20-20-20-20-20-20-20-20-20-20-20-20");
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

    // set global memory limit 15M
    mem::MemoryManager::SetGlobalMemoryLimit(15 * 1 << 20);

    auto sourceVecBatch1 = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 500, true, false);
    auto sourceVecBatch2 = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 500, true, false);
    auto sourceVecBatch3 = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 500, true, false);

    // no row spill threshold, and memory percentage threshold 5%
    SparkSpillConfig spillConfig(GenerateSpillPath(), INT32_MAX, INT32_MAX, 5);
    OperatorConfig operatorConfig(spillConfig);
    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize, operatorConfig);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));

    sortOperator->AddInput(sourceVecBatch1);
    ASSERT_EQ(sortOperator->GetPagesIndex()->GetRowCount(), 0); // page index cleared after spill
    sortOperator->AddInput(sourceVecBatch2);
    ASSERT_EQ(sortOperator->GetPagesIndex()->GetRowCount(), 0);
    sortOperator->AddInput(sourceVecBatch3);
    ASSERT_EQ(sortOperator->GetPagesIndex()->GetRowCount(), 0);
    sortOperator->noMoreInput();

    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, dataSize, 1500, false);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortSpillWithMemoryUnlimit)
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

    // set global memory limit -1, unlimit
    mem::MemoryManager::SetGlobalMemoryLimit(-1);

    auto sourceVecBatch1 = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 500, true, false);
    auto sourceVecBatch2 = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 500, true, false);
    auto sourceVecBatch3 = CreateSortInputForAllTypes(sourceTypes, sortDatas, dataSize, 500, true, false);

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, INT32_MAX, 10);
    OperatorConfig operatorConfig(spillConfig);
    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, sourceTypesSize,
        sortCols, ascendings, nullFirsts, sourceTypesSize, operatorConfig);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));

    sortOperator->AddInput(sourceVecBatch1);
    ASSERT_EQ(sortOperator->GetPagesIndex()->GetRowCount(), dataSize * 500); // rows in page index without spill
    sortOperator->AddInput(sourceVecBatch2);
    ASSERT_EQ(sortOperator->GetPagesIndex()->GetRowCount(), dataSize * 500 * 2);
    sortOperator->AddInput(sourceVecBatch3);
    ASSERT_EQ(sortOperator->GetPagesIndex()->GetRowCount(), dataSize * 500 * 3);
    sortOperator->noMoreInput();

    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateSortExpectForAllTypes(sourceTypes, sortDatas, dataSize, 1500, false);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

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
    int32_t sortCols[] = {1, 0};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};
    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 1);
    OperatorConfig operatorConfig(spillConfig);
    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 3, sortCols,
        ascendings, nullFirsts, 2, operatorConfig);
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));

    sortOperator->AddInput(sourceVecBatch1);
    sortOperator->AddInput(sourceVecBatch2);
    sortOperator->AddInput(sourceVecBatch3);
    sortOperator->AddInput(sourceVecBatch4);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData0[] = {23, 23, 23, 23, 23, 23, 23, 45, 45, 45, 45, 45, 45, 45, 67, 67, 67, 67, 67, 67, 67, 89, 89,
                             89, 89, 89, 89, 89, 23, 23, 23, 45, 45, 45, 67, 67, 67, 89, 89, 89};
    int32_t expectData1[] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2,
                             2, 2, 2, 2, 2, 2, 2, 2, 2, 2};
    int32_t expectData2[] = {12, 12, 12, 12, 12, 12, 12, 24, 24, 24, 24, 24, 24, 24, 36, 36, 36, 36, 36, 36, 36, 48, 48,
                             48, 48, 48, 48, 48, 12, 12, 12, 24, 24, 24, 36, 36, 36, 48, 48, 48};
    auto expectVecBatch = TestUtil::CreateVectorBatch(sourceTypes, 40, expectData0, expectData1, expectData2);
    ASSERT_TRUE(TestUtil::VecBatchMatch(outputVecBatch, expectVecBatch));

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
    int32_t sortCols[] = {1, 0};
    int32_t ascendings[] = {0, 0};
    int32_t nullFirsts[] = {0, 0};
    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 1);
    OperatorConfig operatorConfig(spillConfig);
    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 3, sortCols,
        ascendings, nullFirsts, 2, operatorConfig);
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));

    sortOperator->AddInput(sourceVecBatch1);
    sortOperator->AddInput(sourceVecBatch2);
    sortOperator->AddInput(sourceVecBatch3);
    sortOperator->AddInput(sourceVecBatch4);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData0[] = {89, 89, 89, 67, 67, 67, 45, 45, 45, 23, 23, 23, 89, 89, 89, 89, 89, 89, 89, 67, 67, 67, 67,
                             67, 67, 67, 45, 45, 45, 45, 45, 45, 45, 23, 23, 23, 23, 23, 23, 23};
    int32_t expectData1[] = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                             1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    int32_t expectData2[] = {48, 48, 48, 36, 36, 36, 24, 24, 24, 12, 12, 12, 48, 48, 48, 48, 48, 48, 48, 36, 36, 36, 36,
                             36, 36, 36, 24, 24, 24, 24, 24, 24, 24, 12, 12, 12, 12, 12, 12, 12};
    auto expectVecBatch = TestUtil::CreateVectorBatch(sourceTypes, 40, expectData0, expectData1, expectData2);
    ASSERT_TRUE(TestUtil::VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortAscendingSpillMerge)
{
    const int32_t dataSize = 4;
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), VarcharType(4) }));
    int32_t sourceData10[dataSize] = {4, 5, 6, 7};
    std::string sourceData11[dataSize] = {"d", "e", "f", "g"};
    auto sourceVecBatch1 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData10, sourceData11);

    int32_t sourceData20[dataSize] = {3, 5, 7, 9};
    std::string sourceData21[dataSize] = {"c", "f", "h", "i"};
    auto sourceVecBatch2 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData20, sourceData21);

    int32_t sourceData30[dataSize] = {5, 7, 9, 11};
    std::string sourceData31[dataSize] = {"g", "i", "j", "k"};
    auto sourceVecBatch3 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData30, sourceData31);

    int32_t sourceData40[dataSize] = {1, 4, 6, 8};
    std::string sourceData41[dataSize] = {"a", "e", "g", "h"};
    auto sourceVecBatch4 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData40, sourceData41);

    int32_t sourceData50[dataSize] = {2, 6, 8, 10};
    std::string sourceData51[dataSize] = {"b", "h", "i", "j"};
    auto sourceVecBatch5 = TestUtil::CreateVectorBatch(sourceTypes, dataSize, sourceData50, sourceData51);

    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {0, 1};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {0, 0};
    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 1);
    OperatorConfig operatorConfig(spillConfig);
    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols,
        ascendings, nullFirsts, 2, operatorConfig);
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));

    sortOperator->AddInput(sourceVecBatch1);
    sortOperator->AddInput(sourceVecBatch2);
    sortOperator->AddInput(sourceVecBatch3);
    sortOperator->AddInput(sourceVecBatch4);
    sortOperator->AddInput(sourceVecBatch5);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int32_t expectData0[] = {1, 2, 3, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 9, 9, 10, 11};
    std::string expectData1[] = {"a", "b", "c", "d", "e", "e", "f", "g", "f", "g", "h", "g", "h", "i", "h", "i", "i",
                                 "j", "j", "k"};
    auto expectVecBatch = TestUtil::CreateVectorBatch(sourceTypes, 20, expectData0, expectData1);
    ASSERT_TRUE(TestUtil::VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortAscendings)
{
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(200), LongType() }));
    const int32_t dataSize = 7;
    std::string_view data0[] = {"",
                                "Able villages enforce present holes; users will win increasingly wrong forces.",
                                "Able, strong pictures understand especially.",
                                "A little national lines take.",
                                "Able, widespread elections could not apply to the powers.",
                                "",
                                "Able, widespread elections could not apply to the powers."};
    int64_t data1[] = {0, 7395, 294, 630, 647, 0, 757};

    auto vec0 = new Vector<LargeStringContainer<std::string_view>>(dataSize);
    auto vec1 = new Vector<int64_t>(dataSize);
    for (int32_t i = 0; i < dataSize; i++) {
        if (data0[i].empty()) {
            vec0->SetNull(i);
            vec1->SetNull(i);
        } else {
            vec0->SetValue(i, data0[i]);
            vec1->SetValue(i, data1[i]);
        }
    }

    auto input = new VectorBatch(dataSize);
    input->Append(vec0);
    input->Append(vec1);

    int32_t outputCols[] = {0, 1};
    int32_t sortCols[] = {0, 1};
    int32_t ascendings[] = {1, 1};
    int32_t nullFirsts[] = {1, 1};
    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));

    sortOperator->AddInput(input);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    std::string_view expectData0[] = {"",
                                      "",
                                      "A little national lines take.",
                                      "Able villages enforce present holes; users will win increasingly wrong forces.",
                                      "Able, strong pictures understand especially.",
                                      "Able, widespread elections could not apply to the powers.",
                                      "Able, widespread elections could not apply to the powers."};
    int64_t expectData1[] = {0, 0, 630, 7395, 294, 647, 757};
    auto expectVec0 = new Vector<LargeStringContainer<std::string_view>>(dataSize);
    auto expectVec1 = new Vector<int64_t>(dataSize);
    for (int32_t i = 0; i < dataSize; i++) {
        if (expectData0[i].empty()) {
            expectVec0->SetNull(i);
            expectVec1->SetNull(i);
        } else {
            expectVec0->SetValue(i, expectData0[i]);
            expectVec1->SetValue(i, expectData1[i]);
        }
    }
    auto expectVecBatch = new VectorBatch(dataSize);
    expectVecBatch->Append(expectVec0);
    expectVecBatch->Append(expectVec1);
    ASSERT_TRUE(TestUtil::VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestVarcharSortPerformance)
{
    // construct input data
    const int32_t dataSize = 10000;
    const int32_t vecSize = 2;
    const int32_t nKeys = 980;
    std::string *data1 = new std::string[dataSize];
    std::string *data2 = new std::string[dataSize];
    std::vector<std::pair<std::string, std::string>> dataCombo(dataSize);
    for (int32_t i = 0; i < dataSize; ++i) {
        data1[i] = to_string(i % nKeys);
        data2[i] = to_string(i);
        dataCombo[i].first = data1[i];
        dataCombo[i].second = data2[i];
    }
    std::sort(dataCombo.begin(), dataCombo.end(),
        [](const auto &a, const auto &b) { return a.first > b.first || (a.first == b.first && a.second < b.second); });
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(10), VarcharType(10) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int32_t outputCols[vecSize] = {0, 1};
    int32_t sortCols[vecSize] = {0, 1};
    int32_t ascendings[vecSize] = {false, true};
    int32_t nullFirsts[vecSize] = {true, false};

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, vecSize, sortCols,
        ascendings, nullFirsts, vecSize);

    clock_t start = clock();
    auto sortOperator = CreateTestOperator(operatorFactory);
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    std::cout << "sort and get output elapsed end time: " << static_cast<double>(std::clock() - start) / 1000 <<
        " ms" << std::endl;

    for (int32_t i = 0; i < dataSize; ++i) {
        data1[i] = dataCombo[i].first;
        data2[i] = dataCombo[i].second;
    }
    VectorBatch *expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    delete[] data2;
    delete[] data1;
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, DISABLED_TestAllColumnsCanCastToInt64)
{
    // construct input data
    const int32_t dataSize = 20;
    const int32_t vecSize = 3;
    const int32_t nKeys = 9;
    bool *data1 = new bool[dataSize];
    int32_t *data2 = new int32_t[dataSize];
    int64_t *data3 = new int64_t[dataSize];

    std::vector<std::tuple<bool, int32_t, int64_t>> dataCombo;
    for (int32_t i = 0; i < dataSize; ++i) {
        data1[i] = (i % 3 == 0);
        data2[i] = i % nKeys;
        data3[i] = i / nKeys;
        dataCombo.push_back({ data1[i], data2[i], data3[i] });
    }
    std::sort(dataCombo.begin(), dataCombo.end(), [](const auto &a, const auto &b) {
        return std::get<0>(a) > std::get<0>(b) ||
            (std::get<0>(a) == std::get<0>(b) && std::get<1>(a) < std::get<1>(b)) ||
            (std::get<0>(a) == std::get<0>(b) && std::get<1>(a) == std::get<1>(b) && std::get<2>(a) < std::get<2>(b));
    });
    DataTypes sourceTypes(std::vector<DataTypePtr>({ BooleanType(), IntType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    int32_t outputCols[vecSize] = {0, 1, 2};
    int32_t sortCols[vecSize] = {0, 1, 2};
    int32_t ascendings[vecSize] = {false, true, true};
    int32_t nullFirsts[vecSize] = {true, false, false};

    auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, vecSize, sortCols,
        ascendings, nullFirsts, vecSize);

    clock_t start = clock();
    auto sortOperator = CreateTestOperator(operatorFactory);
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    std::cout << "sort and get output elapsed end time: " << static_cast<double>(std::clock() - start) / 1000 <<
        " ms" << std::endl;

    for (int32_t i = 0; i < dataSize; ++i) {
        data1[i] = std::get<0>(dataCombo[i]);
        data2[i] = std::get<1>(dataCombo[i]);
        data3[i] = std::get<2>(dataCombo[i]);
    }
    VectorBatch *expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    delete[] data3;
    delete[] data2;
    delete[] data1;

    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

template <typename T>
static void TestInplaceSort(T *sourceData, T *expectData, DataTypes &sourceTypes, int32_t dataSize)
{
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, sourceData);

    int outputCols[1] = {0};
    int sortCols[1] = {0};
    int ascendings[1] = {true};
    int nullFirsts[1] = {true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 1, sortCols, ascendings, nullFirsts, 1);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestInplaceSortFortAllType)
{
    const int32_t dataSize = 5;
    // long
    int64_t data1[dataSize] = {1, 0, 2, 4, 3};
    int64_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType() }));
    TestInplaceSort<int64_t>(data1, expectData1, sourceTypes, dataSize);
    sourceTypes = DataTypes(std::vector<DataTypePtr>({ LongType() }));

    // int
    int data2[dataSize] = {1, 0, 2, 4, 3};
    int expectData2[dataSize] = {0, 1, 2, 3, 4};
    sourceTypes = DataTypes(std::vector<DataTypePtr>({ IntType() }));
    TestInplaceSort<int>(data2, expectData2, sourceTypes, dataSize);

    // double
    double data3[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2};
    double expectData3[dataSize] = {2.2, 3.3, 4.4, 5.5, 6.6};
    sourceTypes = DataTypes(std::vector<DataTypePtr>({ DoubleType() }));
    TestInplaceSort<double>(data3, expectData3, sourceTypes, dataSize);

    // short
    int16_t data4[dataSize] = {5, 4, 3, 2, 1};
    int16_t expectData4[dataSize] = {1, 2, 3, 4, 5};
    sourceTypes = DataTypes(std::vector<DataTypePtr>({ ShortType() }));
    TestInplaceSort<int16_t>(data4, expectData4, sourceTypes, dataSize);

    // decimal64
    int64_t data5[dataSize] = {55, 44, 33, 22, 11};
    int64_t expectData5[dataSize] = {11, 22, 33, 44, 55};
    sourceTypes = DataTypes(std::vector<DataTypePtr>({ Decimal64Type(2, 0) }));
    TestInplaceSort<int64_t>(data5, expectData5, sourceTypes, dataSize);

    // decimal128
    Decimal128 data6[dataSize] = {55, 44, 33, 22, 11};
    Decimal128 expectData6[dataSize] = {11, 22, 33, 44, 55};
    sourceTypes = DataTypes(std::vector<DataTypePtr>({ Decimal128Type(2, 0) }));
    TestInplaceSort<Decimal128>(data6, expectData6, sourceTypes, dataSize);
}

TEST(NativeOmniSortTest, TestInplaceSortWithNullFirst)
{
    // construct input data
    const int32_t dataSize = 6;
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, -1};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1);
    vecBatch->Get(0)->SetNull(dataSize - 1);
    std::vector<int32_t> ids = { 2, 4, dataSize - 1 };
    BaseVector *dic =
        VectorHelper::CreateDictionary(ids.data(), ids.size(), reinterpret_cast<Vector<int64_t> *>(vecBatch->Get(0)));
    VectorBatch *vectorBatch2 = new VectorBatch(ids.size());
    vectorBatch2->Append(dic);

    int outputCols[1] = {0};
    int sortCols[1] = {0};
    int ascendings[1] = {false};
    int nullFirsts[1] = {true};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 1, sortCols, ascendings, nullFirsts, 1);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->AddInput(vectorBatch2);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize + 3] = {-1, -1, 4, 4, 3, 2, 2, 1, 0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize + 3, expectData1);
    expectVecBatch->Get(0)->SetNull(0);
    expectVecBatch->Get(0)->SetNull(1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestInplaceSortWithNullLast)
{
    // construct input data
    const int32_t dataSize = 6;
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, -1};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1);
    vecBatch->Get(0)->SetNull(dataSize - 1);

    int outputCols[1] = {0};
    int sortCols[1] = {0};
    int ascendings[1] = {false};
    int nullFirsts[1] = {false};

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 1, sortCols, ascendings, nullFirsts, 1);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);

    int64_t expectData1[dataSize] = {4, 3, 2, 1, 0, -1};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1);
    expectVecBatch->Get(0)->SetNull(dataSize - 1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleAsceCase0)
{
    // construct input data
    const int32_t dataSize = 2;
    double data0[dataSize] = { -601257107.321106, -1785730246.200178 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { true, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { -1785730246.200178, -601257107.321106 };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleAsceCase1)
{
    // construct input data
    const int32_t dataSize = 3;
    double data0[dataSize] = { -1785730246.200178, -52071258.692677, -601257107.321106 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { true, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { -1785730246.200178, -601257107.321106, -52071258.692677 };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleAsceCase2)
{
    // construct input data
    const int32_t dataSize = 4;
    double data0[dataSize] = { -1785730246.200178, -52071258.692677, -601257107.321106, 863885742.930719 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { true, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { -1785730246.200178, -601257107.321106, -52071258.692677, 863885742.930719 };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleAsceCase3)
{
    // construct input data
    const int32_t dataSize = 7;
    double data0[dataSize] = { -1785730246.200178, -52071258.692677,  -601257107.321106, 863885742.930719,
                               402955560.021583,   -682611980.136355, -682611980.136355 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { true, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { -1785730246.200178, -682611980.136355, -682611980.136355, -601257107.321106,
                                     -52071258.692677,   402955560.021583,  863885742.930719 };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleAsceCase4)
{
    // construct input data
    const int32_t dataSize = 8;
    double data0[dataSize] = { -1785730246.200178, -52071258.692677,  -601257107.321106, 863885742.930719,
                               402955560.021583,   -682611980.136355, -682611980.136355, 2355596.356874123 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { true, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { -1785730246.200178, -682611980.136355, -682611980.136355, -601257107.321106,
                                     -52071258.692677,   2355596.356874123, 402955560.021583,  863885742.930719 };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleAsceCase5)
{
    // construct input data
    const int32_t dataSize = 9;
    double data0[dataSize] = { 23.689512254,         2362554.365841,    2256541.897841,
                               4556985.556554122562, -45685112558.363,  565656.2358741,
                               11122112.36587,       235689451232.1178, 226532.362587 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { true, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { -45685112558.363,     23.689512254,   226532.362587,
                                     565656.2358741,       2256541.897841, 2362554.365841,
                                     4556985.556554122562, 11122112.36587, 235689451232.1178 };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleAsceCase6)
{
    // construct input data
    const int32_t dataSize = 11;
    double data0[dataSize] = { -DBL_MAX,          DBL_MAX,           DBL_MIN,          -1785730246.200178,
                               -52071258.692677,  -601257107.321106, 863885742.930719, 402955560.021583,
                               -682611980.136355, -682611980.136355, 2355596.356874123 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { true, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { -DBL_MAX,
                                     -1785730246.200178,
                                     -682611980.136355,
                                     -682611980.136355,
                                     -601257107.321106,
                                     -52071258.692677,
                                     DBL_MIN,
                                     2355596.356874123,
                                     402955560.021583,
                                     863885742.930719,
                                     DBL_MAX };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}
TEST(NativeOmniSortTest, TestSimdSortDoubleAsceCase7)
{
    // construct input data
    const int32_t dataSize = 16;
    double data0[dataSize] = { -1785730246.200178,
                               -52071258.692677,
                               -601257107.321106,
                               863885742.930719,
                               402955560.021583,
                               -682611980.136355,
                               -682611980.136355,
                               2355596.356874123,
                               -DBL_MIN,
                               2365847.369854,
                               -55478201.365852,
                               25465841.36589221,
                               DBL_MAX,
                               25658741.36987,
                               -DBL_MAX,
                               DBL_MIN };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { true, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { -DBL_MAX,          -1785730246.200178, -682611980.136355, -682611980.136355,
                                     -601257107.321106, -55478201.365852,   -52071258.692677,  -DBL_MIN,
                                     DBL_MIN,           2355596.356874123,  2365847.369854,    25465841.36589221,
                                     25658741.36987,    402955560.021583,   863885742.930719,  DBL_MAX };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleAsceCase8)
{
    // construct input data
    const int32_t dataSize = 18;
    double data0[dataSize] = { -1785730246.200178,
                               -52071258.692677,
                               -601257107.321106,
                               863885742.930719,
                               402955560.021583,
                               -682611980.136355,
                               -682611980.136355,
                               2355596.356874123,
                               -DBL_MIN,
                               2365847.369854,
                               -55478201.365852,
                               25465841.36589221,
                               DBL_MAX,
                               25658741.36987,
                               -DBL_MAX,
                               DBL_MIN,
                               863985742.930719,
                               883885742.930719 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { true, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { -DBL_MAX,          -1785730246.200178, -682611980.136355,
                                     -682611980.136355, -601257107.321106,  -55478201.365852,
                                     -52071258.692677,  -DBL_MIN,           DBL_MIN,
                                     2355596.356874123, 2365847.369854,     25465841.36589221,
                                     25658741.36987,    402955560.021583,   863885742.930719,
                                     863985742.930719,  883885742.930719,   DBL_MAX };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleAsceCase9)
{
    // construct input data
    const int32_t dataSize = 27;
    double data0[dataSize] = { -1785730246.200178, -52071258.692677,   -601257107.321106,  863885742.930719,
                               -1785830246.200178, -1786730246.200178, -1787730246.200178, 402955560.021583,
                               -682611980.136355,  -682611980.136355,  2355596.356874123,  -DBL_MIN,
                               2365847.369854,     -55478201.365852,   25465841.36589221,  DBL_MAX,
                               25658741.36987,     883985742.930719,   0.00000001,         0.00000000001,
                               1.11223325666,      2.3366998723622232, -DBL_MAX,           DBL_MIN,
                               863985742.930719,   883885742.930719,   883895742.930719 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { true, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { -DBL_MAX,           -1787730246.200178, -1786730246.200178,
                                     -1785830246.200178, -1785730246.200178, -682611980.136355,
                                     -682611980.136355,  -601257107.321106,  -55478201.365852,
                                     -52071258.692677,   -DBL_MIN,           DBL_MIN,
                                     0.00000000001,      0.00000001,         1.11223325666,
                                     2.3366998723622232, 2355596.356874123,  2365847.369854,
                                     25465841.36589221,  25658741.36987,     402955560.021583,
                                     863885742.930719,   863985742.930719,   883885742.930719,
                                     883895742.930719,   883985742.930719,   DBL_MAX };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleDescCase1)
{
    // construct input data
    const int32_t dataSize = 7;
    double data0[dataSize] = { -1785730246.200178, -52071258.692677,  -601257107.321106, 863885742.930719,
                               402955560.021583,   -682611980.136355, -682611980.136355 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { false, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { 863885742.930719,  402955560.021583,  -52071258.692677,  -601257107.321106,
                                     -682611980.136355, -682611980.136355, -1785730246.200178 };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleDescCase2)
{
    // construct input data
    const int32_t dataSize = 8;
    double data0[dataSize] = { -1785730246.200178, -52071258.692677,  -601257107.321106, 863885742.930719,
                               402955560.021583,   -682611980.136355, -682611980.136355, 2355596.356874123 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { false, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { 863885742.930719,  402955560.021583,  2355596.356874123, -52071258.692677,
                                     -601257107.321106, -682611980.136355, -682611980.136355, -1785730246.200178 };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleDescCase3)
{
    // construct input data
    const int32_t dataSize = 9;
    double data0[dataSize] = { 23.689512254,         2362554.365841,    2256541.897841,
                               4556985.556554122562, -45685112558.363,  565656.2358741,
                               11122112.36587,       235689451232.1178, 226532.362587 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { false, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { 235689451232.1178, 11122112.36587, 4556985.556554122562,
                                     2362554.365841,    2256541.897841, 565656.2358741,
                                     226532.362587,     23.689512254,   -45685112558.363 };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleDescCase4)
{
    // construct input data
    const int32_t dataSize = 16;
    double data0[dataSize] = { -1785730246.200178,
                               -52071258.692677,
                               -601257107.321106,
                               863885742.930719,
                               402955560.021583,
                               -682611980.136355,
                               -682611980.136355,
                               2355596.356874123,
                               -DBL_MIN,
                               2365847.369854,
                               -55478201.365852,
                               25465841.36589221,
                               DBL_MAX,
                               25658741.36987,
                               -DBL_MAX,
                               DBL_MIN };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { false, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { DBL_MAX,           863885742.930719,  402955560.021583,   25658741.36987,
                                     25465841.36589221, 2365847.369854,    2355596.356874123,  DBL_MIN,
                                     -DBL_MIN,          -52071258.692677,  -55478201.365852,   -601257107.321106,
                                     -682611980.136355, -682611980.136355, -1785730246.200178, -DBL_MAX };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSimdSortDoubleDescCase5)
{
    // construct input data
    const int32_t dataSize = 27;
    double data0[dataSize] = { -1785730246.200178, -52071258.692677,   -601257107.321106,  863885742.930719,
                               -1785830246.200178, -1786730246.200178, -1787730246.200178, 402955560.021583,
                               -682611980.136355,  -682611980.136355,  2355596.356874123,  -DBL_MIN,
                               2365847.369854,     -55478201.365852,   25465841.36589221,  DBL_MAX,
                               25658741.36987,     883985742.930719,   0.00000001,         0.00000000001,
                               1.11223325666,      2.3366998723622232, -DBL_MAX,           DBL_MIN,
                               863985742.930719,   883885742.930719,   883895742.930719 };
    int64_t data1[dataSize] = {0};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1);

    int outputCols[2] = { 0, 1 };
    int sortCols[2] = { 0, 1 };
    int ascendings[2] = { false, true };
    int nullFirsts[2] = { false, false };

    auto operatorFactory =
        SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    sortOperator->noMoreInput();
    VectorBatch *outputVecBatch = nullptr;
    sortOperator->GetOutput(&outputVecBatch);
    double expectData0[dataSize] = { DBL_MAX,
                                     883985742.930719,
                                     883895742.930719,
                                     883885742.930719,
                                     863985742.930719,
                                     863885742.930719,
                                     402955560.021583,
                                     25658741.36987,
                                     25465841.36589221,
                                     2365847.369854,
                                     2355596.356874123,
                                     2.3366998723622232,
                                     1.11223325666,
                                     0.00000001,
                                     0.00000000001,
                                     DBL_MIN,
                                     -DBL_MIN,
                                     -52071258.692677,
                                     -55478201.365852,
                                     -601257107.321106,
                                     -682611980.136355,
                                     -682611980.136355,
                                     -1785730246.200178,
                                     -1785830246.200178,
                                     -1786730246.200178,
                                     -1787730246.200178,
                                     -DBL_MAX };
    int64_t expectData1[dataSize] = {0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteSortOperatorFactory(operatorFactory);
}

TEST(NativeOmniSortTest, TestSortRadixSort)
{
    std::vector<std::pair<int64_t, int64_t>> dataRanges { { -126, 126 } };
    static constexpr int32_t dataSize = 10;
    for (size_t ir = 0; ir < dataRanges.size(); ++ir) {
        int32_t data1[dataSize];
        int64_t data2[dataSize];
        std::vector<std::pair<int32_t, int64_t>> dataCombo(dataSize);
        int64_t step = (dataRanges[ir].second - dataRanges[ir].first) / dataSize;
        for (int32_t i = 0; i < dataSize; i++) {
            data1[i] = i;
            data2[i] = dataRanges[ir].first + step * i;
            dataCombo[i] = {data1[i], data2[i]};
        }

        DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
        VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

        int outputCols[2] = {0, 1};
        int sortCols[1] = {1};
        int ascendings[1] = {false};
        int nullFirsts[1] = {true};
        auto operatorFactory = SortOperatorFactory::CreateSortOperatorFactory(sourceTypes, outputCols, 2, sortCols,
            ascendings, nullFirsts, 1);

        auto sortOperator = dynamic_cast<SortOperator *>(CreateTestOperator(operatorFactory));
        sortOperator->AddInput(vecBatch);
        sortOperator->noMoreInput();
        VectorBatch *outputVecBatch = nullptr;
        sortOperator->GetOutput(&outputVecBatch);

        // Create expected output
        std::sort(dataCombo.begin(), dataCombo.end(), [](const auto &a, const auto &b) { return a.second > b.second; });
        int32_t expectData1[dataSize];
        int64_t expectData2[dataSize];
        std::vector<int32_t> nullPosition;
        for (int i = 0; i < dataSize; i++) {
            expectData1[i] = dataCombo[i].first;
            expectData2[i] = dataCombo[i].second;
        }
        AssertVecBatchEquals(outputVecBatch, 2, dataSize, expectData1, expectData2);

        // free memory
        VectorHelper::FreeVecBatch(outputVecBatch);
        omniruntime::op::Operator::DeleteOperator(sortOperator);
        DeleteSortOperatorFactory(operatorFactory);
    }
}

} // namespace SortTest
