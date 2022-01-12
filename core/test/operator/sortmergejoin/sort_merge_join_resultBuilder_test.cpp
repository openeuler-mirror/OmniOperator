/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include <vector>
#include "gtest/gtest.h"
#include "../../../src/vector/vector_helper.h"
#include "../../util/test_util.h"
#include "../../../src/operator/pages_index.h"
#include "../../../src/operator/join/sortmergejoin/dynamic_pages_index.h"
#include "../../../src/operator/join/sortmergejoin/sort_merge_join_resultBuilder.h"

using namespace omniruntime::op;
using namespace std;

TEST(SMJ_RESULT_BUILDER_TESTCASE, testMultiAddValueAndGetOutput)
{
    std::vector<VecType> leftTypes = { IntVecType::Instance(), DoubleVecType::Instance() };
    VecTypes leftSourceTypes(leftTypes);
    std::vector<VecType> rightTypes = { IntVecType::Instance(), DoubleVecType::Instance(), VarcharVecType(3) };
    VecTypes rightSourceTypes(rightTypes);

    auto *leftPagesIndex = new DynamicPagesIndex(leftSourceTypes);
    auto *rightPagesIndex = new DynamicPagesIndex(rightSourceTypes);

    const int32_t DATA_SIZE = 6;
    int32_t leftData1_1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    Vector *leftVector1 = CreateVector<IntVector, int32_t>(leftData1_1, DATA_SIZE);
    // build dictionary vector for test
    double leftData1_2[DATA_SIZE] = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5};
    Vector *dataVector1 = CreateVector<DoubleVector, double>(leftData1_2, DATA_SIZE);
    int32_t ids1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    auto dicVector1 = new DictionaryVector(dataVector1, ids1, DATA_SIZE);
    auto *leftVecBatch1 = new VectorBatch(2, DATA_SIZE);
    leftVecBatch1->SetVector(0, leftVector1);
    leftVecBatch1->SetVector(1, dicVector1);

    int32_t leftData2_1[DATA_SIZE] = {6, 7, 8, 9, 10, 11};
    Vector *leftVector2 = CreateVector<IntVector, int32_t>(leftData2_1, DATA_SIZE);
    double leftData2_2[DATA_SIZE] = {6.6, 7.7, 8.8, 9.9, 10.1, 11.1};
    Vector *dataVector2 = CreateVector<DoubleVector, double>(leftData2_2, DATA_SIZE);
    int32_t ids2[DATA_SIZE] = {6, 7, 8, 9, 10, 11};
    auto dicVector2 = new DictionaryVector(dataVector2, ids2, DATA_SIZE);
    auto *leftVecBatch2 = new VectorBatch(2, DATA_SIZE);
    leftVecBatch2->SetVector(0, leftVector2);
    leftVecBatch2->SetVector(1, dicVector2);

    std::vector<VectorBatch *> leftBatchVector;
    leftBatchVector.push_back(leftVecBatch1);
    leftBatchVector.push_back(leftVecBatch2);
    leftPagesIndex->AddVecBatches(leftBatchVector);
    int32_t leftTableOutputCols[2] = {0, 1};
    int32_t leftTableOutputColsCount = 2;

    int32_t rightData1_1[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    double rightData1_2[DATA_SIZE] = {5.5, 4.4, 3.3, 2.2, 1.1, 0.0};
    std::string rightData1_3[DATA_SIZE] = {"555", "444", "33", "22", "1", "0"};
    int32_t rightData2_1[DATA_SIZE] = {11, 10, 9, 8 , 7 , 6};
    double rightData2_2[DATA_SIZE] = {11.1, 10.1, 9.9, 8.8, 7.7, 6.6};
    std::string rightData2_3[DATA_SIZE] = {"111", "101", "99", "88", "7", "6"};

    VectorBatch *rightVecBatch1 =
        CreateVectorBatch(rightSourceTypes, DATA_SIZE, rightData1_1, rightData1_2, rightData1_3);
    VectorBatch *rightVecBatch2 =
        CreateVectorBatch(rightSourceTypes, DATA_SIZE, rightData2_1, rightData2_2, rightData2_3);
    std::vector<VectorBatch *> rightBatchVector;
    rightBatchVector.push_back(rightVecBatch1);
    rightBatchVector.push_back(rightVecBatch2);
    rightPagesIndex->AddVecBatches(rightBatchVector);
    int32_t rightTableOutputCols[2] = {1, 2};
    int32_t rightTableOutputColsCount = 2;
    string filter;

    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();

    auto *resultBuilder =
        new JoinResultBuilder(leftSourceTypes, leftTableOutputCols, leftTableOutputColsCount, leftPagesIndex,
        rightSourceTypes, rightTableOutputCols, rightTableOutputColsCount, rightPagesIndex, filter, vecAllocator);

    std::vector<int64_t> leftAddress1 = { EncodeSyntheticAddress(0, 1), EncodeSyntheticAddress(0, 3),
        EncodeSyntheticAddress(0, 5), EncodeSyntheticAddress(1, 1),
        EncodeSyntheticAddress(1, 3), EncodeSyntheticAddress(1, 5) };
    std::vector<int64_t> rightAddress1 = { EncodeSyntheticAddress(0, 0), EncodeSyntheticAddress(0, 2),
        EncodeSyntheticAddress(0, 4), EncodeSyntheticAddress(1, 0),
        EncodeSyntheticAddress(1, 2), EncodeSyntheticAddress(1, 4) };

    ASSERT_EQ(resultBuilder->AddJoinValueAddresses(leftAddress1, rightAddress1), 0);

    std::vector<omniruntime::vec::VectorBatch *> outputPages;

    resultBuilder->GetOutput(outputPages);

    ASSERT_EQ(outputPages.size(), 1);

    int32_t expectedData1[6] = {1, 3, 5, 7, 9, 11};
    double expectedData2[6] = {1.1, 3.3, 5.5, 7.7, 9.9, 11.1};
    double expectedData3[6] = {5.5, 3.3, 1.1, 11.1, 9.9, 7.7};
    string expectedData4[6] = {"555", "33", "1", "111", "99", "7"};

    AssertVecBatchEquals(outputPages[0], 4, 6, expectedData1, expectedData2, expectedData3, expectedData4);

    delete dicVector1;
    delete dicVector2;
    delete resultBuilder;
    delete leftPagesIndex;
    delete rightPagesIndex;
    VectorHelper::FreeVecBatches(rightBatchVector);
    VectorHelper::FreeVecBatches(leftBatchVector);
    VectorHelper::FreeVecBatches(outputPages);
}

TEST(SMJ_RESULT_BUILDER_TESTCASE, testAddValueWithFilter)
{
    std::vector<VecType> leftTypes = { IntVecType::Instance(), DoubleVecType::Instance() };
    VecTypes leftSourceTypes(leftTypes);
    std::vector<VecType> rightTypes = { IntVecType::Instance(), DoubleVecType::Instance(), VarcharVecType(3) };
    VecTypes rightSourceTypes(rightTypes);

    auto *leftPagesIndex = new DynamicPagesIndex(leftSourceTypes);
    auto *rightPagesIndex = new DynamicPagesIndex(rightSourceTypes);

    const int32_t DATA_SIZE = 6;
    int32_t leftData1_1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double leftData1_2[DATA_SIZE] = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5};

    VectorBatch *leftVecBatch = CreateVectorBatch(leftSourceTypes, DATA_SIZE, leftData1_1, leftData1_2);
    std::vector<VectorBatch *> leftBatchVector;
    leftBatchVector.push_back(leftVecBatch);
    leftPagesIndex->AddVecBatches(leftBatchVector);
    int32_t leftTableOutputCols[2] = {0, 1};
    int32_t leftTableOutputColsCount = 2;

    int32_t rightData1_1[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    double rightData1_2[DATA_SIZE] = {5.5, 4.4, 3.3, 2.2, 1.1, 0.0};
    std::string rightData1_3[DATA_SIZE] = {"555", "444", "33", "22", "1", "0"};

    VectorBatch *rightVecBatch =
        CreateVectorBatch(rightSourceTypes, DATA_SIZE, rightData1_1, rightData1_2, rightData1_3);
    std::vector<VectorBatch *> rightBatchVector;
    rightBatchVector.push_back(rightVecBatch);
    rightPagesIndex->AddVecBatches(rightBatchVector);
    int32_t rightTableOutputCols[2] = {1, 2};
    int32_t rightTableOutputColsCount = 2;
    string filter = "$operator$GREATER_THAN:4(#0, 1:1)";
    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();

    auto *resultBuilder =
        new JoinResultBuilder(leftSourceTypes, leftTableOutputCols, leftTableOutputColsCount, leftPagesIndex,
        rightSourceTypes, rightTableOutputCols, rightTableOutputColsCount, rightPagesIndex, filter, vecAllocator);

    std::vector<int64_t> leftAddress1 = { EncodeSyntheticAddress(0, 1), EncodeSyntheticAddress(0, 3),
        EncodeSyntheticAddress(0, 5) };
    std::vector<int64_t> rightAddress1 = { EncodeSyntheticAddress(0, 0), EncodeSyntheticAddress(0, 2),
        EncodeSyntheticAddress(0, 4) };

    ASSERT_EQ(resultBuilder->AddJoinValueAddresses(leftAddress1, rightAddress1), 0);

    std::vector<omniruntime::vec::VectorBatch *> outputPages;

    resultBuilder->GetOutput(outputPages);

    ASSERT_EQ(outputPages.size(), 1);

    int32_t expectedData1[2] = {3, 5};
    double expectedData2[2] = {3.3, 5.5};
    double expectedData3[2] = {3.3, 1.1};
    string expectedData4[2] = {"33", "1"};

    AssertVecBatchEquals(outputPages[0], 4, 2, expectedData1, expectedData2, expectedData3, expectedData4);

    delete resultBuilder;
    delete leftPagesIndex;
    delete rightPagesIndex;
    VectorHelper::FreeVecBatch(leftVecBatch);
    VectorHelper::FreeVecBatch(rightVecBatch);
    VectorHelper::FreeVecBatches(outputPages);
}