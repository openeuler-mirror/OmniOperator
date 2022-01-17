/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */
#include "gtest/gtest.h"
#include "../../src/operator/partitionedoutput/partitionedoutput.h"
#include "../util/test_util.h"
#include "../../src/vector/vector_helper.h"
#include "../../src/vector/vector_common.h"
#include <vector>
#include <chrono>

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;

TEST(PartitionedOutputOperatorTest, TestOnePartitionedOutput)
{
    const int32_t DATA_SIZE = 6;
    VecTypes buildTypes(std::vector<VecType>({ IntVecType(), IntVecType(), IntVecType() }));
    int buildData1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData1, buildData1);
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), IntVecType() }));

    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = {0};
    int32_t hashChannelTypes[1] = {0};
    int32_t hashChannelTypesCount = 1;
    int32_t hashChannels[1] = {0};
    int32_t hashChannelsCount = 1;

    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, true, hashChannelTypes,
        hashChannelTypesCount, hashChannels, hashChannelsCount);
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    PartitionedOutputOperator *partitionedOperator =
        static_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 6); // 6 row

    int32_t expectData0[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    int32_t expectData1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    VecTypes expectedTypes(std::vector<VecType>({ IntVecType(), IntVecType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, DATA_SIZE, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestMultiPartitionedOutput)
{
    const int32_t DATA_SIZE = 7;
    VecTypes buildTypes(std::vector<VecType>({ IntVecType(), IntVecType(), IntVecType() }));
    int buildData1[DATA_SIZE] = {0, 1, 2, 3, 4, 5, 6};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData1, buildData1);
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), IntVecType() }));

    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[2] = {0, 1};
    int32_t hashChannelTypes[1] = {0};
    int32_t hashChannelTypesCount = 1;
    int32_t hashChannels[1] = {0};
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 2, true, hashChannelTypes,
        hashChannelTypesCount, hashChannels, hashChannelsCount);
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    PartitionedOutputOperator *partitionedOperator =
        (PartitionedOutputOperator *)partitionedOutputOperatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);
    EXPECT_EQ(outputVecBatch.size(), 2);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 4); // 4 row
    EXPECT_EQ(outputVecBatch[1]->GetRowCount(), 3); // 3 row

    int32_t expectData0[DATA_SIZE] = {0, 2, 4, 6};
    int32_t expectData1[DATA_SIZE] = {1, 3, 5};
    VecTypes expectedTypes(std::vector<VecType>({ IntVecType(), IntVecType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 4, expectData0, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));
    VectorBatch *expectVecBatch1 = CreateVectorBatch(expectedTypes, 3, expectData1, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[1], expectVecBatch1));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch1);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestHashIntPartitionedOutput)
{
    const int32_t DATA_SIZE = 6;
    VecTypes buildTypes(std::vector<VecType>({ IntVecType(), IntVecType(), IntVecType() }));
    int buildData1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData1, buildData1);
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType() }));

    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[2] = {0, 1};
    int32_t partitionCount = 2;
    int32_t bucketToPartition[1] = {0};
    int32_t hashChannelTypes[2] = {1, 1};
    int32_t hashChannelTypesCount = 2;
    int32_t hashChannels[2] = {0, 1};
    int32_t hashChannelsCount = 2;
    bool isHashPrecomputed = false;

    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 2, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannelTypesCount, hashChannels, hashChannelsCount);
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    PartitionedOutputOperator *partitionedOperator =
        (PartitionedOutputOperator *)partitionedOutputOperatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 6); // 6 row
    int32_t expectData0[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    VecTypes expectedTypes(std::vector<VecType>({ IntVecType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, DATA_SIZE, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestHashVarcharPartitionedOutput)
{
    const int32_t DATA_SIZE = 3;
    VecTypes buildTypes(std::vector<VecType>({ VarcharVecType(3), VarcharVecType(3) }));
    std::string buildData1[DATA_SIZE] = {"abc", "de", "f"};
    std::string buildData2[DATA_SIZE] = {"def", "bc", "a"};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);

    bool isHashPrecomputed = false;

    VecTypes sourceTypes(std::vector<VecType>({ VarcharVecType(3) }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = {0};
    int32_t hashChannelTypes[1] = {15};
    int32_t hashChannelTypesCount = 1;
    int32_t hashChannels[1] = {0};
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannelTypesCount, hashChannels, hashChannelsCount);
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    PartitionedOutputOperator *partitionedOperator =
        (PartitionedOutputOperator *)partitionedOutputOperatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 4 row
    string expectData0[3] = {"abc", "de", "f"};
    VecTypes expectedTypes(std::vector<VecType>({ VarcharVecType(3) }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestHashCharPartitionedOutput)
{
    const int32_t DATA_SIZE = 3;
    VecTypes buildTypes(std::vector<VecType>({ CharVecType(3), CharVecType(3) }));
    std::string buildData1[DATA_SIZE] = {"abc", "de", "f"};
    std::string buildData2[DATA_SIZE] = {"def", "bc", "a"};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);

    bool isHashPrecomputed = false;

    VecTypes sourceTypes(std::vector<VecType>({ CharVecType(3) }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = {0};
    int32_t hashChannelTypes[1] = {CharVecType::Instance().GetId()};
    int32_t hashChannelTypesCount = 1;
    int32_t hashChannels[1] = {0};
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannelTypesCount, hashChannels, hashChannelsCount);
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    PartitionedOutputOperator *partitionedOperator =
        (PartitionedOutputOperator *)partitionedOutputOperatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 4 row
    string expectData0[3] = {"abc", "de", "f"};
    VecTypes expectedTypes(std::vector<VecType>({ CharVecType(3) }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestNullPartitionedOutput)
{
    const int32_t DATA_SIZE = 3;
    VecTypes buildTypes(std::vector<VecType>({ VarcharVecType(3), VarcharVecType(3) }));
    std::string buildData1[DATA_SIZE] = {"abc", "de", "f"};
    std::string buildData2[DATA_SIZE] = {"def", "bc", "a"};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);
    vecBatch->GetVector(0)->SetValueNull(0);

    bool isHashPrecomputed = false;
    VecTypes sourceTypes(std::vector<VecType>({ VarcharVecType(3) }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = {0};
    int32_t hashChannelTypes[1] = {15};
    int32_t hashChannelTypesCount = 1;
    int32_t hashChannels[1] = {0};
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannelTypesCount, hashChannels, hashChannelsCount);
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    PartitionedOutputOperator *partitionedOperator =
        (PartitionedOutputOperator *)partitionedOutputOperatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 3 row
    string expectData0[3] = {"abe", "de", "f"};
    int32_t expectData1[DATA_SIZE] = {1, 3, 5};
    VecTypes expectedTypes(std::vector<VecType>({ VarcharVecType(3) }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    expectVecBatch->GetVector(0)->SetValueNull(0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestDecimalPartitionedOutput)
{
    const int32_t DATA_SIZE = 3;
    VecTypes buildTypes(std::vector<VecType>({ Decimal64VecType(2, 0), Decimal64VecType(2, 0) }));
    int64_t buildData1[DATA_SIZE] = {11, 22, 33};
    int64_t buildData2[DATA_SIZE] = {33, 22, 111};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);

    bool isHashPrecomputed = false;

    VecTypes sourceTypes(std::vector<VecType>({ Decimal64VecType(2, 0) }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = {0};
    int32_t hashChannelTypes[1] = {6};
    int32_t hashChannelTypesCount = 1;
    int32_t hashChannels[1] = {0};
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannelTypesCount, hashChannels, hashChannelsCount);
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    PartitionedOutputOperator *partitionedOperator =
        (PartitionedOutputOperator *)partitionedOutputOperatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 3 row
    int64_t expectData0[3] = {11, 22, 33};
    VecTypes expectedTypes(std::vector<VecType>({ Decimal64VecType(2, 0) }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestDoublePartitionedOutput)
{
    const int32_t DATA_SIZE = 3;
    VecTypes buildTypes(std::vector<VecType>({ DoubleVecType(), DoubleVecType() }));
    int64_t buildData1[DATA_SIZE] = {11, 22, 33};
    int64_t buildData2[DATA_SIZE] = {33, 22, 111};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);

    bool isHashPrecomputed = false;

    VecTypes sourceTypes(std::vector<VecType>({ DoubleVecType() }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = {0};
    int32_t hashChannelTypes[1] = {6};
    int32_t hashChannelTypesCount = 1;
    int32_t hashChannels[1] = {0};
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannelTypesCount, hashChannels, hashChannelsCount);
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    PartitionedOutputOperator *partitionedOperator =
        (PartitionedOutputOperator *)partitionedOutputOperatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 3 row
    int64_t expectData0[3] = {11, 22, 33};
    VecTypes expectedTypes(std::vector<VecType>({ DoubleVecType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestBoolPartitionedOutput)
{
    const int32_t DATA_SIZE = 3;
    VecTypes buildTypes(std::vector<VecType>({ BooleanVecType(), BooleanVecType() }));
    int64_t buildData1[DATA_SIZE] = {0, 1, 0};
    int64_t buildData2[DATA_SIZE] = {0, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);

    bool isHashPrecomputed = false;

    VecTypes sourceTypes(std::vector<VecType>({ BooleanVecType() }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = {0};
    int32_t hashChannelTypes[1] = {6};
    int32_t hashChannelTypesCount = 1;
    int32_t hashChannels[1] = {0};
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannelTypesCount, hashChannels, hashChannelsCount);
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    PartitionedOutputOperator *partitionedOperator =
        (PartitionedOutputOperator *)partitionedOutputOperatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 3 row
    int64_t expectData0[3] = {0, 1, 0};
    VecTypes expectedTypes(std::vector<VecType>({ BooleanVecType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestDecimal128PartitionedOutput)
{
    const int32_t DATA_SIZE = 3;
    VecTypes buildTypes(std::vector<VecType>({ Decimal128VecType(2, 0), Decimal128VecType(2, 0) }));
    Decimal128 buildData1[DATA_SIZE] = {11, 22, 33};
    Decimal128 buildData2[DATA_SIZE] = {0, 1, 2};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);

    bool isHashPrecomputed = false;

    VecTypes sourceTypes(std::vector<VecType>({ Decimal128VecType(2, 0) }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = {0};
    int32_t hashChannelTypes[1] = {6};
    int32_t hashChannelTypesCount = 1;
    int32_t hashChannels[1] = {0};
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannelTypesCount, hashChannels, hashChannelsCount);
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    PartitionedOutputOperator *partitionedOperator =
        (PartitionedOutputOperator *)partitionedOutputOperatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 3 row
    Decimal128 expectData0[3] = {11, 22, 33};
    VecTypes expectedTypes(std::vector<VecType>({ Decimal128VecType(2, 0) }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestDictionaryPartitionedOutput)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data

    int64_t data0[dataSize] = {66, 55, 44, 33, 22, 11};
    int64_t data1[dataSize] = {66, 55, 44, 33, 22, 11};
    void *datas[2] = {data0, data1};
    VecTypes buildTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vecBatch = new VectorBatch(2, dataSize);
    for (int32_t i = 0; i < 2; i++) {
        VecType vecType = buildTypes.Get()[i];
        vecBatch->SetVector(i, CreateDictionaryVector(vecType, dataSize, ids, dataSize, datas[i]));
    }

    bool isHashPrecomputed = false;
    VecTypes sourceTypes(std::vector<VecType>({ LongVecType() }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = {0};
    int32_t hashChannelTypes[1] = {6};
    int32_t hashChannelTypesCount = 1;
    int32_t hashChannels[1] = {0};
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannelTypesCount, hashChannels, hashChannelsCount);
    partitionedOutputOperatorFactory->SetJitContext(nullptr);
    PartitionedOutputOperator *partitionedOperator =
        (PartitionedOutputOperator *)partitionedOutputOperatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 6); // 6 row
    int64_t expectData[dataSize] = {66, 55, 44, 33, 22, 11};
    VecTypes expectedTypes(std::vector<VecType>({ LongVecType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 6, expectData);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}