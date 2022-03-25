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
    DataTypes buildTypes(std::vector<DataType>({ IntDataType(), IntDataType(), IntDataType() }));
    int buildData1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData1, buildData1);
    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), IntDataType() }));

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
    DataTypes expectedTypes(std::vector<DataType>({ IntDataType(), IntDataType() }));
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
    DataTypes buildTypes(std::vector<DataType>({ IntDataType(), IntDataType(), IntDataType() }));
    int buildData1[DATA_SIZE] = {0, 1, 2, 3, 4, 5, 6};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData1, buildData1);
    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), IntDataType() }));

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
    DataTypes expectedTypes(std::vector<DataType>({ IntDataType(), IntDataType() }));
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
    DataTypes buildTypes(std::vector<DataType>({ IntDataType(), IntDataType(), IntDataType() }));
    int buildData1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData1, buildData1);
    DataTypes sourceTypes(std::vector<DataType>({ IntDataType() }));

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
    DataTypes expectedTypes(std::vector<DataType>({ IntDataType() }));
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
    DataTypes buildTypes(std::vector<DataType>({ VarcharDataType(3), VarcharDataType(3) }));
    std::string buildData1[DATA_SIZE] = {"abc", "de", "f"};
    std::string buildData2[DATA_SIZE] = {"def", "bc", "a"};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataType>({ VarcharDataType(3) }));
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
    DataTypes expectedTypes(std::vector<DataType>({ VarcharDataType(3) }));
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
    DataTypes buildTypes(std::vector<DataType>({ CharDataType(3), CharDataType(3) }));
    std::string buildData1[DATA_SIZE] = {"abc", "de", "f"};
    std::string buildData2[DATA_SIZE] = {"def", "bc", "a"};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataType>({ CharDataType(3) }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = {0};
    int32_t hashChannelTypes[1] = {CharDataType::Instance().GetId()};
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
    DataTypes expectedTypes(std::vector<DataType>({ CharDataType(3) }));
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
    DataTypes buildTypes(std::vector<DataType>({ VarcharDataType(3), VarcharDataType(3) }));
    std::string buildData1[DATA_SIZE] = {"abc", "de", "f"};
    std::string buildData2[DATA_SIZE] = {"def", "bc", "a"};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);
    vecBatch->GetVector(0)->SetValueNull(0);

    bool isHashPrecomputed = false;
    DataTypes sourceTypes(std::vector<DataType>({ VarcharDataType(3) }));
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
    DataTypes expectedTypes(std::vector<DataType>({ VarcharDataType(3) }));
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
    DataTypes buildTypes(std::vector<DataType>({ Decimal64DataType(2, 0), Decimal64DataType(2, 0) }));
    int64_t buildData1[DATA_SIZE] = {11, 22, 33};
    int64_t buildData2[DATA_SIZE] = {33, 22, 111};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataType>({ Decimal64DataType(2, 0) }));
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
    DataTypes expectedTypes(std::vector<DataType>({ Decimal64DataType(2, 0) }));
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
    DataTypes buildTypes(std::vector<DataType>({ DoubleDataType(), DoubleDataType() }));
    int64_t buildData1[DATA_SIZE] = {11, 22, 33};
    int64_t buildData2[DATA_SIZE] = {33, 22, 111};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataType>({ DoubleDataType() }));
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
    DataTypes expectedTypes(std::vector<DataType>({ DoubleDataType() }));
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
    DataTypes buildTypes(std::vector<DataType>({ BooleanDataType(), BooleanDataType() }));
    int64_t buildData1[DATA_SIZE] = {0, 1, 0};
    int64_t buildData2[DATA_SIZE] = {0, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataType>({ BooleanDataType() }));
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
    DataTypes expectedTypes(std::vector<DataType>({ BooleanDataType() }));
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
    DataTypes buildTypes(std::vector<DataType>({ Decimal128DataType(2, 0), Decimal128DataType(2, 0) }));
    Decimal128 buildData1[DATA_SIZE] = {11, 22, 33};
    Decimal128 buildData2[DATA_SIZE] = {0, 1, 2};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataType>({ Decimal128DataType(2, 0) }));
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
    DataTypes expectedTypes(std::vector<DataType>({ Decimal128DataType(2, 0) }));
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
    DataTypes buildTypes(std::vector<DataType>({ LongDataType(), LongDataType() }));
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vecBatch = new VectorBatch(2, dataSize);
    for (int32_t i = 0; i < 2; i++) {
        DataType dataType = buildTypes.Get()[i];
        vecBatch->SetVector(i, CreateDictionaryVector(dataType, dataSize, ids, dataSize, datas[i]));
    }

    bool isHashPrecomputed = false;
    DataTypes sourceTypes(std::vector<DataType>({ LongDataType() }));
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
    DataTypes expectedTypes(std::vector<DataType>({ LongDataType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 6, expectData);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}