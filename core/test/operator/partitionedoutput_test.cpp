/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#include <vector>
#include <chrono>
#include "gtest/gtest.h"
#include "operator/partitionedoutput/partitionedoutput.h"
#include "../util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;
using namespace TestUtil;

namespace PartitionedOutputTest {
TEST(PartitionedOutputOperatorTest, TestOnePartitionedOutput)
{
    const int32_t dataSize = 6;
    std::vector<DataTypePtr> buildFieldTypes { IntType(), IntType(), IntType() };
    DataTypes buildTypes(buildFieldTypes);
    int buildData1[dataSize] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, dataSize, buildData1, buildData1, buildData1);
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), IntType() }));

    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = {0};
    std::vector<DataTypePtr> hashChannelFieldTypes { NoneType() };
    DataTypes hashChannelTypes(hashChannelFieldTypes);
    int32_t hashChannels[1] = {0};
    int32_t hashChannelsCount = 1;

    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, true, hashChannelTypes, hashChannels,
        hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        static_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 6); // 6 row

    int32_t expectData0[dataSize] = {0, 1, 2, 3, 4, 5};
    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4, 5};
    DataTypes expectedTypes(std::vector<DataTypePtr>({ IntType(), IntType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData0, expectData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestMultiPartitionedOutput)
{
    const int32_t dataSize = 7;
    DataTypes buildTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    int buildData1[dataSize] = { 0, 1, 2, 3, 4, 5, 6 };
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, dataSize, buildData1, buildData1, buildData1);
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), IntType() }));

    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = { 0 };
    int32_t partitionCount = 1;
    int32_t bucketToPartition[2] = { 0, 1 };
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { NoneType() });
    int32_t hashChannels[1] = { 0 };
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 2, true, hashChannelTypes, hashChannels,
        hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        dynamic_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);
    EXPECT_EQ(outputVecBatch.size(), 2);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 4); // 4 row
    EXPECT_EQ(outputVecBatch[1]->GetRowCount(), 3); // 3 row

    int32_t expectData0[dataSize] = { 0, 2, 4, 6 };
    int32_t expectData1[dataSize] = { 1, 3, 5 };
    DataTypes expectedTypes(std::vector<DataTypePtr>({ IntType(), IntType() }));
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
    const int32_t dataSize = 6;
    DataTypes buildTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    int buildData1[dataSize] = { 0, 1, 2, 3, 4, 5 };
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, dataSize, buildData1, buildData1, buildData1);
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType() }));

    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[2] = { 0, 1 };
    int32_t partitionCount = 2;
    int32_t bucketToPartition[1] = { 0 };
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { IntType(), IntType() });
    int32_t hashChannels[2] = { 0, 1 };
    int32_t hashChannelsCount = 2;
    bool isHashPrecomputed = false;

    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 2, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannels, hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        dynamic_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 6); // 6 row
    int32_t expectData0[dataSize] = { 0, 1, 2, 3, 4, 5 };
    DataTypes expectedTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestHashVarcharPartitionedOutput)
{
    const int32_t dataSize = 3;
    DataTypes buildTypes(std::vector<DataTypePtr>({ VarcharType(3), VarcharType(3) }));
    std::string buildData1[dataSize] = { "abc", "de", "f" };
    std::string buildData2[dataSize] = { "def", "bc", "a" };
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, dataSize, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(3) }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = { 0 };
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = { 0 };
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { VarcharType() });
    int32_t hashChannels[1] = { 0 };
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannels, hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        dynamic_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 4 row
    string expectData0[3] = { "abc", "de", "f" };
    DataTypes expectedTypes(std::vector<DataTypePtr>({ VarcharType(3) }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestHashCharPartitionedOutput)
{
    const int32_t dataSize = 3;
    DataTypes buildTypes(std::vector<DataTypePtr>({ CharType(3), CharType(3) }));
    std::string buildData1[dataSize] = { "abc", "de", "f" };
    std::string buildData2[dataSize] = { "def", "bc", "a" };
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, dataSize, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataTypePtr>({ CharType(3) }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = { 0 };
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = { 0 };
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { CharType() });
    int32_t hashChannels[1] = { 0 };
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannels, hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        dynamic_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 4 row
    string expectData0[3] = { "abc", "de", "f" };
    DataTypes expectedTypes(std::vector<DataTypePtr>({ CharType(3) }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestNullPartitionedOutput)
{
    const int32_t dataSize = 3;
    DataTypes buildTypes(std::vector<DataTypePtr>({ VarcharType(3), VarcharType(3) }));
    std::vector<std::string> buildData1 = { "", "de", "f" };
    std::vector<bool> nulls1 = { true, false, false };
    std::vector<string> buildData2 = { "def", "bc", "a" };
    std::vector<bool> nulls2 = { false, false, false };
    VarcharVector *col0 = CreateVarcharVector(buildData1, nulls1);
    VarcharVector *col1 = CreateVarcharVector(buildData2, nulls2);
    std::vector<Vector *> cols = { col0, col1 };
    VectorBatch *vecBatch = CreateVectorBatch(dataSize, cols);

    bool isHashPrecomputed = false;
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(3) }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = { 0 };
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = { 0 };
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { VarcharType() });
    int32_t hashChannels[1] = { 0 };
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannels, hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        dynamic_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 3 row
    std::vector<std::string> expectData0 = { "", "de", "f" };
    std::vector<bool> nulls = { true, false, false };
    DataTypes expectedTypes(std::vector<DataTypePtr>({ VarcharType(3) }));
    std::vector<Vector *> expectCols = { CreateVarcharVector(expectData0, nulls) };
    VectorBatch *expectVecBatch = CreateVectorBatch(3, expectCols);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestDecimalPartitionedOutput)
{
    const int32_t dataSize = 3;
    DataTypes buildTypes(std::vector<DataTypePtr>({ Decimal64Type(2, 0), Decimal64Type(2, 0) }));
    int64_t buildData1[dataSize] = { 11, 22, 33 };
    int64_t buildData2[dataSize] = { 33, 22, 111 };
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, dataSize, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataTypePtr>({ Decimal64Type(2, 0) }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = { 0 };
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = { 0 };
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { Decimal64Type() });
    int32_t hashChannels[1] = { 0 };
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannels, hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        dynamic_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 3 row
    int64_t expectData0[3] = { 11, 22, 33 };
    DataTypes expectedTypes(std::vector<DataTypePtr>({ Decimal64Type(2, 0) }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestDoublePartitionedOutput)
{
    const int32_t dataSize = 3;
    DataTypes buildTypes(std::vector<DataTypePtr>({ DoubleType(), DoubleType() }));
    int64_t buildData1[dataSize] = { 11, 22, 33 };
    int64_t buildData2[dataSize] = { 33, 22, 111 };
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, dataSize, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataTypePtr> { DoubleType() });
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = { 0 };
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = { 0 };
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { Decimal64Type() });
    int32_t hashChannels[1] = { 0 };
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannels, hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        dynamic_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 3 row
    int64_t expectData0[3] = { 11, 22, 33 };
    DataTypes expectedTypes(std::vector<DataTypePtr>({ DoubleType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestShortPartitionedOutput)
{
    const int32_t dataSize = 3;
    DataTypes buildTypes(std::vector<DataTypePtr>({ ShortType(), ShortType() }));
    int16_t buildData1[dataSize] = { 11, 22, 33 };
    int16_t buildData2[dataSize] = { 33, 22, 111 };
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, dataSize, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataTypePtr> { ShortType() });
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = { 0 };
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = { 0 };
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { Decimal64Type() });
    int32_t hashChannels[1] = { 0 };
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannels, hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        dynamic_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 3 row
    int16_t expectData0[3] = { 11, 22, 33 };
    DataTypes expectedTypes(std::vector<DataTypePtr>({ ShortType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestBoolPartitionedOutput)
{
    const int32_t dataSize = 3;
    DataTypes buildTypes(std::vector<DataTypePtr>({ BooleanType(), BooleanType() }));
    int64_t buildData1[dataSize] = { 0, 1, 0 };
    int64_t buildData2[dataSize] = { 0, 1, 0 };
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, dataSize, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataTypePtr> { BooleanType() });
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = { 0 };
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = { 0 };
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { Decimal64Type() });
    int32_t hashChannels[1] = { 0 };
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannels, hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        dynamic_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 3 row
    int64_t expectData0[3] = { 0, 1, 0 };
    DataTypes expectedTypes(std::vector<DataTypePtr>({ BooleanType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 3, expectData0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestDecimal128PartitionedOutput)
{
    const int32_t dataSize = 3;
    DataTypes buildTypes(std::vector<DataTypePtr>({ Decimal128Type(2, 0), Decimal128Type(2, 0) }));
    Decimal128 buildData1[dataSize] = { 11, 22, 33 };
    Decimal128 buildData2[dataSize] = { 0, 1, 2 };
    VectorBatch *vecBatch = CreateVectorBatch(buildTypes, dataSize, buildData1, buildData2);

    bool isHashPrecomputed = false;

    DataTypes sourceTypes(std::vector<DataTypePtr>({ Decimal128Type(2, 0) }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = { 0 };
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = { 0 };
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { Decimal64Type() });
    int32_t hashChannels[1] = { 0 };
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannels, hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        dynamic_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 3); // 3 row
    Decimal128 expectData0[3] = { 11, 22, 33 };
    DataTypes expectedTypes(std::vector<DataTypePtr>({ Decimal128Type(2, 0) }));
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

    int64_t data0[dataSize] = { 66, 55, 44, 33, 22, 11 };
    int64_t data1[dataSize] = { 66, 55, 44, 33, 22, 11 };
    void *datas[2] = { data0, data1 };
    DataTypes buildTypes(std::vector<DataTypePtr>({ LongType(), LongType() }));
    int32_t ids[] = { 0, 1, 2, 3, 4, 5 };
    VectorBatch *vecBatch = new VectorBatch(2, dataSize);
    for (int32_t i = 0; i < 2; i++) {
        DataTypePtr dataType = buildTypes.GetType(i);
        vecBatch->SetVector(i, CreateDictionaryVector(*dataType, dataSize, ids, dataSize, datas[i]));
    }

    bool isHashPrecomputed = false;
    DataTypes sourceTypes(std::vector<DataTypePtr> { LongType() });
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = { 0 };
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = { 0 };
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { Decimal64Type() });
    int32_t hashChannels[1] = { 0 };
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannels, hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        dynamic_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vecBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 6); // 6 row
    int64_t expectData[dataSize] = { 66, 55, 44, 33, 22, 11 };
    DataTypes expectedTypes(std::vector<DataTypePtr>({ LongType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 6, expectData);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}

TEST(PartitionedOutputOperatorTest, TestContainerPartitionedOutput)
{
    const int32_t dataSize = 6;
    int64_t data0[dataSize] = { 66, 55, 44, 33, 22, 11 };
    int64_t data1[dataSize] = { 66, 55, 44, 33, 22, 11 };

    DataTypes dataTypes({ ContainerType({ LongType(), LongType() }), ContainerType({ LongType(), LongType() }) });
    VectorBatch *vectorBatch = CreateVectorBatch(dataTypes, dataSize, data0, data1, data0, data1);

    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ ContainerType(std::vector<DataTypePtr> { LongType(), LongType() }) }));
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = { 0 };
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = { 0 };
    bool isHashPrecomputed = false;
    DataTypes hashChannelTypes(std::vector<DataTypePtr> { Decimal64Type() });
    int32_t hashChannels[1] = { 0 };
    int32_t hashChannelsCount = 1;
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
        PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(sourceTypes, 3, replicatesAnyRow,
        nullChannel, partitionChannels, 1, partitionCount, bucketToPartition, 1, isHashPrecomputed, hashChannelTypes,
        hashChannels, hashChannelsCount);
    PartitionedOutputOperator *partitionedOperator =
        dynamic_cast<PartitionedOutputOperator *>(partitionedOutputOperatorFactory->CreateOperator());
    partitionedOperator->AddInput(vectorBatch);
    std::vector<omniruntime::vec::VectorBatch *> outputVecBatch;
    partitionedOperator->GetOutput(outputVecBatch);

    EXPECT_EQ(outputVecBatch.size(), 1);
    EXPECT_EQ(outputVecBatch[0]->GetRowCount(), 6);
    int64_t expectData0[dataSize] = { 66, 55, 44, 33, 22, 11 };
    int64_t expectData1[dataSize] = { 66, 55, 44, 33, 22, 11 };
    DataTypes expectedTypes(
        std::vector<DataTypePtr>({ ContainerType(std::vector<DataTypePtr> { LongType(), LongType() }) }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 6, expectData0, expectData1);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(partitionedOperator);
    DeleteOperatorFactory(partitionedOutputOperatorFactory);
}
}