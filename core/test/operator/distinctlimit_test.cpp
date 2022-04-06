/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include <vector>
#include "gtest/gtest.h"
#include "vector/vector_helper.h"
#include "operator/limit/distinct_limit.h"
#include "../util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;
using namespace TestUtil;

namespace DistinctLimitTest {
// supported data types cover
TEST(NativeOmniDistinctLimitOperator, TestDistinctLimitBasic)
{
    // construct data
    const int32_t dataSize = 4;
    const int32_t limitSize = dataSize;
    const int32_t resultDataSize = dataSize - 1;

    // table1
    int32_t data1[dataSize] = {0, 1, 2, 0};
    double data2[dataSize] = {0.1, 1.1, 2.1, 0.1};
    std::string data3[dataSize] = {"abc", "hello", "world", "abc"};
    int64_t data4[dataSize] = {10L, 100L, 1000L, 10L};
    Decimal128 data5[dataSize] = {111111, 222222, 333333, 111111};
    int32_t data6[dataSize] = {0, 1, 2, 0};
    int64_t data7[dataSize] = {10L, 100L, 1000L, 10L};
    bool data8[dataSize] = {true, false, false, true};
    std::string data9[dataSize] = {"123", "456", "789", "123"};

    std::vector<DataType> types = { IntDataType::Instance(),       DoubleDataType::Instance(),
        VarcharDataType(10),           LongDataType::Instance(),
        Decimal128DataType(10, 2),     Date32DataType::Instance(),
        Decimal64DataType::Instance(), BooleanDataType::Instance(),
        CharDataType::Instance() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 =
        CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4, data5, data6, data7, data8, data9);

    int32_t distinctCols[] = {0, 1, 2, 3, 4, 5, 6, 7, 8};

    DistinctLimitOperatorFactory *operatorFactory = DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(
        sourceTypes, distinctCols, sizeof(distinctCols) / sizeof(distinctCols[0]), -1, limitSize);
    operatorFactory->SetJitContext(nullptr);

    DistinctLimitOperator *distinctLimitOperator =
        dynamic_cast<DistinctLimitOperator *>(CreateTestOperator(operatorFactory));

    distinctLimitOperator->AddInput(vecBatch1);

    std::vector<VectorBatch *> outputVecBatches;
    distinctLimitOperator->GetOutput(outputVecBatches);

    int32_t expData1[resultDataSize] = {0, 1, 2};
    double expData2[resultDataSize] = {0.1, 1.1, 2.1};
    std::string expData3[resultDataSize] = {"abc", "hello", "world"};
    int64_t expData4[resultDataSize] = {10L, 100L, 1000L};
    Decimal128 expData5[resultDataSize] = {111111, 222222, 333333};
    int32_t expData6[resultDataSize] = {0, 1, 2};
    int64_t expData7[resultDataSize] = {10L, 100L, 1000L};
    bool expData8[resultDataSize] = {true, false, false};
    std::string expData9[resultDataSize] = {"123", "456", "789"};

    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, resultDataSize, expData1, expData2, expData3, expData4,
        expData5, expData6, expData7, expData8, expData9);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expVecBatch1));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatches(outputVecBatches);
    Operator::DeleteOperator(distinctLimitOperator);
    DeleteOperatorFactory(operatorFactory);
}

static void TestDistinctLimitTypeCheckAction(const DataTypes &sourceTypes, int32_t typeId, bool support)
{
    const int32_t limitSize = 1;
    DistinctLimitOperatorFactory *operatorFactory = nullptr;
    Operator *distinctLimitOperator = nullptr;
    int32_t distinctCols[] = {0};

    distinctCols[0] = typeId; // requires: typeId == colId in sourceTypes vector
    operatorFactory = DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(sourceTypes, distinctCols,
        sizeof(distinctCols) / sizeof(distinctCols[0]), -1, limitSize);
    operatorFactory->SetJitContext(nullptr);
    distinctLimitOperator = CreateTestOperator(operatorFactory);
    EXPECT_TRUE((distinctLimitOperator != nullptr) == support);

    if (distinctLimitOperator != nullptr) {
        Operator::DeleteOperator(distinctLimitOperator);
    }
    DeleteOperatorFactory(operatorFactory);
}

// data type check
TEST(NativeOmniDistinctLimitOperator, TestDistinctLimitTypeCheck)
{
    // requires: typeInstance index in types vector equals to dataType value defined in enum DataTypeId
    std::vector<DataType> types = { DataType(), // OMNI_NONE
        IntDataType::Instance(),
        LongDataType::Instance(),
        DoubleDataType::Instance(),
        BooleanDataType::Instance(),
        ShortDataType::Instance(),
        Decimal64DataType::Instance(),
        Decimal128DataType(20, 2),
        Date32DataType::Instance(),
        Date64DataType::Instance(),
        Time32DataType::Instance(),
        Time64DataType::Instance(),
        DataType(), // OMNI_TIMESTAMP
        DataType(), // OMNI_INTERVAL_MONTHS
        DataType(), // OMNI_INTERVAL_DAY_TIME
        VarcharDataType(10),
        CharDataType::Instance(),
        ContainerDataType::Instance() };
    DataTypes sourceTypes(types);

    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_NONE, false);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_INT, true);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_LONG, true);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_DOUBLE, true);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_BOOLEAN, true);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_SHORT, false);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_DECIMAL64, true);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_DECIMAL128, true);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_DATE32, true);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_DATE64, false);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_TIME32, false);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_TIME64, false);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_TIMESTAMP, false);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_INTERVAL_MONTHS, false);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_INTERVAL_DAY_TIME, false);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_VARCHAR, true);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_CHAR, true);
    TestDistinctLimitTypeCheckAction(sourceTypes, OMNI_CONTAINER, false);
}

// test with null
TEST(NativeOmniDistinctLimitOperator, TestDistinctLimitWithNull)
{
    // construct data
    const int32_t dataSize = 6;
    const int32_t limitSize = dataSize;
    const int32_t resultDataSize = dataSize - 1;
    const int32_t nullColIndex0 = 0;
    const int32_t nullColIndex1 = 1;
    const int32_t nullRowIndex = 1;

    // table1
    int32_t data1[dataSize] = {0, 1, 2, 0, 1, 2};
    double data2[dataSize] = {6.6, 5.5, 4.4, 6.6, 2.2, 1.1};

    std::vector<DataType> types = { IntDataType::Instance(), DoubleDataType::Instance() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    Vector **vectors1 = vecBatch1->GetVectors();

    // set data to NULL
    vectors1[nullColIndex0]->SetValueNull(nullRowIndex);
    vectors1[nullColIndex1]->SetValueNull(nullRowIndex);

    int32_t distinctCols[] = {0, 1};

    DistinctLimitOperatorFactory *operatorFactory = DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(
        sourceTypes, distinctCols, sizeof(distinctCols) / sizeof(distinctCols[0]), -1, limitSize);
    operatorFactory->SetJitContext(nullptr);

    DistinctLimitOperator *distinctLimitOperator =
        dynamic_cast<DistinctLimitOperator *>(CreateTestOperator(operatorFactory));

    distinctLimitOperator->AddInput(vecBatch1);

    std::vector<VectorBatch *> outputVecBatches;
    distinctLimitOperator->GetOutput(outputVecBatches);

    // expData1[nullRowIndex] simulates as null value
    int32_t expData1[resultDataSize] = {0, 1000, 2, 1, 2};
    // expData2[nullRowIndex] simulates as null value
    double expData2[resultDataSize] = {6.6, 555.555, 4.4, 2.2, 1.1};
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, resultDataSize, expData1, expData2);

    // set data to NULL
    Vector **vectors2 = expVecBatch1->GetVectors();
    vectors2[nullColIndex0]->SetValueNull(nullRowIndex);
    vectors2[nullColIndex1]->SetValueNull(nullRowIndex);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expVecBatch1));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatches(outputVecBatches);
    Operator::DeleteOperator(distinctLimitOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniDistinctLimitOperator, TestDistinctLimitWithRepeat)
{
    // construct data
    const int32_t dataSize = 6;
    const int32_t limitSize = dataSize;
    const int32_t resultDataSize = dataSize;

    // table1(same data set with different sequence)
    int32_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int32_t data2[dataSize] = {1, 2, 0, 4, 5, 3};
    int32_t data3[dataSize] = {2, 0, 1, 5, 3, 4};

    std::vector<DataType> types = { IntDataType::Instance(), IntDataType::Instance(), IntDataType::Instance() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    int32_t distinctCols[] = {0, 1, 2};

    DistinctLimitOperatorFactory *operatorFactory = DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(
        sourceTypes, distinctCols, sizeof(distinctCols) / sizeof(distinctCols[0]), -1, limitSize);
    operatorFactory->SetJitContext(nullptr);

    DistinctLimitOperator *distinctLimitOperator =
        dynamic_cast<DistinctLimitOperator *>(CreateTestOperator(operatorFactory));

    distinctLimitOperator->AddInput(vecBatch1);

    std::vector<VectorBatch *> outputVecBatches;
    distinctLimitOperator->GetOutput(outputVecBatches);

    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, resultDataSize, data1, data2, data3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expVecBatch1));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatches(outputVecBatches);
    Operator::DeleteOperator(distinctLimitOperator);
    DeleteOperatorFactory(operatorFactory);
}

// core data types cover
TEST(NativeOmniDistinctLimitOperator, TestDistinctLimitTypesCover)
{
    // construct data
    const int32_t dataSize = 6;
    const int32_t limitSize = dataSize;
    const int32_t resultDataSize = dataSize;

    // table1()
    int32_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4, 5};
    double data3[dataSize] = {1.0, 2.1, 3.2, 4.3, 5.4, 6.5};
    std::string data4[dataSize] = {"1", "bc", "def", "000", "ABC", "123def"};
    Decimal128 data5[dataSize] = {Decimal128(1, 2), Decimal128(3, 4),
                                  Decimal128(-1, -2), Decimal128(-1, 2),
                                  Decimal128(0, -2)};

    std::vector<DataType> types = { IntDataType::Instance(), LongDataType::Instance(), DoubleDataType::Instance(),
        VarcharDataType(10), Decimal128DataType::Instance() };
    DataTypes sourceTypes(types);

    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4, data5);

    int32_t distinctCols[] = {0, 2, 3, 4};

    DistinctLimitOperatorFactory *operatorFactory = DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(
        sourceTypes, distinctCols, sizeof(distinctCols) / sizeof(distinctCols[0]), -1, limitSize);
    operatorFactory->SetJitContext(nullptr);

    DistinctLimitOperator *distinctLimitOperator =
        dynamic_cast<DistinctLimitOperator *>(CreateTestOperator(operatorFactory));
    distinctLimitOperator->AddInput(vecBatch1);
    std::vector<VectorBatch *> outputVecBatches;
    distinctLimitOperator->GetOutput(outputVecBatches);

    std::vector<DataType> outTypes = { IntDataType::Instance(), DoubleDataType::Instance(), VarcharDataType(10),
        Decimal128DataType::Instance() };
    DataTypes expectedTypes(outTypes);
    VectorBatch *expVecBatch1 = CreateVectorBatch(expectedTypes, resultDataSize, data1, data3, data4, data5);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expVecBatch1));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatches(outputVecBatches);
    Operator::DeleteOperator(distinctLimitOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(NativeOmniDistinctLimitOperator, TestDistinctLimitVarchar)
{
    // construct data
    const int32_t dataSize = 6;
    const int32_t limitSize = dataSize;
    const int32_t resultDataSize = dataSize - 1;

    // table1
    int32_t data1[dataSize] = {0, 0, 0, 0, 0, 0};
    std::string data2[dataSize] = {"abc", "abc", "Abc", "ab", "abcd",
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrs"
        "tuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKL"
        "MNOPQRSTUVWXYZ1234567890"};

    std::vector<DataType> types = { IntDataType::Instance(), VarcharDataType(256) };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int32_t distinctCols[] = {0, 1};

    DistinctLimitOperatorFactory *operatorFactory = DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(
        sourceTypes, distinctCols, sizeof(distinctCols) / sizeof(distinctCols[0]), -1, limitSize);
    operatorFactory->SetJitContext(nullptr);

    DistinctLimitOperator *distinctLimitOperator =
        dynamic_cast<DistinctLimitOperator *>(CreateTestOperator(operatorFactory));
    distinctLimitOperator->AddInput(vecBatch1);
    std::vector<VectorBatch *> outputVecBatches;
    distinctLimitOperator->GetOutput(outputVecBatches);

    int32_t expData1[resultDataSize] = {0, 0, 0, 0, 0};
    std::string expData2[resultDataSize] = {"abc", "Abc", "ab", "abcd",
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrs"
        "tuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKL"
        "MNOPQRSTUVWXYZ1234567890"};
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, resultDataSize, expData1, expData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expVecBatch1));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatches(outputVecBatches);
    Operator::DeleteOperator(distinctLimitOperator);
    DeleteOperatorFactory(operatorFactory);
}

// test with hash col
TEST(NativeOmniDistinctLimitOperator, TestDistinctLimitHashCol)
{
    // construct data
    const int32_t dataSize = 5;
    const int32_t limitSize = dataSize;
    const int32_t resultDataSize = dataSize - 1;
    const int32_t hashChannelIndex = 2;

    // table1
    int32_t data1[dataSize] = {0, 1, 2, 0, 1};
    double data2[dataSize] = {6.6, 5.5, 4.4, 6.6, 2.2};
    /*
     * data3 will be used directly as hash value of the corresponding row
     * for data3[3]: hash value conflict and data1[3] and data2[3] both has same record, treat as repeat record
     * for data4[4]: hash value conflict but data2[4] is not same as before record, tread as a new record
     */
    int64_t data3[dataSize] = {100000, 110000, 120000, 100000, 110000};

    std::vector<DataType> types = { IntDataType::Instance(), DoubleDataType::Instance(), LongDataType::Instance() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    int32_t distinctCols[] = {0, 1};

    DistinctLimitOperatorFactory *operatorFactory = DistinctLimitOperatorFactory::CreateDistinctLimitOperatorFactory(
        sourceTypes, distinctCols, sizeof(distinctCols) / sizeof(distinctCols[0]), hashChannelIndex, limitSize);
    operatorFactory->SetJitContext(nullptr);

    DistinctLimitOperator *distinctLimitOperator =
        dynamic_cast<DistinctLimitOperator *>(CreateTestOperator(operatorFactory));
    distinctLimitOperator->AddInput(vecBatch1);
    std::vector<VectorBatch *> outputVecBatches;
    distinctLimitOperator->GetOutput(outputVecBatches);

    int32_t expData1[resultDataSize] = {0, 1, 2, 1};
    double expData2[resultDataSize] = {6.6, 5.5, 4.4, 2.2};
    int64_t expData3[dataSize] = {100000, 110000, 120000, 110000};
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, resultDataSize, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expVecBatch1));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatches(outputVecBatches);
    Operator::DeleteOperator(distinctLimitOperator);
    DeleteOperatorFactory(operatorFactory);
}
}