/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#include <vector>
#include "gtest/gtest.h"
#include "type/data_type.h"
#include "vector/vector.h"
#include "vector/map_vector.h"
#include "vector/array_vector.h"
#include "vector/row_vector.h"
#include "vector/vector_helper.h"
#include "operator/limit/limit.h"
#include "util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace std;
using namespace omniruntime::TestUtil;

namespace LimitTest {
// supported data types cover
TEST(NativeOmniLimitOperator, TestLimitBasic)
{
    // construct data
    const int32_t dataSize = 4;
    const int32_t limitCount = dataSize - 1;
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
    std::string data9[dataSize] = {"123", "456", "789", "012"};
    int16_t data10[dataSize] = {0, 1, 2, 3};

    std::vector<DataTypePtr> types = { IntType(),
        DoubleType(),
        VarcharType(10),
        LongType(),
        Decimal128Type(10, 2),
        Date32Type(),
        Decimal64DataType::Instance(),
        BooleanType(),
        CharDataType::Instance(),
        ShortType() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 =
        CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4, data5, data6, data7, data8, data9, data10);

    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount, 0);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    limitOperator->AddInput(vecBatch1);
    VectorBatch *outputVecBatch;
    limitOperator->GetOutput(&outputVecBatch);

    int32_t expData1[resultDataSize] = {0, 1, 2};
    double expData2[resultDataSize] = {0.1, 1.1, 2.1};
    std::string expData3[resultDataSize] = {"abc", "hello", "world"};
    int64_t expData4[resultDataSize] = {10L, 100L, 1000L};
    Decimal128 expData5[resultDataSize] = {111111, 222222, 333333};
    int32_t expData6[resultDataSize] = {0, 1, 2};
    int64_t expData7[resultDataSize] = {10L, 100L, 1000L};
    bool expData8[resultDataSize] = {true, false, false};
    std::string expData9[resultDataSize] = {"123", "456", "789"};
    int16_t expData10[resultDataSize] = {0, 1, 2};

    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, resultDataSize, expData1, expData2, expData3, expData4,
        expData5, expData6, expData7, expData8, expData9, expData10);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expVecBatch1));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    delete operatorFactory;
}

// call input several times
TEST(NativeOmniLimitOperator, TestLimitMultiInput)
{
    // construct data
    const int32_t dataSize = 3;
    const int32_t limitCount = 40;

    // input vecBatch1
    int32_t data01[dataSize] = {0, 1, 2};
    double data02[dataSize] = {6.6, 5.5, 4.4};

    std::vector<DataTypePtr> types = { IntType(), DoubleType() };
    DataTypes sourceTypes(types);

    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data01, data02);
    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount, 0);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    VectorBatch *outputVecBatch1;
    limitOperator->AddInput(vecBatch1);
    limitOperator->GetOutput(&outputVecBatch1);

    // input vecBatch2
    int32_t data11[dataSize] = {3, 4, 5};
    double data12[dataSize] = {3.3, 2.2, 1.1};

    VectorBatch *vecBatch2 = CreateVectorBatch(sourceTypes, dataSize, data11, data12);
    limitOperator->AddInput(vecBatch2);
    VectorBatch *outputVecBatch2;
    limitOperator->GetOutput(&outputVecBatch2);

    int32_t expData01[dataSize] = {0, 1, 2};
    double expData02[dataSize] = {6.6, 5.5, 4.4};
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, (dataSize), expData01, expData02);

    int32_t expData11[dataSize] = {3, 4, 5};
    double expData12[dataSize] = {3.3, 2.2, 1.1};
    VectorBatch *expVecBatch2 = CreateVectorBatch(sourceTypes, (dataSize), expData11, expData12);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch1, expVecBatch1));
    EXPECT_TRUE(VecBatchMatch(outputVecBatch2, expVecBatch2));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatch(expVecBatch2);
    VectorHelper::FreeVecBatch(outputVecBatch1);
    VectorHelper::FreeVecBatch(outputVecBatch2);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    delete operatorFactory;
}

// test null
TEST(NativeOmniLimitOperator, TestLimitWithNull)
{
    // construct data
    const int32_t dataSize = 6;
    const int32_t limitCount = 5;
    // table1
    int32_t data1[dataSize] = {0, 1, 2, 0, 1, 2};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    std::vector<DataTypePtr> types = { IntType(), DoubleType() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount, 0);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));

    // set null to vector batch to simulate null value
    BaseVector *colVector = vecBatch1->Get(0);
    colVector->SetNull(2);
    colVector->SetNull(3);
    BaseVector *colVector1 = vecBatch1->Get(1);
    colVector1->SetNull(3);
    colVector1->SetNull(4);

    limitOperator->AddInput(vecBatch1);
    VectorBatch *outputVecBatch;
    limitOperator->GetOutput(&outputVecBatch);

    int32_t expData1[dataSize] = {0, 1, 2, 0, 1};          // expData1[2],expData1[3] simulate to null
    double expData2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2}; // expData2[3],expData2[4] simulate to null
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, limitCount, expData1, expData2);

    // set null to vector batch to simulate null value
    BaseVector *colVector00 = expVecBatch1->Get(0);
    colVector00->SetNull(2);
    colVector00->SetNull(3);
    BaseVector *colVector01 = expVecBatch1->Get(1);
    colVector01->SetNull(3);
    colVector01->SetNull(4);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expVecBatch1));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    delete operatorFactory;
}

TEST(NativeOmniLimitOperator, TestLimitOffsetBasic)
{
    // construct data
    const int32_t dataSize = 4;
    const int32_t limitCount = dataSize;
    const int32_t offset = 2;
    const int32_t resultDataSize = limitCount - offset;
    // table1
    int32_t data1[dataSize] = {0, 1, 2, 0};
    double data2[dataSize] = {0.1, 1.1, 2.1, 0.1};
    std::string data3[dataSize] = {"abc", "hello", "world", "abc"};
    int64_t data4[dataSize] = {10L, 100L, 1000L, 10L};
    Decimal128 data5[dataSize] = {111111, 222222, 333333, 111111};
    int32_t data6[dataSize] = {0, 1, 2, 0};
    int64_t data7[dataSize] = {10L, 100L, 1000L, 10L};
    bool data8[dataSize] = {true, false, false, true};
    std::string data9[dataSize] = {"123", "456", "789", "012"};
    int16_t data10[dataSize] = {0, 1, 2, 3};
    std::vector<DataTypePtr> types = { IntType(), DoubleType(),
     VarcharType(10),
     LongType(),
     Decimal128Type(10, 2),
     Date32Type(),
     Decimal64DataType::Instance(),
     BooleanType(),
     CharDataType::Instance(),
     ShortType() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 =
    CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4, data5, data6, data7, data8, data9, data10);
    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount, offset);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    limitOperator->AddInput(vecBatch1);
    VectorBatch *outputVecBatch;
    limitOperator->GetOutput(&outputVecBatch);
    int32_t expData1[resultDataSize] = {2, 0};
    double expData2[resultDataSize] = {2.1, 0.1};
    std::string expData3[resultDataSize] = {"world", "abc"};
    int64_t expData4[resultDataSize] = {1000L, 10L};
    Decimal128 expData5[resultDataSize] = {333333, 111111};
    int32_t expData6[resultDataSize] = {2, 0};
    int64_t expData7[resultDataSize] = {1000L, 10L};
    bool expData8[resultDataSize] = {false, true};
    std::string expData9[resultDataSize] = {"789", "012"};
    int16_t expData10[resultDataSize] = {2, 3};
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, resultDataSize, expData1, expData2, expData3, expData4,
                                                  expData5, expData6, expData7, expData8, expData9, expData10);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expVecBatch1));
    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    delete operatorFactory;
}

TEST(NativeOmniLimitOperator, TestLimitOffsetMultiInput)
{
    // construct data
    const int32_t dataSize = 3;
    const int32_t limitCount = 40;
    const int32_t offset = 1;
    const int32_t resultDataSize = dataSize - offset;

    // input vecBatch1
    int32_t data01[dataSize] = {0, 1, 2};
    double data02[dataSize] = {6.6, 5.5, 4.4};

    std::vector<DataTypePtr> types = { IntType(), DoubleType() };
    DataTypes sourceTypes(types);

    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data01, data02);
    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount, offset);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    VectorBatch *outputVecBatch1;
    limitOperator->AddInput(vecBatch1);
    limitOperator->GetOutput(&outputVecBatch1);

    // input vecBatch2
    int32_t data11[dataSize] = {3, 4, 5};
    double data12[dataSize] = {3.3, 2.2, 1.1};

    VectorBatch *vecBatch2 = CreateVectorBatch(sourceTypes, dataSize, data11, data12);
    limitOperator->AddInput(vecBatch2);
    VectorBatch *outputVecBatch2;
    limitOperator->GetOutput(&outputVecBatch2);

    int32_t expData01[resultDataSize] = {1, 2};
    double expData02[resultDataSize] = {5.5, 4.4};
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, (resultDataSize), expData01, expData02);

    int32_t expData11[dataSize] = {3, 4, 5};
    double expData12[dataSize] = {3.3, 2.2, 1.1};
    VectorBatch *expVecBatch2 = CreateVectorBatch(sourceTypes, (dataSize), expData11, expData12);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch1, expVecBatch1));
    EXPECT_TRUE(VecBatchMatch(outputVecBatch2, expVecBatch2));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatch(expVecBatch2);
    VectorHelper::FreeVecBatch(outputVecBatch1);
    VectorHelper::FreeVecBatch(outputVecBatch2);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    delete operatorFactory;
}

// Test limit with Byte type
TEST(NativeOmniLimitOperator, TestLimitWithByte)
{
    const int32_t dataSize = 4;
    const int32_t limitCount = 3;
    const int32_t resultDataSize = 3;

    int8_t data1[dataSize] = {10, 20, 30, 40};

    std::vector<DataTypePtr> types = { ByteType() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1);

    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount, 0);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    limitOperator->AddInput(vecBatch1);
    VectorBatch *outputVecBatch;
    limitOperator->GetOutput(&outputVecBatch);

    int8_t expData1[resultDataSize] = {10, 20, 30};
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, resultDataSize, expData1);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expVecBatch1));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    delete operatorFactory;
}

// Test limit with Float type
TEST(NativeOmniLimitOperator, TestLimitWithFloat)
{
    const int32_t dataSize = 4;
    const int32_t limitCount = 3;
    const int32_t resultDataSize = 3;

    float data1[dataSize] = {1.1f, 2.2f, 3.3f, 4.4f};

    std::vector<DataTypePtr> types = { FloatType() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1);

    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount, 0);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    limitOperator->AddInput(vecBatch1);
    VectorBatch *outputVecBatch;
    limitOperator->GetOutput(&outputVecBatch);

    float expData1[resultDataSize] = {1.1f, 2.2f, 3.3f};
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, resultDataSize, expData1);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expVecBatch1));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    delete operatorFactory;
}

// Test limit with Array type
TEST(NativeOmniLimitOperator, TestLimitWithArray)
{
    const int32_t dataSize = 4;
    const int32_t limitCount = 3;
    const int32_t elementSize = 8;  // Total elements across all arrays

    // Create element vector
    auto *elements = new Vector<int32_t>(elementSize);
    int32_t elementValues[elementSize] = {1, 2, 3, 4, 5, 6, 7, 8};
    for (int i = 0; i < elementSize; i++) {
        elements->SetValue(i, elementValues[i]);
    }

    // Create array vector with offsets
    auto *arrayVector = new ArrayVector(dataSize);
    arrayVector->SetOffset(0, 0);
    arrayVector->SetOffset(1, 2);
    arrayVector->SetOffset(2, 4);
    arrayVector->SetOffset(3, 6);
    arrayVector->SetOffset(4, 8);
    arrayVector->SetElementVectorFromRaw(elements);

    auto *vecBatch = new VectorBatch(dataSize);
    vecBatch->Append(arrayVector);

    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount, 0);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    limitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    limitOperator->GetOutput(&outputVecBatch);

    // Create expected array vector
    auto *expElements = new Vector<int32_t>(6);  // 3 arrays * 2 elements each
    int32_t expElementValues[6] = {1, 2, 3, 4, 5, 6};
    for (int i = 0; i < 6; i++) {
        expElements->SetValue(i, expElementValues[i]);
    }

    auto *expArrayVector = new ArrayVector(limitCount);
    expArrayVector->SetOffset(0, 0);
    expArrayVector->SetOffset(1, 2);
    expArrayVector->SetOffset(2, 4);
    expArrayVector->SetOffset(3, 6);
    expArrayVector->SetElementVectorFromRaw(expElements);

    auto *expVecBatch = new VectorBatch(limitCount);
    expVecBatch->Append(expArrayVector);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expVecBatch));

    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    delete operatorFactory;
}

// Test limit with Map type
TEST(NativeOmniLimitOperator, TestLimitWithMap)
{
    const int32_t dataSize = 4;
    const int32_t limitCount = 3;
    const int32_t totalKeys = 8;

    // Create key vector
    auto *keyVector = new Vector<int32_t>(totalKeys);
    int32_t keys[totalKeys] = {1, 2, 3, 4, 5, 6, 7, 8};
    for (int i = 0; i < totalKeys; i++) {
        keyVector->SetValue(i, keys[i]);
    }

    // Create value vector
    auto *valueVector = new Vector<LargeStringContainer<std::string_view>>(totalKeys);
    std::vector<std::string> values = {"a", "b", "c", "d", "e", "f", "g", "h"};
    for (int i = 0; i < totalKeys; i++) {
        std::string_view valueView(values[i]);
        valueVector->SetValue(i, valueView);
    }

    // Create map vector with offsets
    auto *mapVector = new omniruntime::vec::MapVector(dataSize, 
        std::shared_ptr<BaseVector>(keyVector), 
        std::shared_ptr<BaseVector>(valueVector));
    mapVector->SetOffset(0, 0);
    mapVector->SetOffset(1, 2);
    mapVector->SetOffset(2, 4);
    mapVector->SetOffset(3, 6);
    mapVector->SetOffset(4, 8);

    auto *vecBatch = new VectorBatch(dataSize);
    vecBatch->Append(mapVector);

    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount, 0);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    limitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    limitOperator->GetOutput(&outputVecBatch);

    // Create expected map vector
    auto *expKeyVector = new Vector<int32_t>(6);  // 3 maps * 2 entries each
    int32_t expKeys[6] = {1, 2, 3, 4, 5, 6};
    for (int i = 0; i < 6; i++) {
        expKeyVector->SetValue(i, expKeys[i]);
    }

    auto *expValueVector = new Vector<LargeStringContainer<std::string_view>>(6);
    std::vector<std::string> expValues = {"a", "b", "c", "d", "e", "f"};
    for (int i = 0; i < 6; i++) {
        std::string_view expValueView(expValues[i]);
        expValueVector->SetValue(i, expValueView);
    }

    auto *expMapVector = new omniruntime::vec::MapVector(limitCount,
        std::shared_ptr<BaseVector>(expKeyVector),
        std::shared_ptr<BaseVector>(expValueVector));
    expMapVector->SetOffset(0, 0);
    expMapVector->SetOffset(1, 2);
    expMapVector->SetOffset(2, 4);
    expMapVector->SetOffset(3, 6);

    auto *expVecBatch = new VectorBatch(limitCount);
    expVecBatch->Append(expMapVector);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expVecBatch));

    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    delete operatorFactory;
}

// Test limit with Struct (Row) type
TEST(NativeOmniLimitOperator, TestLimitWithStruct)
{
    const int32_t dataSize = 4;
    const int32_t limitCount = 3;
    const int32_t resultDataSize = 3;

    // Create field vectors for struct
    int32_t field1Data[dataSize] = {10, 20, 30, 40};
    double field2Data[dataSize] = {1.1, 2.2, 3.3, 4.4};
    std::string field3Data[dataSize] = {"a", "b", "c", "d"};

    auto field1Vector = std::shared_ptr<BaseVector>(CreateVector(dataSize, field1Data));
    auto field2Vector = std::shared_ptr<BaseVector>(CreateVector(dataSize, field2Data));
    auto field3Vector = std::shared_ptr<BaseVector>(CreateVarcharVector(field3Data, dataSize));

    // Create RowVector (StructVector)
    std::vector<std::shared_ptr<BaseVector>> children;
    children.push_back(field1Vector);
    children.push_back(field2Vector);
    children.push_back(field3Vector);
    auto *rowVector = new RowVector(dataSize, children);

    auto *vecBatch = new VectorBatch(dataSize);
    vecBatch->Append(rowVector);

    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount, 0);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    limitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    limitOperator->GetOutput(&outputVecBatch);

    // Create expected struct vector
    int32_t expField1Data[resultDataSize] = {10, 20, 30};
    double expField2Data[resultDataSize] = {1.1, 2.2, 3.3};
    std::string expField3Data[resultDataSize] = {"a", "b", "c"};

    auto expField1Vector = std::shared_ptr<BaseVector>(CreateVector(resultDataSize, expField1Data));
    auto expField2Vector = std::shared_ptr<BaseVector>(CreateVector(resultDataSize, expField2Data));
    auto expField3Vector = std::shared_ptr<BaseVector>(CreateVarcharVector(expField3Data, resultDataSize));

    std::vector<std::shared_ptr<BaseVector>> expChildren;
    expChildren.push_back(expField1Vector);
    expChildren.push_back(expField2Vector);
    expChildren.push_back(expField3Vector);
    auto *expRowVector = new RowVector(resultDataSize, expChildren);

    auto *expVecBatch = new VectorBatch(resultDataSize);
    expVecBatch->Append(expRowVector);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expVecBatch));

    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    delete operatorFactory;
}

// Test limit with VarBinary type
TEST(NativeOmniLimitOperator, TestLimitWithVarBinary)
{
    const int32_t dataSize = 4;
    const int32_t limitCount = 3;
    const int32_t resultDataSize = 3;

    // VarBinary uses string_view similar to Varchar
    std::string data1[dataSize] = {"\x01\x02\x03", "\x04\x05\x06", "\x07\x08\x09", "\x0A\x0B\x0C"};

    std::vector<DataTypePtr> types = { VarBinaryType(100) };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1);

    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount, 0);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    limitOperator->AddInput(vecBatch1);
    VectorBatch *outputVecBatch;
    limitOperator->GetOutput(&outputVecBatch);

    std::string expData1[resultDataSize] = {"\x01\x02\x03", "\x04\x05\x06", "\x07\x08\x09"};
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, resultDataSize, expData1);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expVecBatch1));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    delete operatorFactory;
}

// Test limit with complex types combination
TEST(NativeOmniLimitOperator, TestLimitWithComplexTypes)
{
    const int32_t dataSize = 4;
    const int32_t limitCount = 3;
    const int32_t resultDataSize = 3;

    // Byte type
    int8_t byteData[dataSize] = {10, 20, 30, 40};
    
    // Float type
    float floatData[dataSize] = {1.1f, 2.2f, 3.3f, 4.4f};

    // Array type
    const int32_t elementSize = 8;
    auto *elements = new Vector<int32_t>(elementSize);
    int32_t elementValues[elementSize] = {1, 2, 3, 4, 5, 6, 7, 8};
    for (int i = 0; i < elementSize; i++) {
        elements->SetValue(i, elementValues[i]);
    }
    auto *arrayVector = new ArrayVector(dataSize);
    arrayVector->SetOffset(0, 0);
    arrayVector->SetOffset(1, 2);
    arrayVector->SetOffset(2, 4);
    arrayVector->SetOffset(3, 6);
    arrayVector->SetOffset(4, 8);
    arrayVector->SetElementVectorFromRaw(elements);

    auto *vecBatch = new VectorBatch(dataSize);
    vecBatch->Append(CreateVector(dataSize, byteData));
    vecBatch->Append(CreateVector(dataSize, floatData));
    vecBatch->Append(arrayVector);

    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount, 0);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    limitOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    limitOperator->GetOutput(&outputVecBatch);

    // Create expected data
    int8_t expByteData[resultDataSize] = {10, 20, 30};
    float expFloatData[resultDataSize] = {1.1f, 2.2f, 3.3f};
    
    auto *expElements = new Vector<int32_t>(6);
    int32_t expElementValues[6] = {1, 2, 3, 4, 5, 6};
    for (int i = 0; i < 6; i++) {
        expElements->SetValue(i, expElementValues[i]);
    }
    auto *expArrayVector = new ArrayVector(resultDataSize);
    expArrayVector->SetOffset(0, 0);
    expArrayVector->SetOffset(1, 2);
    expArrayVector->SetOffset(2, 4);
    expArrayVector->SetOffset(3, 6);
    expArrayVector->SetElementVectorFromRaw(expElements);

    auto *expVecBatch = new VectorBatch(resultDataSize);
    expVecBatch->Append(CreateVector(resultDataSize, expByteData));
    expVecBatch->Append(CreateVector(resultDataSize, expFloatData));
    expVecBatch->Append(expArrayVector);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expVecBatch));

    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(limitOperator);
    delete operatorFactory;
}
}