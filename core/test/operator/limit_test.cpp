/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#include <vector>
#include "gtest/gtest.h"
#include "vector/vector_helper.h"
#include "operator/limit/limit.h"
#include "../util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;
using namespace TestUtil;

namespace LimitTest {
// supported data types cover
TEST(NativeOmniLimitOperator, TestLimitBasic)
{
    // construct data
    const int32_t dataSize = 4;
    const int64_t limitCount = dataSize - 1;
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

    std::vector<DataType> types = { IntDataType::Instance(),       DoubleDataType::Instance(),
        VarcharDataType(10),           LongDataType::Instance(),
        Decimal128DataType(10, 2),     Date32DataType::Instance(),
        Decimal64DataType::Instance(), BooleanDataType::Instance(),
        CharDataType::Instance() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 =
        CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4, data5, data6, data7, data8, data9);

    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount);
    operatorFactory->SetJitContext(nullptr);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    limitOperator->AddInput(vecBatch1);
    std::vector<VectorBatch *> outputVecBatches;
    limitOperator->GetOutput(outputVecBatches);

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
    Operator::DeleteOperator(limitOperator);
    DeleteOperatorFactory(operatorFactory);
}

// call input several times
TEST(NativeOmniLimitOperator, TestLimitMultiInput)
{
    // construct data
    const int32_t dataSize = 3;
    const int64_t limitCount = 40;

    // input vecBatch1
    int32_t data01[dataSize] = {0, 1, 2};
    double data02[dataSize] = {6.6, 5.5, 4.4};

    std::vector<DataType> types = { IntDataType::Instance(), DoubleDataType::Instance() };
    DataTypes sourceTypes(types);

    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data01, data02);
    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount);
    operatorFactory->SetJitContext(nullptr);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    std::vector<VectorBatch *> outputVecBatches;
    limitOperator->AddInput(vecBatch1);
    limitOperator->GetOutput(outputVecBatches);

    // input vecBatch2
    int32_t data11[dataSize] = {3, 4, 5};
    double data12[dataSize] = {3.3, 2.2, 1.1};

    VectorBatch *vecBatch2 = CreateVectorBatch(sourceTypes, dataSize, data11, data12);
    limitOperator->AddInput(vecBatch2);
    limitOperator->GetOutput(outputVecBatches);

    int32_t expData01[dataSize] = {0, 1, 2};
    double expData02[dataSize] = {6.6, 5.5, 4.4};
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, (dataSize), expData01, expData02);

    int32_t expData11[dataSize] = {3, 4, 5};
    double expData12[dataSize] = {3.3, 2.2, 1.1};
    VectorBatch *expVecBatch2 = CreateVectorBatch(sourceTypes, (dataSize), expData11, expData12);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expVecBatch1));
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[1], expVecBatch2));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatch(expVecBatch2);
    VectorHelper::FreeVecBatches(outputVecBatches);
    Operator::DeleteOperator(limitOperator);
    DeleteOperatorFactory(operatorFactory);
}

// test null
TEST(NativeOmniLimitOperator, TestLimitWithNull)
{
    // construct data
    const int32_t dataSize = 6;
    const int64_t limitCount = 5;
    // table1
    int32_t data1[dataSize] = {0, 1, 2, 0, 1, 2};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    std::vector<DataType> types = { IntDataType::Instance(), DoubleDataType::Instance() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(limitCount);
    operatorFactory->SetJitContext(nullptr);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));

    // set null to vector batch to simulate null value
    Vector *colVector = vecBatch1->GetVector(0);
    colVector->SetValueNull(2);
    colVector->SetValueNull(3);
    Vector *colVector1 = vecBatch1->GetVector(1);
    colVector1->SetValueNull(3);
    colVector1->SetValueNull(4);

    limitOperator->AddInput(vecBatch1);
    std::vector<VectorBatch *> outputVecBatches;
    limitOperator->GetOutput(outputVecBatches);

    int32_t expData1[dataSize] = {0, 1, 2, 0, 1};          // expData1[2],expData1[3] simulate to null
    double expData2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2}; // expData2[3],expData2[4] simulate to null
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, limitCount, expData1, expData2);

    // set null to vector batch to simulate null value
    Vector *colVector00 = expVecBatch1->GetVector(0);
    colVector00->SetValueNull(2);
    colVector00->SetValueNull(3);
    Vector *colVector01 = expVecBatch1->GetVector(1);
    colVector01->SetValueNull(3);
    colVector01->SetValueNull(4);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expVecBatch1));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatches(outputVecBatches);
    Operator::DeleteOperator(limitOperator);
    DeleteOperatorFactory(operatorFactory);
}
}