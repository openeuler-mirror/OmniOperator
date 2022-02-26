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

// supported data types cover
TEST(NativeOmniLimitOperator, TestLimitBasic)
{
    // construct data
    const int32_t DATA_SIZE = 4;
    const int64_t LIMIT_COUNT = DATA_SIZE - 1;
    const int32_t RESULT_DATA_SIZE = DATA_SIZE - 1;

    // table1
    int32_t data1[DATA_SIZE] = {0, 1, 2, 0};
    double data2[DATA_SIZE] = {0.1, 1.1, 2.1, 0.1};
    std::string data3[DATA_SIZE] = {"abc", "hello", "world", "abc"};
    int64_t data4[DATA_SIZE] = {10L, 100L, 1000L, 10L};
    Decimal128 data5[DATA_SIZE] = {111111, 222222, 333333, 111111};
    int32_t data6[DATA_SIZE] = {0, 1, 2, 0};
    int64_t data7[DATA_SIZE] = {10L, 100L, 1000L, 10L};
    bool data8[DATA_SIZE] = {true, false, false, true};
    std::string data9[DATA_SIZE] = {"123", "456", "789", "012"};

    std::vector<DataType> types = { IntDataType::Instance(),       DoubleDataType::Instance(),
        VarcharDataType(10),           LongDataType::Instance(),
        Decimal128DataType(10, 2),     Date32DataType::Instance(),
        Decimal64DataType::Instance(), BooleanDataType::Instance(),
        CharDataType::Instance() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data1, data2, data3, data4, data5, data6, data7, data8, data9);

    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(LIMIT_COUNT);
    operatorFactory->SetJitContext(nullptr);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    limitOperator->AddInput(vecBatch1);
    std::vector<VectorBatch *> outputVecBatches;
    limitOperator->GetOutput(outputVecBatches);

    int32_t expData1[RESULT_DATA_SIZE] = {0, 1, 2};
    double expData2[RESULT_DATA_SIZE] = {0.1, 1.1, 2.1};
    std::string expData3[RESULT_DATA_SIZE] = {"abc", "hello", "world"};
    int64_t expData4[RESULT_DATA_SIZE] = {10L, 100L, 1000L};
    Decimal128 expData5[RESULT_DATA_SIZE] = {111111, 222222, 333333};
    int32_t expData6[RESULT_DATA_SIZE] = {0, 1, 2};
    int64_t expData7[RESULT_DATA_SIZE] = {10L, 100L, 1000L};
    bool expData8[RESULT_DATA_SIZE] = {true, false, false};
    std::string expData9[RESULT_DATA_SIZE] = {"123", "456", "789",};

    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, RESULT_DATA_SIZE, expData1, expData2, expData3, expData4,
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
    const int32_t DATA_SIZE = 3;
    const int64_t LIMIT_COUNT = 40;

    // input vecBatch1
    int32_t data01[DATA_SIZE] = {0, 1, 2};
    double data02[DATA_SIZE] = {6.6, 5.5, 4.4};

    std::vector<DataType> types = { IntDataType::Instance(), DoubleDataType::Instance() };
    DataTypes sourceTypes(types);

    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, DATA_SIZE, data01, data02);
    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(LIMIT_COUNT);
    operatorFactory->SetJitContext(nullptr);
    LimitOperator *limitOperator = dynamic_cast<LimitOperator *>(CreateTestOperator(operatorFactory));
    std::vector<VectorBatch *> outputVecBatches;
    limitOperator->AddInput(vecBatch1);
    limitOperator->GetOutput(outputVecBatches);

    // input vecBatch2
    int32_t data11[DATA_SIZE] = {3, 4, 5};
    double data12[DATA_SIZE] = {3.3, 2.2, 1.1};

    VectorBatch *vecBatch2 = CreateVectorBatch(sourceTypes, DATA_SIZE, data11, data12);
    limitOperator->AddInput(vecBatch2);
    limitOperator->GetOutput(outputVecBatches);

    int32_t expData01[DATA_SIZE] = {0, 1, 2};
    double expData02[DATA_SIZE] = {6.6, 5.5, 4.4};
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, (DATA_SIZE), expData01, expData02);

    int32_t expData11[DATA_SIZE] = {3, 4, 5};
    double expData12[DATA_SIZE] = {3.3, 2.2, 1.1};
    VectorBatch *expVecBatch2 = CreateVectorBatch(sourceTypes, (DATA_SIZE), expData11, expData12);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expVecBatch1));
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[1], expVecBatch2));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatches(outputVecBatches);
    Operator::DeleteOperator(limitOperator);
    DeleteOperatorFactory(operatorFactory);
}

// test null
TEST(NativeOmniLimitOperator, TestLimitWithNull)
{
    // construct data
    const int32_t DATA_SIZE = 6;
    const int64_t LIMIT_COUNT = 5;
    // table1
    int32_t data1[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    std::vector<DataType> types = { IntDataType::Instance(), DoubleDataType::Instance() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, DATA_SIZE, data1, data2);
    LimitOperatorFactory *operatorFactory = LimitOperatorFactory::CreateLimitOperatorFactory(LIMIT_COUNT);
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

    int32_t expData1[DATA_SIZE] = {0, 1, 2, 0, 1};          // expData1[2],expData1[3] simulate to null
    double expData2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2}; // expData2[3],expData2[4] simulate to null
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, LIMIT_COUNT, expData1, expData2);

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
