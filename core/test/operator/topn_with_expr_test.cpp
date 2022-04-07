/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "gtest/gtest.h"
#include "operator/topn/topn_expr.h"
#include "vector/vector_helper.h"
#include "../util/test_util.h"
#include "jit_context/jit_context.h"

using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace std;
using namespace TestUtil;

namespace TopnWithExprTest {
TEST(NativeOmniTopNWithExprOperatorTest, TestTopNWithAllExpr)
{
    // construct input data
    const int32_t dataSize = 8;
    const int32_t expectedDataSize = 5;
    const int32_t sortKeyCnt = 2;

    // prepare data
    int32_t data1[dataSize] = {5, 8, 8, 6, 8, 4, 13, 15};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L};
    int64_t data3[dataSize] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};

    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType(), LongDataType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    int32_t ascendings[sortKeyCnt] = {false, true};
    int32_t nullFirsts[sortKeyCnt] = {false, false};

    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(5, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());
    FieldExpr *modLeft = new FieldExpr(2, LongType());
    LiteralExpr *modRight = new LiteralExpr(3L, LongType());
    BinaryExpr *modExpr = new BinaryExpr(omniruntime::expressions::Operator::MOD, modLeft, modRight, LongType());
    std::vector<Expr *> sortExprs = { addExpr, modExpr };

    auto jitContext = CreateTopNWithExprJitContext(sourceTypes, sortExprs, ascendings, nullFirsts);
    auto topNWithExprOperatorFactory =
        new TopNWithExprOperatorFactory(sourceTypes, expectedDataSize, sortExprs, ascendings, nullFirsts, sortKeyCnt);
    topNWithExprOperatorFactory->SetJitContext(jitContext);
    auto topNWithExprOperator = static_cast<TopNWithExprOperator *>(CreateTestOperator(topNWithExprOperatorFactory));

    topNWithExprOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatchs;
    topNWithExprOperator->GetOutput(outputVecBatchs);

    int32_t expData1[dataSize] = {15, 13, 8, 8, 8};
    int64_t expData2[dataSize] = {23, 0, 5, 4, 3};
    int64_t expData3[dataSize] = {8, 7, 3, 1, 2};
    int32_t expData4[dataSize] = {20, 18, 13, 13, 13};
    int64_t expData5[dataSize] = {2, 1, 0, 1, 2};
    DataTypes expectTypes(
        std::vector<DataType>({ IntDataType(), LongDataType(), LongDataType(), IntDataType(), LongDataType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3, expData4, expData5);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    omniruntime::op::Operator::DeleteOperator(topNWithExprOperator);
    DeleteOperatorFactory(topNWithExprOperatorFactory);

    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}

TEST(NativeOmniTopNWithExprOperatorTest, TestTopNWithPartialExpr)
{
    // construct input data
    const int32_t dataSize = 8;
    const int32_t expectedDataSize = 5;
    const int32_t sortKeyCnt = 2;

    // prepare data
    int32_t data1[dataSize] = {5, 8, 8, 6, 8, 4, 13, 15};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L};
    int64_t data3[dataSize] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};

    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType(), LongDataType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    int32_t ascendings[sortKeyCnt] = {false, true};
    int32_t nullFirsts[sortKeyCnt] = {false, false};

    FieldExpr *col0 = new FieldExpr(0, IntType());
    FieldExpr *modLeft = new FieldExpr(2, LongType());
    LiteralExpr *modRight = new LiteralExpr(3L, LongType());
    BinaryExpr *modExpr = new BinaryExpr(omniruntime::expressions::Operator::MOD, modLeft, modRight, LongType());
    std::vector<Expr *> sortKeys = { col0, modExpr };

    JitContext *jitContext = CreateTopNWithExprJitContext(sourceTypes, sortKeys, ascendings, nullFirsts);
    TopNWithExprOperatorFactory *topNWithExprOperatorFactory =
        new TopNWithExprOperatorFactory(sourceTypes, expectedDataSize, sortKeys, ascendings, nullFirsts, sortKeyCnt);
    topNWithExprOperatorFactory->SetJitContext(jitContext);
    TopNWithExprOperator *topNWithExprOperator =
        static_cast<TopNWithExprOperator *>(CreateTestOperator(topNWithExprOperatorFactory));

    topNWithExprOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatchs;
    topNWithExprOperator->GetOutput(outputVecBatchs);

    int32_t expData1[dataSize] = {15, 13, 8, 8, 8};
    int64_t expData2[dataSize] = {23, 0, 5, 4, 3};
    int64_t expData3[dataSize] = {8, 7, 3, 1, 2};
    int64_t expData4[dataSize] = {2, 1, 0, 1, 2};
    DataTypes expectTypes(std::vector<DataType>({ IntDataType(), LongDataType(), LongDataType(), LongDataType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3, expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    omniruntime::op::Operator::DeleteOperator(topNWithExprOperator);
    DeleteOperatorFactory(topNWithExprOperatorFactory);

    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}

TEST(NativeOmniTopNWithExprOperatorTest, TestTopNWithNoExpr)
{
    // construct input data
    const int32_t dataSize = 8;
    const int32_t expectedDataSize = 5;
    const int32_t sortKeyCnt = 2;

    // prepare data
    int32_t data1[dataSize] = {5, 8, 8, 6, 8, 4, 13, 15};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L};
    int64_t data3[dataSize] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};

    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType(), LongDataType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    int32_t ascendings[sortKeyCnt] = {false, true};
    int32_t nullFirsts[sortKeyCnt] = {false, false};

    FieldExpr *col0 = new FieldExpr(0, IntType());
    FieldExpr *col2 = new FieldExpr(2, LongType());
    std::vector<Expr *> sortExprs = { col0, col2 };

    JitContext *jitContext = CreateTopNWithExprJitContext(sourceTypes, sortExprs, ascendings, nullFirsts);
    TopNWithExprOperatorFactory *topNWithExprOperatorFactory =
        new TopNWithExprOperatorFactory(sourceTypes, expectedDataSize, sortExprs, ascendings, nullFirsts, sortKeyCnt);
    topNWithExprOperatorFactory->SetJitContext(jitContext);
    TopNWithExprOperator *topNWithExprOperator =
        static_cast<TopNWithExprOperator *>(CreateTestOperator(topNWithExprOperatorFactory));

    topNWithExprOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatchs;
    topNWithExprOperator->GetOutput(outputVecBatchs);

    int32_t expData1[dataSize] = {15, 13, 8, 8, 8};
    int64_t expData2[dataSize] = {23, 0, 4, 3, 5};
    int64_t expData3[dataSize] = {8, 7, 1, 2, 3};
    DataTypes expectTypes(std::vector<DataType>({ IntDataType(), LongDataType(), LongDataType() }));
    VectorBatch *expectVecorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    omniruntime::op::Operator::DeleteOperator(topNWithExprOperator);
    DeleteOperatorFactory(topNWithExprOperatorFactory);

    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}
}
