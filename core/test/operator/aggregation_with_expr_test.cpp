/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include <gtest/gtest.h>
#include "operator/aggregation/aggregation.h"
#include "operator/aggregation/group_aggregation_expr.h"
#include "vector/vector_helper.h"
#include "../util/test_util.h"
#include "../../libconfig.h"

namespace omniruntime {
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace TestUtil;

TEST(HashAggregationWithExprOperatorTest, test_hashagg_partial_expr)
{
    using namespace omniruntime::expressions;

    const int32_t dataSize = 8;
    const int32_t groupByNum = 2;
    const int32_t aggNum = 2;
    const int32_t expectDataSize = 1;

    // prepare data
    int64_t data1[] = {2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L};
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};

    DataTypes sourceTypes(std::vector<DataType>({ LongDataType(), LongDataType(), IntDataType(), IntDataType() }));
    DataTypes aggOutputTypes(std::vector<DataType>({ LongDataType(), IntDataType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    // groupByKeys
    LiteralExpr *modRight = new LiteralExpr(3, LongType());
    modRight->longVal = 3;
    BinaryExpr *modExpr =
        new BinaryExpr(omniruntime::expressions::Operator::MOD, new FieldExpr(0, LongType()), modRight, LongType());
    std::vector<Expr *> groupByKeys = { modExpr, new FieldExpr(2, IntType()) };

    // aggKeys
    LiteralExpr *mulRight = new LiteralExpr(5, LongType());
    mulRight->longVal = 5;
    BinaryExpr *mulExpr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, new FieldExpr(1, LongType()), mulRight, LongType());
    std::vector<Expr *> aggKeys = { mulExpr, new FieldExpr(3, IntType()) };

    FunctionType aggFuncTypes[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    uint32_t maskCols[] = {static_cast<uint32_t>(-1), static_cast<uint32_t>(-1)};

    auto *hashAggWithExprOperatorFactory = new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggKeys,
        aggNum, sourceTypes, aggOutputTypes, (uint32_t *)aggFuncTypes, maskCols, true, false);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {5};
    int64_t expData3[] = {180};
    int32_t expData4[] = {36};
    DataTypes expectTypes(std::vector<DataType>({ LongDataType(), IntDataType(), LongDataType(), IntDataType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    DeleteOperatorFactory(hashAggWithExprOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_full_expr)
{
    using namespace omniruntime::expressions;

    const int32_t dataSize = 8;
    const int32_t groupByNum = 2;
    const int32_t aggNum = 2;
    const int32_t expectDataSize = 1;

    // prepare data
    int64_t data1[] = {2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L};
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};

    DataTypes sourceTypes(std::vector<DataType>({ LongDataType(), LongDataType(), IntDataType(), IntDataType() }));
    DataTypes aggOutputTypes(std::vector<DataType>({ LongDataType(), IntDataType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    FieldExpr *modLeft = new FieldExpr(0, LongType());
    LiteralExpr *modRight = new LiteralExpr(3, LongType());
    modRight->longVal = 3;
    BinaryExpr *modExpr = new BinaryExpr(omniruntime::expressions::Operator::MOD, modLeft, modRight, LongType());
    FieldExpr *addLeft = new FieldExpr(2, IntType());
    LiteralExpr *addRight = new LiteralExpr(5, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());
    std::vector<Expr *> groupByKeys = { modExpr, addExpr };

    FieldExpr *mulLeft = new FieldExpr(1, LongType());
    LiteralExpr *mulRight = new LiteralExpr(5, LongType());
    mulRight->longVal = 5;
    BinaryExpr *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, LongType());
    FieldExpr *addLeft2 = new FieldExpr(3, IntType());
    LiteralExpr *addRight2 = new LiteralExpr(5, IntType());
    BinaryExpr *addExpr2 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft2, addRight2, IntType());
    std::vector<Expr *> aggKeys = { mulExpr, addExpr2 };

    FunctionType aggFuncTypes[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    uint32_t maskCols[] = {static_cast<uint32_t>(-1), static_cast<uint32_t>(-1)};

    auto hashAggWithExprOperatorFactory = new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggKeys,
        aggNum, sourceTypes, aggOutputTypes, (uint32_t *)aggFuncTypes, maskCols, true, false);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {10};
    int64_t expData3[] = {180};
    int32_t expData4[] = {76};
    DataTypes expectTypes(std::vector<DataType>({ LongDataType(), IntDataType(), LongDataType(), IntDataType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    DeleteOperatorFactory(hashAggWithExprOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_no_expr)
{
    using namespace omniruntime::expressions;

    const int32_t dataSize = 8;
    const int32_t groupByNum = 2;
    const int32_t aggNum = 3;
    const int32_t expectDataSize = 1;

    // prepare data
    int64_t data1[] = {2L, 2L, 2L, 2L, 2L, 2L, 2L, 2L};
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};

    DataTypes sourceTypes(std::vector<DataType>({ LongDataType(), LongDataType(), IntDataType(), IntDataType() }));
    DataTypes aggOutputTypes(std::vector<DataType>({ LongDataType(), IntDataType(), LongDataType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    std::vector<Expr *> groupByKeys = { new FieldExpr(0, LongType()), new FieldExpr(2, IntType()) };
    std::vector<Expr *> aggKeys = { new FieldExpr(1, LongType()), new FieldExpr(3, IntType()) };

    FunctionType aggFuncTypes[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
                                   OMNI_AGGREGATION_TYPE_COUNT_ALL};
    uint32_t maskCols[] = {static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1) };

    auto hashAggWithExprOperatorFactory = new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggKeys,
        aggNum, sourceTypes, aggOutputTypes, (uint32_t *)aggFuncTypes, maskCols, true, false);
    auto hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {5};
    int64_t expData3[] = {36};
    int32_t expData4[] = {36};
    int64_t expData5[]={8};
    DataTypes expectTypes(
        std::vector<DataType>({ LongDataType(), IntDataType(), LongDataType(), IntDataType(), LongDataType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4, expData5);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    DeleteOperatorFactory(hashAggWithExprOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}
}
