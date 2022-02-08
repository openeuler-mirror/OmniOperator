/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include <gtest/gtest.h>
#include "operator/aggregation/aggregation.h"
#include "operator/aggregation/group_aggregation_expr.h"
#include "../../src/vector/vector_helper.h"
#include "operator/jit_context/jit_context.h"
#include "../util/test_util.h"

using namespace omniruntime::vec;
using namespace omniruntime::op;

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

    VecTypes sourceTypes(std::vector<VecType>({ LongVecType(), LongVecType(), IntVecType(), IntVecType() }));
    VecTypes aggOutputTypes(std::vector<VecType>({ LongVecType(), IntVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    // groupByKeys
    DataExpr *modRight = new DataExpr(3);
    modRight->longVal = 3;
    BinaryExpr *modExpr = new BinaryExpr(MOD, new DataExpr(0, INT64D), modRight, INT64D);
    std::vector<Expr *> groupByKeys = { modExpr, new DataExpr(2, INT32D) };

    // aggKeys
    DataExpr *mulRight = new DataExpr(5);
    mulRight->longVal = 5;
    BinaryExpr *mulExpr = new BinaryExpr(MUL, new DataExpr(1, INT64D), mulRight, INT64D);
    std::vector<Expr *> aggKeys = { mulExpr, new DataExpr(3, INT32D) };

    AggregateType aggFuncTypes[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};

    JitContext *jitContext = CreateHashAggregationWithExprJitContext(sourceTypes, groupByKeys, aggKeys,
        (int32_t *)aggFuncTypes, 2, aggOutputTypes);
    auto *hashAggWithExprOperatorFactory = new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggKeys,
        aggNum, sourceTypes, aggOutputTypes, (uint32_t *)aggFuncTypes, true, false);
    hashAggWithExprOperatorFactory->SetJitContext(jitContext);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {5};
    int64_t expData3[] = {180};
    int32_t expData4[] = {36};
    VecTypes expectTypes(std::vector<VecType>({ LongVecType(), IntVecType(), LongVecType(), IntVecType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

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

    VecTypes sourceTypes(std::vector<VecType>({ LongVecType(), LongVecType(), IntVecType(), IntVecType() }));
    VecTypes aggOutputTypes(std::vector<VecType>({ LongVecType(), IntVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    DataExpr *modLeft = new DataExpr(0, INT64D);
    DataExpr *modRight = new DataExpr(3);
    modRight->longVal = 3;
    BinaryExpr *modExpr = new BinaryExpr(MOD, modLeft, modRight, INT64D);
    DataExpr *addLeft = new DataExpr(2, INT32D);
    DataExpr *addRight = new DataExpr(5);
    BinaryExpr *addExpr = new BinaryExpr(ADD, addLeft, addRight, INT32D);
    std::vector<Expr *> groupByKeys = { modExpr, addExpr };

    DataExpr *mulLeft = new DataExpr(1, INT64D);
    DataExpr *mulRight = new DataExpr(5);
    mulRight->longVal = 5;
    BinaryExpr *mulExpr = new BinaryExpr(MUL, mulLeft, mulRight, INT64D);
    DataExpr *addLeft2 = new DataExpr(3, INT32D);
    DataExpr *addRight2 = new DataExpr(5);
    BinaryExpr *addExpr2 = new BinaryExpr(ADD, addLeft2, addRight2, INT32D);
    std::vector<Expr *> aggKeys = { mulExpr, addExpr2 };

    AggregateType aggFuncTypes[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};

    auto jitContext = CreateHashAggregationWithExprJitContext(sourceTypes, groupByKeys, aggKeys,
        (int32_t *)aggFuncTypes, 2, aggOutputTypes);
    auto hashAggWithExprOperatorFactory = new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggKeys,
        aggNum, sourceTypes, aggOutputTypes, (uint32_t *)aggFuncTypes, true, false);
    hashAggWithExprOperatorFactory->SetJitContext(jitContext);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {10};
    int64_t expData3[] = {180};
    int32_t expData4[] = {76};
    VecTypes expectTypes(std::vector<VecType>({ LongVecType(), IntVecType(), LongVecType(), IntVecType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

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
    const int32_t aggNum = 2;
    const int32_t expectDataSize = 1;

    // prepare data
    int64_t data1[] = {2L, 2L, 2L, 2L, 2L, 2L, 2L, 2L};
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};

    VecTypes sourceTypes(std::vector<VecType>({ LongVecType(), LongVecType(), IntVecType(), IntVecType() }));
    VecTypes aggOutputTypes(std::vector<VecType>({ LongVecType(), IntVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    std::vector<Expr *> groupByKeys = { new DataExpr(0, INT64D), new DataExpr(2, INT32D) };
    std::vector<Expr *> aggKeys = { new DataExpr(1, INT64D), new DataExpr(3, INT32D) };

    AggregateType aggFuncTypes[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};

    auto jitContext = CreateHashAggregationWithExprJitContext(sourceTypes, groupByKeys, aggKeys,
        (int32_t *)aggFuncTypes, 2, aggOutputTypes);
    auto hashAggWithExprOperatorFactory = new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggKeys,
        aggNum, sourceTypes, aggOutputTypes, (uint32_t *)aggFuncTypes, true, false);
    hashAggWithExprOperatorFactory->SetJitContext(jitContext);
    auto hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {5};
    int64_t expData3[] = {36};
    int32_t expData4[] = {36};
    VecTypes expectTypes(std::vector<VecType>({ LongVecType(), IntVecType(), LongVecType(), IntVecType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    DeleteOperatorFactory(hashAggWithExprOperatorFactory);

    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}
