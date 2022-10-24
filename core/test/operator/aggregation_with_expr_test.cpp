/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include <gtest/gtest.h>
#include "operator/aggregation/aggregation.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/group_aggregation_expr.h"
#include "vector/vector_helper.h"
#include "util/test_util.h"

namespace omniruntime {
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace TestUtil;
using namespace omniruntime::expressions;

TEST(HashAggregationWithExprOperatorTest, test_hashagg_partial_expr)
{
    const int32_t dataSize = 8;
    const int32_t groupByNum = 2;
    const int32_t expectDataSize = 1;

    // prepare data
    // sum(c1*5), sum(c3) group by c0%3, c2 => 2, 10, 180, 36
    int64_t data1[] = {2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L}; // c0
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};      // c1
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};              // c2
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};              // c3

    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), IntType() }));
    DataTypes aggOutputTypes(std::vector<DataTypePtr>({ LongType(), IntType() }));
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
    std::vector<Expr *> aggKeys1 = { mulExpr };
    std::vector<Expr *> aggKeys2 = { new FieldExpr(3, IntType()) };
    std::vector<std::vector<omniruntime::expressions::Expr *>> aggAllKeys = { aggKeys1, aggKeys2 };

    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM };
    std::vector<uint32_t> maskCols = { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1) };

    auto overflowConfig = new OverflowConfig();

    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypes.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypes.size());
    auto *hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, sourceTypes, aggOutputTypesWrap,
        aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {5};
    int64_t expData3[] = {180};
    int32_t expData4[] = {36};
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), IntType(), LongType(), IntType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    DeleteOperatorFactory(hashAggWithExprOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
    delete overflowConfig;
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_full_expr)
{
    using namespace omniruntime::expressions;

    const int32_t dataSize = 8;
    const int32_t groupByNum = 2;
    const int32_t expectDataSize = 1;

    // prepare data
    // sum(c1*5), sum(c3+5) group by c0%3, c2+5  => 2, 10, 180, 76
    int64_t data1[] = {2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L};
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};

    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), IntType() }));
    DataTypes aggOutputTypes(std::vector<DataTypePtr>({ LongType(), IntType() }));
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

    std::vector<Expr *> aggKeys1 = { mulExpr };
    std::vector<Expr *> aggKeys2 = { addExpr2 };
    std::vector<std::vector<omniruntime::expressions::Expr *>> aggAllKeys = { aggKeys1, aggKeys2 };
    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM };
    std::vector<uint32_t> maskCols = { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1) };

    auto overflowConfig = new OverflowConfig();

    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypes.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypes.size());
    auto hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, sourceTypes, aggOutputTypesWrap,
        aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {10};
    int64_t expData3[] = {180};
    int32_t expData4[] = {76};
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), IntType(), LongType(), IntType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    DeleteOperatorFactory(hashAggWithExprOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
    delete overflowConfig;
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_no_expr)
{
    using namespace omniruntime::expressions;

    const int32_t dataSize = 8;
    const int32_t groupByNum = 2;
    const int32_t expectDataSize = 1;

    // prepare data
    // sum(c1), sum(c3) count(*) group by c0, c2  => 2, 5, 36, 36, 8
    int64_t data1[] = {2L, 2L, 2L, 2L, 2L, 2L, 2L, 2L}; // c0
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L}; // c1
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};         // c2
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};         // c3

    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), IntType() }));
    DataTypes aggOutputTypes(std::vector<DataTypePtr>({ LongType(), IntType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    std::vector<Expr *> groupByKeys = { new FieldExpr(0, LongType()), new FieldExpr(2, IntType()) };

    std::vector<Expr *> aggKeys1 = { new FieldExpr(1, LongType()) };
    std::vector<Expr *> aggKeys2 = { new FieldExpr(3, IntType()) };
    std::vector<std::vector<omniruntime::expressions::Expr *>> aggAllKeys = { aggKeys1, aggKeys2 };
    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_COUNT_ALL };
    std::vector<uint32_t> maskCols = { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1),
        static_cast<uint32_t>(-1) };

    auto overflowConfig = new OverflowConfig();

    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypes.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypes.size());

    auto hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, sourceTypes, aggOutputTypesWrap,
        aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
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
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), IntType(), LongType(), IntType(), LongType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4, expData5);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    DeleteOperatorFactory(hashAggWithExprOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
    delete overflowConfig;
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_partial_flat_output_expr)
{
    char sparkEngine[] = "Spark";
    EngineUtil::GetInstance().SetEngineType(sparkEngine);
    const int32_t dataSize = 8;
    const int32_t groupByNum = 2;
    const int32_t expectDataSize = 1;

    // prepare data
    // avg(c1*5), avg(c3) group by c0%3, c2 => 2, 10, 23, 32
    int64_t data1[] = {2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L}; // c0
    int64_t data2[] = {5L, 3L, 2L, 6L, 5L, 4L, 7L, 8L};      // c1
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};              // c2
    int32_t data4[] = {5, 1, 2, 6, 1, 4, 7, 8};              // c3

    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), IntType() }));
    DataTypes aggOutputTypes1(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    DataTypes aggOutputTypes2(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
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
    std::vector<Expr *> aggKeys1 = { mulExpr };
    std::vector<Expr *> aggKeys2 = { new FieldExpr(3, IntType()) };
    std::vector<std::vector<omniruntime::expressions::Expr *>> aggAllKeys = { aggKeys1, aggKeys2 };

    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG };
    std::vector<uint32_t> maskCols = { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1) };

    auto overflowConfig = new OverflowConfig();

    std::vector<DataTypes> aggsOutputTypes = { aggOutputTypes1, aggOutputTypes2 };
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypes.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypes.size());
    auto *hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, sourceTypes, aggsOutputTypes,
        aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {5};
    double expData3[] = {200};
    int64_t expData4[] = {8};
    double expData5[] = {34};
    int64_t expData6[] = {8};
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ LongType(), IntType(), DoubleType(), LongType(), DoubleType(), LongType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4, expData5, expData6);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    DeleteOperatorFactory(hashAggWithExprOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
    delete overflowConfig;
    char olkEngine[] = "OLK";
    EngineUtil::GetInstance().SetEngineType(olkEngine);
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_final_flat_input_expr)
{
    char sparkEngine[] = "Spark";
    EngineUtil::GetInstance().SetEngineType(sparkEngine);

    const int32_t dataSize = 1;
    const int32_t groupByNum = 2;
    const int32_t expectDataSize = 1;

    // prepare data
    // avg(c1*5), avg(c3) group by c0%3, c2  final
    int64_t data1[] = {2L}; // c0
    int32_t data2[] = {5L}; // c1
    double data3[] = {400}; // c2
    int64_t data4[] = {8};  // c3
    double data5[] = {32};  // c3
    int64_t data6[] = {8};  // c3

    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ LongType(), IntType(), DoubleType(), LongType(), DoubleType(), LongType() }));
    DataTypes aggOutputTypes1(std::vector<DataTypePtr>({ DoubleType() }));
    DataTypes aggOutputTypes2(std::vector<DataTypePtr>({ DoubleType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4, data5, data6);

    std::vector<Expr *> groupByKeys = { new FieldExpr(0, LongType()), new FieldExpr(1, IntType()) };

    // aggKeys
    std::vector<Expr *> aggKeys1 = { new FieldExpr(2, DoubleType()), new FieldExpr(3, LongType()) }; // agg1 SUM + COUNT
    std::vector<Expr *> aggKeys2 = { new FieldExpr(4, DoubleType()), new FieldExpr(5, LongType()) }; // agg2 SUM + COUNT
    std::vector<std::vector<omniruntime::expressions::Expr *>> aggAllKeys = { aggKeys1, aggKeys2 };

    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG };
    std::vector<uint32_t> maskCols = { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1) };

    auto overflowConfig = new OverflowConfig();

    std::vector<DataTypes> aggsOutputTypes = { aggOutputTypes1, aggOutputTypes2 };
    auto inputRawWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypes.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypes.size());
    auto *hashAggWithExprFinalOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, sourceTypes, aggsOutputTypes,
        aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *hashAggWithExprFinalOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprFinalOperatorFactory));

    hashAggWithExprFinalOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprFinalOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {5};
    double expData3[] = {50};
    double expData4[] = {4};
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), IntType(), DoubleType(), DoubleType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprFinalOperator);
    DeleteOperatorFactory(hashAggWithExprFinalOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
    delete overflowConfig;

    char olkEngine[] = "OLK";
    EngineUtil::GetInstance().SetEngineType(olkEngine);
}
}
