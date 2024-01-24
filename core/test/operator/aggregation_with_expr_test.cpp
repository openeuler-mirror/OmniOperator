/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2023. All rights reserved.
 */

#include <gtest/gtest.h>
#include "operator/aggregation/aggregation.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/group_aggregation_expr.h"
#include "operator/aggregation/non_group_aggregation_expr.h"
#include "vector/vector_helper.h"
#include "util/test_util.h"
#include "util/config_util.h"

namespace omniruntime {
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace TestUtil;
using namespace omniruntime::expressions;

TEST(HashAggregationWithExprOperatorTest, test_hashagg_partial_expr)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);

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
    std::vector<omniruntime::expressions::Expr *> aggFilters;
    auto *hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, aggFilters, sourceTypes,
        aggOutputTypesWrap, aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    hashAggWithExprOperator->GetOutput(&outputVecBatch);

    int64_t expData1[] = {2};
    int32_t expData2[] = {5};
    int64_t expData3[] = {180};
    int32_t expData4[] = {36};
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), IntType(), LongType(), IntType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    delete hashAggWithExprOperatorFactory;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
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
    std::vector<omniruntime::expressions::Expr *> aggFilters;
    aggFilters.reserve(0);

    auto hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, aggFilters, sourceTypes,
        aggOutputTypesWrap, aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    hashAggWithExprOperator->GetOutput(&outputVecBatch);

    int64_t expData1[] = {2};
    int32_t expData2[] = {10};
    int64_t expData3[] = {180};
    int32_t expData4[] = {76};
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), IntType(), LongType(), IntType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    delete hashAggWithExprOperatorFactory;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
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
    std::vector<omniruntime::expressions::Expr *> aggFilters;
    aggFilters.reserve(0);

    auto hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, aggFilters, sourceTypes,
        aggOutputTypesWrap, aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    hashAggWithExprOperator->GetOutput(&outputVecBatch);

    int64_t expData1[] = {2};
    int32_t expData2[] = {5};
    int64_t expData3[] = {36};
    int32_t expData4[] = {36};
    int64_t expData5[]={8};
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), IntType(), LongType(), IntType(), LongType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4, expData5);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    delete hashAggWithExprOperatorFactory;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_partial_flat_output_expr)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::NOT_SUPPORT);
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
    std::vector<omniruntime::expressions::Expr *> aggFilters;
    aggFilters.reserve(2);
    aggFilters.push_back(nullptr);
    aggFilters.push_back(nullptr);

    auto *hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, aggFilters, sourceTypes,
        aggsOutputTypes, aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    hashAggWithExprOperator->GetOutput(&outputVecBatch);

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

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    delete hashAggWithExprOperatorFactory;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_final_flat_input_expr)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::NOT_SUPPORT);

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
    std::vector<omniruntime::expressions::Expr *> aggFilters;
    aggFilters.reserve(2);
    aggFilters.push_back(nullptr);
    aggFilters.push_back(nullptr);

    auto *hashAggWithExprFinalOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, aggFilters, sourceTypes,
        aggsOutputTypes, aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *hashAggWithExprFinalOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprFinalOperatorFactory));

    hashAggWithExprFinalOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    hashAggWithExprFinalOperator->GetOutput(&outputVecBatch);

    int64_t expData1[] = {2};
    int32_t expData2[] = {5};
    double expData3[] = {50};
    double expData4[] = {4};
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), IntType(), DoubleType(), DoubleType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprFinalOperator);
    delete hashAggWithExprFinalOperatorFactory;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;

    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
}

TEST(AggregationWithExprOperatorTest, test_agg_sum_expr)
{
    const int32_t dataSize = 8;
    const int32_t groupByNum = 0;
    const int32_t expectDataSize = 1;

    // prepare data
    // sum(c1*5), sum(c3) group by c0%3, c2 => 2, 10, 180, 36
    int64_t data1[] = {2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L}; // c0
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};      // c1
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};              // c2
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};              // c3

    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), IntType() }));
    DataTypes aggOutputTypes(std::vector<DataTypePtr>({ LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    std::vector<Expr *> groupByKeys = {};

    // aggKeys
    LiteralExpr *mulRight = new LiteralExpr(5, LongType());
    mulRight->longVal = 5;
    BinaryExpr *mulExpr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, new FieldExpr(1, LongType()), mulRight, LongType());
    std::vector<Expr *> aggKeys1 = { mulExpr };
    std::vector<std::vector<omniruntime::expressions::Expr *>> aggAllKeys = { aggKeys1 };

    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_SUM };
    std::vector<uint32_t> maskCols = { static_cast<uint32_t>(-1) };

    auto overflowConfig = new OverflowConfig();

    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypes.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypes.size());

    std::vector<omniruntime::expressions::Expr *> aggFilters;
    aggFilters.reserve(2);
    aggFilters.push_back(nullptr);
    aggFilters.push_back(nullptr);

    auto *aggWithExprOperatorFactory =
        new AggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, sourceTypes, aggOutputTypesWrap,
        aggFuncTypes, aggFilters, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *aggWithExprOperator =
        static_cast<AggregationWithExprOperator *>(CreateTestOperator(aggWithExprOperatorFactory));

    aggWithExprOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    aggWithExprOperator->GetOutput(&outputVecBatch);

    int64_t expData1[] = {180};

    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType() }));
    VectorBatch *expectVecorBatch = CreateVectorBatch(expectTypes, expectDataSize, expData1);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(aggWithExprOperator);
    delete aggWithExprOperatorFactory;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(AggregationWithExprOperatorTest, test_agg_first_expr)
{
    const int32_t dataSize = 8;
    const int32_t groupByNum = 0;
    const int32_t expectDataSize = 1;

    // prepare data
    // sum(c1*5), sum(c3) group by c0%3, c2 => 2, 10, 180, 36
    int64_t data1[] = {2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L}; // c0
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};      // c1
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};              // c2
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};              // c3

    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), IntType() }));
    DataTypes aggOutputTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);


    std::vector<Expr *> aggKeys2 = { new FieldExpr(3, IntType()) };
    std::vector<std::vector<omniruntime::expressions::Expr *>> aggAllKeys = { aggKeys2 };

    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL };
    std::vector<uint32_t> maskCols = { static_cast<uint32_t>(-1) };

    auto overflowConfig = new OverflowConfig();
    std::vector<Expr *> groupByKeys = {};
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypes.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypes.size());
    std::vector<omniruntime::expressions::Expr *> aggFilters;
    aggFilters.reserve(2);
    aggFilters.push_back(nullptr);
    aggFilters.push_back(nullptr);

    auto *aggWithExprOperatorFactory =
        new AggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, sourceTypes, aggOutputTypesWrap,
        aggFuncTypes, aggFilters, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *aggWithExprOperator =
        static_cast<AggregationWithExprOperator *>(CreateTestOperator(aggWithExprOperatorFactory));

    aggWithExprOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    aggWithExprOperator->GetOutput(&outputVecBatch);

    int64_t expData1[] = {5};
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorBatch *expectVecorBatch = CreateVectorBatch(expectTypes, expectDataSize, expData1);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(aggWithExprOperator);
    delete aggWithExprOperatorFactory;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(HashAggregationWithExprOperatorTest, adaptor_header_with_null)
{
    OneRowAdaptor adaptor;
    adaptor.Init({ DataTypeId::OMNI_INT, DataTypeId::OMNI_DECIMAL128 });

    {
        type::Decimal128 decimal128(250);
        int32_t data = 1250;
        uintptr_t addresses[2] = {reinterpret_cast<uintptr_t>(&data),
                                  reinterpret_cast<uintptr_t>(&decimal128)};
        int32_t lens[2] = {0, 0};
        auto vecBatch1 = adaptor.Trans2VectorBatch(addresses, lens);
        auto v0 = reinterpret_cast<Vector<int32_t> *>(vecBatch1->Get(0));
        auto v1 = reinterpret_cast<Vector<Decimal128> *>(vecBatch1->Get(1));
        EXPECT_TRUE(not v0->IsNull(0));
        EXPECT_EQ(v0->GetValue(0), data);
        EXPECT_EQ(v1->GetValue(0), decimal128);
    }

    {
        type::Decimal128 decimal128(100);
        uintptr_t addresses[2] = {0,
                                  reinterpret_cast<uintptr_t>(&decimal128)};
        int32_t lens[2] = {-1, 0};
        auto vecBatch1 = adaptor.Trans2VectorBatch(addresses, lens);
        auto v0 = reinterpret_cast<Vector<int32_t> *>(vecBatch1->Get(0));
        auto v1 = reinterpret_cast<Vector<Decimal128> *>(vecBatch1->Get(1));
        EXPECT_TRUE(v0->IsNull(0));
        EXPECT_EQ(v1->GetValue(0), decimal128);
    }

    {
        uintptr_t addresses[2] = {0, 0};
        int32_t lens[2] = {-1, -1};
        auto vecBatch1 = adaptor.Trans2VectorBatch(addresses, lens);
        auto v0 = reinterpret_cast<Vector<int32_t> *>(vecBatch1->Get(0));
        auto v1 = reinterpret_cast<Vector<Decimal128> *>(vecBatch1->Get(1));
        EXPECT_TRUE(v0->IsNull(0));
        EXPECT_TRUE(v1->IsNull(0));
    }

    {
        OneRowAdaptor adaptor;
        adaptor.Init({ DataTypeId::OMNI_INT, DataTypeId::OMNI_VARCHAR });

        int32_t data1 = 32;
        std::string hello = "hello";
        uintptr_t addresses[2] = {reinterpret_cast<uintptr_t>(&data1),
                                  reinterpret_cast<uintptr_t>(hello.data())};
        int32_t lens[2] = {0, 5};
        auto vecBatch1 = adaptor.Trans2VectorBatch(addresses, lens);
        auto v0 = reinterpret_cast<Vector<int32_t> *>(vecBatch1->Get(0));
        auto v1 = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vecBatch1->Get(1));
        std::string_view strView = v1->GetValue(0);
        EXPECT_EQ(v0->GetValue(0), data1);
        auto len = strView.size();
        EXPECT_EQ(len, lens[1]);
        std::string str(reinterpret_cast<const char *>(strView.data()), len);
        EXPECT_EQ(hello.compare(str), 0);
    }
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_full_expr_by_proces_row)
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
    uint32_t colSize = 4;

    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), IntType() }));
    DataTypes aggOutputTypes(std::vector<DataTypePtr>({ LongType(), IntType() }));

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
    std::vector<omniruntime::expressions::Expr *> aggFilters;
    aggFilters.reserve(0);

    auto hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, aggFilters, sourceTypes,
        aggOutputTypesWrap, aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));
    uintptr_t dataAddress[colSize];
    int32_t dataLens[colSize];

    for (uint32_t i = 0; i < dataSize; ++i) {
        dataAddress[0] = reinterpret_cast<uintptr_t>(&(data1[i]));
        dataAddress[1] = reinterpret_cast<uintptr_t>(&(data2[i]));
        dataAddress[2] = reinterpret_cast<uintptr_t>(&(data3[i]));
        dataAddress[3] = reinterpret_cast<uintptr_t>(&(data4[i]));
        dataLens[0] = 0;
        dataLens[1] = 0;
        dataLens[2] = 0;
        dataLens[3] = 0;
        hashAggWithExprOperator->ProcessRow(dataAddress, dataLens);
    }

    VectorBatch *outputVecBatch = nullptr;
    hashAggWithExprOperator->GetOutput(&outputVecBatch);

    int64_t expData1[] = {2};
    int32_t expData2[] = {10};
    int64_t expData3[] = {180};
    int32_t expData4[] = {76};
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), IntType(), LongType(), IntType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    delete hashAggWithExprOperatorFactory;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}

TEST(AggregationWithExprOperatorTest, test_agg_sum_exprFilter)
{
    ConfigUtil::SetSupportExprFilterRule(SupportExprFilterRule::EXPR_FILTER);

    const int32_t dataSize = 8;
    const int32_t groupByNum = 0;
    const int32_t expectDataSize = 1;
    // prepare data
    // sum(c1*5), sum(c3) group by c0%3, c2 => 2, 10, 180, 36
    int64_t data1[] = {2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L}; // c0
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};      // c1
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};              // c2
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};              // c3
    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), IntType() }));
    DataTypes aggOutputTypes(std::vector<DataTypePtr>({ LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    std::vector<Expr *> groupByKeys = {};
    // aggKeys
    LiteralExpr *mulRight = new LiteralExpr(5, LongType());
    mulRight->longVal = 5;
    BinaryExpr *mulExpr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, new FieldExpr(1, LongType()), mulRight, LongType());
    std::vector<Expr *> aggKeys1 = { mulExpr };
    std::vector<std::vector<omniruntime::expressions::Expr *>> aggAllKeys = { aggKeys1 };
    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_SUM };
    std::vector<uint32_t> maskCols = { static_cast<uint32_t>(-1) };
    auto overflowConfig = new OverflowConfig();
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypes.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypes.size());
    std::vector<omniruntime::expressions::Expr *> aggFilters;
    auto *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, IntType()),
        new LiteralExpr(2000, IntType()), BooleanType());
    aggFilters.push_back(filterExpr);

    auto *aggWithExprOperatorFactory =
        new AggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, sourceTypes, aggOutputTypesWrap,
        aggFuncTypes, aggFilters, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *aggWithExprOperator =
        static_cast<AggregationWithExprOperator *>(CreateTestOperator(aggWithExprOperatorFactory));
    aggWithExprOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    aggWithExprOperator->GetOutput(&outputVecBatch);

    int64_t expData1[] = {180};
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType() }));
    VectorBatch *expectVecorBatch = CreateVectorBatch(expectTypes, expectDataSize, expData1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    Expr::DeleteExprs(aggFilters);
    omniruntime::op::Operator::DeleteOperator(aggWithExprOperator);
    delete aggWithExprOperatorFactory;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;


    ConfigUtil::SetSupportExprFilterRule(SupportExprFilterRule::NO_EXPR);
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_full_expr_filter)
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

    std::vector<omniruntime::expressions::Expr *> aggFilters;
    auto *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, IntType()),
        new LiteralExpr(2000, IntType()), BooleanType());
    aggFilters.push_back(filterExpr);
    aggFilters.push_back(nullptr);

    auto hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, aggFilters, sourceTypes,
        aggOutputTypesWrap, aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch;
    hashAggWithExprOperator->GetOutput(&outputVecBatch);

    int64_t expData1[] = {2};
    int32_t expData2[] = {10};
    int64_t expData3[] = {180};
    int32_t expData4[] = {76};
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), IntType(), LongType(), IntType() }));
    VectorBatch *expectVecorBatch =
        CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3, expData4);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    delete hashAggWithExprOperatorFactory;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
    Expr::DeleteExprs(aggFilters);
}

TEST(HashAggregationWithExprOperatorTest, test_agg_min_max_avg)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::NOT_SUPPORT);
    const int32_t dataSize = 8;
    std::string data0[] = {"IN", "IL", "FL", "TX", "IN", "IL", "FL", "TX"};
    std::string data1[] = {"F", "M", "F", "M", "F", "M", "F", "M"};
    std::string data2[] = {"W", "U", "U", "M", "W", "U", "U", "M"};
    int32_t data3[] = {0, 2, 5, 7, 0, 2, 5, 7};
    int32_t data4[] = {5, 3, 2, 6, 5, 3, 2, 6};
    int32_t data5[] = {1, 2, 3, 4, 1, 2, 3, 4};
    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ VarcharType(2), VarcharType(2), VarcharType(2), IntType(), IntType(), IntType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2, data3, data4, data5);

    std::vector<Expr *> groupByKeys = { new FieldExpr(0, VarcharType(2)), new FieldExpr(1, VarcharType(2)),
        new FieldExpr(2, VarcharType(2)), new FieldExpr(3, IntType()),
        new FieldExpr(4, IntType()),      new FieldExpr(5, IntType()) };
    uint32_t groupByNum = 6;

    // aggKeys
    std::vector<Expr *> aggKeys0 = { new FieldExpr(3, IntType()) };
    std::vector<Expr *> aggKeys1 = { new FieldExpr(3, IntType()) };
    std::vector<Expr *> aggKeys2 = { new FuncExpr("CAST", std::vector<Expr *> { new FieldExpr(3, IntType()) },
        LongType()) };
    std::vector<Expr *> aggKeys3 = { new FieldExpr(4, IntType()) };
    std::vector<Expr *> aggKeys4 = { new FieldExpr(4, IntType()) };
    std::vector<Expr *> aggKeys5 = { new FuncExpr("CAST", std::vector<Expr *> { new FieldExpr(4, IntType()) },
        LongType()) };
    std::vector<Expr *> aggKeys6 = { new FieldExpr(5, IntType()) };
    std::vector<Expr *> aggKeys7 = { new FieldExpr(5, IntType()) };
    std::vector<Expr *> aggKeys8 = { new FuncExpr("CAST", std::vector<Expr *> { new FieldExpr(5, IntType()) },
        LongType()) };
    std::vector<std::vector<omniruntime::expressions::Expr *>> aggAllKeys = { aggKeys0, aggKeys1, aggKeys2,
        aggKeys3, aggKeys4, aggKeys5,
        aggKeys6, aggKeys7, aggKeys8 };

    std::vector<omniruntime::expressions::Expr *> aggFilters {};
    DataTypes aggOutputTypes0(std::vector<DataTypePtr>({ IntType() }));
    DataTypes aggOutputTypes1(std::vector<DataTypePtr>({ IntType() }));
    DataTypes aggOutputTypes2(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    DataTypes aggOutputTypes3(std::vector<DataTypePtr>({ IntType() }));
    DataTypes aggOutputTypes4(std::vector<DataTypePtr>({ IntType() }));
    DataTypes aggOutputTypes5(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    DataTypes aggOutputTypes6(std::vector<DataTypePtr>({ IntType() }));
    DataTypes aggOutputTypes7(std::vector<DataTypePtr>({ IntType() }));
    DataTypes aggOutputTypes8(std::vector<DataTypePtr>({ DoubleType(), LongType() }));
    std::vector<DataTypes> aggOutputTypesWrap = { aggOutputTypes0, aggOutputTypes1, aggOutputTypes2,
        aggOutputTypes3, aggOutputTypes4, aggOutputTypes5,
        aggOutputTypes6, aggOutputTypes7, aggOutputTypes8 };
    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX,
        OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_MIN,
        OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX,
        OMNI_AGGREGATION_TYPE_AVG };
    auto maskCol = static_cast<uint32_t>(-1);
    std::vector<uint32_t> maskCols = {
        maskCol, maskCol, maskCol, maskCol, maskCol, maskCol, maskCol, maskCol, maskCol
    };
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypes.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypes.size());
    auto overflowConfig = new OverflowConfig();

    auto hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, aggFilters, sourceTypes,
        aggOutputTypesWrap, aggFuncTypes, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *hashAggWithExprOperator =
        dynamic_cast<HashAggregationWithExprOperator *>(CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    hashAggWithExprOperator->GetOutput(&outputVecBatch);

    const int32_t expectDataSize = 4;
    std::string expData0[] = { "FL", "IN", "IL", "TX" };
    std::string expData1[] = { "F", "F", "M", "M" };
    std::string expData2[] = { "U", "W", "U", "M" };
    int32_t expData3[] = {5, 0, 2, 7};
    int32_t expData4[] = {2, 5, 3, 6};
    int32_t expData5[] = {3, 1, 2, 4};
    int32_t expData6[] = {5, 0, 2, 7};
    int32_t expData7[] = {5, 0, 2, 7};
    double expData8[] = {10, 0, 4, 14};
    int64_t expData9[] = {2, 2, 2, 2};
    int32_t expData10[] = {2, 5, 3, 6};
    int32_t expData11[] = {2, 5, 3, 6};
    double expData12[] = {4, 10, 6, 12};
    int64_t expData13[] = {2, 2, 2, 2};
    int32_t expData14[] = {3, 1, 2, 4};
    int32_t expData15[] = {3, 1, 2, 4};
    double expData16[] = {6, 2, 4, 8};
    int64_t expData17[] = {2, 2, 2, 2};
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(2), VarcharType(2), VarcharType(2), IntType(),
        IntType(), IntType(), IntType(), IntType(), DoubleType(), LongType(), IntType(), IntType(), DoubleType(),
        LongType(), IntType(), IntType(), DoubleType(), LongType() }));
    VectorBatch *expectVecorBatch = CreateVectorBatch(expectTypes, expectDataSize, expData0, expData1, expData2,
        expData3, expData4, expData5, expData6, expData7, expData8, expData9, expData10, expData11, expData12,
        expData13, expData14, expData15, expData16, expData17);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    Expr::DeleteExprs(aggFilters);
    omniruntime::op::Operator::DeleteOperator(hashAggWithExprOperator);
    delete hashAggWithExprOperatorFactory;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
}

TEST(AggregationWithExprOperatorTest, test_agg_sum_literal)
{
    const int32_t dataSize = 8;
    int64_t data1[] = {2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L}; // c0
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};      // c1
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};              // c2
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};              // c3
    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), IntType() }));
    DataTypes aggOutputTypes(std::vector<DataTypePtr>({ LongType(), LongType(), LongType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    std::vector<Expr *> groupByKeys = {};
    uint32_t groupByNum = 0;

    // aggKeys
    auto filedExpr0 = new FieldExpr(0, LongType());
    auto literalExpr1 = new LiteralExpr(0, LongType());
    literalExpr1->isNull = true;
    int64_t longVal = 1;
    auto literalExpr2 = new LiteralExpr(longVal, LongType());
    std::vector<Expr *> aggKeys0 = { filedExpr0 };
    std::vector<Expr *> aggKeys1 = { literalExpr1 };
    std::vector<Expr *> aggKeys2 = { literalExpr2 };
    std::vector<std::vector<omniruntime::expressions::Expr *>> aggAllKeys = { aggKeys0, aggKeys1, aggKeys2 };
    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_SUM };
    std::vector<uint32_t> maskCols = { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1),
        static_cast<uint32_t>(-1) };
    auto overflowConfig = new OverflowConfig();
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypes.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypes.size());
    std::vector<omniruntime::expressions::Expr *> aggFilters(3, nullptr);

    auto *aggWithExprOperatorFactory =
        new AggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggAllKeys, sourceTypes, aggOutputTypesWrap,
        aggFuncTypes, aggFilters, maskCols, inputRawWrap, outputPartialWrap, overflowConfig);
    auto *aggWithExprOperator =
        static_cast<AggregationWithExprOperator *>(CreateTestOperator(aggWithExprOperatorFactory));
    aggWithExprOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    aggWithExprOperator->GetOutput(&outputVecBatch);

    const int32_t expectDataSize = 1;
    int64_t expData1[] = { 100 };
    int64_t expData2[] = { 0 };
    int64_t expData3[] = { 8 };
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), LongType(), LongType() }));
    VectorBatch *expectVecorBatch = CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3);
    expectVecorBatch->Get(1)->SetNull(0);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecorBatch));

    Expr::DeleteExprs(groupByKeys);
    Expr::DeleteExprs(aggAllKeys);
    Expr::DeleteExprs(aggFilters);
    omniruntime::op::Operator::DeleteOperator(aggWithExprOperator);
    delete aggWithExprOperatorFactory;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete overflowConfig;
}
}
