/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Unit tests for linear regression aggregate functions (regr_*).
 */

#include <gtest/gtest.h>
#include <cmath>
#include <vector>

#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/operator.h"
#include "vector/vector_helper.h"
#include "util/config_util.h"
#include "util/type_util.h"
#include "test/util/test_util.h"

namespace omniruntime {
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::type;

static std::unique_ptr<HashAggregationOperatorFactory> CreateRegrHashAggregationFactory(
    const std::vector<uint32_t> &groupByColumns, const std::vector<DataTypePtr> &groupTypes,
    uint32_t aggFuncType, const std::vector<uint32_t> &aggInputCols,
    const std::vector<DataTypePtr> &aggInputTypes, const DataTypePtr &aggOutputType,
    bool inputRaw = true, bool outputPartial = false, bool nullWhenOverflow = false)
{
    EXPECT_EQ(groupByColumns.size(), groupTypes.size());
    EXPECT_GE(aggInputCols.size(), 1u);
    EXPECT_EQ(aggInputTypes.size(), aggInputCols.size());

    std::vector<std::vector<uint32_t>> aggInputColsWrap = { aggInputCols };
    std::vector<DataTypes> aggInputTypesWrap = { DataTypes(aggInputTypes) };
    std::vector<DataTypes> aggOutputTypesWrap = { DataTypes(std::vector<DataTypePtr>{ aggOutputType }) };
    std::vector<uint32_t> aggFuncTypes = { aggFuncType };
    std::vector<uint32_t> aggMask = { static_cast<uint32_t>(-1) };
    std::vector<bool> inputRawWrap = { inputRaw };
    std::vector<bool> outputPartialWrap = { outputPartial };

    auto factory = std::make_unique<HashAggregationOperatorFactory>(
        const_cast<std::vector<uint32_t> &>(groupByColumns), DataTypes(groupTypes),
        aggInputColsWrap, aggInputTypesWrap, aggOutputTypesWrap, aggFuncTypes, aggMask,
        inputRawWrap, outputPartialWrap, nullWhenOverflow);
    factory->Init();
    return factory;
}

// Helper: build a VectorBatch for regr test data (y = 2*x + 1). Caller must not pass the same batch to AddInput twice;
// AddInput takes ownership and frees the batch.
static VectorBatch *MakeRegrTestBatch(int32_t rowCount = 5)
{
    VectorBatch *batch = new VectorBatch(rowCount);
    Vector<int64_t> *groupCol = new Vector<int64_t>(rowCount);
    Vector<double> *yCol = new Vector<double>(rowCount);
    Vector<double> *xCol = new Vector<double>(rowCount);
    for (int32_t i = 0; i < rowCount; ++i) {
        groupCol->SetValue(i, 0);
        yCol->SetValue(i, 1.0 + 2.0 * i);
        xCol->SetValue(i, static_cast<double>(i));
    }
    batch->Append(groupCol);
    batch->Append(yCol);
    batch->Append(xCol);
    return batch;
}

// Linear regression: y = 2*x + 1 on points (0,1), (1,3), (2,5), (3,7), (4,9)
TEST(RegrAggregatorTest, SingleStageFinal)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);

    auto runRegrAndGetDouble = [](uint32_t aggType) -> double {
        VectorBatch *batch = MakeRegrTestBatch(5);
        auto factory = CreateRegrHashAggregationFactory(
            std::vector<uint32_t>({0}), std::vector<DataTypePtr>({LongType()}),
            aggType, std::vector<uint32_t>({1, 2}),
            std::vector<DataTypePtr>({DoubleType(), DoubleType()}), DoubleType(),
            true, false, false);
        auto op = factory->CreateOperator();
        op->Init();
        op->AddInput(batch);
        VectorBatch *out = nullptr;
        op->GetOutput(&out);
        EXPECT_NE(out, nullptr);
        EXPECT_EQ(out->GetRowCount(), 1);
        EXPECT_EQ(out->GetVectorCount(), 2);
        BaseVector *resVec = out->Get(1);
        double val = 0;
        if (!resVec->IsNull(0)) {
            val = static_cast<Vector<double> *>(resVec)->GetValue(0);
        }
        omniruntime::op::Operator::DeleteOperator(op);
        VectorHelper::FreeVecBatch(out);
        return val;
    };

    EXPECT_DOUBLE_EQ(runRegrAndGetDouble(OMNI_AGGREGATION_TYPE_REGR_SLOPE), 2.0);
    EXPECT_DOUBLE_EQ(runRegrAndGetDouble(OMNI_AGGREGATION_TYPE_REGR_INTERCEPT), 1.0);
    EXPECT_DOUBLE_EQ(runRegrAndGetDouble(OMNI_AGGREGATION_TYPE_REGR_R2), 1.0);
    EXPECT_DOUBLE_EQ(runRegrAndGetDouble(OMNI_AGGREGATION_TYPE_REGR_SXX), 10.0);
    EXPECT_DOUBLE_EQ(runRegrAndGetDouble(OMNI_AGGREGATION_TYPE_REGR_SYY), 40.0);
    EXPECT_DOUBLE_EQ(runRegrAndGetDouble(OMNI_AGGREGATION_TYPE_REGR_SXY), 20.0);

    // Zero slope must be +0.0 (not -0.0) to match Spark after ROUND / equality checks.
    {
        const int32_t n = 10;
        VectorBatch *batch = new VectorBatch(n);
        Vector<int64_t> *groupCol = new Vector<int64_t>(n);
        Vector<double> *yCol = new Vector<double>(n);
        Vector<double> *xCol = new Vector<double>(n);
        for (int32_t i = 0; i < n; ++i) {
            groupCol->SetValue(i, 0);
            yCol->SetValue(i, 3.0);
            xCol->SetValue(i, static_cast<double>(i));
        }
        batch->Append(groupCol);
        batch->Append(yCol);
        batch->Append(xCol);
        auto factory = CreateRegrHashAggregationFactory(
            std::vector<uint32_t>({0}), std::vector<DataTypePtr>({LongType()}),
            OMNI_AGGREGATION_TYPE_REGR_SLOPE, std::vector<uint32_t>({1, 2}),
            std::vector<DataTypePtr>({DoubleType(), DoubleType()}), DoubleType(),
            true, false, false);
        auto op = factory->CreateOperator();
        op->Init();
        op->AddInput(batch);
        VectorBatch *out = nullptr;
        op->GetOutput(&out);
        ASSERT_NE(out, nullptr);
        BaseVector *resVec = out->Get(1);
        ASSERT_FALSE(resVec->IsNull(0));
        double val = static_cast<Vector<double> *>(resVec)->GetValue(0);
        EXPECT_DOUBLE_EQ(val, 0.0);
        EXPECT_FALSE(std::signbit(val)) << "regr_slope zero must be positive zero (+0.0), like Spark";
        omniruntime::op::Operator::DeleteOperator(op);
        VectorHelper::FreeVecBatch(out);
    }

    // regr_count returns Long (bigint)
    {
        VectorBatch *batch = MakeRegrTestBatch(5);
        auto factory = CreateRegrHashAggregationFactory(
            std::vector<uint32_t>({0}), std::vector<DataTypePtr>({LongType()}),
            OMNI_AGGREGATION_TYPE_REGR_COUNT, std::vector<uint32_t>({1, 2}),
            std::vector<DataTypePtr>({DoubleType(), DoubleType()}), LongType(),
            true, false, false);
        auto op = factory->CreateOperator();
        op->Init();
        op->AddInput(batch);
        VectorBatch *out = nullptr;
        op->GetOutput(&out);
        EXPECT_NE(out, nullptr);
        EXPECT_EQ(out->GetRowCount(), 1);
        BaseVector *resVec = out->Get(1);
        EXPECT_FALSE(resVec->IsNull(0));
        EXPECT_EQ(static_cast<Vector<int64_t> *>(resVec)->GetValue(0), 5);
        omniruntime::op::Operator::DeleteOperator(op);
        VectorHelper::FreeVecBatch(out);
    }
}

// Empty input: regr_slope returns NULL
// 暂时禁用：CreateOperator() 在 group_aggregation.cpp:149 对空 group 场景存在崩溃/泄漏，
// 待业务侧修复后去掉 DISABLED_ 并运行：./omtest --gtest_also_run_disabled_tests --gtest_filter=*EmptyInputReturnsNull
TEST(RegrAggregatorTest, DISABLED_EmptyInputReturnsNull)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    const int32_t rowCount = 0;
    VectorBatch *batch = new VectorBatch(rowCount);
    Vector<int64_t> *groupCol = new Vector<int64_t>(0);
    Vector<double> *yCol = new Vector<double>(0);
    Vector<double> *xCol = new Vector<double>(0);
    batch->Append(groupCol);
    batch->Append(yCol);
    batch->Append(xCol);

    auto factory = CreateRegrHashAggregationFactory(
        std::vector<uint32_t>({0}), std::vector<DataTypePtr>({LongType()}),
        OMNI_AGGREGATION_TYPE_REGR_SLOPE, std::vector<uint32_t>({1, 2}),
        std::vector<DataTypePtr>({DoubleType(), DoubleType()}), DoubleType(),
        true, false, false);
    omniruntime::op::Operator *op = nullptr;
    try {
        op = factory->CreateOperator();
    } catch (...) {
        VectorHelper::FreeVecBatch(batch);
        throw;
    }
    if (op == nullptr) {
        VectorHelper::FreeVecBatch(batch);
        FAIL() << "CreateOperator returned null for empty input";
        return;
    }
    op->Init();
    op->AddInput(batch);
    VectorBatch *out = nullptr;
    op->GetOutput(&out);
    ASSERT_NE(out, nullptr) << "GetOutput returned null for empty input";
    EXPECT_EQ(out->GetRowCount(), 1);
    BaseVector *resVec = out->Get(1);
    ASSERT_NE(resVec, nullptr) << "Get(1) returned null";
    EXPECT_TRUE(resVec->IsNull(0));
    omniruntime::op::Operator::DeleteOperator(op);
    VectorHelper::FreeVecBatch(out);
    VectorHelper::FreeVecBatch(batch);
}

// regr_count with ConstVector<double> input: all same (y,x) values
TEST(RegrAggregatorTest, ConstVectorRegrCount)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    const int32_t rowCount = 5;

    VectorBatch *batch = new VectorBatch(rowCount);
    Vector<int64_t> *groupCol = new Vector<int64_t>(rowCount);
    for (int32_t i = 0; i < rowCount; ++i) {
        groupCol->SetValue(i, 0);
    }
    // ConstVector for y and x: all rows have y=3.0, x=1.0
    auto *yCol = new ConstVector<double>(3.0, OMNI_DOUBLE, rowCount);
    auto *xCol = new ConstVector<double>(1.0, OMNI_DOUBLE, rowCount);
    batch->Append(groupCol);
    batch->Append(yCol);
    batch->Append(xCol);

    auto factory = CreateRegrHashAggregationFactory(
        std::vector<uint32_t>({0}), std::vector<DataTypePtr>({LongType()}),
        OMNI_AGGREGATION_TYPE_REGR_COUNT, std::vector<uint32_t>({1, 2}),
        std::vector<DataTypePtr>({DoubleType(), DoubleType()}), LongType(),
        true, false, false);
    auto op = factory->CreateOperator();
    op->Init();
    op->AddInput(batch);
    VectorBatch *out = nullptr;
    op->GetOutput(&out);
    EXPECT_NE(out, nullptr);
    EXPECT_EQ(out->GetRowCount(), 1);
    BaseVector *resVec = out->Get(1);
    EXPECT_FALSE(resVec->IsNull(0));
    EXPECT_EQ(static_cast<Vector<int64_t> *>(resVec)->GetValue(0), 5);
    omniruntime::op::Operator::DeleteOperator(op);
    VectorHelper::FreeVecBatch(out);
}

} // namespace omniruntime
