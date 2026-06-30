/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include <gtest/gtest.h>

#include "operator/aggregation/aggregator/regr/regr_avgy_aggregator.h"
#include "operator/aggregation/aggregator/regr/regr_state.h"
#include "operator/util/function_type.h"
#include "regr_test_util.h"
#include "util/config_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
using namespace omniruntime::op;
using namespace omniruntime::test;
using namespace omniruntime::type;

TEST(RegrAvgYAggregatorTest, GetStateSize)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_AVGY, inD, outF, ch, true, false);
    EXPECT_EQ(agg->GetStateSize(), sizeof(RegrState));
}

TEST(RegrAvgYAggregatorTest, FinalResultHashAgg)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_DOUBLE_EQ(RunRegrAndGetDouble(OMNI_AGGREGATION_TYPE_REGR_AVGY), 5.0);
}

TEST(RegrAvgYAggregatorTest, FinalResultNonGroup)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_DOUBLE_EQ(RunNonGroupRegrGetDouble(OMNI_AGGREGATION_TYPE_REGR_AVGY), 5.0);
}

TEST(RegrAvgYAggregatorTest, InitStateThenExtractIsNull)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_TRUE(RegrUtFinalIsNullAfterInitOnly(OMNI_AGGREGATION_TYPE_REGR_AVGY));
}

TEST(RegrAvgYAggregatorTest, SpillUnspillMatchesDirect)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch();
    double direct = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_AVGY, yx);
    double viaSpill = RegrUtPearsonSpillUnspillFinal(OMNI_AGGREGATION_TYPE_REGR_AVGY, yx);
    EXPECT_DOUBLE_EQ(direct, viaSpill);
    VectorHelper::FreeVecBatch(yx);
}

TEST(RegrAvgYAggregatorTest, GetSpillTypeLayout)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_AVGY, inD, outF, ch, true, false);
    auto ids = RegrUtGetSpillTypeIds(agg.get());
    ASSERT_EQ(ids.size(), 6U);
    EXPECT_EQ(ids[0], OMNI_LONG);
    for (size_t i = 1; i < ids.size(); ++i) {
        EXPECT_EQ(ids[i], OMNI_DOUBLE);
    }
}

TEST(RegrAvgYAggregatorTest, ExtractValuesBatchMatchesDirectSlices)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    double a = 0.0;
    double b = 0.0;
    bool n0 = true;
    bool n1 = true;
    RegrUtExtractValuesBatchTwoStates(OMNI_AGGREGATION_TYPE_REGR_AVGY, &a, &b, &n0, &n1);
    auto *s0 = MakeRegrYxLinearSlice(0, 3);
    auto *s1 = MakeRegrYxLinearSlice(3, 2);
    double e0 = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_AVGY, s0);
    double e1 = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_AVGY, s1);
    VectorHelper::FreeVecBatch(s0);
    VectorHelper::FreeVecBatch(s1);
    EXPECT_FALSE(n0);
    EXPECT_FALSE(n1);
    EXPECT_DOUBLE_EQ(a, e0);
    EXPECT_DOUBLE_EQ(b, e1);
    EXPECT_DOUBLE_EQ(a, 3.0);
    EXPECT_DOUBLE_EQ(b, 8.0);
}

TEST(RegrAvgYAggregatorTest, MergePartialEqualsRaw)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch(4);
    double raw = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_AVGY, yx);
    VectorHelper::FreeVecBatch(yx);
    double merged = RegrUtPearsonMergeFromPartialRow(OMNI_AGGREGATION_TYPE_REGR_AVGY, 4);
    EXPECT_DOUBLE_EQ(raw, merged);
}

TEST(RegrAvgYAggregatorTest, AlignAggSchemaEmpty)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_AVGY, inD, outF, ch, true, false);
    VectorBatch in(0);
    VectorBatch out(0);
    agg->AlignAggSchema(&out, &in);
    ASSERT_EQ(out.GetVectorCount(), 6);
    out.FreeAllVectors();
}

TEST(RegrAvgYAggregatorTest, AlignAggSchemaRawOneRow)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outPartial(
        {DoubleType(), DoubleType(), DoubleType(), DoubleType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_AVGY, inD, outPartial, ch, true, true);
    auto *batchIn = new VectorBatch(1);
    auto *yv = new Vector<double>(1);
    auto *xv = new Vector<double>(1);
    yv->SetValue(0, 5.0);
    xv->SetValue(0, 2.0);
    batchIn->Append(yv);
    batchIn->Append(xv);
    auto *batchOut = new VectorBatch(0);
    agg->AlignAggSchema(batchOut, batchIn);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(0))->GetValue(0), 1.0);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(1))->GetValue(0), 2.0);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(2))->GetValue(0), 5.0);
    VectorHelper::FreeVecBatch(batchIn);
    VectorHelper::FreeVecBatch(batchOut);
}

} // namespace omniruntime
