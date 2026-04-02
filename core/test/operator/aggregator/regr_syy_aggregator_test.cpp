/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include <gtest/gtest.h>

#include "operator/aggregation/aggregator/regr/regr_syy_aggregator.h"
#include "operator/aggregation/aggregator/regr/regr_state.h"
#include "operator/util/function_type.h"
#include "regr_test_util.h"
#include "util/config_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
using namespace omniruntime::op;
using namespace omniruntime::test;
using namespace omniruntime::type;

TEST(RegrSyyAggregatorTest, GetStateSize)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SYY, inD, outF, ch, true, false);
    EXPECT_EQ(agg->GetStateSize(), sizeof(RegrVarPopState));
}

TEST(RegrSyyAggregatorTest, FinalResultHashAgg)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_DOUBLE_EQ(RunRegrAndGetDouble(OMNI_AGGREGATION_TYPE_REGR_SYY), 40.0);
}

TEST(RegrSyyAggregatorTest, FinalResultNonGroup)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_DOUBLE_EQ(RunNonGroupRegrGetDouble(OMNI_AGGREGATION_TYPE_REGR_SYY), 40.0);
}

TEST(RegrSyyAggregatorTest, InitStateThenExtractIsNull)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_TRUE(RegrUtFinalIsNullAfterInitOnly(OMNI_AGGREGATION_TYPE_REGR_SYY));
}

TEST(RegrSyyAggregatorTest, SpillUnspillMatchesDirect)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch();
    double direct = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SYY, yx);
    double viaSpill = RegrUtVarPopSpillUnspillFinal(OMNI_AGGREGATION_TYPE_REGR_SYY, yx);
    EXPECT_DOUBLE_EQ(direct, viaSpill);
    VectorHelper::FreeVecBatch(yx);
}

TEST(RegrSyyAggregatorTest, GetSpillTypeLayout)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SYY, inD, outF, ch, true, false);
    auto ids = RegrUtGetSpillTypeIds(agg.get());
    ASSERT_EQ(ids.size(), 3U);
    for (auto id : ids) {
        EXPECT_EQ(id, OMNI_DOUBLE);
    }
}

TEST(RegrSyyAggregatorTest, ExtractValuesBatchMatchesDirectSlices)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    double a = 0.0;
    double b = 0.0;
    bool n0 = true;
    bool n1 = true;
    RegrUtExtractValuesBatchTwoStates(OMNI_AGGREGATION_TYPE_REGR_SYY, &a, &b, &n0, &n1);
    auto *s0 = MakeRegrYxLinearSlice(0, 3);
    auto *s1 = MakeRegrYxLinearSlice(3, 2);
    double e0 = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SYY, s0);
    double e1 = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SYY, s1);
    VectorHelper::FreeVecBatch(s0);
    VectorHelper::FreeVecBatch(s1);
    EXPECT_FALSE(n0);
    EXPECT_FALSE(n1);
    EXPECT_DOUBLE_EQ(a, e0);
    EXPECT_DOUBLE_EQ(b, e1);
}

TEST(RegrSyyAggregatorTest, MergePartialEqualsRaw)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch(4);
    double raw = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SYY, yx);
    VectorHelper::FreeVecBatch(yx);
    double merged = RegrUtVarPopMergeFromPartialRow(OMNI_AGGREGATION_TYPE_REGR_SYY, 4);
    EXPECT_DOUBLE_EQ(raw, merged);
}

TEST(RegrSyyAggregatorTest, AlignAggSchemaEmpty)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SYY, inD, outF, ch, true, false);
    VectorBatch in(0);
    VectorBatch out(0);
    agg->AlignAggSchema(&out, &in);
    ASSERT_EQ(out.GetVectorCount(), 3);
    out.FreeAllVectors();
}

TEST(RegrSyyAggregatorTest, AlignAggSchemaRawOneRow)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outPartial({DoubleType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SYY, inD, outPartial, ch, true, true);
    auto *batchIn = new VectorBatch(1);
    auto *y = new Vector<double>(1);
    auto *x = new Vector<double>(1);
    y->SetValue(0, 5.0);
    x->SetValue(0, 2.0);
    batchIn->Append(y);
    batchIn->Append(x);
    auto *batchOut = new VectorBatch(0);
    agg->AlignAggSchema(batchOut, batchIn);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(0))->GetValue(0), 1.0);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(1))->GetValue(0), 5.0);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(2))->GetValue(0), 0.0);
    VectorHelper::FreeVecBatch(batchIn);
    VectorHelper::FreeVecBatch(batchOut);
}

} // namespace omniruntime
