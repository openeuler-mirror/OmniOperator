/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include <gtest/gtest.h>

#include "operator/aggregation/aggregator/regr/regr_slope_aggregator.h"
#include "operator/aggregation/aggregator/regr/regr_state.h"
#include "operator/util/function_type.h"
#include "regr_test_util.h"
#include "util/config_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
using namespace omniruntime::op;
using namespace omniruntime::test;
using namespace omniruntime::type;

TEST(RegrSlopeAggregatorTest, GetStateSize)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SLOPE, inD, outF, ch, true, false);
    EXPECT_EQ(agg->GetStateSize(), sizeof(RegrSlopeInterceptState));
}

TEST(RegrSlopeAggregatorTest, FinalResultHashAgg)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_DOUBLE_EQ(RunRegrAndGetDouble(OMNI_AGGREGATION_TYPE_REGR_SLOPE), 2.0);
}

TEST(RegrSlopeAggregatorTest, FinalResultNonGroup)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_DOUBLE_EQ(RunNonGroupRegrGetDouble(OMNI_AGGREGATION_TYPE_REGR_SLOPE), 2.0);
}

TEST(RegrSlopeAggregatorTest, InitStateThenExtractIsNull)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_TRUE(RegrUtFinalIsNullAfterInitOnly(OMNI_AGGREGATION_TYPE_REGR_SLOPE));
}

TEST(RegrSlopeAggregatorTest, SpillUnspillMatchesDirect)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch();
    double direct = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SLOPE, yx);
    double viaSpill = RegrUtSlopeFamilySpillUnspillFinal(OMNI_AGGREGATION_TYPE_REGR_SLOPE, yx);
    EXPECT_DOUBLE_EQ(direct, viaSpill);
    VectorHelper::FreeVecBatch(yx);
}

TEST(RegrSlopeAggregatorTest, GetSpillTypeLayout)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SLOPE, inD, outF, ch, true, false);
    auto ids = RegrUtGetSpillTypeIds(agg.get());
    ASSERT_EQ(ids.size(), 7U);
    for (auto id : ids) {
        EXPECT_EQ(id, OMNI_DOUBLE);
    }
}

TEST(RegrSlopeAggregatorTest, ExtractValuesBatchMatchesDirectSlices)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    double a = 0.0;
    double b = 0.0;
    bool n0 = true;
    bool n1 = true;
    RegrUtExtractValuesBatchTwoStates(OMNI_AGGREGATION_TYPE_REGR_SLOPE, &a, &b, &n0, &n1);
    auto *s0 = MakeRegrYxLinearSlice(0, 3);
    auto *s1 = MakeRegrYxLinearSlice(3, 2);
    double e0 = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SLOPE, s0);
    double e1 = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SLOPE, s1);
    VectorHelper::FreeVecBatch(s0);
    VectorHelper::FreeVecBatch(s1);
    EXPECT_FALSE(n0);
    EXPECT_FALSE(n1);
    EXPECT_DOUBLE_EQ(a, e0);
    EXPECT_DOUBLE_EQ(b, e1);
    EXPECT_DOUBLE_EQ(a, 2.0);
    EXPECT_DOUBLE_EQ(b, 2.0);
}

TEST(RegrSlopeAggregatorTest, SpillUnspillConsumesSevenVectors)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SLOPE, inD, outF, ch, true, false);
    agg->SetStateOffset(0);
    std::vector<uint8_t> buf(agg->GetStateSize());
    auto *yx = MakeRegrYxLinearBatch();
    agg->InitState(buf.data());
    agg->ProcessGroup(buf.data(), yx, 0, yx->GetRowCount());
    VectorBatch spillBatch(1);
    for (int i = 0; i < 7; ++i) {
        spillBatch.Append(new Vector<double>(1));
    }
    std::vector<BaseVector *> spillPtrs;
    for (int c = 0; c < 7; ++c) {
        spillPtrs.push_back(spillBatch.Get(c));
    }
    std::vector<AggregateState *> gs = {buf.data()};
    agg->ExtractValuesForSpill(gs, spillPtrs);
    std::vector<uint8_t> buf2(agg->GetStateSize());
    agg->InitState(buf2.data());
    UnspillRowInfo ur{buf2.data(), &spillBatch, 0};
    std::vector<UnspillRowInfo> rows = {ur};
    int32_t vi = 0;
    agg->ProcessGroupUnspill(rows, 1, vi);
    EXPECT_EQ(vi, RegrUtSlopeFamilySpillVectorIndexAfterUnspill());
    VectorHelper::FreeVecBatch(yx);
}

TEST(RegrSlopeAggregatorTest, MergePartialEqualsRaw)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch(4);
    double raw = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SLOPE, yx);
    VectorHelper::FreeVecBatch(yx);
    double merged = RegrUtSlopeFamilyMergeFromPartialRow(OMNI_AGGREGATION_TYPE_REGR_SLOPE, 4);
    EXPECT_DOUBLE_EQ(raw, merged);
}

TEST(RegrSlopeAggregatorTest, AlignAggSchemaEmpty)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SLOPE, inD, outF, ch, true, false);
    VectorBatch in(0);
    VectorBatch out(0);
    agg->AlignAggSchema(&out, &in);
    ASSERT_EQ(out.GetVectorCount(), 7);
    out.FreeAllVectors();
}

TEST(RegrSlopeAggregatorTest, AlignAggSchemaRawOneRow)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outPartial({DoubleType(), DoubleType(), DoubleType(), DoubleType(), DoubleType(), DoubleType(),
        DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SLOPE, inD, outPartial, ch, true, true);
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
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(3))->GetValue(0), 0.0);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(4))->GetValue(0), 1.0);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(5))->GetValue(0), 2.0);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(6))->GetValue(0), 0.0);
    VectorHelper::FreeVecBatch(batchIn);
    VectorHelper::FreeVecBatch(batchOut);
}

} // namespace omniruntime
