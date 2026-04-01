/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include <gtest/gtest.h>

#include "operator/aggregation/aggregator/regr/regr_r2_aggregator.h"
#include "operator/aggregation/aggregator/regr/regr_state.h"
#include "operator/util/function_type.h"
#include "regr_test_util.h"
#include "util/config_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
using namespace omniruntime::op;
using namespace omniruntime::test;
using namespace omniruntime::type;

TEST(RegrR2AggregatorTest, GetStateSize)
{
    DataTypes inRaw({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_R2, inRaw, outF, ch, true, false);
    EXPECT_EQ(agg->GetStateSize(), sizeof(RegrState));
}

TEST(RegrR2AggregatorTest, FinalResultHashAgg)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_DOUBLE_EQ(RunRegrAndGetDouble(OMNI_AGGREGATION_TYPE_REGR_R2), 1.0);
}

TEST(RegrR2AggregatorTest, FinalResultNonGroup)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_DOUBLE_EQ(RunNonGroupRegrGetDouble(OMNI_AGGREGATION_TYPE_REGR_R2), 1.0);
}

TEST(RegrR2AggregatorTest, InitStateThenExtractIsNull)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_TRUE(RegrUtFinalIsNullAfterInitOnly(OMNI_AGGREGATION_TYPE_REGR_R2));
}

TEST(RegrR2AggregatorTest, DirectRawFinalMatchesReference)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch();
    EXPECT_DOUBLE_EQ(RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_R2, yx), 1.0);
    VectorHelper::FreeVecBatch(yx);
}

TEST(RegrR2AggregatorTest, SpillUnspillMatchesDirect)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch();
    double direct = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_R2, yx);
    double viaSpill = RegrUtPearsonSpillUnspillFinal(OMNI_AGGREGATION_TYPE_REGR_R2, yx);
    EXPECT_DOUBLE_EQ(direct, viaSpill);
    VectorHelper::FreeVecBatch(yx);
}

TEST(RegrR2AggregatorTest, SpillUnspillConsumesSixVectors)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    DataTypes inRaw({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_R2, inRaw, outF, ch, true, false);
    agg->SetStateOffset(0);
    std::vector<uint8_t> buf(agg->GetStateSize());
    auto *yx = MakeRegrYxLinearBatch();
    agg->InitState(buf.data());
    agg->ProcessGroup(buf.data(), yx, 0, yx->GetRowCount());
    VectorBatch spillBatch(1);
    spillBatch.Append(new Vector<int64_t>(1));
    for (int i = 0; i < 5; ++i) {
        spillBatch.Append(new Vector<double>(1));
    }
    std::vector<BaseVector *> spillPtrs;
    for (int c = 0; c < 6; ++c) {
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
    EXPECT_EQ(vi, RegrUtPearsonSpillVectorIndexAfterUnspill());
    VectorHelper::FreeVecBatch(yx);
}

TEST(RegrR2AggregatorTest, GetSpillTypeLayout)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_R2, inD, outF, ch, true, false);
    auto ids = RegrUtGetSpillTypeIds(agg.get());
    ASSERT_EQ(ids.size(), 6U);
    EXPECT_EQ(ids[0], OMNI_LONG);
    for (size_t i = 1; i < ids.size(); ++i) {
        EXPECT_EQ(ids[i], OMNI_DOUBLE);
    }
}

TEST(RegrR2AggregatorTest, ExtractValuesBatchTwoIndependentStates)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    double a = 0.0;
    double b = 0.0;
    bool n0 = true;
    bool n1 = true;
    RegrUtExtractValuesBatchTwoStates(OMNI_AGGREGATION_TYPE_REGR_R2, &a, &b, &n0, &n1);
    EXPECT_FALSE(n0);
    EXPECT_FALSE(n1);
    EXPECT_DOUBLE_EQ(a, 1.0);
    EXPECT_DOUBLE_EQ(b, 1.0);
}

TEST(RegrR2AggregatorTest, MergePartialSingleRowEqualsRawShortBatch)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch(4);
    double raw = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_R2, yx);
    VectorHelper::FreeVecBatch(yx);
    double merged = RegrUtPearsonMergeFromPartialRow(OMNI_AGGREGATION_TYPE_REGR_R2, 4);
    EXPECT_DOUBLE_EQ(raw, merged);
}

TEST(RegrR2AggregatorTest, AlignAggSchemaEmpty)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_R2, inD, outF, ch, true, false);
    VectorBatch in(0);
    VectorBatch out(0);
    agg->AlignAggSchema(&out, &in);
    ASSERT_EQ(out.GetVectorCount(), 6);
    out.FreeAllVectors();
}

TEST(RegrR2AggregatorTest, AlignAggSchemaRawOneRowR2PartialLayout)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outPartial(
        {LongType(), DoubleType(), DoubleType(), DoubleType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_R2, inD, outPartial, ch, true, true);
    auto *batchIn = new VectorBatch(1);
    auto *yv = new Vector<double>(1);
    auto *xv = new Vector<double>(1);
    yv->SetValue(0, 5.0);
    xv->SetValue(0, 2.0);
    batchIn->Append(yv);
    batchIn->Append(xv);
    auto *batchOut = new VectorBatch(0);
    agg->AlignAggSchema(batchOut, batchIn);
    EXPECT_EQ(static_cast<Vector<int64_t> *>(batchOut->Get(0))->GetValue(0), 1);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(1))->GetValue(0), 5.0);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(2))->GetValue(0), 2.0);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(4))->GetValue(0), 0.0);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(5))->GetValue(0), 0.0);
    VectorHelper::FreeVecBatch(batchIn);
    VectorHelper::FreeVecBatch(batchOut);
}

} // namespace omniruntime
