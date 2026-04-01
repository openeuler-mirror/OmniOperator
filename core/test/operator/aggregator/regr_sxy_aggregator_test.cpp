/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include <gtest/gtest.h>

#include "operator/aggregation/aggregator/regr/regr_sxy_aggregator.h"
#include "operator/aggregation/aggregator/regr/regr_state.h"
#include "operator/util/function_type.h"
#include "regr_test_util.h"
#include "util/config_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
using namespace omniruntime::op;
using namespace omniruntime::test;
using namespace omniruntime::type;

TEST(RegrSxyAggregatorTest, GetStateSize)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SXY, inD, outF, ch, true, false);
    EXPECT_EQ(agg->GetStateSize(), sizeof(RegrCov4State));
}

TEST(RegrSxyAggregatorTest, FinalResultHashAgg)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_DOUBLE_EQ(RunRegrAndGetDouble(OMNI_AGGREGATION_TYPE_REGR_SXY), 20.0);
}

TEST(RegrSxyAggregatorTest, FinalResultNonGroup)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_DOUBLE_EQ(RunNonGroupRegrGetDouble(OMNI_AGGREGATION_TYPE_REGR_SXY), 20.0);
}

TEST(RegrSxyAggregatorTest, InitStateThenExtractIsNull)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_TRUE(RegrUtFinalIsNullAfterInitOnly(OMNI_AGGREGATION_TYPE_REGR_SXY));
}

TEST(RegrSxyAggregatorTest, SpillUnspillMatchesDirect)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch();
    double direct = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SXY, yx);
    double viaSpill = RegrUtCov4SpillUnspillFinal(OMNI_AGGREGATION_TYPE_REGR_SXY, yx);
    EXPECT_DOUBLE_EQ(direct, viaSpill);
    VectorHelper::FreeVecBatch(yx);
}

TEST(RegrSxyAggregatorTest, GetSpillTypeLayout)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SXY, inD, outF, ch, true, false);
    auto ids = RegrUtGetSpillTypeIds(agg.get());
    ASSERT_EQ(ids.size(), 4U);
    for (auto id : ids) {
        EXPECT_EQ(id, OMNI_DOUBLE);
    }
}

TEST(RegrSxyAggregatorTest, ExtractValuesBatchMatchesDirectSlices)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    double a = 0.0;
    double b = 0.0;
    bool n0 = true;
    bool n1 = true;
    RegrUtExtractValuesBatchTwoStates(OMNI_AGGREGATION_TYPE_REGR_SXY, &a, &b, &n0, &n1);
    auto *s0 = MakeRegrYxLinearSlice(0, 3);
    auto *s1 = MakeRegrYxLinearSlice(3, 2);
    double e0 = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SXY, s0);
    double e1 = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SXY, s1);
    VectorHelper::FreeVecBatch(s0);
    VectorHelper::FreeVecBatch(s1);
    EXPECT_FALSE(n0);
    EXPECT_FALSE(n1);
    EXPECT_DOUBLE_EQ(a, e0);
    EXPECT_DOUBLE_EQ(b, e1);
}

TEST(RegrSxyAggregatorTest, SpillUnspillConsumesFourVectors)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SXY, inD, outF, ch, true, false);
    agg->SetStateOffset(0);
    std::vector<uint8_t> buf(agg->GetStateSize());
    auto *yx = MakeRegrYxLinearBatch();
    agg->InitState(buf.data());
    agg->ProcessGroup(buf.data(), yx, 0, yx->GetRowCount());
    VectorBatch spillBatch(1);
    for (int i = 0; i < 4; ++i) {
        spillBatch.Append(new Vector<double>(1));
    }
    std::vector<BaseVector *> spillPtrs = {
        spillBatch.Get(0), spillBatch.Get(1), spillBatch.Get(2), spillBatch.Get(3)};
    std::vector<AggregateState *> gs = {buf.data()};
    agg->ExtractValuesForSpill(gs, spillPtrs);
    std::vector<uint8_t> buf2(agg->GetStateSize());
    agg->InitState(buf2.data());
    UnspillRowInfo ur{buf2.data(), &spillBatch, 0};
    std::vector<UnspillRowInfo> rows = {ur};
    int32_t vi = 0;
    agg->ProcessGroupUnspill(rows, 1, vi);
    EXPECT_EQ(vi, RegrUtCov4SpillVectorIndexAfterUnspill());
    VectorHelper::FreeVecBatch(yx);
}

TEST(RegrSxyAggregatorTest, MergePartialEqualsRaw)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch(4);
    double raw = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SXY, yx);
    VectorHelper::FreeVecBatch(yx);
    double merged = RegrUtCov4MergeFromPartialRow(OMNI_AGGREGATION_TYPE_REGR_SXY, 4);
    EXPECT_DOUBLE_EQ(raw, merged);
}

TEST(RegrSxyAggregatorTest, AlignAggSchemaEmpty)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SXY, inD, outF, ch, true, false);
    VectorBatch in(0);
    VectorBatch out(0);
    agg->AlignAggSchema(&out, &in);
    ASSERT_EQ(out.GetVectorCount(), 4);
    out.FreeAllVectors();
}

TEST(RegrSxyAggregatorTest, AlignAggSchemaRawOneRow)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outPartial({DoubleType(), DoubleType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SXY, inD, outPartial, ch, true, true);
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
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(1))->GetValue(0), 5.0);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(2))->GetValue(0), 2.0);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(3))->GetValue(0), 0.0);
    VectorHelper::FreeVecBatch(batchIn);
    VectorHelper::FreeVecBatch(batchOut);
}

} // namespace omniruntime
