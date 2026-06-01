/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include <gtest/gtest.h>

#include "operator/aggregation/aggregator/regr/regr_sxx_aggregator.h"
#include "operator/aggregation/aggregator/regr/regr_state.h"
#include "operator/util/function_type.h"
#include "regr_test_util.h"
#include "util/config_util.h"
#include "vector/vector_helper.h"

namespace omniruntime {
using namespace omniruntime::exception;
using namespace omniruntime::op;
using namespace omniruntime::test;
using namespace omniruntime::type;

TEST(RegrSxxAggregatorTest, GetStateSize)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SXX, inD, outF, ch, true, false);
    EXPECT_EQ(agg->GetStateSize(), sizeof(RegrVarPopState));
}

TEST(RegrSxxAggregatorTest, FinalResultHashAgg)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_DOUBLE_EQ(RunRegrAndGetDouble(OMNI_AGGREGATION_TYPE_REGR_SXX), 10.0);
}

TEST(RegrSxxAggregatorTest, FinalResultNonGroup)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_DOUBLE_EQ(RunNonGroupRegrGetDouble(OMNI_AGGREGATION_TYPE_REGR_SXX), 10.0);
}

TEST(RegrSxxAggregatorTest, InitStateThenExtractIsNull)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    EXPECT_TRUE(RegrUtFinalIsNullAfterInitOnly(OMNI_AGGREGATION_TYPE_REGR_SXX));
}

TEST(RegrSxxAggregatorTest, SpillUnspillMatchesDirect)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch();
    double direct = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SXX, yx);
    double viaSpill = RegrUtVarPopSpillUnspillFinal(OMNI_AGGREGATION_TYPE_REGR_SXX, yx);
    EXPECT_DOUBLE_EQ(direct, viaSpill);
    VectorHelper::FreeVecBatch(yx);
}

TEST(RegrSxxAggregatorTest, GetSpillTypeLayout)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SXX, inD, outF, ch, true, false);
    auto ids = RegrUtGetSpillTypeIds(agg.get());
    ASSERT_EQ(ids.size(), 3U);
    for (auto id : ids) {
        EXPECT_EQ(id, OMNI_DOUBLE);
    }
}

TEST(RegrSxxAggregatorTest, ExtractValuesBatchMatchesDirectSlices)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    double a = 0.0;
    double b = 0.0;
    bool n0 = true;
    bool n1 = true;
    RegrUtExtractValuesBatchTwoStates(OMNI_AGGREGATION_TYPE_REGR_SXX, &a, &b, &n0, &n1);
    auto *s0 = MakeRegrYxLinearSlice(0, 3);
    auto *s1 = MakeRegrYxLinearSlice(3, 2);
    double e0 = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SXX, s0);
    double e1 = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SXX, s1);
    VectorHelper::FreeVecBatch(s0);
    VectorHelper::FreeVecBatch(s1);
    EXPECT_FALSE(n0);
    EXPECT_FALSE(n1);
    EXPECT_DOUBLE_EQ(a, e0);
    EXPECT_DOUBLE_EQ(b, e1);
}

TEST(RegrSxxAggregatorTest, SpillUnspillConsumesThreeVectors)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SXX, inD, outF, ch, true, false);
    agg->SetStateOffset(0);
    std::vector<uint8_t> buf(agg->GetStateSize());
    auto *yx = MakeRegrYxLinearBatch();
    agg->InitState(buf.data());
    agg->ProcessGroup(buf.data(), yx, 0, yx->GetRowCount());
    VectorBatch spillBatch(1);
    for (int i = 0; i < 3; ++i) {
        spillBatch.Append(new Vector<double>(1));
    }
    std::vector<BaseVector *> spillPtrs = {spillBatch.Get(0), spillBatch.Get(1), spillBatch.Get(2)};
    std::vector<AggregateState *> gs = {buf.data()};
    agg->ExtractValuesForSpill(gs, spillPtrs);
    std::vector<uint8_t> buf2(agg->GetStateSize());
    agg->InitState(buf2.data());
    UnspillRowInfo ur{buf2.data(), &spillBatch, 0};
    std::vector<UnspillRowInfo> rows = {ur};
    int32_t vi = 0;
    agg->ProcessGroupUnspill(rows, 1, vi);
    EXPECT_EQ(vi, RegrUtVarPopSpillVectorIndexAfterUnspill());
    VectorHelper::FreeVecBatch(yx);
}

TEST(RegrSxxAggregatorTest, MergePartialEqualsRaw)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *yx = MakeRegrYxLinearBatch(4);
    double raw = RegrUtDirectRawFinal(OMNI_AGGREGATION_TYPE_REGR_SXX, yx);
    VectorHelper::FreeVecBatch(yx);
    double merged = RegrUtVarPopMergeFromPartialRow(OMNI_AGGREGATION_TYPE_REGR_SXX, 4);
    EXPECT_DOUBLE_EQ(raw, merged);
}

TEST(RegrSxxAggregatorTest, IdenticalLargeInputMatchesRegrSxyAfterMerge)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    auto *left = MakeRegrYxIdenticalLargeSlice(0, 3);
    auto *right = MakeRegrYxIdenticalLargeSlice(3, 3);
    double sxx = RegrUtMergeTwoPartialRows(OMNI_AGGREGATION_TYPE_REGR_SXX, left, right, 3);
    double sxy = RegrUtMergeTwoPartialRows(OMNI_AGGREGATION_TYPE_REGR_SXY, left, right, 4);
    EXPECT_DOUBLE_EQ(sxx, sxy);
    VectorHelper::FreeVecBatch(left);
    VectorHelper::FreeVecBatch(right);
}

TEST(RegrSxxAggregatorTest, AlignAggSchemaEmpty)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SXX, inD, outF, ch, true, false);
    VectorBatch in(0);
    VectorBatch out(0);
    agg->AlignAggSchema(&out, &in);
    ASSERT_EQ(out.GetVectorCount(), 3);
    EXPECT_EQ(out.GetRowCount(), 0);
    out.FreeAllVectors();
}

TEST(RegrSxxAggregatorTest, AlignAggSchemaRawOneRow)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outPartial({DoubleType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SXX, inD, outPartial, ch, true, true);
    auto *batchIn = new VectorBatch(1);
    auto *y = new Vector<double>(1);
    auto *x = new Vector<double>(1);
    y->SetValue(0, 5.0);
    x->SetValue(0, 2.0);
    batchIn->Append(y);
    batchIn->Append(x);
    auto *batchOut = new VectorBatch(0);
    agg->AlignAggSchema(batchOut, batchIn);
    ASSERT_EQ(batchOut->GetVectorCount(), 3);
    auto *n = static_cast<Vector<double> *>(batchOut->Get(0));
    auto *avg = static_cast<Vector<double> *>(batchOut->Get(1));
    auto *m2 = static_cast<Vector<double> *>(batchOut->Get(2));
    EXPECT_DOUBLE_EQ(n->GetValue(0), 1.0);
    EXPECT_DOUBLE_EQ(avg->GetValue(0), 2.0);
    EXPECT_DOUBLE_EQ(m2->GetValue(0), 0.0);
    VectorHelper::FreeVecBatch(batchIn);
    VectorHelper::FreeVecBatch(batchOut);
}

TEST(RegrSxxAggregatorTest, AlignAggSchemaWithFilterSkipsRow)
{
    DataTypes inD({DoubleType(), DoubleType()});
    DataTypes outPartial({DoubleType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0, 1};
    int32_t filterIdx = 2;
    auto agg = CreateRegrAggregatorForUt(OMNI_AGGREGATION_TYPE_REGR_SXX, inD, outPartial, ch, true, true);
    auto *batchIn = new VectorBatch(1);
    auto *y = new Vector<double>(1);
    auto *x = new Vector<double>(1);
    auto *f = new Vector<bool>(1);
    y->SetValue(0, 5.0);
    x->SetValue(0, 2.0);
    f->SetValue(0, false);
    batchIn->Append(y);
    batchIn->Append(x);
    batchIn->Append(f);
    auto *batchOut = new VectorBatch(0);
    agg->AlignAggSchemaWithFilter(batchOut, batchIn, filterIdx);
    ASSERT_EQ(batchOut->GetVectorCount(), 3);
    EXPECT_TRUE(static_cast<Vector<double> *>(batchOut->Get(0))->IsNull(0));
    VectorHelper::FreeVecBatch(batchIn);
    VectorHelper::FreeVecBatch(batchOut);
}

} // namespace omniruntime
