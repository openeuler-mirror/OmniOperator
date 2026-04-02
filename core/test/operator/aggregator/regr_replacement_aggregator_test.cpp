/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include <gtest/gtest.h>

#include "operator/aggregation/aggregator/aggregator_factory.h"
#include "operator/aggregation/aggregator/regr/regr_replacement_aggregator.h"
#include "operator/aggregation/aggregator/regr/regr_state.h"
#include "type/data_type.h"
#include "vector/vector.h"
#include "vector/vector_batch.h"
#include "vector/vector_helper.h"

namespace omniruntime {
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace omniruntime::vec;

TEST(RegrReplacementAggregatorTest, GetStateSize)
{
    DataTypes inRaw({DoubleType()});
    DataTypes outP({LongType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0};
    RegrReplacementAggregatorFactory factory;
    auto agg = factory.CreateAggregator(inRaw, outP, ch, true, true, false);
    EXPECT_EQ(agg->GetStateSize(), sizeof(RegrReplacementState));
}

TEST(RegrReplacementAggregatorTest, GetSpillTypeLayout)
{
    DataTypes inRaw({DoubleType()});
    DataTypes outP({LongType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0};
    RegrReplacementAggregatorFactory factory;
    auto agg = factory.CreateAggregator(inRaw, outP, ch, true, true, false);
    auto types = agg->GetSpillType();
    ASSERT_EQ(types.size(), 3U);
    EXPECT_EQ(types[0]->GetId(), OMNI_LONG);
    EXPECT_EQ(types[1]->GetId(), OMNI_DOUBLE);
    EXPECT_EQ(types[2]->GetId(), OMNI_DOUBLE);
}

TEST(RegrReplacementAggregatorTest, AlignAggSchemaEmpty)
{
    DataTypes inRaw({DoubleType()});
    DataTypes outP({LongType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0};
    RegrReplacementAggregatorFactory factory;
    auto agg = factory.CreateAggregator(inRaw, outP, ch, true, true, false);
    VectorBatch in(0);
    VectorBatch out(0);
    agg->AlignAggSchema(&out, &in);
    ASSERT_EQ(out.GetVectorCount(), 3);
    out.FreeAllVectors();
}

TEST(RegrReplacementAggregatorTest, AlignAggSchemaRawOneRow)
{
    DataTypes inRaw({DoubleType()});
    DataTypes outP({LongType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0};
    RegrReplacementAggregatorFactory factory;
    auto agg = factory.CreateAggregator(inRaw, outP, ch, true, true, false);
    auto *batchIn = new VectorBatch(1);
    auto *v = new Vector<double>(1);
    v->SetValue(0, 2.5);
    batchIn->Append(v);
    auto *batchOut = new VectorBatch(0);
    agg->AlignAggSchema(batchOut, batchIn);
    ASSERT_EQ(batchOut->GetVectorCount(), 3);
    EXPECT_EQ(static_cast<Vector<int64_t> *>(batchOut->Get(0))->GetValue(0), 1);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(1))->GetValue(0), 2.5);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(2))->GetValue(0), 0.0);
    VectorHelper::FreeVecBatch(batchIn);
    VectorHelper::FreeVecBatch(batchOut);
}

TEST(RegrReplacementAggregatorTest, AlignAggSchemaWithFilterSkipsRow)
{
    DataTypes inRaw({DoubleType()});
    DataTypes outP({LongType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0};
    RegrReplacementAggregatorFactory factory;
    auto agg = factory.CreateAggregator(inRaw, outP, ch, true, true, false);
    auto *batchIn = new VectorBatch(1);
    auto *v = new Vector<double>(1);
    auto *f = new Vector<bool>(1);
    v->SetValue(0, 1.0);
    f->SetValue(0, false);
    batchIn->Append(v);
    batchIn->Append(f);
    auto *batchOut = new VectorBatch(0);
    agg->AlignAggSchemaWithFilter(batchOut, batchIn, 1);
    EXPECT_TRUE(static_cast<Vector<int64_t> *>(batchOut->Get(0))->IsNull(0));
    VectorHelper::FreeVecBatch(batchIn);
    VectorHelper::FreeVecBatch(batchOut);
}

TEST(RegrReplacementAggregatorTest, AlignAggSchemaPartialPassthroughSlices)
{
    DataTypes mergeIn({LongType(), DoubleType(), DoubleType()});
    DataTypes outF({DoubleType()});
    std::vector<int32_t> ch = {0, 1, 2};
    RegrReplacementAggregatorFactory factory;
    auto agg = factory.CreateAggregator(mergeIn, outF, ch, false, false, false);
    auto *batchIn = new VectorBatch(2);
    auto *n = new Vector<int64_t>(2);
    auto *a = new Vector<double>(2);
    auto *m = new Vector<double>(2);
    n->SetValue(0, 3);
    n->SetValue(1, 1);
    a->SetValue(0, 1.0);
    a->SetValue(1, 2.0);
    m->SetValue(0, 4.0);
    m->SetValue(1, 0.0);
    batchIn->Append(n);
    batchIn->Append(a);
    batchIn->Append(m);
    auto *batchOut = new VectorBatch(0);
    agg->AlignAggSchema(batchOut, batchIn);
    EXPECT_EQ(static_cast<Vector<int64_t> *>(batchOut->Get(0))->GetValue(1), 1);
    EXPECT_DOUBLE_EQ(static_cast<Vector<double> *>(batchOut->Get(2))->GetValue(1), 0.0);
    VectorHelper::FreeVecBatch(batchIn);
    VectorHelper::FreeVecBatch(batchOut);
}

TEST(RegrReplacementAggregatorTest, ProcessAlignAggSchemaNullOriginAppendsEmptyPartial)
{
    DataTypes inRaw({DoubleType()});
    DataTypes outP({LongType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0};
    RegrReplacementAggregatorFactory factory;
    auto agg = factory.CreateAggregator(inRaw, outP, ch, true, true, false);
    VectorBatch out(0);
    static_cast<RegrReplacementAggregator *>(agg.get())->ProcessAlignAggSchema(&out, nullptr, nullptr, false);
    ASSERT_EQ(out.GetVectorCount(), 3);
    out.FreeAllVectors();
}

TEST(RegrReplacementAggregatorTest, SpillUnspillPreservesPartialM2)
{
    DataTypes inRaw({DoubleType()});
    DataTypes outP({LongType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0};
    RegrReplacementAggregatorFactory factory;
    auto agg = factory.CreateAggregator(inRaw, outP, ch, true, true, false);
    agg->SetStateOffset(0);
    std::vector<uint8_t> buf(agg->GetStateSize());
    agg->InitState(buf.data());
    auto *batch = new VectorBatch(2);
    auto *col = new Vector<double>(2);
    col->SetValue(0, 2.0);
    col->SetValue(1, 4.0);
    batch->Append(col);
    agg->ProcessGroup(buf.data(), batch, 0, 2);
    VectorBatch spillBatch(1);
    spillBatch.Append(new Vector<int64_t>(1));
    spillBatch.Append(new Vector<double>(1));
    spillBatch.Append(new Vector<double>(1));
    std::vector<BaseVector *> spillPtrs = {spillBatch.Get(0), spillBatch.Get(1), spillBatch.Get(2)};
    std::vector<AggregateState *> gs = {buf.data()};
    agg->ExtractValuesForSpill(gs, spillPtrs);

    std::vector<uint8_t> buf2(agg->GetStateSize());
    agg->InitState(buf2.data());
    UnspillRowInfo ur{buf2.data(), &spillBatch, 0};
    std::vector<UnspillRowInfo> rows = {ur};
    int32_t vi = 0;
    agg->ProcessGroupUnspill(rows, 1, vi);
    EXPECT_EQ(vi, 3);

    auto *nOut = new Vector<int64_t>(1);
    auto *aOut = new Vector<double>(1);
    auto *m2Out = new Vector<double>(1);
    std::vector<BaseVector *> ov = {nOut, aOut, m2Out};
    agg->ExtractValues(reinterpret_cast<const AggregateState *>(buf2.data()), ov, 0);
    EXPECT_EQ(nOut->GetValue(0), 2);
    EXPECT_DOUBLE_EQ(aOut->GetValue(0), 3.0);
    EXPECT_DOUBLE_EQ(m2Out->GetValue(0), 2.0);
    delete nOut;
    delete aOut;
    delete m2Out;
    VectorHelper::FreeVecBatch(batch);
}

TEST(RegrReplacementAggregatorTest, UnspillAcceptsDoubleCountColumn)
{
    DataTypes mergeIn({DoubleType(), DoubleType(), DoubleType()});
    DataTypes outP({LongType(), DoubleType(), DoubleType()});
    std::vector<int32_t> ch = {0, 1, 2};
    RegrReplacementAggregatorFactory factory;
    auto agg = factory.CreateAggregator(mergeIn, outP, ch, false, true, false);
    agg->SetStateOffset(0);
    VectorBatch spillBatch(1);
    spillBatch.Append(new Vector<double>(1));
    spillBatch.Append(new Vector<double>(1));
    spillBatch.Append(new Vector<double>(1));
    static_cast<Vector<double> *>(spillBatch.Get(0))->SetValue(0, 2.0);
    static_cast<Vector<double> *>(spillBatch.Get(1))->SetValue(0, 3.0);
    static_cast<Vector<double> *>(spillBatch.Get(2))->SetValue(0, 2.0);
    std::vector<uint8_t> buf(agg->GetStateSize());
    agg->InitState(buf.data());
    UnspillRowInfo ur{buf.data(), &spillBatch, 0};
    std::vector<UnspillRowInfo> rows = {ur};
    int32_t vi = 0;
    agg->ProcessGroupUnspill(rows, 1, vi);
    auto *nOut = new Vector<int64_t>(1);
    auto *aOut = new Vector<double>(1);
    auto *m2Out = new Vector<double>(1);
    std::vector<BaseVector *> ov = {nOut, aOut, m2Out};
    agg->ExtractValues(reinterpret_cast<const AggregateState *>(buf.data()), ov, 0);
    EXPECT_EQ(nOut->GetValue(0), 2);
    EXPECT_DOUBLE_EQ(m2Out->GetValue(0), 2.0);
    delete nOut;
    delete aOut;
    delete m2Out;
}

} // namespace omniruntime
