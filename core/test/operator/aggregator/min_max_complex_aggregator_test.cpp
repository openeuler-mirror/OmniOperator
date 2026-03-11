/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Unit tests for Min/Max aggregation on complex types (ARRAY<LONG>, ARRAY<STRING>, ROW).
 */

#include <memory>
#include <vector>
#include <string>
#include <gtest/gtest.h>
#include "operator/aggregation/aggregator/aggregator_factory.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/aggregator/aggregator.h"
#include "operator/aggregation/aggregator/complex_aggregator_util.h"
#include "vector/vector_helper.h"
#include "vector/vector.h"
#include "vector/array_vector.h"
#include "vector/row_vector.h"
#include "vector/large_string_container.h"
#include "type/data_type.h"
#include "util/type_util.h"
#include "util/test_util.h"
#include "operator/execution_context.h"

namespace omniruntime {
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace TestUtil;

static std::unique_ptr<AggregateState[]> NewAndInitState(Aggregator *agg, int32_t off = 0)
{
    auto state = std::make_unique<AggregateState[]>(agg->GetStateSize());
    agg->SetStateOffset(off);
    agg->InitState(state.get());
    return state;
}

// Build ArrayVector with int64_t elements: rows given by (start, length) in flat element array.
static ArrayVector *MakeArrayLongVector(int32_t rowCount, const int64_t *elements, const int32_t *rowOffsets)
{
    int32_t totalElem = rowOffsets[rowCount];
    auto *elemVec = new Vector<int64_t>(totalElem);
    for (int32_t i = 0; i < totalElem; i++) {
        elemVec->SetValue(i, elements[i]);
    }
    auto *arrVec = new ArrayVector(rowCount, std::shared_ptr<BaseVector>(elemVec));
    for (int32_t r = 0; r < rowCount; r++) {
        arrVec->SetOffset(r, rowOffsets[r]);
        arrVec->SetSize(r, rowOffsets[r + 1] - rowOffsets[r]);
    }
    return arrVec;
}

// MinComplex: Array<Long> — rows [1,2], [1,3], [1] -> min=[1], max=[1,3]
TEST(MinMaxComplexAggregatorTest, MinMax_ArrayLong_Basic)
{
    MinAggregatorFactory minFactory;
    MaxAggregatorFactory maxFactory;
    std::vector<int32_t> ch = {0};
    auto arrayLongType = std::make_shared<ArrayType>(LongType());
    DataTypes inTypes(std::vector<DataTypePtr>{arrayLongType});
    DataTypes outTypes(std::vector<DataTypePtr>{arrayLongType});

    auto minAgg = minFactory.CreateAggregator(inTypes, outTypes, ch, true, false, false);
    auto maxAgg = maxFactory.CreateAggregator(inTypes, outTypes, ch, true, false, false);
    ASSERT_NE(minAgg, nullptr);
    ASSERT_NE(maxAgg, nullptr);

    auto ctx = std::make_unique<ExecutionContext>();
    minAgg->SetExecutionContext(ctx.get());
    maxAgg->SetExecutionContext(ctx.get());

    // Rows [1,2], [1,3], [1] -> 2+2+1=5 elements; rowOffsets[i]=start index for row i, rowOffsets[rowCount]=total
    int64_t elements[] = {1, 2, 1, 3, 1};
    int32_t rowOffsets[] = {0, 2, 4, 5};
    ArrayVector *arrInput = MakeArrayLongVector(3, elements, rowOffsets);
    VectorBatch *batch = new VectorBatch(1);
    batch->Append(arrInput);

    BaseVector *minOutVec = VectorHelper::CreateComplexVector(arrayLongType.get(), 1);
    BaseVector *maxOutVec = VectorHelper::CreateComplexVector(arrayLongType.get(), 1);
    std::vector<BaseVector *> minExtract = {minOutVec};
    std::vector<BaseVector *> maxExtract = {maxOutVec};

    auto minState = NewAndInitState(minAgg.get());
    auto maxState = NewAndInitState(maxAgg.get());
    minAgg->ProcessGroup(minState.get(), batch, 0, 3);
    maxAgg->ProcessGroup(maxState.get(), batch, 0, 3);
    minAgg->ExtractValues(minState.get(), minExtract, 0);
    maxAgg->ExtractValues(maxState.get(), maxExtract, 0);

    ASSERT_FALSE(minOutVec->IsNull(0));
    ASSERT_FALSE(maxOutVec->IsNull(0));
    ArrayVector *minArr = static_cast<ArrayVector *>(minOutVec);
    ArrayVector *maxArr = static_cast<ArrayVector *>(maxOutVec);
    EXPECT_EQ(static_cast<int64_t>(minArr->GetSize(0)), 1);
    EXPECT_EQ(static_cast<int64_t>(maxArr->GetSize(0)), 2);
    BaseVector *minElem = minArr->GetValue(0);
    BaseVector *maxElem = maxArr->GetValue(0);
    EXPECT_EQ(static_cast<Vector<int64_t> *>(minElem)->GetValue(0), 1);
    EXPECT_EQ(static_cast<Vector<int64_t> *>(maxElem)->GetValue(0), 1);
    EXPECT_EQ(static_cast<Vector<int64_t> *>(maxElem)->GetValue(1), 3);

    VectorHelper::FreeVecBatch(batch);
    delete minOutVec;
    delete maxOutVec;
}

// MinComplex: Array<String> — rows ['a','b'], ['a','c'], ['a'], ['b'] -> min=['a'], max=['b'] (Spark lex order)
TEST(MinMaxComplexAggregatorTest, MinMax_ArrayString_Basic)
{
    MinAggregatorFactory minFactory;
    MaxAggregatorFactory maxFactory;
    std::vector<int32_t> ch = {0};
    auto arrayStrType = std::make_shared<ArrayType>(VarcharType(10));
    DataTypes inTypes(std::vector<DataTypePtr>{arrayStrType});
    DataTypes outTypes(std::vector<DataTypePtr>{arrayStrType});

    auto minAgg = minFactory.CreateAggregator(inTypes, outTypes, ch, true, false, false);
    auto maxAgg = maxFactory.CreateAggregator(inTypes, outTypes, ch, true, false, false);
    ASSERT_NE(minAgg, nullptr);
    ASSERT_NE(maxAgg, nullptr);

    auto ctx = std::make_unique<ExecutionContext>();
    minAgg->SetExecutionContext(ctx.get());
    maxAgg->SetExecutionContext(ctx.get());

    std::string elemStrs[] = {"a", "b", "a", "c", "a", "b"};
    const int32_t totalElem = 6;
    auto *elemVec = new Vector<LargeStringContainer<std::string_view>>(totalElem);
    for (int32_t i = 0; i < totalElem; i++) {
        elemVec->SetValue(i, std::string_view(elemStrs[i].data(), elemStrs[i].size()));
    }
    int32_t rowCount = 4;
    auto *arrInput = new ArrayVector(rowCount, std::shared_ptr<BaseVector>(elemVec));
    arrInput->SetOffset(0, 0);
    arrInput->SetSize(0, 2);
    arrInput->SetOffset(1, 2);
    arrInput->SetSize(1, 3);
    arrInput->SetOffset(2, 5);
    arrInput->SetSize(2, 1);
    arrInput->SetOffset(3, 6);
    arrInput->SetSize(3, 1);

    VectorBatch *batch = new VectorBatch(1);
    batch->Append(arrInput);

    BaseVector *minOutVec = VectorHelper::CreateComplexVector(arrayStrType.get(), 1);
    BaseVector *maxOutVec = VectorHelper::CreateComplexVector(arrayStrType.get(), 1);
    std::vector<BaseVector *> minExtract = {minOutVec};
    std::vector<BaseVector *> maxExtract = {maxOutVec};

    auto minState = NewAndInitState(minAgg.get());
    auto maxState = NewAndInitState(maxAgg.get());
    minAgg->ProcessGroup(minState.get(), batch, 0, rowCount);
    maxAgg->ProcessGroup(maxState.get(), batch, 0, rowCount);
    minAgg->ExtractValues(minState.get(), minExtract, 0);
    maxAgg->ExtractValues(maxState.get(), maxExtract, 0);

    ASSERT_FALSE(minOutVec->IsNull(0));
    ASSERT_FALSE(maxOutVec->IsNull(0));
    ArrayVector *minArr = static_cast<ArrayVector *>(minOutVec);
    ArrayVector *maxArr = static_cast<ArrayVector *>(maxOutVec);
    EXPECT_EQ(static_cast<int64_t>(minArr->GetSize(0)), 1);
    EXPECT_EQ(static_cast<int64_t>(maxArr->GetSize(0)), 1);
    BaseVector *minElem = minArr->GetValue(0);
    BaseVector *maxElem = maxArr->GetValue(0);
    auto minStr = static_cast<Vector<LargeStringContainer<std::string_view>> *>(minElem)->GetValue(0);
    auto maxStr = static_cast<Vector<LargeStringContainer<std::string_view>> *>(maxElem)->GetValue(0);
    EXPECT_EQ(std::string(minStr.data(), minStr.size()), "a");
    EXPECT_EQ(std::string(maxStr.data(), maxStr.size()), "b");

    VectorHelper::FreeVecBatch(batch);
    delete minOutVec;
    delete maxOutVec;
}

// Factory creates MinComplexAggregator / MaxComplexAggregator for ARRAY type
TEST(MinMaxComplexAggregatorTest, Factory_ArrayLong)
{
    MinAggregatorFactory minFactory;
    MaxAggregatorFactory maxFactory;
    std::vector<int32_t> ch = {0};
    auto arrayLongType = std::make_shared<ArrayType>(LongType());
    DataTypes inTypes(std::vector<DataTypePtr>{arrayLongType});
    DataTypes outTypes(std::vector<DataTypePtr>{arrayLongType});

    auto minAgg = minFactory.CreateAggregator(inTypes, outTypes, ch, true, false, false);
    auto maxAgg = maxFactory.CreateAggregator(inTypes, outTypes, ch, true, false, false);
    ASSERT_NE(minAgg, nullptr);
    ASSERT_NE(maxAgg, nullptr);
    EXPECT_GT(minAgg->GetStateSize(), 0u);
    EXPECT_GT(maxAgg->GetStateSize(), 0u);
}

// Factory creates complex aggregator for ROW type
TEST(MinMaxComplexAggregatorTest, Factory_Row)
{
    MinAggregatorFactory minFactory;
    MaxAggregatorFactory maxFactory;
    std::vector<int32_t> ch = {0};
    std::vector<DataTypePtr> fieldTypes = {LongType(), VarcharType(10)};
    auto rowType = std::make_shared<RowType>(fieldTypes, std::vector<std::string>{"a", "b"});
    DataTypes inTypes(std::vector<DataTypePtr>{rowType});
    DataTypes outTypes(std::vector<DataTypePtr>{rowType});

    auto minAgg = minFactory.CreateAggregator(inTypes, outTypes, ch, true, false, false);
    auto maxAgg = maxFactory.CreateAggregator(inTypes, outTypes, ch, true, false, false);
    ASSERT_NE(minAgg, nullptr);
    ASSERT_NE(maxAgg, nullptr);
    EXPECT_GT(minAgg->GetStateSize(), 0u);
    EXPECT_GT(maxAgg->GetStateSize(), 0u);
}

// GetSpillType returns single complex type
TEST(MinMaxComplexAggregatorTest, GetSpillType_ArrayLong)
{
    MinAggregatorFactory minFactory;
    std::vector<int32_t> ch = {0};
    auto arrayLongType = std::make_shared<ArrayType>(LongType());
    DataTypes inTypes(std::vector<DataTypePtr>{arrayLongType});
    DataTypes outTypes(std::vector<DataTypePtr>{arrayLongType});
    auto agg = minFactory.CreateAggregator(inTypes, outTypes, ch, true, false, false);
    ASSERT_NE(agg, nullptr);

    std::vector<DataTypePtr> spillTypes = agg->GetSpillType();
    EXPECT_EQ(spillTypes.size(), 1u);
    EXPECT_EQ(spillTypes[0]->GetId(), OMNI_ARRAY);
}

// All-null input -> result null
TEST(MinMaxComplexAggregatorTest, MinMax_ArrayLong_AllNull)
{
    MinAggregatorFactory minFactory;
    MaxAggregatorFactory maxFactory;
    std::vector<int32_t> ch = {0};
    auto arrayLongType = std::make_shared<ArrayType>(LongType());
    DataTypes inTypes(std::vector<DataTypePtr>{arrayLongType});
    DataTypes outTypes(std::vector<DataTypePtr>{arrayLongType});

    auto minAgg = minFactory.CreateAggregator(inTypes, outTypes, ch, true, false, false);
    auto maxAgg = maxFactory.CreateAggregator(inTypes, outTypes, ch, true, false, false);
    ASSERT_NE(minAgg, nullptr);
    ASSERT_NE(maxAgg, nullptr);

    auto ctx = std::make_unique<ExecutionContext>();
    minAgg->SetExecutionContext(ctx.get());
    maxAgg->SetExecutionContext(ctx.get());

    auto *elemVec = new Vector<int64_t>(0);
    auto *arrInput = new ArrayVector(3, std::shared_ptr<BaseVector>(elemVec));
    arrInput->SetOffset(0, 0);
    arrInput->SetSize(0, 0);
    arrInput->SetNull(0);
    arrInput->SetSize(1, 0);
    arrInput->SetNull(1);
    arrInput->SetSize(2, 0);
    arrInput->SetNull(2);

    VectorBatch *batch = new VectorBatch(1);
    batch->Append(arrInput);

    BaseVector *minOutVec = VectorHelper::CreateComplexVector(arrayLongType.get(), 1);
    BaseVector *maxOutVec = VectorHelper::CreateComplexVector(arrayLongType.get(), 1);
    std::vector<BaseVector *> minExtract = {minOutVec};
    std::vector<BaseVector *> maxExtract = {maxOutVec};

    auto minState = NewAndInitState(minAgg.get());
    auto maxState = NewAndInitState(maxAgg.get());
    minAgg->ProcessGroup(minState.get(), batch, 0, 3);
    maxAgg->ProcessGroup(maxState.get(), batch, 0, 3);
    minAgg->ExtractValues(minState.get(), minExtract, 0);
    maxAgg->ExtractValues(maxState.get(), maxExtract, 0);

    EXPECT_TRUE(minOutVec->IsNull(0));
    EXPECT_TRUE(maxOutVec->IsNull(0));

    VectorHelper::FreeVecBatch(batch);
    delete minOutVec;
    delete maxOutVec;
}

}  // namespace omniruntime
