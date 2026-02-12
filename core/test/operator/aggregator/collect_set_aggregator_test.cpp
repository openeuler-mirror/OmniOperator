/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
 * Description: Unit tests for CollectSet aggregation (CollectSetAggregator and CollectSetAggregatorFactory).
 * Coverage target: collect_set_aggregator and factory >= 75%.
 */

#include <memory>
#include <vector>
#include <algorithm>
#include <gtest/gtest.h>
#include "operator/aggregation/aggregator/collect_set_aggregator.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/aggregator/aggregator.h"
#include "vector/vector_helper.h"
#include "vector/array_vector.h"
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

static DataTypePtr ArrayOf(const DataTypePtr &elementType)
{
    return std::make_shared<ArrayType>(elementType);
}

/** Call before freeing state buffer to avoid leak. CollectSet-specific (no base class DestroyState). */
static void DestroyCollectSetState(Aggregator *agg, AggregateState *state)
{
    if (auto *p = dynamic_cast<CollectSetAggregator<OMNI_INT, OMNI_INT> *>(agg)) {
        p->DestroyState(state);
    } else if (auto *q = dynamic_cast<CollectSetAggregator<OMNI_LONG, OMNI_LONG> *>(agg)) {
        q->DestroyState(state);
    }
}

// ---- CollectSetAggregatorFactory tests ----

TEST(CollectSetAggregatorTest, FactoryPartialInt)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{IntType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(IntType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);
    EXPECT_TRUE(agg->IsInputRaw());
    EXPECT_TRUE(agg->IsTypedAggregator());
    EXPECT_GT(agg->GetStateSize(), 0u);
}

TEST(CollectSetAggregatorTest, FactoryPartialLong)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{LongType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(LongType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, false, false);
    ASSERT_NE(agg, nullptr);
}

TEST(CollectSetAggregatorTest, FactoryPartialBoolean)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{BooleanType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(BooleanType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);
}

TEST(CollectSetAggregatorTest, FactoryPartialShort)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{ShortType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(ShortType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);
}

TEST(CollectSetAggregatorTest, FactoryPartialFloat)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{FloatType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(FloatType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);
}

TEST(CollectSetAggregatorTest, FactoryPartialDouble)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{DoubleType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(DoubleType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);
}

TEST(CollectSetAggregatorTest, FactoryPartialDecimal64)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{Decimal64Type()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(Decimal64Type())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);
}

TEST(CollectSetAggregatorTest, FactoryPartialDecimal128NotImplemented)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{Decimal128Type()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(Decimal128Type())});
    EXPECT_THROW(factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false),
        omniruntime::exception::OmniException);
}

TEST(CollectSetAggregatorTest, FactoryFinalArrayInt)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{ArrayOf(IntType())});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(IntType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, false, false, false);
    ASSERT_NE(agg, nullptr);
    EXPECT_FALSE(agg->IsInputRaw());
}

TEST(CollectSetAggregatorTest, FactoryFinalArrayLong)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{ArrayOf(LongType())});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(LongType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, false, false, false);
    ASSERT_NE(agg, nullptr);
}

TEST(CollectSetAggregatorTest, FactoryFinalNonArrayThrows)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{IntType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(IntType())});
    EXPECT_THROW(
        factory.CreateAggregator(inputTypes, outputTypes, channels, false, false, false),
        omniruntime::exception::OmniException);
}

TEST(CollectSetAggregatorTest, FactoryPartialWithArrayInput)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{ArrayOf(LongType())});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(LongType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);
}

TEST(CollectSetAggregatorTest, FactoryUnsupportedElementTypeThrows)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{NoneType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(NoneType())});
    EXPECT_THROW(
        factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false),
        omniruntime::exception::OmniException);
}

TEST(CollectSetAggregatorTest, FactoryVarcharVarBinaryNotImplemented)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputVarchar(std::vector<DataTypePtr>{VarcharType(100)});
    DataTypes outputVarchar(std::vector<DataTypePtr>{ArrayOf(VarcharType(100))});
    EXPECT_THROW(factory.CreateAggregator(inputVarchar, outputVarchar, channels, true, true, false),
        omniruntime::exception::OmniException);

    DataTypes inputVarbinary(std::vector<DataTypePtr>{VarBinaryType(100)});
    DataTypes outputVarbinary(std::vector<DataTypePtr>{ArrayOf(VarBinaryType(100))});
    EXPECT_THROW(factory.CreateAggregator(inputVarbinary, outputVarbinary, channels, true, true, false),
        omniruntime::exception::OmniException);
}

// ---- CollectSetAggregator direct Create / behaviour tests ----

TEST(CollectSetAggregatorTest, CreateBasicTypeSuccess)
{
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{IntType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(IntType())});
    auto agg = CollectSetAggregator<OMNI_INT, OMNI_INT>::Create(
        inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);
}

TEST(CollectSetAggregatorTest, PartialProcessGroupAndExtractValuesInt)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{IntType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(IntType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);

    auto executionContext = std::make_unique<ExecutionContext>();
    agg->SetExecutionContext(executionContext.get());

    const int32_t rowCnt = 8;
    int32_t data[rowCnt] = {10, 20, 10, 30, 20, 10, 40, 30};
    VectorBatch *vecBatch = CreateVectorBatch(DataTypes(std::vector<DataTypePtr>{IntType()}), rowCnt, data);
    ASSERT_EQ(vecBatch->GetRowCount(), rowCnt);

    auto state = NewAndInitState(agg.get());
    agg->ProcessGroup(state.get(), vecBatch, 0, rowCnt);

    Vector<int32_t> *outputElements = new Vector<int32_t>(0);
    BaseVector *outputVector = new ArrayVector(1, std::shared_ptr<BaseVector>(outputElements));
    std::vector<BaseVector *> extractVectors = {outputVector};
    agg->ExtractValues(state.get(), extractVectors, 0);

    auto *arrayVec = static_cast<ArrayVector *>(extractVectors[0]);
    EXPECT_FALSE(arrayVec->IsNull(0));
    std::shared_ptr<BaseVector> elemVec = arrayVec->GetArrayAt(0, false);
    ASSERT_NE(elemVec, nullptr);
    auto *intVec = static_cast<Vector<int32_t> *>(elemVec.get());
    int32_t uniqueCount = static_cast<int32_t>(intVec->GetSize());
    EXPECT_EQ(uniqueCount, 4);

    VectorHelper::FreeVecBatch(vecBatch);
    delete outputVector;
    DestroyCollectSetState(agg.get(), state.get());
}

TEST(CollectSetAggregatorTest, PartialProcessGroupEmptyInputExtractNull)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{LongType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(LongType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);

    auto executionContext = std::make_unique<ExecutionContext>();
    agg->SetExecutionContext(executionContext.get());

    const int32_t rowCnt = 0;
    VectorBatch *vecBatch = new VectorBatch(rowCnt);
    vecBatch->Append(new Vector<int64_t>(rowCnt));

    auto state = NewAndInitState(agg.get());
    agg->ProcessGroup(state.get(), vecBatch, 0, rowCnt);

    Vector<int64_t> *outputElements = new Vector<int64_t>(0);
    BaseVector *outputVector = new ArrayVector(1, std::shared_ptr<BaseVector>(outputElements));
    std::vector<BaseVector *> extractVectors = {outputVector};
    agg->ExtractValues(state.get(), extractVectors, 0);

    auto *arrayVec = static_cast<ArrayVector *>(extractVectors[0]);
    EXPECT_TRUE(arrayVec->IsNull(0));

    VectorHelper::FreeVecBatch(vecBatch);
    delete outputVector;
    DestroyCollectSetState(agg.get(), state.get());
}

TEST(CollectSetAggregatorTest, PartialProcessGroupAllNullsExtractNull)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{IntType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(IntType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);

    auto executionContext = std::make_unique<ExecutionContext>();
    agg->SetExecutionContext(executionContext.get());

    const int32_t rowCnt = 3;
    Vector<int32_t> *col = new Vector<int32_t>(rowCnt);
    col->SetNull(0);
    col->SetNull(1);
    col->SetNull(2);
    col->SetNullFlag(true);  // ensure aggregator gets nullMap (nullMap: true = null)
    VectorBatch *vecBatch = new VectorBatch(rowCnt);
    vecBatch->Append(col);

    auto state = NewAndInitState(agg.get());
    agg->ProcessGroup(state.get(), vecBatch, 0, rowCnt);

    Vector<int32_t> *outputElements = new Vector<int32_t>(0);
    BaseVector *outputVector = new ArrayVector(1, std::shared_ptr<BaseVector>(outputElements));
    std::vector<BaseVector *> extractVectors = {outputVector};
    agg->ExtractValues(state.get(), extractVectors, 0);

    auto *arrayVec = static_cast<ArrayVector *>(extractVectors[0]);
    EXPECT_TRUE(arrayVec->IsNull(0));

    VectorHelper::FreeVecBatch(vecBatch);
    delete outputVector;
    DestroyCollectSetState(agg.get(), state.get());
}

TEST(CollectSetAggregatorTest, InitStatesMultiGroup)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{IntType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(IntType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);

    const int32_t groupCount = 4;
    std::vector<AggregateState *> groupStates(groupCount);
    std::unique_ptr<AggregateState[]> stateBuf =
        std::make_unique<AggregateState[]>(agg->GetStateSize() * groupCount);
    for (int32_t i = 0; i < groupCount; i++) {
        groupStates[i] = stateBuf.get() + i * agg->GetStateSize();
    }
    agg->SetStateOffset(0);
    agg->InitStates(groupStates);
    EXPECT_GT(agg->GetStateSize(), 0u);
    for (int32_t i = 0; i < groupCount; i++) {
        DestroyCollectSetState(agg.get(), groupStates[i]);
    }
}

TEST(CollectSetAggregatorTest, GetStateSize)
{
    std::vector<int32_t> channels = {0};
    auto agg = CollectSetAggregator<OMNI_LONG, OMNI_LONG>::Create(
        DataTypes(std::vector<DataTypePtr>{LongType()}),
        DataTypes(std::vector<DataTypePtr>{ArrayOf(LongType())}),
        channels, true, true, false);
    ASSERT_NE(agg, nullptr);
    EXPECT_GT(agg->GetStateSize(), 0u);
}

TEST(CollectSetAggregatorTest, CreateAggregatorFactorySwitch)
{
    auto factory = CreateAggregatorFactory(OMNI_AGGREGATION_TYPE_COLLECT_SET);
    ASSERT_NE(factory, nullptr);

    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{IntType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(IntType())});
    auto agg = factory->CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);
}

// ---- ProcessGroupUnspill tests ----

TEST(CollectSetAggregatorTest, ProcessGroupUnspillMergeSpilledArrays)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{IntType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(IntType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);

    auto executionContext = std::make_unique<ExecutionContext>();
    agg->SetExecutionContext(executionContext.get());
    agg->SetStateOffset(0);

    const int32_t groupCount = 2;
    std::vector<AggregateState *> groupStates(groupCount);
    std::unique_ptr<AggregateState[]> stateBuf =
        std::make_unique<AggregateState[]>(agg->GetStateSize() * groupCount);
    for (int32_t i = 0; i < groupCount; i++) {
        groupStates[i] = stateBuf.get() + i * agg->GetStateSize();
    }
    agg->InitStates(groupStates);

    // Build spill batch: one ArrayVector column with 2 rows: row0 = [1,2,3], row1 = [4,5]
    const int32_t elemCount = 5;
    Vector<int32_t> *allElements = new Vector<int32_t>(elemCount);
    int32_t flatData[] = {1, 2, 3, 4, 5};
    for (int32_t i = 0; i < elemCount; i++) {
        allElements->SetValue(i, flatData[i]);
    }
    ArrayVector *spillArray = new ArrayVector(2, std::shared_ptr<BaseVector>(allElements));
    spillArray->SetOffset(0, 0);
    spillArray->SetSize(0, 3);
    spillArray->SetSize(1, 2);

    VectorBatch *spillBatch = new VectorBatch(2);
    spillBatch->Append(spillArray);

    std::vector<UnspillRowInfo> unspillRows(2);
    unspillRows[0] = UnspillRowInfo{groupStates[0], spillBatch, 0};
    unspillRows[1] = UnspillRowInfo{groupStates[1], spillBatch, 1};

    int32_t vectorIndex = 0;
    agg->ProcessGroupUnspill(unspillRows, 2, vectorIndex);
    EXPECT_EQ(vectorIndex, 1);

    // ArrayVector(size) does not initialize elements; SetValue would crash. Use constructor with element vector.
    Vector<int32_t> *outputElements = new Vector<int32_t>(0);
    BaseVector *outputVector = new ArrayVector(2, std::shared_ptr<BaseVector>(outputElements));
    std::vector<BaseVector *> extractVectors = {outputVector};
    agg->ExtractValuesBatch(groupStates, extractVectors, 0, 2);

    ArrayVector *resultArray = static_cast<ArrayVector *>(extractVectors[0]);
    ASSERT_FALSE(resultArray->IsNull(0));
    std::shared_ptr<BaseVector> elem0 = resultArray->GetArrayAt(0, false);
    ASSERT_NE(elem0, nullptr);
    Vector<int32_t> *intVec0 = static_cast<Vector<int32_t> *>(elem0.get());
    EXPECT_EQ(intVec0->GetSize(), 3);
    std::vector<int32_t> row0Values = {intVec0->GetValue(0), intVec0->GetValue(1), intVec0->GetValue(2)};
    std::sort(row0Values.begin(), row0Values.end());
    EXPECT_EQ(row0Values[0], 1);
    EXPECT_EQ(row0Values[1], 2);
    EXPECT_EQ(row0Values[2], 3);

    ASSERT_FALSE(resultArray->IsNull(1));
    std::shared_ptr<BaseVector> elem1 = resultArray->GetArrayAt(1, false);
    ASSERT_NE(elem1, nullptr);
    Vector<int32_t> *intVec1 = static_cast<Vector<int32_t> *>(elem1.get());
    EXPECT_EQ(intVec1->GetSize(), 2);
    std::vector<int32_t> row1Values = {intVec1->GetValue(0), intVec1->GetValue(1)};
    std::sort(row1Values.begin(), row1Values.end());
    EXPECT_EQ(row1Values[0], 4);
    EXPECT_EQ(row1Values[1], 5);

    for (int32_t i = 0; i < groupCount; i++) {
        DestroyCollectSetState(agg.get(), groupStates[i]);
    }
    VectorHelper::FreeVecBatch(spillBatch);
    delete outputVector;
}

TEST(CollectSetAggregatorTest, ProcessGroupUnspillSkipsNullRow)
{
    CollectSetAggregatorFactory factory;
    std::vector<int32_t> channels = {0};
    DataTypes inputTypes(std::vector<DataTypePtr>{IntType()});
    DataTypes outputTypes(std::vector<DataTypePtr>{ArrayOf(IntType())});
    auto agg = factory.CreateAggregator(inputTypes, outputTypes, channels, true, true, false);
    ASSERT_NE(agg, nullptr);

    auto executionContext = std::make_unique<ExecutionContext>();
    agg->SetExecutionContext(executionContext.get());
    agg->SetStateOffset(0);

    const int32_t groupCount = 2;
    std::vector<AggregateState *> groupStates(groupCount);
    std::unique_ptr<AggregateState[]> stateBuf =
        std::make_unique<AggregateState[]>(agg->GetStateSize() * groupCount);
    for (int32_t i = 0; i < groupCount; i++) {
        groupStates[i] = stateBuf.get() + i * agg->GetStateSize();
    }
    agg->InitStates(groupStates);

    // Spill batch: row0 = [10, 20], row1 = null
    Vector<int32_t> *elements = new Vector<int32_t>(2);
    elements->SetValue(0, 10);
    elements->SetValue(1, 20);
    ArrayVector *spillArray = new ArrayVector(2, std::shared_ptr<BaseVector>(elements));
    spillArray->SetOffset(0, 0);
    spillArray->SetSize(0, 2);
    spillArray->SetNull(1);

    VectorBatch *spillBatch = new VectorBatch(2);
    spillBatch->Append(spillArray);

    std::vector<UnspillRowInfo> unspillRows(2);
    unspillRows[0] = UnspillRowInfo{groupStates[0], spillBatch, 0};
    unspillRows[1] = UnspillRowInfo{groupStates[1], spillBatch, 1};

    int32_t vectorIndex = 0;
    agg->ProcessGroupUnspill(unspillRows, 2, vectorIndex);

    // ArrayVector(size) does not initialize elements; SetValue would crash. Use constructor with element vector.
    Vector<int32_t> *outputElements = new Vector<int32_t>(0);
    BaseVector *outputVector = new ArrayVector(2, std::shared_ptr<BaseVector>(outputElements));
    std::vector<BaseVector *> extractVectors = {outputVector};
    agg->ExtractValuesBatch(groupStates, extractVectors, 0, 2);

    ArrayVector *resultArray = static_cast<ArrayVector *>(extractVectors[0]);
    EXPECT_FALSE(resultArray->IsNull(0));
    std::shared_ptr<BaseVector> elem0 = resultArray->GetArrayAt(0, false);
    ASSERT_NE(elem0, nullptr);
    EXPECT_EQ(static_cast<Vector<int32_t> *>(elem0.get())->GetSize(), 2);

    EXPECT_TRUE(resultArray->IsNull(1));

    for (int32_t i = 0; i < groupCount; i++) {
        DestroyCollectSetState(agg.get(), groupStates[i]);
    }
    VectorHelper::FreeVecBatch(spillBatch);
    delete outputVector;
}
}
