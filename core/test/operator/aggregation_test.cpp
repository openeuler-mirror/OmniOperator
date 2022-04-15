/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

#include <vector>
#include <iostream>
#include <thread>
#include <cstdlib>
#include <mutex>
#include <cstdarg>
#include <gtest/gtest.h>
#include "operator/aggregation/aggregator/aggregator_factory.h"
#include "operator/aggregation/aggregator/all_aggregators.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "jit_context/jit_context.h"
#include "vector/vector_helper.h"
#include "util/perf_util.h"
#include "../util/test_util.h"
#include "../../libconfig.h"
namespace omniruntime {
using namespace omniruntime::vec;
const int32_t VEC_BATCH_NUM = 10;
const int32_t ROW_PER_VEC_BATCH = 2000000;
const int32_t CARDINALITY = 4;
const int32_t COLUMN_NUM = 4;
const bool INPUT_MODE = true;
const bool OUTPUT_MODE = false;
const int CONST_VALUE_2 = 2;
const int CONST_VALUE_7 = 7;
const int CONST_VALUE_24 = 24;
const int CONST_VALUE_32 = 32;
const int CONST_VALUE_38 = 38;

static Decimal64DataType SHORT_DECIMAL_TYPE(CONST_VALUE_7, CONST_VALUE_2);
static VarcharDataType SUM_IMMEDIATE_VARBINARY(CONST_VALUE_24);
static VarcharDataType AVG_IMMEDIATE_VARBINARY(CONST_VALUE_32);
static Decimal128DataType LONG_DECIMAL_TYPE(CONST_VALUE_38, 0);

long lrand()
{
    const int CONST_VALUE_8 = 8;
    if (sizeof(int) < sizeof(long)) {
        return (static_cast<long>(rand())) << (sizeof(int) * CONST_VALUE_8) | rand();
    }
    return rand();
}

using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace TestUtil;

Vector *BuildHashInput(const DataType &groupType, int32_t rowPerVecBatch, int32_t cardinality)
{
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation_buildHashInput");
    switch (groupType.GetId()) {
        case OMNI_INT:
        case OMNI_DATE32: {
            IntVector *col = new IntVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                if (cardinality != 0) {
                    col->SetValue(j, j % cardinality);
                }
            }
            return col;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            LongVector *col = new LongVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                if (cardinality != 0) {
                    col->SetValue(j, j % cardinality);
                }
            }
            return col;
        }
        case OMNI_DOUBLE: {
            DoubleVector *col = new DoubleVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, j % cardinality);
            }
            return col;
        }
        case OMNI_BOOLEAN: {
            BooleanVector *col = new BooleanVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, j % cardinality);
            }
            return col;
        }
        case OMNI_DECIMAL128: {
            Decimal128Vector *col = new Decimal128Vector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, Decimal128(0, j % cardinality));
            }
            return col;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            VarcharDataType charDataType = (VarcharDataType &)groupType;
            VarcharVector *col =
                new VarcharVector(vecAllocator, charDataType.GetWidth() * rowPerVecBatch, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                std::string str = std::to_string(j % cardinality);
                col->SetValue(j, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
            }
            return col;
        }
        default: {
            LogError("No such %d type support", groupType.GetId());
            return nullptr;
        }
    }
}

Vector *BuildAggregateInput(VectorAllocator *vecAllocator, const DataType &aggType, int32_t rowPerVecBatch)
{
    switch (aggType.GetId()) {
        case OMNI_NONE: {
            LongVector *col = new LongVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValueNull(j);
            }
            return col;
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            IntVector *col = new IntVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            LongVector *col = new LongVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_DOUBLE: {
            DoubleVector *col = new DoubleVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_BOOLEAN: {
            BooleanVector *col = new BooleanVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, true);
            }
            return col;
        }
        case OMNI_DECIMAL128: {
            Decimal128Vector *col = new Decimal128Vector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, Decimal128(0, 1));
            }
            return col;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            VarcharDataType charType = (VarcharDataType &)aggType;
            VarcharVector *col = new VarcharVector(vecAllocator, charType.GetWidth() * rowPerVecBatch, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                std::string str = std::to_string(j);
                col->SetValue(j, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
            }
            return col;
        }
        default: {
            LogError("No such %d type support", aggType.GetId());
            return nullptr;
        }
    }
}

VectorBatch **buildAggInput(int32_t vecBatchNum, int32_t rowPerVecBatch, int32_t cardinality, int32_t groupColNum,
    int32_t aggColNum, const std::vector<DataType> &groupTypes, const std::vector<DataType> &aggTypes)
{
    VectorBatch **input = new VectorBatch *[vecBatchNum];
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_count_buildAggInput");
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        VectorBatch *vecBatch = new VectorBatch(groupColNum + aggColNum);
        for (int32_t index = 0; index < groupColNum; ++index) {
            Vector *vec = BuildHashInput(groupTypes[index], rowPerVecBatch, cardinality);
            vecBatch->SetVector(index, vec);
        }
        for (int32_t index = 0; index < aggColNum; ++index) {
            Vector *vec = BuildAggregateInput(vectorAllocator, aggTypes[index], rowPerVecBatch);
            vecBatch->SetVector(groupColNum + index, vec);
        }
        input[i] = vecBatch;
    }
    return input;
}

VectorBatch **BuildVarCharInput(int32_t vecBatchNum, int32_t colNum, int32_t rowPerVecBatch, ...)
{
    va_list args;
    va_start(args, rowPerVecBatch);
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation_buildVarCharInput");
    VectorBatch **input = new VectorBatch *[vecBatchNum];
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        VectorBatch *vecBatch = new VectorBatch(colNum);
        for (int32_t c = 0; c < colNum; ++c) {
            VarcharVector *col = new VarcharVector(vecAllocator, rowPerVecBatch * 10, rowPerVecBatch);
            std::string *values = va_arg(args, std::string *);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, reinterpret_cast<const uint8_t *>(values[j].c_str()), values[j].length());
            }
            vecBatch->SetVector(c, col);
        }
        input[i] = vecBatch;
    }
    va_end(args);
    return input;
}

// create a factory and make it optimized
uintptr_t CreateHashFactoryWithJit(bool inputRaw, bool outputPartial)
{
    uint32_t groupCols[2] = {0, 1};
    std::vector<DataType> groupByTypeVec = { LongDataType::Instance(), LongDataType::Instance() };
    DataTypes groupByTypes(groupByTypeVec);
    uint32_t aggCols[2] = {2, 3};
    std::vector<DataType> aggInputTypeVec = { LongDataType::Instance(), LongDataType::Instance() };
    DataTypes aggInputTypes(aggInputTypeVec);
    std::vector<DataType> aggOutputTypeVec = { LongDataType::Instance(), LongDataType::Instance() };
    DataTypes aggOutputTypes(aggOutputTypeVec);
    uint32_t aggFunType[2] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG};
    PrepareContext groupByColContext = { groupCols, 2 };

    PrepareContext aggColContext = { aggCols, 2 };
    PrepareContext aggFuncTypeContext = { aggFunType, 2 };

    auto jitContext = CreateHashAggregationJitContext(groupByTypes, 2);
    std::cout << "after jit" << std::endl;
    omniruntime::op::HashAggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByTypes, aggColContext,
        aggInputTypes, aggOutputTypes, aggFuncTypeContext, inputRaw, outputPartial);
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->SetJitContext(jitContext);
    nativeOperatorFactory->Init();
    return reinterpret_cast<uintptr_t>(nativeOperatorFactory);
}

uintptr_t CreateAggFactoryWithJit()
{
    const int CONST_VALUE_4 = 4;
    DataTypes sourceTypes(
        { LongDataType::Instance(), LongDataType::Instance(), LongDataType::Instance(), LongDataType::Instance() });
    uint32_t aggFuncTypes[CONST_VALUE_4] = {0, 0, 0, 0};
    omniruntime::op::PrepareContext aggFuncTypeContext = { aggFuncTypes, CONST_VALUE_4 };
    uint32_t aggInputCols[CONST_VALUE_4] = {0, 1, 2, 3};
    omniruntime::op::PrepareContext aggInputColsContext = { aggInputCols, CONST_VALUE_4 };
    uint32_t maskCols[CONST_VALUE_4] = {static_cast<uint32_t>(-1), static_cast<uint32_t>(-1),
                                        static_cast<uint32_t>(-1),
                                        static_cast<uint32_t>(-1)};
    omniruntime::op::PrepareContext maskColsContext = { maskCols, CONST_VALUE_4 };

    auto jitContext = CreateAggregationJitContext();
    std::cout << "after jit" << std::endl;
    auto nativeOperatorFactory = new AggregationOperatorFactory(sourceTypes, aggFuncTypeContext, aggInputColsContext,
        maskColsContext, sourceTypes, true, false);
    nativeOperatorFactory->Init();
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->SetJitContext(jitContext);
    return reinterpret_cast<uintptr_t>(nativeOperatorFactory);
}

using HAFactoryParameters = struct HashAggregationFactoryParameters {
public:
    bool inputRaw;
    bool outputPartial;
    std::vector<uint32_t> groupCols;
    std::vector<DataType> groupByTypes;
    std::vector<uint32_t> aggCols;
    std::vector<DataType> aggInputTypes;
    std::vector<DataType> aggOutputTypes;
    std::vector<uint32_t> aggFuncTypes;
};

uintptr_t CreateHashFactoryWithoutJit(HAFactoryParameters &parameters)
{
    DataTypes groupByTypes(parameters.groupByTypes);
    DataTypes aggInputTypes(parameters.aggInputTypes);
    DataTypes aggOutputTypes(parameters.aggOutputTypes);

    PrepareContext groupByColContext = { parameters.groupCols.data(), parameters.groupCols.size() };
    PrepareContext aggColContext = { parameters.aggCols.data(), parameters.aggCols.size() };
    PrepareContext aggFuncTypeContext = { parameters.aggFuncTypes.data(), parameters.aggFuncTypes.size() };

    omniruntime::op::HashAggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByTypes, aggColContext,
        aggInputTypes, aggOutputTypes, aggFuncTypeContext, parameters.inputRaw, parameters.outputPartial);
    nativeOperatorFactory->Init();
    return reinterpret_cast<uintptr_t>(nativeOperatorFactory);
}

double g_totalCpuTime;
double g_totalWallTime;

void PerfTestOriginal(int64_t moduleAddr, VectorBatch **input)
{
    // create operator
    HashAggregationOperatorFactory *nativeOperatorFactory =
        reinterpret_cast<HashAggregationOperatorFactory *>(moduleAddr);
    auto groupBy = nativeOperatorFactory->CreateOperator();

    // execution
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        auto copiedBatch = DuplicateVectorBatch(input[i]);
        groupBy->AddInput(copiedBatch);
    }
    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = groupBy->GetOutput(result);
    EXPECT_EQ(vecBatchCount, 1);
    EXPECT_EQ(result[0]->GetVectorCount(), 4);
    EXPECT_EQ(result[0]->GetRowCount(), 4);
    for (auto res : result) {
        VectorHelper::FreeVecBatch(res);
    }
    Operator::DeleteOperator(groupBy);
}

void PerfTest(int64_t moduleAddr, VectorBatch **input, int32_t vecBatchNum, int32_t *rowCount)
{
    // create operatory
    HashAggregationOperatorFactory *nativeOperatorFactory =
        reinterpret_cast<HashAggregationOperatorFactory *>(moduleAddr);
    auto groupBy = reinterpret_cast<HashAggModule>(nativeOperatorFactory->GetJitContext()->func)(nativeOperatorFactory);

    // execution
    for (int pageIndex = 0; pageIndex < vecBatchNum; ++pageIndex) {
        auto copiedBatch = DuplicateVectorBatch(input[pageIndex]);
        auto errNo = groupBy->AddInput(copiedBatch);
        EXPECT_EQ(errNo, OMNI_STATUS_NORMAL);
    }
    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = groupBy->GetOutput(result);
    EXPECT_EQ(vecBatchCount, 1);
    EXPECT_EQ(result[0]->GetVectorCount(), 4);
    EXPECT_EQ(result[0]->GetRowCount(), 4);
    for (auto res : result) {
        VectorHelper::FreeVecBatch(res);
    }
    Operator::DeleteOperator(groupBy);
}

void PerfTestNonGroup(int64_t moduleAddr, bool codegenMode, VectorBatch **input, int32_t vecBatchNum, int32_t *rowCount)
{
    // create operatory
    auto nativeOperatorFactory = reinterpret_cast<AggregationOperatorFactory *>(moduleAddr);
    Operator *aggregation;
    if (codegenMode) {
        aggregation = reinterpret_cast<OptModule>(nativeOperatorFactory->GetJitContext()->func)(nativeOperatorFactory);
    } else {
        aggregation = nativeOperatorFactory->CreateOperator();
    }

    // execution
    for (int pageIndex = 0; pageIndex < vecBatchNum; ++pageIndex) {
        auto copiedBatch = DuplicateVectorBatch(input[pageIndex]);
        auto errNo = aggregation->AddInput(copiedBatch);
        EXPECT_EQ(errNo, OMNI_STATUS_NORMAL);
    }
    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = aggregation->GetOutput(result);
    EXPECT_EQ(vecBatchCount, 1);
    EXPECT_EQ(result[0]->GetVectorCount(), 4);
    EXPECT_EQ(result[0]->GetRowCount(), 1);
    for (auto res : result) {
        VectorHelper::FreeVecBatch(res);
    }
}

TEST(HashAggregationOperatorTest, verify_correctness)
{
    // create 10 pages
    const int vecBatchNum = 10;
    const int rowSize = 2000;
    const int cardinality = 10;

    std::string aggNames[] = {"group", "group", "sum", "avg", "count", "min", "max"};
    std::vector<DataType> groupTypes = { LongDataType(), LongDataType() };
    std::vector<DataType> aggTypes = { LongDataType(), LongDataType(), LongDataType(), LongDataType(), LongDataType() };
    VectorBatch **input1 = buildAggInput(vecBatchNum, rowSize, cardinality, 2, 5, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    // First stage
    ColumnIndex c0 = { 0, LongDataType::Instance(), LongDataType::Instance() };
    ColumnIndex c1 = { 1, LongDataType::Instance(), LongDataType::Instance() };
    std::vector<int32_t> aggInputCols1 = { 2, 3, 4, 5, 6 };
    DataTypes aggInputTypes1(std::vector<DataType> { LongDataType::Instance(), LongDataType::Instance(),
        LongDataType::Instance(), LongDataType::Instance(), LongDataType::Instance() });
    DataTypes aggOutputTypes1(std::vector<DataType> { LongDataType::Instance(), ContainerDataType::Instance(),
        LongDataType::Instance(), LongDataType::Instance(), LongDataType::Instance() });
    std::vector<ColumnIndex> groupByColumns1 = { c0, c1 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 2, true, true));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(LongDataType::Instance(),
        ContainerDataType::Instance(), 3, true, true));
    aggs1.push_back(std::make_unique<CountColumnAggregator>(LongDataType::Instance(), 4, true, true));
    aggs1.push_back(std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 5, true, true));
    aggs1.push_back(std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 6, true, true));
    HashAggregationOperator *groupBy1 = new HashAggregationOperator(groupByColumns1, aggInputCols1, aggInputTypes1,
        aggOutputTypes1, std::move(aggs1), true, true);
    groupBy1->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupBy1->AddInput(input1[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t vecBatchCount = groupBy1->GetOutput(result1);
    EXPECT_EQ(vecBatchCount, 1);
    Operator::DeleteOperator(groupBy1);

    VectorBatch **input2 = buildAggInput(vecBatchNum, rowSize, cardinality, 2, 5, groupTypes, aggTypes);
    if (input2 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    ColumnIndex c2 = { 0, LongDataType::Instance(), LongDataType::Instance() };
    ColumnIndex c3 = { 1, LongDataType::Instance(), LongDataType::Instance() };
    std::vector<int32_t> aggInputCols2 = { 2, 3, 4, 5, 6 };
    DataTypes aggInputTypes2(std::vector<DataType> { LongDataType::Instance(), LongDataType::Instance(),
        LongDataType::Instance(), LongDataType::Instance(), LongDataType::Instance() });
    DataTypes aggOutputTypes2(std::vector<DataType> { LongDataType::Instance(), ContainerDataType::Instance(),
        LongDataType::Instance(), LongDataType::Instance(), LongDataType::Instance() });
    groupByColumns1 = { c2, c3 };

    aggs1.clear();
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 2, true, true));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(LongDataType::Instance(),
        ContainerDataType::Instance(), 3, true, true));
    aggs1.push_back(std::make_unique<CountColumnAggregator>(LongDataType::Instance(), 4, true, true));
    aggs1.push_back(std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 5, true, true));
    aggs1.push_back(std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 6, true, true));
    HashAggregationOperator *groupBy2 = new HashAggregationOperator(groupByColumns1, aggInputCols2, aggInputTypes2,
        aggOutputTypes2, std::move(aggs1), true, true);
    groupBy2->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupBy2->AddInput(input2[i]);
    }

    std::vector<VectorBatch *> result2;
    int32_t tableCount2 = groupBy2->GetOutput(result2);
    EXPECT_EQ(tableCount2, 1);
    Operator::DeleteOperator(groupBy2);

    // Second stage
    ColumnIndex c4 = { 0, LongDataType::Instance(), LongDataType::Instance() };
    ColumnIndex c5 = { 1, LongDataType::Instance(), LongDataType::Instance() };
    std::vector<int32_t> aggInputCols3 = { 2, 3, 4, 5, 6 };
    DataTypes aggInputTypes3(std::vector<DataType> { LongDataType::Instance(), LongDataType::Instance(),
        LongDataType::Instance(), LongDataType::Instance(), LongDataType::Instance() });
    DataTypes aggOutputTypes3(std::vector<DataType> { LongDataType::Instance(), DoubleDataType::Instance(),
        LongDataType::Instance(), LongDataType::Instance(), LongDataType::Instance() });

    std::vector<ColumnIndex> groupByColumns2 = { c4, c5 };
    std::vector<std::unique_ptr<Aggregator>> aggs2;
    aggs2.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 2, false, false));
    aggs2.push_back(std::make_unique<AverageAggregator<LongVector>>(LongDataType::Instance(),
        ContainerDataType::Instance(), 3, false, false));
    aggs2.push_back(std::make_unique<CountColumnAggregator>(LongDataType::Instance(), 4, false, false));
    aggs2.push_back(std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 5, false, false));
    aggs2.push_back(std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 6, false, false));
    HashAggregationOperator *groupBy3 = new HashAggregationOperator(groupByColumns2, aggInputCols3, aggInputTypes3,
        aggOutputTypes3, std::move(aggs2), false, false);
    groupBy3->Init();

    for (uint32_t i = 0; i < result1.size(); ++i) {
        groupBy3->AddInput(result1[i]);
    }
    for (uint32_t i = 0; i < result2.size(); ++i) {
        groupBy3->AddInput(result2[i]);
    }

    std::vector<VectorBatch *> result3;
    groupBy3->GetOutput(result3);
    Operator::DeleteOperator(groupBy3);

    // construct the output data
    DataTypes expectTypes(std::vector<DataType>({ LongDataType(), LongDataType(), LongDataType(), DoubleDataType(),
        LongDataType(), LongDataType(), LongDataType() }));
    int64_t expectData1[cardinality] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int64_t expectData2[cardinality] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int64_t expectData3[cardinality] = {4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000};
    double expectData4[cardinality] = {1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0};
    int64_t expectData5[cardinality] = {4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000};
    int64_t expectData6[cardinality] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    int64_t expectData7[cardinality] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, cardinality, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6, expectData7);

    EXPECT_TRUE(VecBatchMatch(result3[0], expectVecBatch));
    EXPECT_EQ(result3[0]->GetVectorCount(), 7);

    delete[] input1;
    delete[] input2;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(result3);
}

TEST(HashAggregationOperatorTest, verify_varchar_vector_correctness)
{
    // create 10 pages
    const int vecBatchNum = 1;
    const int rowSize = 8;
    const int columnCount = 4; // groupby + count + min + max
    std::string aggNames[] = {"group", "count", "min", "max" };
    std::string data0[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data1[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data2[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data3[8] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1", "1.1", "1"};
    VectorBatch **input = BuildVarCharInput(vecBatchNum, columnCount, rowSize, data0, data1, data2, data3);
    // First stage
    VarcharDataType type1(1);
    VarcharDataType type2(1);
    VarcharDataType type3(1);
    VarcharDataType type4(4);
    ColumnIndex c0 = { 0, type1, type1 };
    std::vector<int32_t> aggInputCols1 = { 1, 2, 3 };
    DataTypes aggInputTypes1(std::vector<DataType> { type2, type3, type4 });
    DataTypes aggOutputTypes1(std::vector<DataType> { LongDataType(), type3, type4 });

    std::vector<ColumnIndex> groupByColumns1 = { c0 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<CountColumnAggregator>(LongDataType::Instance(), 1, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MinVarcharAggregator>(VarcharDataType::Instance(), VarcharDataType::Instance(), 2,
        INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MaxVarcharAggregator>(VarcharDataType::Instance(), VarcharDataType::Instance(), 3,
        INPUT_MODE, OUTPUT_MODE));

    HashAggregationOperator *groupByVarChar = new HashAggregationOperator(groupByColumns1, aggInputCols1,
        aggInputTypes1, aggOutputTypes1, std::move(aggs1), true, false);
    groupByVarChar->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupByVarChar->AddInput(input[i]);
    }
    std::vector<VectorBatch *> result1;
    int32_t vecBatchCount = groupByVarChar->GetOutput(result1);
    EXPECT_EQ(vecBatchCount, 1);
    auto resBatch = VectorHelper::ConcatVectorBatches(result1);
    for (auto res : result1) {
        VectorHelper::FreeVecBatch(res);
    }

    Operator::DeleteOperator(groupByVarChar);
    std::string expectData1[3] = {"2", "1", "0"};
    int64_t expectData2[3] = {2, 3, 3};
    std::string expectData3[3] = {"2", "1", "0"};
    std::string expectData4[3] = {"4.4", "5.5", "6.6"};
    DataTypes expectedTypes(
        std::vector<DataType>({ VarcharDataType(1), LongDataType(), VarcharDataType(1), VarcharDataType(3) }));
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectedTypes, 3, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatch(resBatch, expectVecBatch));
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(resBatch);
}

TEST(HashAggregationOperatorTest, verify_char_vector_correctness)
{
    // create 10 pages
    const int vecBatchNum = 1;
    const int rowSize = 8;
    const int columnCount = 4; // groupby + count + min + max
    std::string aggNames[] = {"group", "count", "min", "max" };
    std::string data0[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data1[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data2[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data3[8] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1", "1.1", "1"};
    VectorBatch **input = BuildVarCharInput(vecBatchNum, columnCount, rowSize, data0, data1, data2, data3);
    // First stage
    CharDataType type1(1);
    CharDataType type2(1);
    CharDataType type3(1);
    CharDataType type4(4);
    ColumnIndex c0 = { 0, type1, type1 };
    std::vector<int32_t> aggInputCols1 = { 1, 2, 3 };
    DataTypes aggInputTypes1(std::vector<DataType> { type2, type3, type4 });
    DataTypes aggOutputTypes1(std::vector<DataType> { LongDataType(), type3, type4 });

    std::vector<ColumnIndex> groupByColumns1 = { c0 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<CountColumnAggregator>(LongDataType::Instance(), 1, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MinVarcharAggregator>(CharDataType::Instance(), CharDataType::Instance(), 2,
        INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MaxVarcharAggregator>(CharDataType::Instance(), CharDataType::Instance(), 3,
        INPUT_MODE, OUTPUT_MODE));
    HashAggregationOperator *groupByVarChar = new HashAggregationOperator(groupByColumns1, aggInputCols1,
        aggInputTypes1, aggOutputTypes1, std::move(aggs1), true, false);
    groupByVarChar->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupByVarChar->AddInput(input[i]);
    }
    std::vector<VectorBatch *> result1;
    int32_t vecBatchCount = groupByVarChar->GetOutput(result1);
    EXPECT_EQ(vecBatchCount, 1);

    auto resBatch = VectorHelper::ConcatVectorBatches(result1);
    for (auto res : result1) {
        VectorHelper::FreeVecBatch(res);
    }

    Operator::DeleteOperator(groupByVarChar);
    std::string expectData1[3] = {"2", "1", "0"};
    int64_t expectData2[3] = {2, 3, 3};
    std::string expectData3[3] = {"2", "1", "0"};
    std::string expectData4[3] = {"4.4", "5.5", "6.6"};
    DataTypes expectedTypes(
        std::vector<DataType>({ CharDataType(1), LongDataType(), CharDataType(1), CharDataType(3) }));
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectedTypes, 3, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatch(resBatch, expectVecBatch));
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(resBatch);
}

TEST(HashAggregationOperatorTest, verify_null_correctness)
{
    // create 10 pages
    const int vecBatchNum = 1;
    const int ROW_SIZE = 6;
    const int cardinality = 1;
    std::string aggNames[] = {"group", "sum", "avg", "count", "min", "max"};
    std::vector<DataType> groupTypes = { LongDataType() };
    std::vector<DataType> aggTypes = { LongDataType(), LongDataType(), LongDataType(), LongDataType(), LongDataType() };
    VectorBatch **input = buildAggInput(vecBatchNum, ROW_SIZE, cardinality, 1, 5, groupTypes, aggTypes);
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
        return;
    }

    input[0]->GetVector(2)->SetValueNull(0);
    input[0]->GetVector(2)->SetValueNull(1);
    input[0]->GetVector(2)->SetValueNull(2);
    input[0]->GetVector(2)->SetValueNull(3);
    input[0]->GetVector(2)->SetValueNull(4);

    input[0]->GetVector(3)->SetValueNull(1);
    input[0]->GetVector(4)->SetValueNull(1);
    input[0]->GetVector(5)->SetValueNull(1);

    // First stage
    ColumnIndex c0 = { 0, LongDataType::Instance(), LongDataType::Instance() };
    std::vector<int32_t> aggInputCols1 = { 1, 2, 3, 4, 5 };
    DataTypes aggInputTypes1(std::vector<DataType> { LongDataType::Instance(), LongDataType::Instance(),
        LongDataType::Instance(), LongDataType::Instance(), LongDataType::Instance() });
    DataTypes aggOutputTypes1(std::vector<DataType> { LongDataType::Instance(), DoubleDataType::Instance(),
        LongDataType::Instance(), LongDataType::Instance(), LongDataType::Instance() });

    std::vector<ColumnIndex> groupByColumns1 = { c0 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 1, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(LongDataType::Instance(),
        DoubleDataType::Instance(), 2, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<CountColumnAggregator>(LongDataType::Instance(), 3, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 4, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 5, INPUT_MODE, OUTPUT_MODE));

    HashAggregationOperator *groupByNULL = new HashAggregationOperator(groupByColumns1, aggInputCols1, aggInputTypes1,
        aggOutputTypes1, std::move(aggs1), true, false);
    groupByNULL->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupByNULL->AddInput(input[i]);
    }
    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = groupByNULL->GetOutput(result);
    EXPECT_EQ(vecBatchCount, 1);

    Operator::DeleteOperator(groupByNULL);

    int64_t expectData1[1] = {0};
    int64_t expectData2[1] = {6};
    double expectData3[1] = {1};
    int64_t expectData4[1] = {5};
    int64_t expectData5[1] = {1};
    int64_t expectData6[1] = {1};
    DataTypes expectedTypes(std::vector<DataType>(
        { LongDataType(), LongDataType(), DoubleDataType(), LongDataType(), LongDataType(), LongDataType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 1, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);

    EXPECT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatches(result);
    VectorHelper::FreeVecBatch(expectVecBatch);
}

TEST(HashAggregationOperatorTest, verfify_correctness_group_by_agg_same_cols)
{
    // create 10 vecBatches
    const int vecBatchNum = 10;
    VectorBatch **input = new VectorBatch *[vecBatchNum];
    const int dataSize = 10;
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
            "aggregation_verfify_correctness_group_by_agg_same_cols");
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        VectorBatch *vecBatch = new VectorBatch(2);
        LongVector *col1 = new LongVector(vecAllocator, dataSize);
        for (int32_t j = 0; j < dataSize; ++j) {
            col1->SetValue(j, j % 3);
        }

        LongVector *col2 = new LongVector(vecAllocator, dataSize);
        for (int32_t j = 0; j < dataSize; ++j) {
            col2->SetValue(j, j % 3);
        }
        vecBatch->SetVector(0, col1);
        vecBatch->SetVector(1, col2);
        input[i] = vecBatch;
    }
    ColumnIndex c0 = { 0, LongDataType::Instance(), LongDataType::Instance() };
    ColumnIndex c1 = { 1, LongDataType::Instance(), LongDataType::Instance() };
    std::vector<int32_t> aggInputCols = { 0, 1 };
    DataTypes aggInputTypes(std::vector<DataType> { LongDataType::Instance(), LongDataType::Instance() });
    DataTypes aggOutputTypes(std::vector<DataType> { LongDataType::Instance(), LongDataType::Instance() });
    std::vector<ColumnIndex> groupByColumns = { c0, c1 };
    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(IntDataType::Instance(),
        IntDataType::Instance(), 0, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 1, INPUT_MODE, OUTPUT_MODE));
    HashAggregationOperator *groupBy = new HashAggregationOperator(groupByColumns, aggInputCols, aggInputTypes,
        aggOutputTypes, std::move(aggs), true, false);
    groupBy->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupBy->AddInput(input[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = groupBy->GetOutput(result);
    EXPECT_EQ(vecBatchCount, 1);

    Operator::DeleteOperator(groupBy);

    EXPECT_EQ(result[0]->GetVectorCount(), 4);

    VectorHelper::FreeVecBatches(result);
}

TEST(HashAggregationOperatorTest, DISABLED_original_multiple_threads)
{
    using namespace std;
    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;


    FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    DataTypes groupTypes({ LongDataType::Instance(), LongDataType::Instance() });
    DataTypes aggInputTypes({ LongDataType::Instance(), LongDataType::Instance() });
    DataTypes aggOutputTypes({ LongDataType::Instance(), LongDataType::Instance() });
    VectorBatch **input =
        buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes.Get(), aggInputTypes.Get());
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    uint32_t groupCols[2] = {0, 1};
    uint32_t aggCols[2] = {2, 3};
    PrepareContext groupByColContext = { groupCols, 2 };
    PrepareContext aggColContext = { aggCols, 2 };
    PrepareContext aggFuncTypeContext = { reinterpret_cast<uint32_t *>(aggFunType), 2 };
    HashAggregationOperatorFactory *nativeOperatorFactory = new HashAggregationOperatorFactory(groupByColContext,
        groupTypes, aggColContext, aggInputTypes, aggOutputTypes, aggFuncTypeContext, true, false);
    nativeOperatorFactory->Init();
    uint64_t factoryObjAddr = reinterpret_cast<uint64_t>(nativeOperatorFactory);

    uint32_t threadNums[] = {1, 2, 4, 8, 16};
    for (uint32_t i = 0; i < sizeof(threadNums) / sizeof(uint32_t); ++i) {
        g_totalWallTime = 0;
        g_totalCpuTime = 0;
        auto t_ = threadNums[i] < processorCount ? processorCount / threadNums[i] : 1;

        uint32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.SetStart();
        for (uint32_t j = 0; j < threadNum; ++j) {
            // same stage Id
            std::thread t(PerfTestOriginal, factoryObjAddr, input);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.CalculateElapse();
        double wallElapsed = timer.GetWallElapse();
        double cpuElapsed = timer.GetCpuElapse();
        std::cout << threadNum << " wallElapsed time: " << wallElapsed << "s" << std::endl;
        std::cout << threadNum << " cpuElapsed time: " << cpuElapsed / processorCount * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    DeleteOperatorFactory(nativeOperatorFactory);
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(HashAggregationOperatorTest, DISABLED_perf_via_API_multiple_threads)
{
    using namespace std;
    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;

    vector<DataType> groupTypes = { LongDataType(), LongDataType() };
    vector<DataType> aggTypes = { LongDataType(), LongDataType() };
    VectorBatch **input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    int32_t *rowCount = new int32_t[VEC_BATCH_NUM];
    for (int32_t i = 0; i < VEC_BATCH_NUM; i++) {
        rowCount[i] = ROW_PER_VEC_BATCH;
    }
    uint64_t factoryObjAddr = CreateHashFactoryWithJit(true, false);
    std::cout << "after prepare" << std::endl;
    uint32_t threadNums[] = {1, 2, 4, 8, 16};
    for (uint32_t i = 0; i < sizeof(threadNums) / sizeof(uint32_t); ++i) {
        g_totalWallTime = 0;
        g_totalCpuTime = 0;
        auto t_ = threadNums[i] < processorCount ? processorCount / threadNums[i] : 1;

        uint32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.SetStart();
        for (uint32_t j = 0; j < threadNum; ++j) {
            // same stage Id
            std::thread t(PerfTest, factoryObjAddr, input, VEC_BATCH_NUM, rowCount);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.CalculateElapse();
        double wallElapsed = timer.GetWallElapse();
        double cpuElapsed = timer.GetCpuElapse();
        std::cout << threadNum << " wallElapsed time: " << wallElapsed << "s" << std::endl;
        std::cout << threadNum << " cpuElapsed time: " << cpuElapsed / processorCount * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    DeleteOperatorFactory(reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(factoryObjAddr));
    delete[] rowCount;
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(AggregationOperatorTest, verify_correctness)
{
    // create 10 vecBatches
    const int vecBatchNum = 10;
    const int cardinality = 4;
    std::string aggNames[] = {"sum", "avg", "count", "min", "max"};
    std::vector<DataType> groupTypes;
    std::vector<DataType> aggTypes = { LongDataType(), LongDataType(), LongDataType(), LongDataType(), LongDataType() };
    VectorBatch **input1 = buildAggInput(vecBatchNum, ROW_PER_VEC_BATCH, cardinality, 0, 5, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 0, true, true));
    aggs.push_back(std::make_unique<AverageAggregator<LongVector>>(LongDataType::Instance(),
        ContainerDataType::Instance(), 1, true, true));
    aggs.push_back(std::make_unique<CountColumnAggregator>(LongDataType::Instance(), 2, true, true));
    aggs.push_back(std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 3, true, true));
    aggs.push_back(std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 4, true, true));
    std::vector<int32_t> aggInputCols = { 0, 1, 2, 3, 4 };
    std::vector<int32_t> maskCols = { -1, -1, -1, -1, -1 };
    DataTypes aggPartialOutputTypes(
        std::vector<DataType> { LongDataType(), ContainerDataType(), LongDataType(), LongDataType(), LongDataType() });
    auto aggregate1 =
        new AggregationOperator(std::move(aggs), aggInputCols, maskCols, aggPartialOutputTypes, true, true);

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggregate1->AddInput(input1[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t tableCount1 = aggregate1->GetOutput(result);
    EXPECT_EQ(tableCount1, 1);
    omniruntime::op::Operator::DeleteOperator(aggregate1);

    VectorBatch **input2 = buildAggInput(vecBatchNum, ROW_PER_VEC_BATCH, cardinality, 0, 5, groupTypes, aggTypes);
    ASSERT(!(input2 == nullptr));
    aggs.clear();
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 0, true, true));
    aggs.push_back(std::make_unique<AverageAggregator<LongVector>>(LongDataType::Instance(),
        ContainerDataType::Instance(), 1, true, true));
    aggs.push_back(std::make_unique<CountColumnAggregator>(LongDataType::Instance(), 2, true, true));
    aggs.push_back(std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 3, true, true));
    aggs.push_back(std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 4, true, true));
    auto aggregate2 =
        new AggregationOperator(std::move(aggs), aggInputCols, maskCols, aggPartialOutputTypes, true, true);

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggregate2->AddInput(input2[i]);
    }
    int32_t tableCount2 = aggregate2->GetOutput(result);
    EXPECT_EQ(tableCount2, 1);
    omniruntime::op::Operator::DeleteOperator(aggregate2);

    // Second stage
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 0, false, false));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(LongDataType::Instance(),
        DoubleDataType::Instance(), 1, false, false));
    aggs1.push_back(std::make_unique<CountColumnAggregator>(LongDataType::Instance(), 2, false, false));
    aggs1.push_back(std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 3, false, false));
    aggs1.push_back(std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 4, false, false));
    DataTypes aggFinalOutputTypes(
        std::vector<DataType> { LongDataType(), DoubleDataType(), LongDataType(), LongDataType(), LongDataType() });
    auto aggregate3 =
        new AggregationOperator(std::move(aggs1), aggInputCols, maskCols, aggFinalOutputTypes, false, false);

    for (uint32_t i = 0; i < result.size(); ++i) {
        aggregate3->AddInput(result[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t tableCount3 = aggregate3->GetOutput(result1);
    EXPECT_EQ(tableCount3, 1);
    EXPECT_EQ(result1[0]->GetRowCount(), 1);
    EXPECT_EQ(result1[0]->GetVectorCount(), 5);

    omniruntime::op::Operator::DeleteOperator(aggregate3);
    VectorHelper::FreeVecBatches(result1);
}

TEST(AggregationOperatorTest, verify_agg_distinct)
{
    // construct data
    const int32_t dataSize = 4;
    const int32_t resultDataSize = 1;

    // table1
    int64_t data0[dataSize] = {10L, 20L, 10L, 30L};
    int64_t data1[dataSize] = {1L, 1L, 2L, 3L};
    int64_t data2[dataSize] = {3L, 5L, 5L, 7L};
    int64_t data3[dataSize] = {4L, 2L, 1L, 2L};
    int64_t data4[dataSize] = {4L, 2L, 1L, 2L};
    bool data5[dataSize] = {true, true, false, true};
    bool data6[dataSize] = {true, false, true, true};
    bool data7[dataSize] = {true, true, false, true};
    bool data8[dataSize] = {true, true, true, false};
    bool data9[dataSize] = {true, true, true, false};

    std::vector<DataType> types = { LongDataType::Instance(),    LongDataType::Instance(),
        LongDataType::Instance(),    LongDataType::Instance(),
        LongDataType::Instance(),    BooleanDataType::Instance(),
        BooleanDataType::Instance(), BooleanDataType::Instance(),
        BooleanDataType::Instance(), BooleanDataType::Instance() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 =
        CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2, data3, data4, data5, data6, data7, data8, data9);

    std::vector<int32_t> aggInputCols = { 0, 1, 2, 3, 4 };
    std::vector<int32_t> maskCols = { 5, 6, 7, 8, 9 };

    // STAGE1:
    std::vector<std::unique_ptr<Aggregator>> aggs;
    std::unique_ptr<Aggregator> aggregator =
        std::make_unique<CountColumnAggregator>(LongDataType::Instance(), 0, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(5, std::move(aggregator)));
    aggregator = std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 1, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(6, std::move(aggregator)));
    aggregator = std::make_unique<AverageAggregator<LongVector>>(LongDataType::Instance(),
        ContainerDataType::Instance(), 2, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(7, std::move(aggregator)));
    aggregator = std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 3, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(8, std::move(aggregator)));
    aggregator = std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 4, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(9, std::move(aggregator)));

    DataTypes aggPartialOutputTypes(
        std::vector<DataType> { LongDataType(), LongDataType(), ContainerDataType(), LongDataType(), LongDataType() });
    auto aggregate1 =
        new AggregationOperator(std::move(aggs), aggInputCols, maskCols, aggPartialOutputTypes, true, true);

    aggregate1->AddInput(vecBatch1);

    std::vector<VectorBatch *> result;
    int32_t tableCount1 = aggregate1->GetOutput(result);
    EXPECT_EQ(tableCount1, 1);

    // STAGE2:
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<CountColumnAggregator>(LongDataType::Instance(), 0, false, false));
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 1, false, false));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(LongDataType::Instance(),
        DoubleDataType::Instance(), 2, false, false));
    aggs1.push_back(std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 3, false, false));
    aggs1.push_back(std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 4, false, false));
    DataTypes aggFinalOutputTypes(
        std::vector<DataType> { LongDataType(), LongDataType(), DoubleDataType(), LongDataType(), LongDataType() });
    std::vector<int32_t> maskCols1 = { -1, -1, -1, -1, -1 }; // no mask cols at final stage
    auto aggregate2 =
        new AggregationOperator(std::move(aggs1), aggInputCols, maskCols1, aggFinalOutputTypes, false, false);

    for (uint32_t i = 0; i < result.size(); ++i) {
        aggregate2->AddInput(result[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t tableCount = aggregate2->GetOutput(result1);
    EXPECT_EQ(tableCount, 1);
    EXPECT_EQ(result1[0]->GetRowCount(), 1);
    EXPECT_EQ(result1[0]->GetVectorCount(), 5);

    int64_t expData0[resultDataSize] = {3L};
    int64_t expData1[resultDataSize] = {6L};
    double expData2[resultDataSize] = {5.0};
    int64_t expData3[resultDataSize] = {4L};
    int64_t expData4[resultDataSize] = {1L};
    std::vector<DataType> resultType = { LongDataType::Instance(), LongDataType::Instance(), DoubleDataType::Instance(),
        LongDataType::Instance(), LongDataType::Instance() };
    DataTypes resultTypes(resultType);
    VectorBatch *expVecBatch1 =
        CreateVectorBatch(resultTypes, resultDataSize, expData0, expData1, expData2, expData3, expData4);

    EXPECT_TRUE(VecBatchMatch(result1[0], expVecBatch1));

    omniruntime::op::Operator::DeleteOperator(aggregate1);
    omniruntime::op::Operator::DeleteOperator(aggregate2);
    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatches(result1);
}

TEST(AggregationOperatorTest, avg_correctness_test)
{
    // create 10 pages
    const int vecBatchNum = 10;
    const int cardinality = 100;
    std::vector<DataType> groupTypes;
    std::vector<DataType> aggTypes = { LongDataType() };
    VectorBatch **input = buildAggInput(vecBatchNum, ROW_PER_VEC_BATCH, cardinality, 0, 1, groupTypes, aggTypes);
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    ColumnIndex c0 = { 0, LongDataType::Instance(), LongDataType::Instance() };
    std::vector<ColumnIndex> aggregateColumns = { c0 };
    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(
        std::make_unique<AverageAggregator<LongVector>>(LongDataType::Instance(), DoubleDataType::Instance(), 0));
    std::vector<int32_t> aggInputCols = { 0 };
    std::vector<int32_t> maskCols = { -1 };
    DataTypes aggOutputTypes(std::vector<DataType> { DoubleDataType() });
    auto aggregate = new AggregationOperator(std::move(aggs), aggInputCols, maskCols, aggOutputTypes, true, false);

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggregate->AddInput(input[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t tableCount = aggregate->GetOutput(result);

    EXPECT_EQ(tableCount, 1);
    EXPECT_EQ(result[0]->GetVectorCount(), 1);
    EXPECT_EQ(result[0]->GetRowCount(), 1);

    VectorHelper::FreeVecBatches(result);
    Operator::DeleteOperator(aggregate);
}

TEST(AggregationOperatorTest, min_max_varchar_correctness)
{
    std::string data0[] = {"operators", "operator", "operators", "helloha", "hello", "helloha"};
    std::string data1[] = {"hello", "helloha", "hello", "operator", "operators", "operator"};
    DataTypes sourceTypes(std::vector<DataType>({ VarcharDataType(10), VarcharDataType(10) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, 6, data0, data1);

    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<MinVarcharAggregator>(VarcharDataType(10), VarcharDataType(10), 0));
    aggs.push_back(std::make_unique<MaxVarcharAggregator>(VarcharDataType(10), VarcharDataType(10), 1));

    std::vector<int32_t> aggInputCols = { 0, 1 };
    std::vector<int32_t> maskCols = { -1, -1 };
    DataTypes aggPartialOutputTypes(std::vector<DataType> { VarcharDataType(10), VarcharDataType(10) });

    auto aggOperator =
        new AggregationOperator(std::move(aggs), aggInputCols, maskCols, aggPartialOutputTypes, true, true);
    aggOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = aggOperator->GetOutput(result);
    EXPECT_EQ(vecBatchCount, 1);

    auto resultVec0 = static_cast<VarcharVector *>(result[0]->GetVector(0));
    EXPECT_EQ(resultVec0->GetSize(), 1);
    uint8_t *minVal = nullptr;
    int32_t minValLen = resultVec0->GetValue(0, &minVal);
    std::string minStr(minVal, minVal + minValLen);
    EXPECT_EQ(minValLen, data0[4].size());
    EXPECT_EQ(data0[4].compare(minStr), 0);

    auto resultVec1 = static_cast<VarcharVector *>(result[0]->GetVector(1));
    EXPECT_EQ(resultVec1->GetSize(), 1);
    uint8_t *maxVal = nullptr;
    int32_t maxValLen = resultVec1->GetValue(0, &maxVal);
    std::string maxStr(maxVal, maxVal + maxValLen);
    EXPECT_EQ(maxValLen, data1[4].size());
    EXPECT_EQ(data1[4].compare(maxStr), 0);

    omniruntime::op::Operator::DeleteOperator(aggOperator);
    VectorHelper::FreeVecBatches(result);
}

TEST(AggregationOperatorTest, DISABLED_perf_original)
{
    DataTypes sourceTypes(std::vector<DataType>({ LongDataType(), LongDataType(), LongDataType(), LongDataType() }));
    DataTypes aggOutputTypes(
        { LongDataType::Instance(), LongDataType::Instance(), LongDataType::Instance(), LongDataType::Instance() });
    FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_SUM};
    uint32_t aggInputCols[] = {0, 1, 2, 3};
    PrepareContext aggFuncTypesContext = { reinterpret_cast<uint32_t *>(aggFunType), 4 };
    PrepareContext aggInputColsContext = { aggInputCols, 4 };
    uint32_t maskCols[4] = {static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1),
        static_cast<uint32_t>(-1)};
    omniruntime::op::PrepareContext maskColsContext = { maskCols, 4 };

    auto nativeOperatorFactory = new AggregationOperatorFactory(sourceTypes, aggFuncTypesContext, aggInputColsContext,
        maskColsContext, aggOutputTypes, true, false);
    nativeOperatorFactory->Init();
    int64_t factoryAddr = reinterpret_cast<int64_t>(nativeOperatorFactory);
    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;

    VectorBatch **input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 0, COLUMN_NUM,
        std::vector<DataType>(), sourceTypes.Get());
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    int32_t *rowCount = new int32_t[VEC_BATCH_NUM];
    for (int32_t i = 0; i < VEC_BATCH_NUM; i++) {
        rowCount[i] = ROW_PER_VEC_BATCH;
    }
    std::cout << "after prepare" << std::endl;
    uint32_t threadNums[] = {1, 2, 4, 8, 16};
    for (uint32_t i = 0; i < sizeof(threadNums) / sizeof(uint32_t); ++i) {
        g_totalWallTime = 0;
        g_totalCpuTime = 0;
        auto t_ = threadNums[i] < processorCount ? processorCount / threadNums[i] : 1;

        uint32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.SetStart();
        for (uint32_t j = 0; j < threadNum; ++j) {
            // same stage Id
            std::thread t(PerfTestNonGroup, factoryAddr, false, input, VEC_BATCH_NUM, rowCount);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.CalculateElapse();
        double wallElapsed = timer.GetWallElapse();
        double cpuElapsed = timer.GetCpuElapse();
        std::cout << threadNum << " wallElapsed time: " << wallElapsed << "s" << std::endl;
        std::cout << threadNum << " cpuElapsed time: " << cpuElapsed / processorCount * t_ << "s" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    delete[] rowCount;
    DeleteOperatorFactory(nativeOperatorFactory);
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(AggregationOperatorTest, DISABLED_perf_codegen)
{
    using namespace std;

    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;

    std::vector<DataType> groupTypes;
    std::vector<DataType> aggTypes = { LongDataType(), LongDataType(), LongDataType(), LongDataType() };
    VectorBatch **input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 0, 4, groupTypes, aggTypes);
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    int32_t *rowCount = new int32_t[VEC_BATCH_NUM];
    for (int32_t i = 0; i < VEC_BATCH_NUM; i++) {
        rowCount[i] = ROW_PER_VEC_BATCH;
    }
    uint64_t factoryObjAddr = CreateAggFactoryWithJit();
    std::cout << "after prepare" << std::endl;
    uint32_t threadNums[] = {1, 2, 4, 8, 16};
    for (uint32_t i = 0; i < sizeof(threadNums) / sizeof(uint32_t); ++i) {
        g_totalWallTime = 0;
        g_totalCpuTime = 0;
        auto t_ = threadNums[i] < processorCount ? processorCount / threadNums[i] : 1;

        uint32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.SetStart();
        for (uint32_t j = 0; j < threadNum; ++j) {
            // same stage Id
            std::thread t(PerfTestNonGroup, factoryObjAddr, true, input, VEC_BATCH_NUM, rowCount);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.CalculateElapse();
        double wallElapsed = timer.GetWallElapse();
        double cpuElapsed = timer.GetCpuElapse();
        std::cout << threadNum << " wallElapsed time: " << wallElapsed << "s" << std::endl;
        std::cout << threadNum << " cpuElapsed time: " << cpuElapsed / processorCount * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    delete[] rowCount;
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(HashAggregationOperatorTest, compare_perf)
{
    uint32_t groupCols[] = {0, 1};

    DataTypes groupInputTypes({ LongDataType::Instance(), LongDataType::Instance() });
    uint32_t aggCols[] = {2, 3};
    DataTypes aggInput({ LongDataType::Instance(), LongDataType::Instance() });
    DataTypes aggOutput({ LongDataType::Instance(), LongDataType::Instance() });
    FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    PrepareContext groupByColContext = { groupCols, 2 };
    PrepareContext aggColContext = { aggCols, 2 };
    PrepareContext aggFuncTypeContext = { reinterpret_cast<uint32_t *>(aggFunType), 2 };

    // ------------------------------------------Create operator--------------------------------------------
    auto jitContext = CreateHashAggregationJitContext(groupInputTypes, 2);
    std::cout << "after JIT" << std::endl;
    omniruntime::op::HashAggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupInputTypes, aggColContext, aggInput,
        aggOutput, aggFuncTypeContext, true, false);
    nativeOperatorFactory->Init();
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->SetJitContext(jitContext);
    // create operator
    auto jitGroupBy = CreateTestOperator(nativeOperatorFactory);

    // ------------------------------------------Process Input--------------------------------------------
    VectorBatch **input1 =
        buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupInputTypes.Get(), aggInput.Get());
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    std::vector<VectorBatch *> jittedResult;
    Timer timer;
    timer.SetStart();

    auto *perfUtil = new PerfUtil();
    perfUtil->Init();

    perfUtil->Reset();
    perfUtil->Start();

    for (int pageIndex = 0; pageIndex < VEC_BATCH_NUM; ++pageIndex) {
        auto errNo = jitGroupBy->AddInput(input1[pageIndex]);
        EXPECT_EQ(errNo, OMNI_STATUS_NORMAL);
    }

    perfUtil->Stop();
    long instCount = perfUtil->GetData();
    if (instCount != -1) {
        printf("HashAgg with OmniJit, used %lld instructions\n", perfUtil->GetData());
    }

    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse();
    double cpuElapsed = timer.GetCpuElapse();
    std::cout << "wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;
    jitGroupBy->GetOutput(jittedResult);

    HashAggregationOperatorFactory *nativeOperatorFactory2 = new HashAggregationOperatorFactory(groupByColContext,
        groupInputTypes, aggColContext, aggInput, aggOutput, aggFuncTypeContext, true, false);
    nativeOperatorFactory2->Init();
    auto groupBy = nativeOperatorFactory2->CreateOperator();

    std::vector<VectorBatch *> result;
    VectorBatch **input2 =
        buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupInputTypes.Get(), aggInput.Get());
    if (input2 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    timer.Reset();

    perfUtil->Reset();
    perfUtil->Start();
    for (int pageIndex = 0; pageIndex < VEC_BATCH_NUM; ++pageIndex) {
        groupBy->AddInput(input2[pageIndex]);
    }

    perfUtil->Stop();
    instCount = perfUtil->GetData();
    if (instCount != -1) {
        printf("HashAgg without OmniJit, used %lld instructions\n", perfUtil->GetData());
    }

    delete perfUtil;

    timer.CalculateElapse();
    wallElapsed = timer.GetWallElapse();
    cpuElapsed = timer.GetCpuElapse();

    std::cout << "wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;
    groupBy->GetOutput(result);

    Operator::DeleteOperator(jitGroupBy);
    Operator::DeleteOperator(groupBy);
    DeleteOperatorFactory(nativeOperatorFactory);
    DeleteOperatorFactory(nativeOperatorFactory2);

    EXPECT_EQ(jittedResult.size(), result.size());
    for (uint32_t i = 0; i < jittedResult.size(); ++i) {
        EXPECT_TRUE(VecBatchMatch(jittedResult[i], result[i]));
    }
    VectorHelper::FreeVecBatches(jittedResult);
    VectorHelper::FreeVecBatches(result);
}

TEST(HashAggregationOperatorTest, multi_stage)
{
    std::vector<DataType> groupTypes = { LongDataType(), LongDataType() };
    std::vector<DataType> aggTypes = { LongDataType(), LongDataType(), Decimal64DataType(7, 2),
        Decimal64DataType(7, 2) };
    VectorBatch **input1 = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 4, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    HAFactoryParameters parameters1 = { true,
        true,
        { 0, 1 },
        { LongDataType::Instance(), LongDataType::Instance() },
        { 2, 3, 4, 5 },
        { LongDataType::Instance(), LongDataType::Instance(), SHORT_DECIMAL_TYPE, SHORT_DECIMAL_TYPE },
        { LongDataType::Instance(), ContainerDataType::Instance(), SUM_IMMEDIATE_VARBINARY, AVG_IMMEDIATE_VARBINARY },
        { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG } };

    uintptr_t partialFactoryAddr1 = CreateHashFactoryWithoutJit(parameters1);
    auto partialFactory1 = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(partialFactoryAddr1);
    auto partialOperator1 = partialFactory1->CreateOperator();
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        partialOperator1->AddInput(input1[i]);
    }
    std::vector<VectorBatch *> resultFromPartial1;
    partialOperator1->GetOutput(resultFromPartial1);

    VectorBatch **input2 = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 4, groupTypes, aggTypes);
    if (input2 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    HAFactoryParameters parameters2 = { true,
        true,
        { 0, 1 },
        { LongDataType::Instance(), LongDataType::Instance() },
        { 2, 3, 4, 5 },
        { LongDataType::Instance(), LongDataType::Instance(), SHORT_DECIMAL_TYPE, SHORT_DECIMAL_TYPE },
        { LongDataType::Instance(), ContainerDataType::Instance(), SUM_IMMEDIATE_VARBINARY, AVG_IMMEDIATE_VARBINARY },
        { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG } };

    uintptr_t partialFactoryAddr2 = CreateHashFactoryWithoutJit(parameters2);
    auto partialFactory2 = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(partialFactoryAddr2);
    auto partialOperator2 = partialFactory2->CreateOperator();
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        partialOperator2->AddInput(input2[i]);
    }
    std::vector<VectorBatch *> resultFromPartial2;
    partialOperator2->GetOutput(resultFromPartial2);

    HAFactoryParameters parameters3 = { false,
        false,
        { 0, 1 },
        { LongDataType::Instance(), LongDataType::Instance() },
        { 2, 3, 4, 5 },
        { LongDataType::Instance(), ContainerDataType::Instance(), SUM_IMMEDIATE_VARBINARY, AVG_IMMEDIATE_VARBINARY },
        { LongDataType::Instance(), DoubleDataType::Instance(), LONG_DECIMAL_TYPE, LongDataType::Instance() },
        { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG } };

    uintptr_t finalFactoryAddr = CreateHashFactoryWithoutJit(parameters3);
    auto finalFactory = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(finalFactoryAddr);
    auto operator2 = finalFactory->CreateOperator();
    for (uint32_t i = 0; i < resultFromPartial1.size(); ++i) {
        operator2->AddInput(resultFromPartial1[i]);
    }
    for (uint32_t i = 0; i < resultFromPartial2.size(); ++i) {
        operator2->AddInput(resultFromPartial2[i]);
    }
    std::vector<VectorBatch *> resultFromFinal;
    operator2->GetOutput(resultFromFinal);

    Operator::DeleteOperator(partialOperator1);
    Operator::DeleteOperator(partialOperator2);
    Operator::DeleteOperator(operator2);
    DeleteOperatorFactory(partialFactory1);
    DeleteOperatorFactory(partialFactory2);
    DeleteOperatorFactory(finalFactory);

    // construct the output data
    DataTypes expectTypes(std::vector<DataType>(
        { LongDataType(), LongDataType(), LongDataType(), DoubleDataType(), LONG_DECIMAL_TYPE, LongDataType() }));
    int64_t expectData1[CARDINALITY] = {0, 1, 2, 3};
    int64_t expectData2[CARDINALITY] = {0, 1, 2, 3};
    int64_t expectData3[CARDINALITY] = {10000000, 10000000, 10000000, 10000000};
    double expectData4[CARDINALITY] = {1.0, 1.0, 1.0, 1.0};
    Decimal128 expectedDecimal(10000000L);
    Decimal128 expectData5[CARDINALITY] = {expectedDecimal, expectedDecimal, expectedDecimal, expectedDecimal};
    int64_t expectData6[CARDINALITY] = {1, 1, 1, 1};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, CARDINALITY, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);
    EXPECT_TRUE(VecBatchMatch(resultFromFinal[0], expectVecBatch));
    VectorHelper::FreeVecBatches(resultFromFinal);
    VectorHelper::FreeVecBatch(expectVecBatch);
}

TEST(HashAggregationOperatorTest, supported_type_test)
{
    std::vector<DataType> groupTypes = { LongDataType(), LongDataType() };
    std::vector<DataType> aggTypes = { LongDataType(), LongDataType() };
    VectorBatch **input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    ColumnIndex c0 = { 0, LongDataType::Instance(), LongDataType::Instance() };
    ColumnIndex c1 = { 1, LongDataType::Instance(), LongDataType::Instance() };
    std::vector<int32_t> aggInputCols = { 2, 3 };
    DataTypes aggInputTypes(std::vector<DataType> { LongDataType::Instance(), LongDataType::Instance() });
    DataTypes aggOutputTypes(std::vector<DataType> { LongDataType::Instance(), LongDataType::Instance() });
    std::vector<ColumnIndex> groupByColumns = { c0, c1 };
    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 3, INPUT_MODE, OUTPUT_MODE));

    HashAggregationOperator *groupBy = new HashAggregationOperator(groupByColumns, aggInputCols, aggInputTypes,
        aggOutputTypes, std::move(aggs), true, false);
    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = groupBy->GetOutput(result);
    ASSERT_EQ(vecBatchCount, 1);
    Operator::DeleteOperator(groupBy);
    groupByColumns.clear();
    aggInputCols.clear();
    aggs.clear();
    VectorHelper::FreeVecBatches(result);
    result.clear();
    delete[] input;

    groupTypes[0] = Date32DataType(DAY);
    groupTypes[1] = Date32DataType(DAY);
    aggTypes[0] = Date32DataType(DAY);
    aggTypes[1] = Date32DataType(DAY);
    input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);
    c0 = { 0, Date32DataType::Instance(), Date32DataType::Instance() };
    c1 = { 1, Date32DataType::Instance(), Date32DataType::Instance() };
    aggInputCols = { 2, 3 };
    DataTypes aggInputTypes1(std::vector<DataType> { Date32DataType::Instance(), Date32DataType::Instance() });
    DataTypes aggOutputTypes1(std::vector<DataType> { Date32DataType::Instance(), Date32DataType::Instance() });

    groupByColumns = { c0, c1 };
    aggs.push_back(std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(Date32DataType::Instance(),
        Date32DataType::Instance(), 2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(Date32DataType::Instance(),
        Date32DataType::Instance(), 3, INPUT_MODE, OUTPUT_MODE));

    groupBy = new HashAggregationOperator(groupByColumns, aggInputCols, aggInputTypes1, aggOutputTypes1,
        std::move(aggs), true, false);
    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }

    vecBatchCount = groupBy->GetOutput(result);
    Operator::DeleteOperator(groupBy);
    groupByColumns.clear();
    aggInputCols.clear();
    aggs.clear();
    VectorHelper::FreeVecBatches(result);
    result.clear();
    delete[] input;

    // dictionary test
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    void *datas[2] = {data0, data1};
    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType() }));
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vectorBatch = new VectorBatch(2, dataSize);
    for (int32_t i = 0; i < 2; i++) {
        DataType dataType = sourceTypes.Get()[i];
        vectorBatch->SetVector(i, CreateDictionaryVector(dataType, dataSize, ids, dataSize, datas[i]));
    }

    groupTypes[0] = IntDataType();
    aggTypes[0] = LongDataType();
    c0 = { 0, IntDataType::Instance(), IntDataType::Instance() };
    aggInputCols = { 1 };
    omniruntime::type::DataTypes aggInputTypes2(std::vector<DataType> { LongDataType::Instance() });
    omniruntime::type::DataTypes aggOutputTypes2(std::vector<DataType> { LongDataType::Instance() });
    groupByColumns = { c0 };
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongDataType::Instance(),
        LongDataType::Instance(), 1, INPUT_MODE, OUTPUT_MODE));

    groupBy = new HashAggregationOperator(groupByColumns, aggInputCols, aggInputTypes2, aggOutputTypes2,
        std::move(aggs), true, false);

    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        // create a copy of input
        VectorBatch *copied = DuplicateVectorBatch(vectorBatch);
        groupBy->AddInput(copied);
    }

    vecBatchCount = groupBy->GetOutput(result);
    Operator::DeleteOperator(groupBy);
    VectorHelper::FreeVecBatch(vectorBatch);
    VectorHelper::FreeVecBatches(result);
    groupByColumns.clear();
    aggInputCols.clear();
    aggs.clear();
    result.clear();
}

TEST(AggregatorTest, sum_test)
{
    int32_t rowPerVecBatch = 200;
    auto sumFactory = new SumAggregatorFactory();
    // sum test types : long + decimal + dictionary + null
    auto sumLong = sumFactory->CreateAggregator(LongDataType::Instance(), LongDataType::Instance(), 0, true, false);
    auto sumShortDecimal = sumFactory->CreateAggregator(SHORT_DECIMAL_TYPE, LONG_DECIMAL_TYPE, 0, true, false);
    auto sumNull = sumFactory->CreateAggregator(LongDataType::Instance(), LongDataType::Instance(), 3, true, false);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_sum_test");
    auto longInputVec = BuildAggregateInput(vectorAllocator, LongDataType(), rowPerVecBatch);
    auto decimalInputVec = BuildAggregateInput(vectorAllocator, Decimal128DataType(38, 2), rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(vectorAllocator, DataType(OMNI_NONE), rowPerVecBatch);

    VectorBatch *vecBatch = new VectorBatch(4);
    vecBatch->SetVector(0, longInputVec);
    vecBatch->SetVector(1, decimalInputVec);
    vecBatch->SetVector(2, dictInputVec);
    vecBatch->SetVector(3, nullInputVec);
    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            sumLong->InitiateGroup(state, vecBatch, i);
        } else {
            sumLong->ProcessGroup(state, vecBatch, i);
        }
    }
    EXPECT_EQ(200, *static_cast<int64_t *>(state.val));
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            sumNull->InitiateGroup(state, vecBatch, i);
        } else {
            sumNull->ProcessGroup(state, vecBatch, i);
        }
    }
    EXPECT_EQ(nullptr, state.val);
    state.val = nullptr;

    // process short decimal
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            sumShortDecimal->InitiateGroup(state, vecBatch, i);
        } else {
            sumShortDecimal->ProcessGroup(state, vecBatch, i);
        }
    }
    Decimal128 actual;
    int64_t overflow;
    DecimalOperations::DecodeSumDecimal(state.val, actual, overflow);
    Decimal128 expected(200L);
    EXPECT_EQ(0, overflow);
    EXPECT_EQ(expected, actual);
    state.val = nullptr;

    VectorHelper::FreeVecBatch(vecBatch);
    delete sumFactory;
}

TEST(AggregatorTest, count_column_test)
{
    int32_t rowPerVecBatch = 200;
    auto countFactory = new CountColumnAggregatorFactory();
    // count test types : long + dictionary + null
    auto countLong = countFactory->CreateAggregator(LongDataType::Instance(), LongDataType::Instance(), 0, true, false);
    auto countNull = countFactory->CreateAggregator(LongDataType::Instance(), LongDataType::Instance(), 2, true, false);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_count_column_test");
    auto longInputVec = BuildAggregateInput(vectorAllocator, LongDataType(), rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(vectorAllocator, DataType(OMNI_NONE), rowPerVecBatch);

    VectorBatch *vecBatch = new VectorBatch(3);
    vecBatch->SetVector(0, longInputVec);
    vecBatch->SetVector(1, dictInputVec);
    vecBatch->SetVector(2, nullInputVec);

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            countLong->InitiateGroup(state, vecBatch, i);
        } else {
            countLong->ProcessGroup(state, vecBatch, i);
        }
    }
    EXPECT_EQ(200, state.count);
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            countNull->InitiateGroup(state, vecBatch, i);
        } else {
            countNull->ProcessGroup(state, vecBatch, i);
        }
    }
    EXPECT_EQ(0, state.count);
    state.val = nullptr;

    VectorHelper::FreeVecBatch(vecBatch);
    delete countFactory;
}

TEST(AggregatorTest, count_all_test)
{
    int32_t rowPerVecBatch = 200;
    auto countAllFactory = new CountAllAggregatorFactory();
    auto countLong = countAllFactory->CreateAggregator(NoneDataType::Instance(), LongDataType::Instance(),
        Aggregator::INVALID_INPUT_COL, true, false);
    auto countNull = countAllFactory->CreateAggregator(NoneDataType::Instance(), LongDataType::Instance(),
        Aggregator::INVALID_INPUT_COL, true, false);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_count_all_test");
    auto longInputVec = BuildAggregateInput(vectorAllocator, LongDataType(), rowPerVecBatch);
    LongDataType longDataType;
    auto nullInputVec = BuildAggregateInput(vectorAllocator, DataType(OMNI_NONE), ROW_PER_VEC_BATCH);

    VectorBatch *vecBatch = new VectorBatch(2, rowPerVecBatch);
    vecBatch->SetVector(0, longInputVec);
    vecBatch->SetVector(1, nullInputVec);

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            countLong->InitiateGroup(state, vecBatch, i);
        } else {
            countLong->ProcessGroup(state, vecBatch, i);
        }
    }
    EXPECT_EQ(200, state.count);
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            countNull->InitiateGroup(state, vecBatch, i);
        } else {
            countNull->ProcessGroup(state, vecBatch, i);
        }
    }
    EXPECT_EQ(200, state.count);
    state.val = nullptr;

    VectorHelper::FreeVecBatch(vecBatch);
    delete countAllFactory;
}

TEST(AggregatorTest, min_test)
{
    int32_t rowPerVecBatch = 200;
    auto minFactory = new MinAggregatorFactory();
    // min test types : long + decimal + varchar + dictionary + null
    auto minLong = minFactory->CreateAggregator(LongDataType::Instance(), LongDataType::Instance(), 0, true, false);
    auto minDecimal = minFactory->CreateAggregator(LONG_DECIMAL_TYPE, LONG_DECIMAL_TYPE, 3, true, false);
    auto minVarchar =
        minFactory->CreateAggregator(VarcharDataType::Instance(), VarcharDataType::Instance(), 4, true, false);
    auto minNull = minFactory->CreateAggregator(LongDataType::Instance(), LongDataType::Instance(), 2, true, false);
    auto minIntLong = minFactory->CreateAggregator(IntDataType::Instance(), LongDataType::Instance(), 5, true, false);
    auto minLongInt = minFactory->CreateAggregator(LongDataType::Instance(), IntDataType::Instance(), 0, true, false);
    auto minBoolean =
        minFactory->CreateAggregator(BooleanDataType::Instance(), BooleanDataType::Instance(), 6, true, false);

    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation_minTest");
    auto longInputVec = BuildAggregateInput(vectorAllocator, LongDataType(), rowPerVecBatch);
    auto decimalInputVec = BuildAggregateInput(vectorAllocator, Decimal128DataType(38, 0), rowPerVecBatch);
    VarcharDataType varcharDataType(1);
    std::string stringVals[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        stringVals[i] = "1";
    }
    Vector *varcharInputVec = CreateVarcharVector(varcharDataType, stringVals, rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(vectorAllocator, DataType(OMNI_NONE), rowPerVecBatch);
    auto intInputVec = BuildAggregateInput(vectorAllocator, IntDataType(), rowPerVecBatch);
    auto BoolInputVec = BuildAggregateInput(vectorAllocator, BooleanDataType(), rowPerVecBatch);

    VectorBatch *vectorBatch = new VectorBatch(7);
    vectorBatch->SetVector(0, longInputVec);
    vectorBatch->SetVector(1, dictInputVec);
    vectorBatch->SetVector(2, nullInputVec);
    vectorBatch->SetVector(3, decimalInputVec);
    vectorBatch->SetVector(4, varcharInputVec);
    vectorBatch->SetVector(5, intInputVec);
    vectorBatch->SetVector(6, BoolInputVec);

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            minLong->InitiateGroup(state, vectorBatch, i);
        } else {
            minLong->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(1, *static_cast<int64_t *>(state.val));
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            minNull->InitiateGroup(state, vectorBatch, i);
        } else {
            minNull->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(nullptr, state.val);
    state.val = nullptr;

    // process varchar
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            minVarchar->InitiateGroup(state, vectorBatch, i);
        } else {
            minVarchar->ProcessGroup(state, vectorBatch, i);
        }
    }
    std::string expectedStr = "1";
    EXPECT_EQ(0, std::memcmp(state.val, expectedStr.c_str(), 1));
    VarcharVector minVarcharOutput(vectorAllocator, 1, 1);
    minVarchar->ExtractValue(state, &minVarcharOutput, 0);
    uint8_t *strRes = nullptr;
    minVarcharOutput.GetValue(0, &strRes);
    EXPECT_EQ(0, std::memcmp(strRes, expectedStr.c_str(), 1));
    state.val = nullptr;

    // process int to long
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            minIntLong->InitiateGroup(state, vectorBatch, i);
        } else {
            minIntLong->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(1, *static_cast<int64_t *>(state.val));
    state.val = nullptr;

    // process long to int
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            minLongInt->InitiateGroup(state, vectorBatch, i);
        } else {
            minLongInt->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(1, *static_cast<int32_t *>(state.val));
    state.val = nullptr;

    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            minBoolean->InitiateGroup(state, vectorBatch, i);
        } else {
            minBoolean->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(true, *static_cast<bool *>(state.val));
    state.val = nullptr;


    VectorHelper::FreeVecBatch(vectorBatch);
    delete minFactory;
}

TEST(AggregatorTest, max_test)
{
    int32_t rowPerVecBatch = 200;
    auto maxFactory = new MaxAggregatorFactory();
    // max test types : long + decimal + varchar + dictionary + null
    auto maxLong = maxFactory->CreateAggregator(LongDataType::Instance(), LongDataType::Instance(), 0, true, false);
    auto maxDecimal = maxFactory->CreateAggregator(LONG_DECIMAL_TYPE, LONG_DECIMAL_TYPE, 1, true, false);
    auto maxVarchar =
        maxFactory->CreateAggregator(VarcharDataType::Instance(), VarcharDataType::Instance(), 2, true, false);
    auto maxNull = maxFactory->CreateAggregator(LongDataType::Instance(), LongDataType::Instance(), 4, true, false);
    auto maxIntLong = maxFactory->CreateAggregator(IntDataType::Instance(), LongDataType::Instance(), 5, true, false);
    auto maxLongInt = maxFactory->CreateAggregator(LongDataType::Instance(), IntDataType::Instance(), 0, true, false);
    auto maxBoolean =
        maxFactory->CreateAggregator(BooleanDataType::Instance(), BooleanDataType::Instance(), 6, true, false);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_count_max_test1");
    auto longInputVec = BuildAggregateInput(vectorAllocator, LongDataType(), rowPerVecBatch);
    auto decimalInputVec = BuildAggregateInput(vectorAllocator, Decimal128DataType(38, 0), rowPerVecBatch);
    VarcharDataType varcharDataType(1);
    std::string stringVals[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        stringVals[i] = "1";
    }
    Vector *varcharInputVec = CreateVarcharVector(varcharDataType, stringVals, rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(vectorAllocator, DataType(OMNI_NONE), rowPerVecBatch);
    auto intInputVec = BuildAggregateInput(vectorAllocator, IntDataType(), rowPerVecBatch);
    auto boolInputVec = BuildAggregateInput(vectorAllocator, BooleanDataType(), rowPerVecBatch);

    VectorBatch *vectorBatch = new VectorBatch(7);
    vectorBatch->SetVector(0, longInputVec);
    vectorBatch->SetVector(1, decimalInputVec);
    vectorBatch->SetVector(2, varcharInputVec);
    vectorBatch->SetVector(3, dictInputVec);
    vectorBatch->SetVector(4, nullInputVec);
    vectorBatch->SetVector(5, intInputVec);
    vectorBatch->SetVector(6, boolInputVec);

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            maxLong->InitiateGroup(state, vectorBatch, i);
        } else {
            maxLong->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(1, *static_cast<int64_t *>(state.val));
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            maxNull->InitiateGroup(state, vectorBatch, i);
        } else {
            maxNull->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(nullptr, state.val);
    state.val = nullptr;

    // process varchar
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            maxVarchar->InitiateGroup(state, vectorBatch, i);
        } else {
            maxVarchar->ProcessGroup(state, vectorBatch, i);
        }
    }
    std::string expectedStr = "1";
    EXPECT_EQ(0, std::memcmp(state.val, expectedStr.c_str(), 1));
    VarcharVector maxVarcharOutput(VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation_maxTest2"), 1,
        1);
    maxVarchar->ExtractValue(state, &maxVarcharOutput, 0);
    uint8_t *strRes = nullptr;
    maxVarcharOutput.GetValue(0, &strRes);
    EXPECT_EQ(0, std::memcmp(strRes, expectedStr.c_str(), 1));
    state.val = nullptr;

    // process decimal
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            maxDecimal->InitiateGroup(state, vectorBatch, i);
        } else {
            maxDecimal->ProcessGroup(state, vectorBatch, i);
        }
    }
    Decimal128 expected(1);
    EXPECT_EQ(expected, *static_cast<Decimal128 *>(state.val));
    state.val = nullptr;

    // process int to long
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            maxIntLong->InitiateGroup(state, vectorBatch, i);
        } else {
            maxIntLong->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(1, *static_cast<int64_t *>(state.val));
    state.val = nullptr;

    // process long to int
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            maxLongInt->InitiateGroup(state, vectorBatch, i);
        } else {
            maxLongInt->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(1, *static_cast<int32_t *>(state.val));
    state.val = nullptr;

    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            maxBoolean->InitiateGroup(state, vectorBatch, i);
        } else {
            maxBoolean->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(true, *static_cast<bool *>(state.val));
    state.val = nullptr;

    VectorHelper::FreeVecBatch(vectorBatch);
    delete maxFactory;
}

TEST(AggregatorTest, avg_test)
{
    int32_t rowPerVecBatch = 200;

    auto avgFactory = new AverageAggregatorFactory();
    // avg test types : long + decimal + dictionary + null
    auto avgLong = avgFactory->CreateAggregator(LongDataType::Instance(), DoubleDataType::Instance(), 0, true, false);
    auto avgDecimal = avgFactory->CreateAggregator(LONG_DECIMAL_TYPE, LONG_DECIMAL_TYPE, 1, true, false);
    auto avgNull = avgFactory->CreateAggregator(LongDataType::Instance(), DoubleDataType::Instance(), 3, true, false);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_avg_test");
    auto longInputVec = BuildAggregateInput(vectorAllocator, LongDataType(), rowPerVecBatch);
    auto decimalInputVec = BuildAggregateInput(vectorAllocator, Decimal128DataType(38, 0), rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(vectorAllocator, DataType(OMNI_NONE), rowPerVecBatch);

    VectorBatch *vectorBatch = new VectorBatch(4);
    vectorBatch->SetVector(0, longInputVec);
    vectorBatch->SetVector(1, decimalInputVec);
    vectorBatch->SetVector(2, dictInputVec);
    vectorBatch->SetVector(3, nullInputVec);

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            avgLong->InitiateGroup(state, vectorBatch, i);
        } else {
            avgLong->ProcessGroup(state, vectorBatch, i);
        }
    }
    DoubleVector avgLongOutput(VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation_avgTest"), 1);
    avgLong->ExtractValue(state, &avgLongOutput, 0);
    EXPECT_TRUE(avgLongOutput.GetValue(0) - 1 <= DBL_EPSILON);
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        if (i == 0) {
            avgNull->InitiateGroup(state, vectorBatch, i);
        } else {
            avgNull->ProcessGroup(state, vectorBatch, i);
        }
    }
    DoubleVector avgNullOutput(VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation_avgTest"), 1);
    avgNull->ExtractValue(state, &avgNullOutput, 0);
    EXPECT_TRUE(avgNullOutput.IsValueNull(0));
    state.val = nullptr;

    VectorHelper::FreeVecBatch(vectorBatch);
    delete avgFactory;
}
}