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
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "vector/vector_helper.h"
#include "util/perf_util.h"
#include "../util/test_util.h"
#include "../../libconfig.h"

namespace omniruntime {
using namespace omniruntime::vec;
const int32_t VEC_BATCH_NUM = 10;
const int32_t ROW_PER_VEC_BATCH = 200000;
const int32_t CARDINALITY = 4;
const int32_t COLUMN_NUM = 4;
const bool INPUT_MODE = true;
const bool OUTPUT_MODE = false;
const int CONST_VALUE_2 = 2;
const int CONST_VALUE_7 = 7;
const int CONST_VALUE_24 = 24;
const int CONST_VALUE_32 = 32;
const int CONST_VALUE_38 = 38;

static DataTypePtr SHORT_DECIMAL_TYPE = Decimal64Type(CONST_VALUE_7, CONST_VALUE_2);
static DataTypePtr SUM_IMMEDIATE_VARBINARY = VarcharType(CONST_VALUE_24);
static DataTypePtr AVG_IMMEDIATE_VARBINARY = VarcharType(CONST_VALUE_32);
static DataTypePtr LONG_DECIMAL_TYPE = Decimal128Type(CONST_VALUE_38, 0);

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

Vector *BuildHashInput(const DataTypePtr groupType, int32_t rowPerVecBatch, int32_t cardinality)
{
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation_buildHashInput");
    switch (groupType->GetId()) {
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
        case OMNI_SHORT: {
            ShortVector *col = new ShortVector(vecAllocator, rowPerVecBatch);
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
            VarcharVector *col = new VarcharVector(vecAllocator,
                static_cast<const VarcharDataType *>(groupType.get())->GetWidth() * rowPerVecBatch, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                std::string str = std::to_string(j % cardinality);
                col->SetValue(j, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
            }
            return col;
        }
        default: {
            LogError("No such %d type support", groupType->GetId());
            return nullptr;
        }
    }
}

Vector *BuildAggregateInput(VectorAllocator *vecAllocator, const DataTypePtr aggType, int32_t rowPerVecBatch)
{
    switch (aggType->GetId()) {
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
        case OMNI_SHORT: {
            ShortVector *col = new ShortVector(vecAllocator, rowPerVecBatch);
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
            VarcharVector *col = new VarcharVector(vecAllocator,
                static_cast<VarcharDataType *>(aggType.get())->GetWidth() * rowPerVecBatch, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                std::string str = std::to_string(j);
                col->SetValue(j, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
            }
            return col;
        }
        default: {
            LogError("No such %d type support", aggType->GetId());
            return nullptr;
        }
    }
}

VectorBatch **buildAggInput(int32_t vecBatchNum, int32_t rowPerVecBatch, int32_t cardinality, int32_t groupColNum,
    int32_t aggColNum, const std::vector<DataTypePtr> &groupTypes, const std::vector<DataTypePtr> &aggTypes)
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
    std::vector<DataTypePtr> groupByTypeVec = { LongType(), LongType() };
    DataTypes groupByTypes(groupByTypeVec);
    uint32_t aggCols[2] = {2, 3};
    std::vector<DataTypePtr> aggInputTypeVec = { LongType(), LongType() };
    DataTypes aggInputTypes(aggInputTypeVec);
    std::vector<DataTypePtr> aggOutputTypeVec = { LongType(), LongType() };
    DataTypes aggOutputTypes(aggOutputTypeVec);
    uint32_t aggFunType[2] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG};
    uint32_t maskCols[2] = {static_cast<uint32_t>(-1), static_cast<uint32_t>(-1)};
    std::vector<uint32_t> groupByColVector = std::vector<uint32_t>(groupCols, groupCols + 2);
    std::vector<uint32_t> aggColVector = std::vector<uint32_t>(aggCols, aggCols + 2);
    std::vector<uint32_t> aggFuncTypeVector = std::vector<uint32_t>(aggFunType, aggFunType + 2);
    std::vector<uint32_t> maskColsVector = std::vector<uint32_t>(maskCols, maskCols + 2);

    auto aggColVectorWrap = AggregatorUtil::WrapWithVector(aggColVector);
    auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(aggInputTypes);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWraps = AggregatorUtil::WrapWithVector(inputRaw, 2);
    auto outputPartialWraps = AggregatorUtil::WrapWithVector(outputPartial, 2);
    std::cout << "after jit" << std::endl;
    auto *nativeOperatorFactory =
        new omniruntime::op::HashAggregationOperatorFactory(groupByColVector, groupByTypes, aggColVectorWrap,
        aggInputTypesWrap, aggOutputTypesWrap, aggFuncTypeVector, maskColsVector, inputRawWraps, outputPartialWraps);
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->Init();
    return reinterpret_cast<uintptr_t>(nativeOperatorFactory);
}

uintptr_t CreateAggFactoryWithJit()
{
    const int CONST_VALUE_4 = 4;
    std::vector<DataTypePtr> dataTypeFields { LongType(), LongType(), LongType(), LongType() };
    DataTypes sourceTypes(dataTypeFields);
    uint32_t aggFuncTypes[CONST_VALUE_4] = {0, 0, 0, 0};
    std::vector<uint32_t> aggFuncTypeVector = std::vector<uint32_t>(aggFuncTypes, aggFuncTypes + CONST_VALUE_4);
    uint32_t aggInputCols[CONST_VALUE_4] = {0, 1, 2, 3};
    std::vector<uint32_t> aggInputColsVector = std::vector<uint32_t>(aggInputCols, aggInputCols + CONST_VALUE_4);
    uint32_t maskCols[CONST_VALUE_4] = {static_cast<uint32_t>(-1), static_cast<uint32_t>(-1),
                                        static_cast<uint32_t>(-1),
                                        static_cast<uint32_t>(-1)};
    std::vector<uint32_t> maskColsVector = std::vector<uint32_t>(maskCols, maskCols + CONST_VALUE_4);

    std::cout << "after jit" << std::endl;
    auto aggInputColsVectorWrap = AggregatorUtil::WrapWithVector(aggInputColsVector);
    auto sourceTypesWrap = AggregatorUtil::WrapWithVector(sourceTypes);
    auto trueWraps = AggregatorUtil::WrapWithVector(true, CONST_VALUE_4);
    auto falseWraps = AggregatorUtil::WrapWithVector(false, CONST_VALUE_4);
    auto nativeOperatorFactory = new AggregationOperatorFactory(sourceTypes, aggFuncTypeVector, aggInputColsVectorWrap,
        maskColsVector, sourceTypesWrap, trueWraps, falseWraps);
    nativeOperatorFactory->Init();
    std::cout << "after create factory" << std::endl;
    return reinterpret_cast<uintptr_t>(nativeOperatorFactory);
}

using HAFactoryParameters = struct HashAggregationFactoryParameters {
public:
    bool inputRaw;
    bool outputPartial;
    std::vector<uint32_t> groupCols;
    std::vector<DataTypePtr> groupByTypes;
    std::vector<uint32_t> aggCols;
    std::vector<DataTypePtr> aggInputTypes;
    std::vector<DataTypePtr> aggOutputTypes;
    std::vector<uint32_t> aggFuncTypes;
    std::vector<uint32_t> maskCols;
};

uintptr_t CreateHashFactoryWithoutJit(HAFactoryParameters &parameters)
{
    DataTypes groupByTypes(parameters.groupByTypes);
    DataTypes aggInputTypes(parameters.aggInputTypes);
    DataTypes aggOutputTypes(parameters.aggOutputTypes);

    std::vector<uint32_t> groupByColVector =
        std::vector<uint32_t>(parameters.groupCols.data(), parameters.groupCols.data() + parameters.groupCols.size());
    std::vector<uint32_t> aggColVector =
        std::vector<uint32_t>(parameters.aggCols.data(), parameters.aggCols.data() + parameters.aggCols.size());
    std::vector<uint32_t> aggFuncTypeVector = std::vector<uint32_t>(parameters.aggFuncTypes.data(),
        parameters.aggFuncTypes.data() + parameters.aggFuncTypes.size());
    std::vector<uint32_t> maskColsVector =
        std::vector<uint32_t>(parameters.maskCols.data(), parameters.maskCols.data() + parameters.maskCols.size());

    auto aggColVectorWrap = AggregatorUtil::WrapWithVector(aggColVector);
    auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(aggInputTypes);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWraps = AggregatorUtil::WrapWithVector(parameters.inputRaw, aggColVector.size());
    auto outputPartialWraps = AggregatorUtil::WrapWithVector(parameters.outputPartial, aggColVector.size());
    omniruntime::op::HashAggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::HashAggregationOperatorFactory(groupByColVector, groupByTypes, aggColVectorWrap,
        aggInputTypesWrap, aggOutputTypesWrap, aggFuncTypeVector, maskColsVector, inputRawWraps, outputPartialWraps);
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
    auto groupBy = nativeOperatorFactory->CreateOperator();

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
    Operator *aggregation = nullptr;
    if (codegenMode) {
        aggregation = CreateTestOperator(nativeOperatorFactory);
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
    std::vector<DataTypePtr> groupTypes = { LongType(), LongType() };
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType(), LongType(), LongType(), LongType() };
    VectorBatch **input1 = buildAggInput(vecBatchNum, rowSize, cardinality, 2, 5, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    // First stage
    ColumnIndex c0 = { 0, LongType(), LongType() };
    ColumnIndex c1 = { 1, LongType(), LongType() };
    std::vector<int32_t> aggInputCols1 = { 2, 3, 4, 5, 6 };
    std::vector<DataTypePtr> inputTypes1 { LongType(), LongType(), LongType(), LongType(), LongType() };
    DataTypes aggInputTypes1(inputTypes1);
    std::vector<DataTypePtr> outputTypes1 { LongType(), ContainerType(), LongType(), LongType(), LongType() };
    DataTypes aggOutputTypes1(outputTypes1);
    std::vector<ColumnIndex> groupByColumns1 = { c0, c1 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    auto aggInputCols1Wrap = AggregatorUtil::WrapWithVector(aggInputCols1);
    aggs1.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputCols1Wrap[0], true, true));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(ContainerType()), aggInputCols1Wrap[1], true, true));
    aggs1.push_back(std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()),
        aggInputCols1Wrap[2], true, true));
    aggs1.push_back(
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputCols1Wrap[3], true, true));
    aggs1.push_back(
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputCols1Wrap[4], true, true));

    auto aggInputTypes1Wrap = AggregatorUtil::WrapWithVector(aggInputTypes1);
    auto aggOutputTypes1Wrap = AggregatorUtil::WrapWithVector(aggOutputTypes1);
    auto inputRawWrap1 = AggregatorUtil::WrapWithVector(true, 5);
    auto outputPartialWrap1 = AggregatorUtil::WrapWithVector(true, 5);
    HashAggregationOperator *groupBy1 = new HashAggregationOperator(groupByColumns1, aggInputCols1Wrap, 5,
        aggInputTypes1Wrap, aggOutputTypes1Wrap, std::move(aggs1), inputRawWrap1, outputPartialWrap1);
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
    ColumnIndex c2 = { 0, LongType(), LongType() };
    ColumnIndex c3 = { 1, LongType(), LongType() };
    std::vector<int32_t> aggInputCols2 = { 2, 3, 4, 5, 6 };
    std::vector<DataTypePtr> inputTypes2 { LongType(), LongType(), LongType(), LongType(), LongType() };
    DataTypes aggInputTypes2(inputTypes2);
    std::vector<DataTypePtr> outputTypes2 { LongType(), ContainerType(), LongType(), LongType(), LongType() };
    DataTypes aggOutputTypes2(outputTypes2);
    groupByColumns1 = { c2, c3 };

    auto aggInputCols2Wrap = AggregatorUtil::WrapWithVector(aggInputCols2);
    aggs1.clear();
    aggs1.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputCols2Wrap[0], true, true));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(ContainerType()), aggInputCols2Wrap[1], true, true));
    aggs1.push_back(std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()),
        aggInputCols2Wrap[2], true, true));
    aggs1.push_back(
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputCols2Wrap[3], true, true));
    aggs1.push_back(
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputCols2Wrap[4], true, true));

    auto aggInputTypes2Wrap = AggregatorUtil::WrapWithVector(aggInputTypes2);
    auto aggOutputTypes2Wrap = AggregatorUtil::WrapWithVector(aggOutputTypes2);
    auto inputRawWrap2 = AggregatorUtil::WrapWithVector(true, 5);
    auto outputPartialWrap2 = AggregatorUtil::WrapWithVector(true, 5);
    HashAggregationOperator *groupBy2 = new HashAggregationOperator(groupByColumns1, aggInputCols2Wrap, 5,
        aggInputTypes2Wrap, aggOutputTypes2Wrap, std::move(aggs1), inputRawWrap2, outputPartialWrap2);
    groupBy2->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupBy2->AddInput(input2[i]);
    }

    std::vector<VectorBatch *> result2;
    int32_t tableCount2 = groupBy2->GetOutput(result2);
    EXPECT_EQ(tableCount2, 1);
    Operator::DeleteOperator(groupBy2);

    // Second stage
    ColumnIndex c4 = { 0, LongType(), LongType() };
    ColumnIndex c5 = { 1, LongType(), LongType() };
    std::vector<int32_t> aggInputCols3 = { 2, 3, 4, 5, 6 };
    std::vector<DataTypePtr> inputTypes3 { LongType(), LongType(), LongType(), LongType(), LongType() };
    DataTypes aggInputTypes3(inputTypes3);
    std::vector<DataTypePtr> outputType3 { LongType(), DoubleType(), LongType(), LongType(), LongType() };
    DataTypes aggOutputTypes3(outputType3);

    std::vector<ColumnIndex> groupByColumns2 = { c4, c5 };
    std::vector<std::unique_ptr<Aggregator>> aggs2;
    auto aggInputCols3Wrap = AggregatorUtil::WrapWithVector(aggInputCols3);

    aggs2.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputCols3Wrap[0], false, false));
    aggs2.push_back(std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(ContainerType()), aggInputCols3Wrap[1], false, false));
    aggs2.push_back(std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()),
        aggInputCols3Wrap[2], false, false));
    aggs2.push_back(
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputCols3Wrap[3], false, false));
    aggs2.push_back(
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputCols3Wrap[4], false, false));

    auto aggInputTypes3Wrap = AggregatorUtil::WrapWithVector(aggInputTypes3);
    auto aggOutputTypes3Wrap = AggregatorUtil::WrapWithVector(aggOutputTypes3);
    auto inputRawWrap3 = AggregatorUtil::WrapWithVector(false, 5);
    auto outputPartialWrap3 = AggregatorUtil::WrapWithVector(false, 5);
    HashAggregationOperator *groupBy3 = new HashAggregationOperator(groupByColumns2, aggInputCols3Wrap, 5,
        aggInputTypes3Wrap, aggOutputTypes3Wrap, std::move(aggs2), inputRawWrap3, outputPartialWrap3);
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

    std::vector<DataTypePtr> expectFieldTypes { LongType(), LongType(), LongType(), DoubleType(),
        LongType(), LongType(), LongType() };
    // construct the output data
    DataTypes expectTypes(expectFieldTypes);
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
    DataTypePtr type1 = VarcharType(1);
    DataTypePtr type2 = VarcharType(1);
    DataTypePtr type3 = VarcharType(1);
    DataTypePtr type4 = VarcharType(4);
    ColumnIndex c0 = { 0, type1, type1 };
    std::vector<int32_t> aggInputCols1 = { 1, 2, 3 };
    std::vector<DataTypePtr> inputTypes1 { type2, type3, type4 };
    DataTypes aggInputTypes1(inputTypes1);
    std::vector<DataTypePtr> outputTypes1 { LongType(), type3, type4 };
    DataTypes aggOutputTypes1(outputTypes1);

    std::vector<ColumnIndex> groupByColumns1 = { c0 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    auto aggInputCols1Wrap = AggregatorUtil::WrapWithVector(aggInputCols1);

    aggs1.push_back(std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()),
        aggInputCols1Wrap[0], INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MinVarcharAggregator>(AggregatorUtil::WrapWithDataTypes(VarcharType()),
        AggregatorUtil::WrapWithDataTypes(VarcharType()), aggInputCols1Wrap[1], INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MaxVarcharAggregator>(AggregatorUtil::WrapWithDataTypes(VarcharType()),
        AggregatorUtil::WrapWithDataTypes(VarcharType()), aggInputCols1Wrap[2], INPUT_MODE, OUTPUT_MODE));

    auto aggInputTypes1Wrap = AggregatorUtil::WrapWithVector(aggInputTypes1);
    auto aggOutputTypes1Wrap = AggregatorUtil::WrapWithVector(aggOutputTypes1);
    auto inputRawWrap1 = AggregatorUtil::WrapWithVector(true, 3);
    auto outputPartialWrap1 = AggregatorUtil::WrapWithVector(false, 3);
    HashAggregationOperator *groupByVarChar = new HashAggregationOperator(groupByColumns1, aggInputCols1Wrap, 3,
        aggInputTypes1Wrap, aggOutputTypes1Wrap, std::move(aggs1), inputRawWrap1, outputPartialWrap1);
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
    std::vector<DataTypePtr> expectedFieldTypes { VarcharType(1), LongType(), VarcharType(1), VarcharType(3) };
    DataTypes expectedTypes(expectedFieldTypes);
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectedTypes, 3, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatch(resBatch, expectVecBatch));

    delete[] input;
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
    DataTypePtr type1 = CharType(1);
    DataTypePtr type2 = CharType(1);
    DataTypePtr type3 = CharType(1);
    DataTypePtr type4 = CharType(4);
    ColumnIndex c0 = { 0, type1, type1 };
    std::vector<int32_t> aggInputCols1 = { 1, 2, 3 };
    std::vector<DataTypePtr> inputTypes1 { type2, type3, type4 };
    DataTypes aggInputTypes1(inputTypes1);
    std::vector<DataTypePtr> outputTypes1 { LongType(), type3, type4 };
    DataTypes aggOutputTypes1(outputTypes1);

    std::vector<ColumnIndex> groupByColumns1 = { c0 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    auto aggInputCols1Wrap = AggregatorUtil::WrapWithVector(aggInputCols1);
    aggs1.push_back(std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()),
        aggInputCols1Wrap[0], INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MinVarcharAggregator>(AggregatorUtil::WrapWithDataTypes(CharType()),
        AggregatorUtil::WrapWithDataTypes(CharType()), aggInputCols1Wrap[1], INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MaxVarcharAggregator>(AggregatorUtil::WrapWithDataTypes(CharType()),
        AggregatorUtil::WrapWithDataTypes(CharType()), aggInputCols1Wrap[2], INPUT_MODE, OUTPUT_MODE));

    auto aggInputTypes1Wrap = AggregatorUtil::WrapWithVector(aggInputTypes1);
    auto aggOutputTypes1Wrap = AggregatorUtil::WrapWithVector(aggOutputTypes1);
    auto inputRawWrap1 = AggregatorUtil::WrapWithVector(true, 3);
    auto outputPartialWrap1 = AggregatorUtil::WrapWithVector(false, 3);
    HashAggregationOperator *groupByVarChar = new HashAggregationOperator(groupByColumns1, aggInputCols1Wrap, 3,
        aggInputTypes1Wrap, aggOutputTypes1Wrap, std::move(aggs1), inputRawWrap1, outputPartialWrap1);
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

    std::vector<DataTypePtr> expectedFieldTypes { CharType(1), LongType(), CharType(1), CharType(3) };
    DataTypes expectedTypes(expectedFieldTypes);
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectedTypes, 3, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatch(resBatch, expectVecBatch));

    delete[] input;
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
    std::vector<DataTypePtr> groupTypes = { LongType() };
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType(), LongType(), LongType(), LongType() };
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
    ColumnIndex c0 = { 0, LongType(), LongType() };
    std::vector<int32_t> aggInputCols1 = { 1, 2, 3, 4, 5 };
    std::vector<DataTypePtr> inputTypes1 { LongType(), LongType(), LongType(), LongType(), LongType() };
    DataTypes aggInputTypes1(inputTypes1);
    std::vector<DataTypePtr> outputTypes1 { LongType(), DoubleType(), LongType(), LongType(), LongType() };
    DataTypes aggOutputTypes1(outputTypes1);

    std::vector<ColumnIndex> groupByColumns1 = { c0 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    auto aggInputCols1Wrap = AggregatorUtil::WrapWithVector(aggInputCols1);
    aggs1.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputCols1Wrap[0], INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(DoubleType()), aggInputCols1Wrap[1], INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()),
        aggInputCols1Wrap[2], INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputCols1Wrap[3], INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputCols1Wrap[4], INPUT_MODE, OUTPUT_MODE));

    auto aggInputTypes1Wrap = AggregatorUtil::WrapWithVector(aggInputTypes1);
    auto aggOutputTypes1Wrap = AggregatorUtil::WrapWithVector(aggOutputTypes1);
    auto inputRawWrap1 = AggregatorUtil::WrapWithVector(true, 5);
    auto outputPartialWrap1 = AggregatorUtil::WrapWithVector(false, 5);
    HashAggregationOperator *groupByNULL = new HashAggregationOperator(groupByColumns1, aggInputCols1Wrap, 5,
        aggInputTypes1Wrap, aggOutputTypes1Wrap, std::move(aggs1), inputRawWrap1, outputPartialWrap1);
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

    std::vector<DataTypePtr> expectedFieldTypes { LongType(), LongType(), DoubleType(),
        LongType(), LongType(), LongType() };
    DataTypes expectedTypes(expectedFieldTypes);
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 1, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);

    EXPECT_TRUE(VecBatchMatch(result[0], expectVecBatch));

    delete[] input;
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
    ColumnIndex c0 = { 0, LongType(), LongType() };
    ColumnIndex c1 = { 1, LongType(), LongType() };
    std::vector<int32_t> aggInputCols = { 0, 1 };
    std::vector<DataTypePtr> inputTypes { LongType(), LongType() };
    DataTypes aggInputTypes(inputTypes);
    std::vector<DataTypePtr> outputTypes { LongType(), LongType() };
    DataTypes aggOutputTypes(outputTypes);
    std::vector<ColumnIndex> groupByColumns = { c0, c1 };
    std::vector<std::unique_ptr<Aggregator>> aggs;
    auto aggInputColsWrap = AggregatorUtil::WrapWithVector(aggInputCols);
    aggs.push_back(
        std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(IntType()),
        AggregatorUtil::WrapWithDataTypes(IntType()), aggInputColsWrap[0], INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), aggInputColsWrap[1], INPUT_MODE, OUTPUT_MODE));

    auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(aggInputTypes);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, 2);
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(false, 2);
    HashAggregationOperator *groupBy = new HashAggregationOperator(groupByColumns, aggInputColsWrap, 2,
        aggInputTypesWrap, aggOutputTypesWrap, std::move(aggs), inputRawWrap, outputPartialWrap);
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

    delete[] input;
}

TEST(HashAggregationOperatorTest, verify_distinct_correctness)
{
    // construct data
    const int32_t dataSize = 4;
    const int32_t resultDataSize = 1;

    // table1
    int64_t dataHash[dataSize] = {1L, 1L, 1L, 1L}; // all rows will be in same hash group
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

    std::vector<DataTypePtr> types = { LongType(),    LongType(),    LongType(),    LongType(),
        LongType(),    LongType(),    BooleanType(), BooleanType(),
        BooleanType(), BooleanType(), BooleanType() };
    DataTypes sourceTypes(types);

    std::vector<DataTypePtr> inputTypes = { LongType(),    LongType(),    LongType(),
                                       LongType(),    LongType() };
    DataTypes inputDataTypes(inputTypes);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, dataHash, data0, data1, data2, data3, data4,
        data5, data6, data7, data8, data9);

    std::vector<int32_t> aggInputCols = { 1, 2, 3, 4, 5 };
    ColumnIndex c0 = { 0, LongType(), LongType() };
    std::vector<ColumnIndex> groupByColumns = { c0 };
    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal1 = { 1 };
    std::vector<int32_t> channal2 = { 2 };
    std::vector<int32_t> channal3 = { 3 };
    std::vector<int32_t> channal4 = { 4 };
    std::vector<int32_t> channal5 = { 5 };

    // STAGE1:
    std::vector<std::unique_ptr<Aggregator>> aggs;
    auto aggInputColsWrap = AggregatorUtil::WrapWithVector(aggInputCols);
    std::unique_ptr<Aggregator> aggregator =
        std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()), channal1, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(6, std::move(aggregator)));
    aggregator =
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal2, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(7, std::move(aggregator)));
    aggregator = std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(ContainerType()), channal3, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(8, std::move(aggregator)));
    aggregator =
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal4, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(9, std::move(aggregator)));
    aggregator =
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal5, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(10, std::move(aggregator)));

    std::vector<DataTypePtr> partialOutputTypes { LongType(), LongType(), ContainerType(), LongType(), LongType() };
    DataTypes aggPartialOutputTypes(partialOutputTypes);

    auto inputTypesWrap = AggregatorUtil::WrapWithVector(inputDataTypes);
    auto aggPartialOutputTypesWrap = AggregatorUtil::WrapWithVector(aggPartialOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto aggregate1 = new HashAggregationOperator(groupByColumns, aggInputColsWrap, aggs.size(), inputTypesWrap,
        aggPartialOutputTypesWrap, std::move(aggs), inputRawWrap, outputPartialWrap);

    aggregate1->Init();
    aggregate1->AddInput(vecBatch1);

    std::vector<VectorBatch *> result;
    int32_t tableCount1 = aggregate1->GetOutput(result);
    EXPECT_EQ(tableCount1, 1);

    // STAGE2:
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(
        std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()), channal1, false, false));
    aggs1.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal2, false, false));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(DoubleType()), channal3, false, false));
    aggs1.push_back(
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal4, false, false));
    aggs1.push_back(
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal5, false, false));
    std::vector<DataTypePtr> finalOutputTypes { LongType(), LongType(), DoubleType(), LongType(), LongType() };
    DataTypes aggFinalOutputTypes(finalOutputTypes);

    auto aggFinalOutputTypesWrap = AggregatorUtil::WrapWithVector(aggFinalOutputTypes);
    auto inputRawWrap2 = AggregatorUtil::WrapWithVector(false, aggs1.size());
    auto outputPartialWrap2 = AggregatorUtil::WrapWithVector(false, aggs1.size());
    auto aggregate2 = new HashAggregationOperator(groupByColumns, aggInputColsWrap, aggs1.size(), inputTypesWrap,
        aggFinalOutputTypesWrap, std::move(aggs1), inputRawWrap2, outputPartialWrap2);

    aggregate2->Init();
    for (uint32_t i = 0; i < result.size(); ++i) {
        aggregate2->AddInput(result[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t tableCount = aggregate2->GetOutput(result1);
    EXPECT_EQ(tableCount, 1);
    EXPECT_EQ(result1[0]->GetRowCount(), 1);
    EXPECT_EQ(result1[0]->GetVectorCount(), 6);

    int64_t expHashData[resultDataSize] = {1L};
    int64_t expData0[resultDataSize] = {3L};
    int64_t expData1[resultDataSize] = {6L};
    double expData2[resultDataSize] = {5.0};
    int64_t expData3[resultDataSize] = {4L};
    int64_t expData4[resultDataSize] = {1L};
    std::vector<DataTypePtr> resultType = { LongType(), LongType(), LongType(), DoubleType(), LongType(), LongType() };
    DataTypes resultTypes(resultType);
    VectorBatch *expVecBatch1 =
        CreateVectorBatch(resultTypes, resultDataSize, expHashData, expData0, expData1, expData2, expData3, expData4);

    EXPECT_TRUE(VecBatchMatch(result1[0], expVecBatch1));

    omniruntime::op::Operator::DeleteOperator(aggregate1);
    omniruntime::op::Operator::DeleteOperator(aggregate2);
    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatches(result1);
}

TEST(HashAggregationOperatorTest, DISABLED_original_multiple_threads)
{
    using namespace std;
    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;


    FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    int32_t maskCols[] = {-1, -1};
    std::vector<DataTypePtr> groupFieldTypes { LongType(), LongType() };
    DataTypes groupTypes(groupFieldTypes);
    std::vector<DataTypePtr> inputTypes { LongType(), LongType() };
    DataTypes aggInputTypes(inputTypes);
    std::vector<DataTypePtr> outputTypes { LongType(), LongType() };
    DataTypes aggOutputTypes(outputTypes);
    VectorBatch **input =
        buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes.Get(), aggInputTypes.Get());
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    uint32_t groupCols[2] = {0, 1};
    uint32_t aggCols[2] = {2, 3};
    std::vector<uint32_t> groupByColVector = std::vector<uint32_t>(groupCols, groupCols + 2);
    std::vector<uint32_t> aggColVector = std::vector<uint32_t>(aggCols, aggCols + 2);
    std::vector<uint32_t> aggFuncTypeVector =
        std::vector<uint32_t>(reinterpret_cast<uint32_t *>(aggFunType), reinterpret_cast<uint32_t *>(aggFunType) + 2);
    std::vector<uint32_t> maskColsVector =
        std::vector<uint32_t>(reinterpret_cast<uint32_t *>(maskCols), reinterpret_cast<uint32_t *>(maskCols) + 2);

    auto aggColVectorWrap = AggregatorUtil::WrapWithVector(aggColVector);
    auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(aggInputTypes);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, 2);
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(false, 2);
    HashAggregationOperatorFactory *nativeOperatorFactory =
        new HashAggregationOperatorFactory(groupByColVector, groupTypes, aggColVectorWrap, aggInputTypesWrap,
        aggOutputTypesWrap, aggFuncTypeVector, maskColsVector, inputRawWrap, outputPartialWrap);

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

    vector<DataTypePtr> groupTypes = { LongType(), LongType() };
    vector<DataTypePtr> aggTypes = { LongType(), LongType() };
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

TEST(AggregationOperatorTest, hmpp_min_max)
{
    const int32_t dataSize = 5;
    const int32_t resultDataSize = 1;

    int32_t data0[dataSize] = {2, 1, 5, 3, 1};
    int64_t data1[dataSize] = {3L, 10L, 2L, 7L, 3L};
    double data2[dataSize] = {12.3, 7.2, 20.5, 6.1, 12.3};
    Decimal128 data3[dataSize] = {4000L, 2000L, 1000L, 2000L, 5000L};
    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal1 = { 1 };
    std::vector<int32_t> channal2 = { 2 };
    std::vector<int32_t> channal3 = { 3 };
    std::vector<int32_t> channal4 = { 4 };
    std::vector<int32_t> channal5 = { 5 };
    std::vector<int32_t> channal6 = { 6 };
    std::vector<int32_t> channal7 = { 7 };
    std::vector<int32_t> channal8 = { 8 };
    std::vector<int32_t> channal9 = { 9 };

    std::string aggNames[] = {"min", "min", "min", "min", "min", "max", "max", "max", "max", "max"};
    std::vector<DataTypePtr> groupTypes;
    std::vector<DataTypePtr> aggTypes = { IntType(), IntType(), LongType(), DoubleType(), Decimal128Type(20, 5),
        IntType(), IntType(), LongType(), DoubleType(), Decimal128Type(20, 5) };
    DataTypes sourceTypes(aggTypes);
    VectorBatch *input =
        CreateVectorBatch(sourceTypes, dataSize, data0, data0, data1, data2, data3, data0, data0, data1, data2, data3);

    ASSERT(!(input == nullptr));

    // STAGE1:
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(
        std::make_unique<MinAggregator<IntVector, IntVector, int32_t>>(AggregatorUtil::WrapWithDataTypes(IntType()),
        AggregatorUtil::WrapWithDataTypes(IntType()), channal0, true, true));
    aggs1.push_back(
        std::make_unique<MinAggregator<IntVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(IntType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal1, true, true));
    aggs1.push_back(
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal2, true, true));
    aggs1.push_back(std::make_unique<MinAggregator<DoubleVector, DoubleVector, double>>(
        AggregatorUtil::WrapWithDataTypes(DoubleType()), AggregatorUtil::WrapWithDataTypes(DoubleType()), channal3,
        true, true));
    aggs1.push_back(std::make_unique<MinAggregator<Decimal128Vector, Decimal128Vector, Decimal128>>(
        AggregatorUtil::WrapWithDataTypes(Decimal128Type(20, 5)),
        AggregatorUtil::WrapWithDataTypes(Decimal128Type(20, 5)), channal4, true, true));
    aggs1.push_back(
        std::make_unique<MaxAggregator<IntVector, IntVector, int32_t>>(AggregatorUtil::WrapWithDataTypes(IntType()),
        AggregatorUtil::WrapWithDataTypes(IntType()), channal5, true, true));
    aggs1.push_back(
        std::make_unique<MaxAggregator<IntVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(IntType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal6, true, true));
    aggs1.push_back(
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal7, true, true));
    aggs1.push_back(std::make_unique<MaxAggregator<DoubleVector, DoubleVector, double>>(
        AggregatorUtil::WrapWithDataTypes(DoubleType()), AggregatorUtil::WrapWithDataTypes(DoubleType()), channal8,
        true, true));
    aggs1.push_back(std::make_unique<MaxAggregator<Decimal128Vector, Decimal128Vector, Decimal128>>(
        AggregatorUtil::WrapWithDataTypes(Decimal128Type(20, 5)),
        AggregatorUtil::WrapWithDataTypes(Decimal128Type(20, 5)), channal9, true, true));
    std::vector<DataTypePtr> partialOutputTypes {
        IntType(), LongType(), LongType(), DoubleType(), Decimal128Type(20, 5),
        IntType(), LongType(), LongType(), DoubleType(), Decimal128Type(20, 5)
    };
    DataTypes aggPartialOutputTypes(partialOutputTypes);
    auto aggPartialOutputTypesWrap = AggregatorUtil::WrapWithVector(aggPartialOutputTypes);
    auto inputRaws1Wrap = AggregatorUtil::WrapWithVector(true, aggs1.size());
    auto outputPartials1Wrap = AggregatorUtil::WrapWithVector(true, aggs1.size());

    auto aggregate1 =
        new AggregationOperator(std::move(aggs1), aggPartialOutputTypesWrap, inputRaws1Wrap, outputPartials1Wrap);

    aggregate1->AddInput(input);

    std::vector<VectorBatch *> result;
    int32_t tableCount = aggregate1->GetOutput(result);
    EXPECT_EQ(tableCount, 1);

    // STAGE2:
    std::vector<std::unique_ptr<Aggregator>> aggs2;
    aggs2.push_back(
        std::make_unique<MinAggregator<IntVector, IntVector, int32_t>>(AggregatorUtil::WrapWithDataTypes(IntType()),
        AggregatorUtil::WrapWithDataTypes(IntType()), channal0, false, false));
    aggs2.push_back(
        std::make_unique<MinAggregator<LongVector, IntVector, int32_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(IntType()), channal1, false, false));
    aggs2.push_back(
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal2, false, false));
    aggs2.push_back(std::make_unique<MinAggregator<DoubleVector, DoubleVector, double>>(
        AggregatorUtil::WrapWithDataTypes(DoubleType()), AggregatorUtil::WrapWithDataTypes(DoubleType()), channal3,
        false, false));
    aggs2.push_back(std::make_unique<MinAggregator<Decimal128Vector, Decimal128Vector, Decimal128>>(
        AggregatorUtil::WrapWithDataTypes(Decimal128Type(20, 5)),
        AggregatorUtil::WrapWithDataTypes(Decimal128Type(20, 5)), channal4, false, false));
    aggs2.push_back(
        std::make_unique<MaxAggregator<IntVector, IntVector, int32_t>>(AggregatorUtil::WrapWithDataTypes(IntType()),
        AggregatorUtil::WrapWithDataTypes(IntType()), channal5, false, false));
    aggs2.push_back(
        std::make_unique<MaxAggregator<LongVector, IntVector, int32_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(IntType()), channal6, false, false));
    aggs2.push_back(
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal7, false, false));
    aggs2.push_back(std::make_unique<MaxAggregator<DoubleVector, DoubleVector, double>>(
        AggregatorUtil::WrapWithDataTypes(DoubleType()), AggregatorUtil::WrapWithDataTypes(DoubleType()), channal8,
        false, false));
    aggs2.push_back(std::make_unique<MaxAggregator<Decimal128Vector, Decimal128Vector, Decimal128>>(
        AggregatorUtil::WrapWithDataTypes(Decimal128Type(20, 5)),
        AggregatorUtil::WrapWithDataTypes(Decimal128Type(20, 5)), channal9, false, false));
    std::vector<DataTypePtr> finalOutputTypes { IntType(), IntType(), LongType(), DoubleType(), Decimal128Type(20, 5),
        IntType(), IntType(), LongType(), DoubleType(), Decimal128Type(20, 5) };
    DataTypes aggFinalOutputTypes(finalOutputTypes);
    auto aggFinalOutputTypesWrap = AggregatorUtil::WrapWithVector(aggFinalOutputTypes);
    auto inputRaws2Wrap = AggregatorUtil::WrapWithVector(false, aggs2.size());
    auto outputPartials2Wrap = AggregatorUtil::WrapWithVector(false, aggs2.size());

    auto aggregate2 =
        new AggregationOperator(std::move(aggs2), aggFinalOutputTypesWrap, inputRaws2Wrap, outputPartials2Wrap);

    for (uint32_t i = 0; i < result.size(); i++) {
        aggregate2->AddInput(result[i]);
    }

    std::vector<VectorBatch *> finalResult;
    tableCount = aggregate2->GetOutput(finalResult);
    EXPECT_EQ(tableCount, 1);
    EXPECT_EQ(finalResult[0]->GetRowCount(), 1);
    EXPECT_EQ(finalResult[0]->GetVectorCount(), 10);

    int32_t expData0[resultDataSize] = {1};
    int64_t expData1[resultDataSize] = {2L};
    double expData2[resultDataSize] = {6.1};
    Decimal128 expData3[resultDataSize] = {1000L};
    int32_t expData4[resultDataSize] = {5};
    int64_t expData5[resultDataSize] = {10L};
    double expData6[resultDataSize] = {20.5};
    Decimal128 expData7[resultDataSize] = {5000L};
    std::vector<DataTypePtr> resultType = { IntType(), IntType(), LongType(), DoubleType(), Decimal128Type(20, 5),
        IntType(), IntType(), LongType(), DoubleType(), Decimal128Type(20, 5) };
    DataTypes resultTypes(resultType);
    VectorBatch *expVecBatch = CreateVectorBatch(resultTypes, resultDataSize, expData0, expData0, expData1, expData2,
        expData3, expData4, expData4, expData5, expData6, expData7);

    EXPECT_TRUE(VecBatchMatch(finalResult[0], expVecBatch));

    omniruntime::op::Operator::DeleteOperator(aggregate2);
    omniruntime::op::Operator::DeleteOperator(aggregate1);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatches(finalResult);
}

TEST(AggregationOperatorTest, hmpp_min_max_varchar)
{
    std::string data0[] = {"Zulma.Carter@MfvjVN43Udd95KeZ.com", "*", "Aaron.Anderson@0CQ4QUkBY2Q.edu",
                           "Zulema.Ruiz@J2XvbX7.com", "*", "Aaron.Artis@bv.org"};
    std::string data1[] = {"*", "Zulma.Carter@MfvjVN43Udd95KeZ.com", "Aaron.Anderson@0CQ4QUkBY2Q.edu", "*",
                           "Zulema.Ruiz@J2XvbX7.com", "Aaron.Artis@bv.org"};
    std::vector<DataTypePtr> types = std::vector<DataTypePtr> { VarcharType(100), VarcharType(100) };
    int32_t rowCount = 6;
    auto vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("min_varchar_test");
    auto vector1 = new VarcharVector(vectorAllocator,
        static_cast<VarcharDataType *>(types.at(0).get())->GetWidth() * rowCount, rowCount);
    auto vector2 = new VarcharVector(vectorAllocator,
        static_cast<VarcharDataType *>(types.at(1).get())->GetWidth() * rowCount, rowCount);
    for (int32_t i = 0; i < rowCount; i++) {
        if (i == 1 || i == 4) {
            vector1->SetValueNull(i);
        } else {
            vector1->SetValue(i, reinterpret_cast<const uint8_t *>(data0[i].c_str()), data0[i].size());
        }
        if (i == 0 || i == 3) {
            vector2->SetValueNull(i);
        } else {
            vector2->SetValue(i, reinterpret_cast<const uint8_t *>(data1[i].c_str()), data1[i].size());
        }
    }

    auto input = new VectorBatch(2, 6);
    input->SetVector(0, vector1);
    input->SetVector(1, vector2);

    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal1 = { 1 };
    // STAGE1:
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<MinVarcharAggregator>(AggregatorUtil::WrapWithDataTypes(VarcharType(100)),
        AggregatorUtil::WrapWithDataTypes(VarcharType(100)), channal0, true, true));
    aggs1.push_back(std::make_unique<MaxVarcharAggregator>(AggregatorUtil::WrapWithDataTypes(VarcharType(100)),
        AggregatorUtil::WrapWithDataTypes(VarcharType(100)), channal1, true, true));

    std::vector<DataTypePtr> partialOutputTypes { VarcharType(100), VarcharType(100) };
    DataTypes aggPartialOutputTypes(partialOutputTypes);
    auto aggPartialOutputTypesWrap = AggregatorUtil::WrapWithVector(aggPartialOutputTypes);
    auto inputRaws1Wrap = AggregatorUtil::WrapWithVector(true, aggs1.size());
    auto outputPartials1Wrap = AggregatorUtil::WrapWithVector(true, aggs1.size());

    auto aggOperator1 =
        new AggregationOperator(std::move(aggs1), aggPartialOutputTypesWrap, inputRaws1Wrap, outputPartials1Wrap);
    aggOperator1->AddInput(input);
    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = aggOperator1->GetOutput(result);
    EXPECT_EQ(vecBatchCount, 1);

    // STAGE2:
    std::vector<std::unique_ptr<Aggregator>> aggs2;
    aggs2.push_back(std::make_unique<MinVarcharAggregator>(AggregatorUtil::WrapWithDataTypes(VarcharType(100)),
        AggregatorUtil::WrapWithDataTypes(VarcharType(100)), channal0, false, false));
    aggs2.push_back(std::make_unique<MaxVarcharAggregator>(AggregatorUtil::WrapWithDataTypes(VarcharType(100)),
        AggregatorUtil::WrapWithDataTypes(VarcharType(100)), channal1, false, false));

    std::vector<DataTypePtr> finalOutputTypes { VarcharType(100), VarcharType(100) };
    DataTypes aggFinalOutputTypes(finalOutputTypes);
    auto aggFinalOutputTypesWrap = AggregatorUtil::WrapWithVector(aggFinalOutputTypes);
    auto inputRaws2Wrap = AggregatorUtil::WrapWithVector(false, aggs2.size());
    auto outputPartials2Wrap = AggregatorUtil::WrapWithVector(false, aggs2.size());

    auto aggOperator2 =
        new AggregationOperator(std::move(aggs2), aggFinalOutputTypesWrap, inputRaws2Wrap, inputRaws2Wrap);
    for (uint32_t i = 0; i < result.size(); i++) {
        aggOperator2->AddInput(result[i]);
    }

    std::vector<VectorBatch *> finalResult;
    vecBatchCount = aggOperator2->GetOutput(finalResult);
    EXPECT_EQ(vecBatchCount, 1);
    auto resultVec0 = static_cast<VarcharVector *>(finalResult[0]->GetVector(0));
    EXPECT_EQ(resultVec0->GetSize(), 1);
    uint8_t *minVal = nullptr;
    int32_t minValLen = resultVec0->GetValue(0, &minVal);
    std::string minStr(minVal, minVal + minValLen);
    EXPECT_EQ(minValLen, data0[2].size());
    EXPECT_EQ(data0[2].compare(minStr), 0);

    auto resultVec1 = static_cast<VarcharVector *>(finalResult[0]->GetVector(1));
    EXPECT_EQ(resultVec1->GetSize(), 1);
    uint8_t *maxVal = nullptr;
    int32_t maxValLen = resultVec1->GetValue(0, &maxVal);
    std::string maxStr(maxVal, maxVal + maxValLen);
    EXPECT_EQ(maxValLen, data1[1].size());
    EXPECT_EQ(data1[1].compare(maxStr), 0);

    omniruntime::op::Operator::DeleteOperator(aggOperator2);
    omniruntime::op::Operator::DeleteOperator(aggOperator1);
    VectorHelper::FreeVecBatches(finalResult);
    delete vectorAllocator;
}

TEST(AggregationOperatorTest, hmpp_sum_avg)
{
    const int32_t dataSize = 5;
    const int32_t resultDataSize = 1;

    int64_t data0[dataSize] = {3L, 10L, 2L, 7L, 3L};
    Decimal128 data1[dataSize] = {Decimal128(4000L, 0), Decimal128(2000L, 0), Decimal128(1000L, 0),
                                  Decimal128(3000L, 0), Decimal128(5000L, 0)};

    std::string aggNames[] = {"sum", "sum", "avg", "avg"};
    std::vector<DataTypePtr> groupTypes;
    std::vector<DataTypePtr> aggTypes = { LongType(), Decimal128Type(20, 5), LongType(), Decimal128Type(20, 5) };
    DataTypes sourceTypes(aggTypes);
    VectorBatch *input = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data0, data1);

    ASSERT(!(input == nullptr));

    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal1 = { 1 };
    std::vector<int32_t> channal2 = { 2 };
    std::vector<int32_t> channal3 = { 3 };

    // STAGE1:
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(
        std::make_unique<SumAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true, true));
    aggs1.push_back(std::make_unique<SumLongDecimalAggregator>(AggregatorUtil::WrapWithDataTypes(Decimal128Type(20, 5)),
        AggregatorUtil::WrapWithDataTypes(SUM_IMMEDIATE_VARBINARY), channal1, true, true));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(ContainerType()), channal2, true, true));
    aggs1.push_back(std::make_unique<AverageDecimalAggregator>(AggregatorUtil::WrapWithDataTypes(Decimal128Type(20, 5)),
        AggregatorUtil::WrapWithDataTypes(AVG_IMMEDIATE_VARBINARY), channal3, true, true));
    std::vector<DataTypePtr> partialOutputTypes { LongType(), SUM_IMMEDIATE_VARBINARY,
        ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), AVG_IMMEDIATE_VARBINARY };
    DataTypes aggPartialOutputTypes(partialOutputTypes);
    auto aggPartialOutputTypesWrap = AggregatorUtil::WrapWithVector(aggPartialOutputTypes);
    auto inputRaws1Wrap = AggregatorUtil::WrapWithVector(true, aggs1.size());
    auto outputPartials1Wrap = AggregatorUtil::WrapWithVector(true, aggs1.size());

    auto aggregate1 =
        new AggregationOperator(std::move(aggs1), aggPartialOutputTypesWrap, inputRaws1Wrap, outputPartials1Wrap);

    aggregate1->AddInput(input);

    std::vector<VectorBatch *> result;
    int32_t tableCount = aggregate1->GetOutput(result);
    EXPECT_EQ(tableCount, 1);

    // STAGE2:
    std::vector<std::unique_ptr<Aggregator>> aggs2;
    aggs2.push_back(
        std::make_unique<SumAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, false, false));
    aggs2.push_back(
        std::make_unique<SumFinalDecimalAggregator>(AggregatorUtil::WrapWithDataTypes(SUM_IMMEDIATE_VARBINARY),
        AggregatorUtil::WrapWithDataTypes(Decimal128Type(20, 5)), channal1, false, false));
    aggs2.push_back(std::make_unique<AverageAggregator<IntVector>>(AggregatorUtil::WrapWithDataTypes(ContainerType()),
        AggregatorUtil::WrapWithDataTypes(DoubleType()), channal2, false, false));
    aggs2.push_back(
        std::make_unique<AverageDecimalAggregator>(AggregatorUtil::WrapWithDataTypes(AVG_IMMEDIATE_VARBINARY),
        AggregatorUtil::WrapWithDataTypes(Decimal128Type(20, 5)), channal3, false, false));
    std::vector<DataTypePtr> finalOutputTypes { LongType(), Decimal128Type(20, 5), DoubleType(),
        Decimal128Type(20, 5) };
    DataTypes aggFinalOutputTypes(finalOutputTypes);
    auto aggFinalOutputTypesWrap = AggregatorUtil::WrapWithVector(aggFinalOutputTypes);
    auto inputRaws2Wrap = AggregatorUtil::WrapWithVector(false, aggs2.size());
    auto outputPartials2Wrap = AggregatorUtil::WrapWithVector(false, aggs1.size());

    auto aggregate2 =
        new AggregationOperator(std::move(aggs2), aggFinalOutputTypesWrap, inputRaws2Wrap, outputPartials2Wrap);

    for (uint32_t i = 0; i < result.size(); i++) {
        aggregate2->AddInput(result[i]);
    }

    std::vector<VectorBatch *> finalResult;
    tableCount = aggregate2->GetOutput(finalResult);
    EXPECT_EQ(tableCount, 1);
    EXPECT_EQ(finalResult[0]->GetRowCount(), 1);
    EXPECT_EQ(finalResult[0]->GetVectorCount(), 4);

    int64_t expData0[resultDataSize] = {25L};
    Decimal128 expData1[resultDataSize] = {Decimal128(15000L, 0)};
    double expData2[resultDataSize] = {5.0};
    Decimal128 expData3[resultDataSize] = {Decimal128(3000L, 0)};
    std::vector<DataTypePtr> resultType = { LongType(), Decimal128Type(20, 5), DoubleType(), Decimal128Type(20, 5) };
    DataTypes resultTypes(resultType);
    VectorBatch *expVecBatch = CreateVectorBatch(resultTypes, resultDataSize, expData0, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(finalResult[0], expVecBatch));

    omniruntime::op::Operator::DeleteOperator(aggregate2);
    omniruntime::op::Operator::DeleteOperator(aggregate1);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatches(finalResult);
}

TEST(AggregationOperatorTest, verify_correctness)
{
    // create 10 vecBatches
    const int vecBatchNum = 10;
    const int cardinality = 4;
    std::string aggNames[] = {"sum", "avg", "count", "min", "max"};
    std::vector<DataTypePtr> groupTypes;
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType(), LongType(), LongType(), LongType() };
    VectorBatch **input1 = buildAggInput(vecBatchNum, ROW_PER_VEC_BATCH, cardinality, 0, 5, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    std::vector<std::unique_ptr<Aggregator>> aggs;
    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal1 = { 1 };
    std::vector<int32_t> channal2 = { 2 };
    std::vector<int32_t> channal3 = { 3 };
    std::vector<int32_t> channal4 = { 4 };
    aggs.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true, true));
    aggs.push_back(std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(ContainerType()), channal1, true, true));
    aggs.push_back(
        std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()), channal2, true, true));
    aggs.push_back(
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal3, true, true));
    aggs.push_back(
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal4, true, true));
    std::vector<DataTypePtr> partialOutputTypes { LongType(),
        ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), LongType(), LongType(), LongType() };
    DataTypes aggPartialOutputTypes(partialOutputTypes);

    auto aggPartialOutputTypesWrap = AggregatorUtil::WrapWithVector(aggPartialOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto aggregate1 =
        new AggregationOperator(std::move(aggs), aggPartialOutputTypesWrap, inputRawWrap, outputPartialWrap);

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
    aggs.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true, true));
    aggs.push_back(std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(ContainerType()), channal1, true, true));
    aggs.push_back(
        std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()), channal2, true, true));
    aggs.push_back(
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal3, true, true));
    aggs.push_back(
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal4, true, true));
    std::vector<DataTypePtr> partialOutputTypes2 { LongType(),
        ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), LongType(), LongType(), LongType() };
    DataTypes aggPartialOutputTypes2(partialOutputTypes2);
    auto aggPartialOutputTypes2Wrap = AggregatorUtil::WrapWithVector(aggPartialOutputTypes2);
    auto inputRaw2Wrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto outputPartial2Wrap = AggregatorUtil::WrapWithVector(true, aggs.size());

    auto aggregate2 =
        new AggregationOperator(std::move(aggs), aggPartialOutputTypes2Wrap, inputRaw2Wrap, outputPartial2Wrap);

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggregate2->AddInput(input2[i]);
    }
    int32_t tableCount2 = aggregate2->GetOutput(result);
    EXPECT_EQ(tableCount2, 1);
    omniruntime::op::Operator::DeleteOperator(aggregate2);

    // Second stage
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, false, false));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(DoubleType()), channal1, false, false));
    aggs1.push_back(
        std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()), channal2, false, false));
    aggs1.push_back(
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal3, false, false));
    aggs1.push_back(
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal4, false, false));
    std::vector<DataTypePtr> finalOutputTypes { LongType(), DoubleType(), LongType(), LongType(), LongType() };
    DataTypes aggFinalOutputTypes(finalOutputTypes);
    auto aggFinalOutputTypesWrap = AggregatorUtil::WrapWithVector(aggFinalOutputTypes);
    auto inputRaw3Wrap = AggregatorUtil::WrapWithVector(false, aggs1.size());
    auto outputPartial3Wrap = AggregatorUtil::WrapWithVector(false, aggs1.size());
    auto aggregate3 =
        new AggregationOperator(std::move(aggs1), aggFinalOutputTypesWrap, inputRaw3Wrap, outputPartial3Wrap);

    for (uint32_t i = 0; i < result.size(); ++i) {
        aggregate3->AddInput(result[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t tableCount3 = aggregate3->GetOutput(result1);
    EXPECT_EQ(tableCount3, 1);
    EXPECT_EQ(result1[0]->GetRowCount(), 1);
    EXPECT_EQ(result1[0]->GetVectorCount(), 5);

    delete[] input1;
    delete[] input2;
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

    std::vector<DataTypePtr> types = { LongType(),    LongType(),    LongType(),    LongType(),    LongType(),
        BooleanType(), BooleanType(), BooleanType(), BooleanType(), BooleanType() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 =
        CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2, data3, data4, data5, data6, data7, data8, data9);

    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal1 = { 1 };
    std::vector<int32_t> channal2 = { 2 };
    std::vector<int32_t> channal3 = { 3 };
    std::vector<int32_t> channal4 = { 4 };

    // STAGE1:
    std::vector<std::unique_ptr<Aggregator>> aggs;
    std::unique_ptr<Aggregator> aggregator =
        std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(5, std::move(aggregator)));
    aggregator =
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal1, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(6, std::move(aggregator)));
    aggregator = std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(ContainerType()), channal2, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(7, std::move(aggregator)));
    aggregator =
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal3, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(8, std::move(aggregator)));
    aggregator =
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal4, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(9, std::move(aggregator)));

    DataTypes aggPartialOutputTypes(std::vector<DataTypePtr> { LongType(), LongType(),
        ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), LongType(), LongType() });

    auto aggPartialOutputTypesWrap = AggregatorUtil::WrapWithVector(aggPartialOutputTypes);
    auto inputRaw1Wrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto outputPartial1Wrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto aggregate1 =
        new AggregationOperator(std::move(aggs), aggPartialOutputTypesWrap, inputRaw1Wrap, outputPartial1Wrap);

    aggregate1->AddInput(vecBatch1);

    std::vector<VectorBatch *> result;
    int32_t tableCount1 = aggregate1->GetOutput(result);
    EXPECT_EQ(tableCount1, 1);

    // STAGE2:
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(
        std::make_unique<CountColumnAggregator>(AggregatorUtil::WrapWithDataTypes(LongType()), channal0, false, false));
    aggs1.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal1, false, false));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(DoubleType()), channal2, false, false));
    aggs1.push_back(
        std::make_unique<MaxAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal3, false, false));
    aggs1.push_back(
        std::make_unique<MinAggregator<LongVector, LongVector, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal4, false, false));
    std::vector<DataTypePtr> finalOutputTypes { LongType(), LongType(), DoubleType(), LongType(), LongType() };
    DataTypes aggFinalOutputTypes(finalOutputTypes);

    auto aggFinalOutputTypesWrap = AggregatorUtil::WrapWithVector(aggFinalOutputTypes);
    auto inputRaw2Wrap = AggregatorUtil::WrapWithVector(false, aggs1.size());
    auto outputPartial2Wrap = AggregatorUtil::WrapWithVector(false, aggs1.size());
    auto aggregate2 =
        new AggregationOperator(std::move(aggs1), aggFinalOutputTypesWrap, inputRaw2Wrap, outputPartial2Wrap);

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
    std::vector<DataTypePtr> resultType = { LongType(), LongType(), DoubleType(), LongType(), LongType() };
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
    std::vector<DataTypePtr> groupTypes;
    std::vector<DataTypePtr> aggTypes = { LongType() };
    VectorBatch **input = buildAggInput(vecBatchNum, ROW_PER_VEC_BATCH, cardinality, 0, 1, groupTypes, aggTypes);
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    ColumnIndex c0 = { 0, LongType(), LongType() };
    std::vector<ColumnIndex> aggregateColumns = { c0 };
    std::vector<std::unique_ptr<Aggregator>> aggs;
    std::vector<int32_t> channal0 = { 0 };
    aggs.push_back(std::make_unique<AverageAggregator<LongVector>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(DoubleType()), channal0));
    std::vector<DataTypePtr> outputTypes { DoubleType() };
    DataTypes aggOutputTypes(outputTypes);

    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRaw1Wrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto outputPartial1Wrap = AggregatorUtil::WrapWithVector(false, aggs.size());
    auto aggregate = new AggregationOperator(std::move(aggs), aggOutputTypesWrap, inputRaw1Wrap, outputPartial1Wrap);

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggregate->AddInput(input[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t tableCount = aggregate->GetOutput(result);

    EXPECT_EQ(tableCount, 1);
    EXPECT_EQ(result[0]->GetVectorCount(), 1);
    EXPECT_EQ(result[0]->GetRowCount(), 1);

    delete[] input;
    VectorHelper::FreeVecBatches(result);
    Operator::DeleteOperator(aggregate);
}

TEST(AggregationOperatorTest, min_max_varchar_correctness)
{
    std::string data0[] = {"operators", "operator", "operators", "helloha", "hello", "helloha"};
    std::string data1[] = {"hello", "helloha", "hello", "operator", "operators", "operator"};
    std::vector<DataTypePtr> types = std::vector<DataTypePtr> { VarcharType(10), VarcharType(10) };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, 6, data0, data1);

    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal1 = { 1 };

    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<MinVarcharAggregator>(AggregatorUtil::WrapWithDataTypes(VarcharType(10)),
        AggregatorUtil::WrapWithDataTypes(VarcharType(10)), channal0));
    aggs.push_back(std::make_unique<MaxVarcharAggregator>(AggregatorUtil::WrapWithDataTypes(VarcharType(10)),
        AggregatorUtil::WrapWithDataTypes(VarcharType(10)), channal1));

    std::vector<DataTypePtr> partialOutputTypes { VarcharType(10), VarcharType(10) };
    DataTypes aggPartialOutputTypes(partialOutputTypes);

    auto aggPartialOutputTypesWrap = AggregatorUtil::WrapWithVector(aggPartialOutputTypes);
    auto inputRaw1Wrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto outputPartial1Wrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto aggOperator =
        new AggregationOperator(std::move(aggs), aggPartialOutputTypesWrap, inputRaw1Wrap, outputPartial1Wrap);
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
    std::vector<DataTypePtr> sourceFieldTypes { LongType(), LongType(), LongType(), LongType() };
    DataTypes sourceTypes(sourceFieldTypes);
    std::vector<DataTypePtr> outputTypes { LongType(), LongType(), LongType(), LongType() };
    DataTypes aggOutputTypes(outputTypes);
    FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_SUM};
    uint32_t aggInputCols[] = {0, 1, 2, 3};
    std::vector<uint32_t> aggFuncTypesVector =
        std::vector<uint32_t>(reinterpret_cast<uint32_t *>(aggFunType), reinterpret_cast<uint32_t *>(aggFunType) + 4);
    std::vector<uint32_t> aggInputColsVector = std::vector<uint32_t>(aggInputCols, aggInputCols + 4);
    uint32_t maskCols[4] = { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1),
                            static_cast<uint32_t>(-1) };
    std::vector<uint32_t> maskColsVector = std::vector<uint32_t>(maskCols, maskCols + 4);

    auto aggInputColsVectorWrap = AggregatorUtil::WrapWithVector(aggInputColsVector);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypesVector.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypesVector.size());
    auto nativeOperatorFactory = new AggregationOperatorFactory(sourceTypes, aggFuncTypesVector, aggInputColsVectorWrap,
        maskColsVector, aggOutputTypesWrap, inputRawWrap, outputPartialWrap);
    nativeOperatorFactory->Init();
    int64_t factoryAddr = reinterpret_cast<int64_t>(nativeOperatorFactory);
    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;

    VectorBatch **input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 0, COLUMN_NUM,
        std::vector<DataTypePtr>(), sourceTypes.Get());
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

    std::vector<DataTypePtr> groupTypes;
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType(), LongType(), LongType() };
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
    std::vector<DataTypePtr> groupInputFieldTypes { LongType(), LongType() };
    DataTypes groupInputTypes(groupInputFieldTypes);
    uint32_t aggCols[] = {2, 3};
    std::vector<DataTypePtr> aggInputFieldTypes { LongType(), LongType() };
    DataTypes aggInputTypes(aggInputFieldTypes);
    std::vector<DataTypePtr> aggOutputFieldTypes { LongType(), LongType() };
    DataTypes aggOutputTypes(aggOutputFieldTypes);
    FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    uint32_t maskCols[] = {static_cast<uint32_t>(-1), static_cast<uint32_t>(-1)};
    std::vector<uint32_t> groupByColVector = std::vector<uint32_t>(groupCols, groupCols + 2);
    std::vector<uint32_t> aggColVector = std::vector<uint32_t>(aggCols, aggCols + 2);
    std::vector<uint32_t> aggFuncTypeVector =
        std::vector<uint32_t>(reinterpret_cast<uint32_t *>(aggFunType), reinterpret_cast<uint32_t *>(aggFunType) + 2);
    std::vector<uint32_t> maskColsVector =
        std::vector<uint32_t>(reinterpret_cast<uint32_t *>(maskCols), reinterpret_cast<uint32_t *>(maskCols) + 2);

    // ------------------------------------------Create operator--------------------------------------------
    std::cout << "after JIT" << std::endl;

    auto aggColVectorWrap = AggregatorUtil::WrapWithVector(aggColVector);
    auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(aggInputTypes);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawsWrap = AggregatorUtil::WrapWithVector(true, aggFuncTypeVector.size());
    auto outputPartialsWrap = AggregatorUtil::WrapWithVector(false, aggFuncTypeVector.size());
    omniruntime::op::HashAggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::HashAggregationOperatorFactory(groupByColVector, groupInputTypes, aggColVectorWrap,
        aggInputTypesWrap, aggOutputTypesWrap, aggFuncTypeVector, maskColsVector, inputRawsWrap, outputPartialsWrap);
    nativeOperatorFactory->Init();
    std::cout << "after create factory" << std::endl;
    // create operator
    auto jitGroupBy = CreateTestOperator(nativeOperatorFactory);

    // ------------------------------------------Process Input--------------------------------------------
    VectorBatch **input1 =
        buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupInputTypes.Get(), aggInputTypes.Get());
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

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
    std::vector<VectorBatch *> jittedResult;
    jitGroupBy->GetOutput(jittedResult);

    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse();
    double cpuElapsed = timer.GetCpuElapse();
    std::cout << "HashAgg with OmniJit, wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;

    auto aggColVector2Wrap = AggregatorUtil::WrapWithVector(aggColVector);
    auto aggInputTypes2Wrap = AggregatorUtil::WrapWithVector(aggInputTypes);
    auto aggOutputTypes2Wrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRaw2Wrap = AggregatorUtil::WrapWithVector(true, aggFuncTypeVector.size());
    auto outputPartial2Wrap = AggregatorUtil::WrapWithVector(false, aggFuncTypeVector.size());
    HashAggregationOperatorFactory *nativeOperatorFactory2 =
        new HashAggregationOperatorFactory(groupByColVector, groupInputTypes, aggColVector2Wrap, aggInputTypes2Wrap,
        aggOutputTypes2Wrap, aggFuncTypeVector, maskColsVector, inputRaw2Wrap, outputPartial2Wrap);
    nativeOperatorFactory2->Init();
    std::cout << "after create factory" << std::endl;
    auto groupBy = nativeOperatorFactory2->CreateOperator();

    VectorBatch **input2 =
        buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupInputTypes.Get(), aggInputTypes.Get());
    if (input2 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    timer.Reset();

    perfUtil->Reset();
    perfUtil->Start();
    for (int pageIndex = 0; pageIndex < VEC_BATCH_NUM; ++pageIndex) {
        auto errNo = groupBy->AddInput(input2[pageIndex]);
        EXPECT_EQ(errNo, OMNI_STATUS_NORMAL);
    }

    perfUtil->Stop();
    instCount = perfUtil->GetData();
    if (instCount != -1) {
        printf("HashAgg without OmniJit, used %lld instructions\n", perfUtil->GetData());
    }

    std::vector<VectorBatch *> result;
    groupBy->GetOutput(result);

    timer.CalculateElapse();
    wallElapsed = timer.GetWallElapse();
    cpuElapsed = timer.GetCpuElapse();
    std::cout << "HashAgg without OmniJit, wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;

    delete perfUtil;
    delete[] input1;
    delete[] input2;
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
    std::vector<DataTypePtr> groupTypes = { LongType(), LongType() };
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType(), Decimal64Type(7, 2), Decimal64Type(7, 2) };
    VectorBatch **input1 = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 4, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    HAFactoryParameters parameters1 = {
        true,
        true,
        { 0, 1 },
        { LongType(), LongType() },
        { 2, 3, 4, 5 },
        { LongType(), LongType(), SHORT_DECIMAL_TYPE, SHORT_DECIMAL_TYPE },
        { LongType(), ContainerType(), SUM_IMMEDIATE_VARBINARY, AVG_IMMEDIATE_VARBINARY },
        { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG },
        { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1) },
    };

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
        { LongType(), LongType() },
        { 2, 3, 4, 5 },
        { LongType(), LongType(), SHORT_DECIMAL_TYPE, SHORT_DECIMAL_TYPE },
        { LongType(), ContainerType(), SUM_IMMEDIATE_VARBINARY, AVG_IMMEDIATE_VARBINARY },
        { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG },
        { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1),
        static_cast<uint32_t>(-1) } };

    uintptr_t partialFactoryAddr2 = CreateHashFactoryWithoutJit(parameters2);
    auto partialFactory2 = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(partialFactoryAddr2);
    auto partialOperator2 = partialFactory2->CreateOperator();
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        partialOperator2->AddInput(input2[i]);
    }
    std::vector<VectorBatch *> resultFromPartial2;
    partialOperator2->GetOutput(resultFromPartial2);

    HAFactoryParameters parameters3 = {
        false,
        false,
        { 0, 1 },
        { LongType(), LongType() },
        { 2, 3, 4, 5 },
        { LongType(), ContainerType(), SUM_IMMEDIATE_VARBINARY, AVG_IMMEDIATE_VARBINARY },
        { LongType(), DoubleType(), LONG_DECIMAL_TYPE, SHORT_DECIMAL_TYPE },
        { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG },
        { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1) },
    };

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
    std::vector<DataTypePtr> expectFieldTypes { LongType(),   LongType(),        LongType(),
        DoubleType(), LONG_DECIMAL_TYPE, LongType() };
    DataTypes expectTypes(expectFieldTypes);
    int64_t expectData1[CARDINALITY] = {0, 1, 2, 3};
    int64_t expectData2[CARDINALITY] = {0, 1, 2, 3};
    int64_t expectData3[CARDINALITY] = {1000000, 1000000, 1000000, 1000000};
    double expectData4[CARDINALITY] = {1.0, 1.0, 1.0, 1.0};
    Decimal128 expectedDecimal(1000000L);
    Decimal128 expectData5[CARDINALITY] = {expectedDecimal, expectedDecimal, expectedDecimal, expectedDecimal};
    int64_t expectData6[CARDINALITY] = {1, 1, 1, 1};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, CARDINALITY, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);
    EXPECT_TRUE(VecBatchMatch(resultFromFinal[0], expectVecBatch));

    delete[] input1;
    delete[] input2;
    VectorHelper::FreeVecBatches(resultFromFinal);
    VectorHelper::FreeVecBatch(expectVecBatch);
}

TEST(HashAggregationOperatorTest, supported_type_test)
{
    std::vector<DataTypePtr> groupTypes = { LongType(), LongType() };
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType() };
    VectorBatch **input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    ColumnIndex c0 = { 0, LongType(), LongType() };
    ColumnIndex c1 = { 1, LongType(), LongType() };
    std::vector<int32_t> aggInputCols = { 2, 3 };
    std::vector<DataTypePtr> aggInputFieldTypes { LongType(), LongType() };
    DataTypes aggInputTypes(aggInputFieldTypes);
    std::vector<DataTypePtr> aggOutputFieldTypes { LongType(), LongType() };
    DataTypes aggOutputTypes(aggOutputFieldTypes);
    std::vector<ColumnIndex> groupByColumns = { c0, c1 };
    std::vector<int32_t> channal1 = { 1 };
    std::vector<int32_t> channal2 = { 2 };
    std::vector<int32_t> channal3 = { 3 };

    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal3, INPUT_MODE, OUTPUT_MODE));

    auto aggInputColsWrap = AggregatorUtil::WrapWithVector(aggInputCols);
    auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(aggInputTypes);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(aggOutputTypes);
    auto inputRawWrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(false, aggs.size());
    HashAggregationOperator *groupBy = new HashAggregationOperator(groupByColumns, aggInputColsWrap, 2,
        aggInputTypesWrap, aggOutputTypesWrap, std::move(aggs), inputRawWrap, outputPartialWrap);
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

    groupTypes[0] = Date32Type();
    groupTypes[1] = Date32Type();
    aggTypes[0] = Date32Type();
    aggTypes[1] = Date32Type();
    input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);
    c0 = { 0, Date32DataType::Instance(), Date32DataType::Instance() };
    c1 = { 1, Date32DataType::Instance(), Date32DataType::Instance() };
    aggInputCols = { 2, 3 };
    std::vector<DataTypePtr> inputTypes1 { Date32DataType::Instance(), Date32DataType::Instance() };
    DataTypes aggInputTypes1(inputTypes1);
    std::vector<DataTypePtr> outputTypes1 { Date32DataType::Instance(), Date32DataType::Instance() };
    DataTypes aggOutputTypes1(outputTypes1);

    groupByColumns = { c0, c1 };
    aggs.push_back(std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(
        AggregatorUtil::WrapWithDataTypes(Date32DataType::Instance()),
        AggregatorUtil::WrapWithDataTypes(Date32DataType::Instance()), channal2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(
        AggregatorUtil::WrapWithDataTypes(Date32DataType::Instance()),
        AggregatorUtil::WrapWithDataTypes(Date32DataType::Instance()), channal3, INPUT_MODE, OUTPUT_MODE));

    auto aggInputCols2Wrap = AggregatorUtil::WrapWithVector(aggInputCols);
    auto aggInputTypes1Wrap = AggregatorUtil::WrapWithVector(aggInputTypes1);
    auto aggOutputTypes1Wrap = AggregatorUtil::WrapWithVector(aggOutputTypes1);
    auto inputRaw2Wrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto outputPartial2Wrap = AggregatorUtil::WrapWithVector(false, aggs.size());
    groupBy = new HashAggregationOperator(groupByColumns, aggInputCols2Wrap, 2, aggInputTypes1Wrap, aggOutputTypes1Wrap,
        std::move(aggs), inputRaw2Wrap, outputPartial2Wrap);
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
    std::vector<DataTypePtr> sourceFieldTypes { IntType(), LongType() };
    DataTypes sourceTypes(sourceFieldTypes);
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vectorBatch = new VectorBatch(2, dataSize);
    for (int32_t i = 0; i < 2; i++) {
        DataTypePtr dataType = sourceTypes.GetType(i);
        vectorBatch->SetVector(i, CreateDictionaryVector(*dataType, dataSize, ids, dataSize, datas[i]));
    }

    groupTypes[0] = IntType();
    aggTypes[0] = LongType();
    c0 = { 0, IntType(), IntType() };
    aggInputCols = { 1 };
    std::vector<DataTypePtr> inputTypes2 { LongType() };
    DataTypes aggInputTypes2(inputTypes2);
    std::vector<DataTypePtr> outputTypes2 { LongType() };
    DataTypes aggOutputTypes2(outputTypes2);
    groupByColumns = { c0 };
    aggs.push_back(
        std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal1, INPUT_MODE, OUTPUT_MODE));

    auto aggInputCols3Wrap = AggregatorUtil::WrapWithVector(aggInputCols);
    auto aggInputTypes2Wrap = AggregatorUtil::WrapWithVector(aggInputTypes2);
    auto aggOutputTypes2Wrap = AggregatorUtil::WrapWithVector(aggOutputTypes2);
    auto inputRaw3Wrap = AggregatorUtil::WrapWithVector(true, aggs.size());
    auto outputPartial3Wrap = AggregatorUtil::WrapWithVector(false, aggs.size());
    groupBy = new HashAggregationOperator(groupByColumns, aggInputCols3Wrap, 1, aggInputTypes2Wrap, aggOutputTypes2Wrap,
        std::move(aggs), inputRaw3Wrap, outputPartial3Wrap);

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
    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal3 = { 3 };
    // sum test types : long + decimal + dictionary + null
    auto sumLong = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true, false);
    auto sumShortDecimal = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(SHORT_DECIMAL_TYPE),
        AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE), channal0, true, false);
    auto sumNull = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal3, true, false);

    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_sum_test");
    auto longInputVec = BuildAggregateInput(vectorAllocator, LongType(), rowPerVecBatch);
    auto decimalInputVec = BuildAggregateInput(vectorAllocator, Decimal128Type(38, 2), rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(vectorAllocator, NoneType(), rowPerVecBatch);

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
    DecimalOperations::DecodeSumDecimal(static_cast<DecimalSumState *>(state.val), actual, overflow);
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
    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal2 = { 2 };
    // count test types : long + dictionary + null
    auto countLong = countFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true, false);
    auto countNull = countFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal2, true, false);

    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_count_column_test");
    auto longInputVec = BuildAggregateInput(vectorAllocator, LongType(), rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(vectorAllocator, NoneType(), rowPerVecBatch);

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
    std::vector<int32_t> channal0 = { -1 };

    auto countLong = countAllFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(NoneType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true, false);
    auto countNull = countAllFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(NoneType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true, false);

    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_count_all_test");
    auto longInputVec = BuildAggregateInput(vectorAllocator, LongType(), rowPerVecBatch);
    LongDataType longDataType;
    auto nullInputVec = BuildAggregateInput(vectorAllocator, NoneType(), ROW_PER_VEC_BATCH);

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
    std::vector<int32_t> countNullChannels;
    countNullChannels.push_back(-1);
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
    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal2 = { 2 };
    std::vector<int32_t> channal3 = { 3 };
    std::vector<int32_t> channal4 = { 4 };
    std::vector<int32_t> channal5 = { 5 };
    std::vector<int32_t> channal6 = { 6 };
    // min test types : long + decimal + varchar + dictionary + null
    auto minLong = minFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true, false);
    auto minDecimal = minFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE),
        AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE), channal3, true, false);
    auto minVarchar = minFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(VarcharType()),
        AggregatorUtil::WrapWithDataTypes(VarcharType()), channal4, true, false);
    auto minNull = minFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal2, true, false);
    auto minIntLong = minFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(IntType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal5, true, false);
    auto minLongInt = minFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(IntType()), channal0, true, false);
    auto minBoolean = minFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(BooleanType()),
        AggregatorUtil::WrapWithDataTypes(BooleanType()), channal6, true, false);

    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation_minTest");
    auto longInputVec = BuildAggregateInput(vectorAllocator, LongType(), rowPerVecBatch);
    auto decimalInputVec = BuildAggregateInput(vectorAllocator, Decimal128Type(38, 0), rowPerVecBatch);
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
    auto nullInputVec = BuildAggregateInput(vectorAllocator, NoneType(), rowPerVecBatch);
    auto intInputVec = BuildAggregateInput(vectorAllocator, IntType(), rowPerVecBatch);
    auto BoolInputVec = BuildAggregateInput(vectorAllocator, BooleanType(), rowPerVecBatch);

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
    std::vector<Vector *> minVarcharOutputVector;
    minVarcharOutputVector.push_back(&minVarcharOutput);
    minVarchar->ExtractValues(state, minVarcharOutputVector, 0);
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
    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal1 = { 1 };
    std::vector<int32_t> channal2 = { 2 };
    std::vector<int32_t> channal4 = { 4 };
    std::vector<int32_t> channal5 = { 5 };
    std::vector<int32_t> channal6 = { 6 };
    // max test types : long + decimal + varchar + dictionary + null
    auto maxLong = maxFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true, false);
    auto maxDecimal = maxFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE),
        AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE), channal1, true, false);
    auto maxVarchar = maxFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(VarcharType()),
        AggregatorUtil::WrapWithDataTypes(VarcharType()), channal2, true, false);
    auto maxNull = maxFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal4, true, false);
    auto maxIntLong = maxFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(IntType()),
        AggregatorUtil::WrapWithDataTypes(LongType()), channal5, true, false);
    auto maxLongInt = maxFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(IntType()), channal0, true, false);
    auto maxBoolean = maxFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(BooleanType()),
        AggregatorUtil::WrapWithDataTypes(BooleanType()), channal6, true, false);

    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_count_max_test1");
    auto longInputVec = BuildAggregateInput(vectorAllocator, LongType(), rowPerVecBatch);
    auto decimalInputVec = BuildAggregateInput(vectorAllocator, Decimal128Type(38, 0), rowPerVecBatch);
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
    auto nullInputVec = BuildAggregateInput(vectorAllocator, NoneType(), rowPerVecBatch);
    auto intInputVec = BuildAggregateInput(vectorAllocator, IntType(), rowPerVecBatch);
    auto boolInputVec = BuildAggregateInput(vectorAllocator, BooleanType(), rowPerVecBatch);

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
    std::vector<Vector *> maxVarcharOutputVector;
    maxVarcharOutputVector.push_back(&maxVarcharOutput);
    maxVarchar->ExtractValues(state, maxVarcharOutputVector, 0);
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
    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal1 = { 1 };
    std::vector<int32_t> channal3 = { 3 };
    // avg test types : long + decimal + dictionary + null
    auto avgLong = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(DoubleType()), channal0, true, false);
    auto avgDecimal = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE),
        AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE), channal1, true, false);
    auto avgNull = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
        AggregatorUtil::WrapWithDataTypes(DoubleType()), channal3, true, false);

    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_avg_test");
    auto longInputVec = BuildAggregateInput(vectorAllocator, LongType(), rowPerVecBatch);
    auto decimalInputVec = BuildAggregateInput(vectorAllocator, Decimal128Type(38, 0), rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(vectorAllocator, NoneType(), rowPerVecBatch);

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
    std::vector<Vector *> avgLongOutputVector;
    avgLongOutputVector.push_back(&avgLongOutput);
    avgLong->ExtractValues(state, avgLongOutputVector, 0);
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
    std::vector<Vector *> avgNullOutputVector;
    avgNullOutputVector.push_back(&avgNullOutput);
    avgNull->ExtractValues(state, avgNullOutputVector, 0);
    EXPECT_TRUE(avgNullOutput.IsValueNull(0));
    state.val = nullptr;

    VectorHelper::FreeVecBatch(vectorBatch);
    delete avgFactory;
}

TEST(AggregatorTest, spark_sum_decimal64_normal)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumDeciAggPartial = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal64Type(18, 6)),
                                                          AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)), channal0, true,
                                                          true);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_decimal64_normal");
    auto *deci18_6Vec = new LongVector(vectorAllocator, 3);
    deci18_6Vec->SetValue(0, 999999999999999999L);
    deci18_6Vec->SetValue(1, 999999999999999999L);
    deci18_6Vec->SetValue(2, 999999999999999999L);

    auto *isOverflowVec = new BooleanVector(vectorAllocator, 3);
    isOverflowVec->SetValue(0, false);
    isOverflowVec->SetValue(1, false);
    isOverflowVec->SetValue(2, false);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector*> extractVec = {resultVec, isOverflowVec};

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci18_6Vec);
    vecBatch->SetVector(1, isOverflowVec);

    AggregateState state { nullptr };
    sumDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    sumDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1 = 0;
    int32_t precision1 = 0;
    int32_t scale1 = 0;
    DecimalOperations::StringToDecimal128("999999999999.999999", expected1, precision1, scale1);

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));
    EXPECT_FALSE(isOverflowVec->GetValue(0));

    sumDeciAggPartial->ProcessGroup(state, vecBatch, 1);
    sumDeciAggPartial->ProcessGroup(state, vecBatch, 2);
    auto sumDeciAggFinal = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)),
                                                        AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)), channal0, false, false);

    sumDeciAggFinal->ExtractValues(state, extractVec, 0);

    Decimal128 expected2 = 0;
    int32_t precision2 = 0;
    int32_t scale2 = 0;
    DecimalOperations::StringToDecimal128("2999999999999.999997", expected2, precision2, scale2);
    EXPECT_EQ(expected2.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected2, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete sumFactory;
}

TEST(AggregatorTest, spark_sum_decimal128_normal)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumDeciAggPartial = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(25, 8)),
                                                          AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)), channal0, true,
                                                          true);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_decimal128_normal");
    Decimal128 deci = 0;
    int32_t inputPrec = 0;
    int32_t inputScale = 0;
    DecimalOperations::StringToDecimal128("99999999999999999.99999999", deci, inputPrec, inputScale);
    auto *deci25_8Vec = new Decimal128Vector (vectorAllocator, 3);
    deci25_8Vec->SetValue(0, deci);
    deci25_8Vec->SetValue(1, deci);
    deci25_8Vec->SetValue(2, deci);

    auto *isOverflowVec = new BooleanVector(vectorAllocator, 3);
    isOverflowVec->SetValue(0, false);
    isOverflowVec->SetValue(1, false);
    isOverflowVec->SetValue(2, false);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector*> extractVec = {resultVec, isOverflowVec};

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci25_8Vec);
    vecBatch->SetVector(1, isOverflowVec);

    AggregateState state { nullptr };
    sumDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    sumDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1 = 0;
    int32_t precision1 = 0;
    int32_t scale1 = 0;
    DecimalOperations::StringToDecimal128("99999999999999999.99999999", expected1, precision1, scale1);

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));
    EXPECT_FALSE(isOverflowVec->GetValue(0));

    auto sumDeciAggFinal = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)),
                                                        AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)), channal0, false, false);

    sumDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    sumDeciAggFinal->ProcessGroup(state, vecBatch, 2);
    sumDeciAggFinal->ExtractValues(state, extractVec, 0);

    Decimal128 expected2 = 0;
    int32_t precision2 = 0;
    int32_t scale2 = 0;
    DecimalOperations::StringToDecimal128("299999999999999999.99999997", expected2, precision2, scale2);
    EXPECT_EQ(expected2.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected2, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete sumFactory;
}

TEST(AggregatorTest, spark_sum_decimal128_overflow_throw_exception_when_isOverflowAsNull_is_false)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumDeciAggPartial = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)),
                                                          AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)), channal0, true,
                                                          true, false);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_decimal128_overflow_throw_exception_when_isOverflowAsNull_is_false");
    Decimal128 deci = 0;
    int32_t inputPrec = 0;
    int32_t inputScale = 0;
    DecimalOperations::StringToDecimal128("99999999999999999999999999999999999999", deci, inputPrec, inputScale);
    auto *deci38_0Vec = new Decimal128Vector (vectorAllocator, 3);
    deci38_0Vec->SetValue(0, deci);
    deci38_0Vec->SetValue(1, deci);
    deci38_0Vec->SetValue(2, deci);

    auto *isOverflowVec = new BooleanVector(vectorAllocator, 3);
    isOverflowVec->SetValue(0, false);
    isOverflowVec->SetValue(1, false);
    isOverflowVec->SetValue(2, false);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector*> extractVec = {resultVec, isOverflowVec};

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci38_0Vec);
    vecBatch->SetVector(1, isOverflowVec);

    AggregateState state { nullptr };
    sumDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    sumDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1 = 0;
    int32_t precision1 = 0;
    int32_t scale1 = 0;
    DecimalOperations::StringToDecimal128("99999999999999999999999999999999999999", expected1, precision1, scale1);

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));
    EXPECT_FALSE(isOverflowVec->GetValue(0));

    auto sumDeciAggFinal = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)),
                                                        AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)),
                                                        channal0, false, false, false);

    sumDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    sumDeciAggFinal->ProcessGroup(state, vecBatch, 2);

    bool isThrowException = false;
    try {
        sumDeciAggFinal->ExtractValues(state, extractVec, 0);
    } catch (OmniException& e) {
        isThrowException = true;
    }
    EXPECT_TRUE(isThrowException);

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete sumFactory;
}

TEST(AggregatorTest, spark_sum_decimal128_overflow_return_null_when_isOverflowAsNull_is_true)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumDeciAggPartial = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)),
                                                          AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)), channal0, true,
                                                          true, true);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_decimal128_overflow_return_null_when_isOverflowAsNull_is_true");
    Decimal128 deci = 0;
    int32_t inputPrec = 0;
    int32_t inputScale = 0;
    DecimalOperations::StringToDecimal128("99999999999999999999999999999999999999", deci, inputPrec, inputScale);
    auto *deci38_0Vec = new Decimal128Vector (vectorAllocator, 3);
    deci38_0Vec->SetValue(0, deci);
    deci38_0Vec->SetValue(1, deci);
    deci38_0Vec->SetValue(2, deci);

    auto *isOverflowVec = new BooleanVector(vectorAllocator, 3);
    isOverflowVec->SetValue(0, false);
    isOverflowVec->SetValue(1, false);
    isOverflowVec->SetValue(2, false);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector*> extractVec = {resultVec, isOverflowVec};

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci38_0Vec);
    vecBatch->SetVector(1, isOverflowVec);

    AggregateState state { nullptr };
    sumDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    sumDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1 = 0;
    int32_t precision1 = 0;
    int32_t scale1 = 0;
    DecimalOperations::StringToDecimal128("99999999999999999999999999999999999999", expected1, precision1, scale1);

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));
    EXPECT_FALSE(isOverflowVec->GetValue(0));

    auto sumDeciAggFinal = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)),
                                                        AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)),
                                                        channal0, false, false, true);

    sumDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    sumDeciAggFinal->ProcessGroup(state, vecBatch, 2);
    sumDeciAggFinal->ExtractValues(state, extractVec, 0);

    EXPECT_TRUE(resultVec->IsValueNull(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete sumFactory;
}

TEST(AggregatorTest, spark_avg_decimal64_normal)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgDeciAggPartial = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal64Type(18, 6)),
                                                          AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)), channal0, true,
                                                          true);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_decimal64_normal");
    auto *deci18_6Vec = new LongVector(vectorAllocator, 3);
    deci18_6Vec->SetValue(0, 999999999999999999L);
    deci18_6Vec->SetValue(1, 999999999999999999L);
    deci18_6Vec->SetValue(2, 999999999999999999L);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector*> extractVec = {resultVec, avgCountVec};

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci18_6Vec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1 = 0;
    int32_t precision1 = 0;
    int32_t scale1 = 0;
    DecimalOperations::StringToDecimal128("999999999999.999999", expected1, precision1, scale1);

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));

    avgDeciAggPartial->ProcessGroup(state, vecBatch, 1);
    avgDeciAggPartial->ProcessGroup(state, vecBatch, 2);
    auto avgDeciAggFinal = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)),
                                                        AggregatorUtil::WrapWithDataTypes(Decimal128Type(22, 10)), channal0, false, false);

    EXPECT_EQ(3, static_cast<DecimalAverageState *>(state.val)->count);

    avgDeciAggFinal->ExtractValues(state, extractVec, 0);

    Decimal128 expected2 = 0;
    int32_t precision2 = 0;
    int32_t scale2 = 0;
    DecimalOperations::StringToDecimal128("999999999999.9999990000", expected2, precision2, scale2);
    EXPECT_EQ(expected2.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected2, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_avg_decimal128_normal)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgDeciAggPartial = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(25, 8)),
                                                          AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)), channal0, true,
                                                          true);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_decimal128_normal");
    Decimal128 deci = 0;
    int32_t inputPrec = 0;
    int32_t inputScale = 0;
    DecimalOperations::StringToDecimal128("99999999999999999.99999999", deci, inputPrec, inputScale);
    auto *deci25_8Vec = new Decimal128Vector (vectorAllocator, 3);
    deci25_8Vec->SetValue(0, deci);
    deci25_8Vec->SetValue(1, deci);
    deci25_8Vec->SetValue(2, deci);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector*> extractVec = {resultVec, avgCountVec};

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci25_8Vec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1 = 0;
    int32_t precision1 = 0;
    int32_t scale1 = 0;
    DecimalOperations::StringToDecimal128("99999999999999999.99999999", expected1, precision1, scale1);

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));

    auto avgDeciAggFinal = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)),
                                                        AggregatorUtil::WrapWithDataTypes(Decimal128Type(29, 12)), channal0, false, false);

    avgDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    avgDeciAggFinal->ProcessGroup(state, vecBatch, 2);
    avgDeciAggFinal->ExtractValues(state, extractVec, 0);

    Decimal128 expected2 = 0;
    int32_t precision2 = 0;
    int32_t scale2 = 0;
    DecimalOperations::StringToDecimal128("99999999999999999.999999990000", expected2, precision2, scale2);
    EXPECT_EQ(expected2.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected2, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_avg_decimal128_overflow_throw_exception_when_isOverflowAsNull_is_false)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgDeciAggPartial = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)),
                                                          AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)), channal0, true,
                                                          true, false);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_decimal128_overflow_throw_exception_when_isOverflowAsNull_is_false");
    Decimal128 deci = 0;
    int32_t inputPrec = 0;
    int32_t inputScale = 0;
    DecimalOperations::StringToDecimal128("99999999999999999999999999999999999999", deci, inputPrec, inputScale);
    auto *deci38_0Vec = new Decimal128Vector (vectorAllocator, 3);
    deci38_0Vec->SetValue(0, deci);
    deci38_0Vec->SetValue(1, deci);
    deci38_0Vec->SetValue(2, deci);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector*> extractVec = {resultVec, avgCountVec};

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci38_0Vec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1 = 0;
    int32_t precision1 = 0;
    int32_t scale1 = 0;
    DecimalOperations::StringToDecimal128("99999999999999999999999999999999999999", expected1, precision1, scale1);

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));

    auto avgDeciAggFinal = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)),
                                                        AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 4)), channal0, false,
                                                        false, false);

    avgDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    avgDeciAggFinal->ProcessGroup(state, vecBatch, 2);

    bool isThrowException = false;
    try {
        avgDeciAggFinal->ExtractValues(state, extractVec, 0);
    } catch (OmniException& e) {
        isThrowException = true;
    }
    EXPECT_TRUE(isThrowException);

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_avg_decimal128_overflow_return_null_when_isOverflowAsNull_is_true)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgDeciAggPartial = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)),
                                                          AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)), channal0, true,
                                                          true, true);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_decimal128_overflow_return_null_when_isOverflowAsNull_is_true");
    Decimal128 deci = 0;
    int32_t inputPrec = 0;
    int32_t inputScale = 0;
    DecimalOperations::StringToDecimal128("99999999999999999999999999999999999999", deci, inputPrec, inputScale);
    auto *deci38_0Vec = new Decimal128Vector (vectorAllocator, 3);
    deci38_0Vec->SetValue(0, deci);
    deci38_0Vec->SetValue(1, deci);
    deci38_0Vec->SetValue(2, deci);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector*> extractVec = {resultVec, avgCountVec};

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci38_0Vec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1 = 0;
    int32_t precision1 = 0;
    int32_t scale1 = 0;
    DecimalOperations::StringToDecimal128("99999999999999999999999999999999999999", expected1, precision1, scale1);

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));

    auto avgDeciAggFinal = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)),
                                                        AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 4)), channal0, false,
                                                        false, true);

    avgDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    avgDeciAggFinal->ProcessGroup(state, vecBatch, 2);
    avgDeciAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_TRUE(resultVec->IsValueNull(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_sum_short_normal)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumShortAggPartial = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(ShortType()),
                                                           AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true,
                                                           true);
    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_short_normal");

    auto* shortVec = new ShortVector(vectorAllocator, 3);
    shortVec->SetValue(0, 12345);
    shortVec->SetValue(1, 23451);
    shortVec->SetValue(2, 12345);

    auto *resultVec = new LongVector(vectorAllocator, 1);
    std::vector<Vector*> extractVec = {resultVec};

    auto *vecBatch = new VectorBatch(1);
    vecBatch->SetVector(0, shortVec);

    AggregateState state { nullptr };
    sumShortAggPartial->InitiateGroup(state, vecBatch, 0);
    sumShortAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(12345, resultVec->GetValue(0));

    sumShortAggPartial->ProcessGroup(state, vecBatch, 1);
    sumShortAggPartial->ProcessGroup(state, vecBatch, 2);

    auto sumShortAggFinal = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
                                                         AggregatorUtil::WrapWithDataTypes(LongType()), channal0, false,
                                                         false);
    sumShortAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(48141, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete sumFactory;
}

TEST(AggregatorTest, spark_sum_int_normal)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumIntAggPartial = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(IntType()),
                                                         AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true,
                                                         true);
    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_int_normal");

    auto* intVec = new IntVector(vectorAllocator, 3);
    intVec->SetValue(0, 1234567890);
    intVec->SetValue(1, 2045678901);
    intVec->SetValue(2, 1234567890);

    auto *resultVec = new LongVector(vectorAllocator, 1);
    std::vector<Vector*> extractVec = {resultVec};

    auto *vecBatch = new VectorBatch(1);
    vecBatch->SetVector(0, intVec);

    AggregateState state { nullptr };
    sumIntAggPartial->InitiateGroup(state, vecBatch, 0);
    sumIntAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(1234567890, resultVec->GetValue(0));

    sumIntAggPartial->ProcessGroup(state, vecBatch, 1);
    sumIntAggPartial->ProcessGroup(state, vecBatch, 2);

    auto sumIntAggFinal = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
                                                       AggregatorUtil::WrapWithDataTypes(LongType()), channal0, false,
                                                       false);
    sumIntAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(4514814681, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete sumFactory;
}

TEST(AggregatorTest, spark_sum_long_normal)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumLongAggPartial = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
                                                          AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true,
                                                          true);
    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_long_normal");

    auto* longVec = new LongVector(vectorAllocator, 3);
    longVec->SetValue(0, 1234567890123456789);
    longVec->SetValue(1, 2345678901234567891);
    longVec->SetValue(2, 3456789012345678912);

    auto *resultVec = new LongVector(vectorAllocator, 1);
    std::vector<Vector*> extractVec = {resultVec};

    auto *vecBatch = new VectorBatch(1);
    vecBatch->SetVector(0, longVec);

    AggregateState state { nullptr };
    sumLongAggPartial->InitiateGroup(state, vecBatch, 0);
    sumLongAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(1234567890123456789, resultVec->GetValue(0));

    sumLongAggPartial->ProcessGroup(state, vecBatch, 1);
    sumLongAggPartial->ProcessGroup(state, vecBatch, 2);

    auto sumLongAggFinal = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
                                                        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, false,
                                                        false);
    sumLongAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(7037035803703703592, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete sumFactory;
}

TEST(AggregatorTest, spark_sum_long_overflow)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumLongAggPartial = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
                                                          AggregatorUtil::WrapWithDataTypes(LongType()), channal0, true,
                                                          true);
    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_long_overflow");

    auto* longVec = new LongVector(vectorAllocator, 3);
    longVec->SetValue(0, 9223372036854774807);
    longVec->SetValue(1, 9223372036854774807);
    longVec->SetValue(2, 9223372036854774807);

    auto *resultVec = new LongVector(vectorAllocator, 1);
    std::vector<Vector*> extractVec = {resultVec};

    auto *vecBatch = new VectorBatch(1);
    vecBatch->SetVector(0, longVec);

    AggregateState state { nullptr };
    sumLongAggPartial->InitiateGroup(state, vecBatch, 0);
    sumLongAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(9223372036854774807, resultVec->GetValue(0));

    sumLongAggPartial->ProcessGroup(state, vecBatch, 1);
    sumLongAggPartial->ProcessGroup(state, vecBatch, 2);

    auto sumLongAggFinal = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
                                                        AggregatorUtil::WrapWithDataTypes(LongType()), channal0, false,
                                                        false);
    sumLongAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(9223372036854772805, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete sumFactory;
}

TEST(AggregatorTest, spark_sum_double_normal)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumDoubleAggPartial = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(DoubleType()),
                                                            AggregatorUtil::WrapWithDataTypes(DoubleType()), channal0, true,
                                                            true);
    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_double_normal");

    auto* doubleVec = new DoubleVector (vectorAllocator, 3);
    doubleVec->SetValue(0, 123456789012.3456789);
    doubleVec->SetValue(1, 234567890123.4567891);
    doubleVec->SetValue(2, 345678901234.5678912);

    auto *resultVec = new DoubleVector(vectorAllocator, 1);
    std::vector<Vector*> extractVec = {resultVec};

    auto *vecBatch = new VectorBatch(1);
    vecBatch->SetVector(0, doubleVec);

    AggregateState state { nullptr };
    sumDoubleAggPartial->InitiateGroup(state, vecBatch, 0);
    sumDoubleAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(123456789012.3456789, resultVec->GetValue(0));

    sumDoubleAggPartial->ProcessGroup(state, vecBatch, 1);
    sumDoubleAggPartial->ProcessGroup(state, vecBatch, 2);

    auto sumDoubleAggFinal = sumFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(DoubleType()),
                                                          AggregatorUtil::WrapWithDataTypes(DoubleType()), channal0, false,
                                                          false);
    sumDoubleAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(7.037035803703704E11, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete sumFactory;
}


TEST(AggregatorTest, spark_avg_short_normal)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgShortAggPartial = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(ShortType()),
                                                           AggregatorUtil::WrapWithDataTypes(DoubleType()), channal0, true,
                                                           true);
    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_short_normal");

    auto* shortVec = new ShortVector(vectorAllocator, 3);
    shortVec->SetValue(0, 12345);
    shortVec->SetValue(1, 23451);
    shortVec->SetValue(2, 12345);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new DoubleVector (vectorAllocator, 1);
    std::vector<Vector*> extractVec = {resultVec, avgCountVec};

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, shortVec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgShortAggPartial->InitiateGroup(state, vecBatch, 0);
    avgShortAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(12345.0, resultVec->GetValue(0));

    avgShortAggPartial->ProcessGroup(state, vecBatch, 1);
    avgShortAggPartial->ProcessGroup(state, vecBatch, 2);

    auto avgShortAggFinal = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(DoubleType()),
                                                         AggregatorUtil::WrapWithDataTypes(DoubleType()), channal0, false,
                                                         false);
    avgShortAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(16047.0, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_avg_int_normal)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgIntAggPartial = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(IntType()),
                                                         AggregatorUtil::WrapWithDataTypes(DoubleType()), channal0, true,
                                                         true);
    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_int_normal");

    auto* intVec = new IntVector(vectorAllocator, 3);
    intVec->SetValue(0, 1234567890);
    intVec->SetValue(1, 2045678901);
    intVec->SetValue(2, 1234567890);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new DoubleVector (vectorAllocator, 1);
    std::vector<Vector*> extractVec = {resultVec, avgCountVec};

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, intVec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgIntAggPartial->InitiateGroup(state, vecBatch, 0);
    avgIntAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(1234567890.0, resultVec->GetValue(0));

    avgIntAggPartial->ProcessGroup(state, vecBatch, 1);
    avgIntAggPartial->ProcessGroup(state, vecBatch, 2);

    auto avgIntAggFinal = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(DoubleType()),
                                                       AggregatorUtil::WrapWithDataTypes(DoubleType()), channal0, false,
                                                       false);
    avgIntAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(1.504938227E9, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_avg_long_normal)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgLongAggPartial = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(LongType()),
                                                          AggregatorUtil::WrapWithDataTypes(DoubleType()), channal0, true,
                                                          true);
    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_long_normal");

    auto* longVec = new LongVector(vectorAllocator, 3);
    longVec->SetValue(0, 9223372036854774807L);
    longVec->SetValue(1, 9223372036854774807L);
    longVec->SetValue(2, 9223372036854774807L);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new DoubleVector (vectorAllocator, 1);
    std::vector<Vector*> extractVec = {resultVec, avgCountVec};

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, longVec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgLongAggPartial->InitiateGroup(state, vecBatch, 0);
    avgLongAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(9.2233720368547748E18, resultVec->GetValue(0));

    avgLongAggPartial->ProcessGroup(state, vecBatch, 1);
    avgLongAggPartial->ProcessGroup(state, vecBatch, 2);

    auto avgLongAggFinal = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(DoubleType()),
                                                        AggregatorUtil::WrapWithDataTypes(DoubleType()), channal0, false,
                                                        false);
    avgLongAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(9.2233720368547748E18, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_avg_double_normal)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgDoubleAggPartial = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(DoubleType()),
                                                            AggregatorUtil::WrapWithDataTypes(DoubleType()), channal0, true,
                                                            true);
    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_double_normal");

    auto* doubleVec = new DoubleVector(vectorAllocator, 3);
    doubleVec->SetValue(0, 123456789012.3456789);
    doubleVec->SetValue(1, 234567890123.4567891);
    doubleVec->SetValue(2, 345678901234.5678912);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new DoubleVector (vectorAllocator, 1);
    std::vector<Vector*> extractVec = {resultVec, avgCountVec};

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, doubleVec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgDoubleAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDoubleAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(123456789012.3456789, resultVec->GetValue(0));

    avgDoubleAggPartial->ProcessGroup(state, vecBatch, 1);
    avgDoubleAggPartial->ProcessGroup(state, vecBatch, 2);

    auto avgDoubleAggFinal = avgFactory->CreateAggregator(AggregatorUtil::WrapWithDataTypes(DoubleType()),
                                                          AggregatorUtil::WrapWithDataTypes(DoubleType()), channal0, false,
                                                          false);
    avgDoubleAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(2.345678601234568E11, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

//first basic function:  first with ignore null
TEST(AggregatorTest, first_int_ignorenull_test)
{
    auto firstIgnoreNullFactory = new FirstAggregatorFactory(OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL);
    std::vector<int32_t> channal0 = { 0 };
    auto firstIgnoreNullIntAggPartial = firstIgnoreNullFactory->CreateAggregator(
            AggregatorUtil::WrapWithDataTypes(IntType()),
            AggregatorUtil::WrapWithDataTypes(IntType()),
         channal0, true, true);
    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("first_int_ignorenull_test");

    auto* inputIntVec1 = new IntVector(vectorAllocator, 5);
    inputIntVec1->SetValueNull(0);
    inputIntVec1->SetValueNull(1);
    inputIntVec1->SetValueNull(2);
    inputIntVec1->SetValueNull(3);
    inputIntVec1->SetValueNull(4);

    auto *vecBatch1 = new VectorBatch(1);
    vecBatch1->SetVector(0, inputIntVec1);

    auto *resultfirstVec1 = new IntVector(vectorAllocator, 1);
    auto *resultValueSetVec1 = new BooleanVector(vectorAllocator, 1);
    std::vector<Vector*> extractVecs = {resultfirstVec1, resultValueSetVec1};

    AggregateState state { nullptr };

    //add first VectorBatch
    firstIgnoreNullIntAggPartial->InitiateGroup(state, vecBatch1, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(false, resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 1);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(false, resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 2);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(false, resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 3);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(false, resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 4);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(false, resultValueSetVec1->GetValue(0));

    //add second VectorBatch,keep value not change
    auto* inputIntVec2 = new IntVector(vectorAllocator, 3);
    inputIntVec2->SetValueNull(0);
    inputIntVec2->SetValue(1, 211);
    inputIntVec2->SetValueNull(2);
    auto *vecBatch2 = new VectorBatch(1);
    vecBatch2->SetVector(0, inputIntVec2);

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch2, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(false, resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch2, 1);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(211, resultfirstVec1->GetValue(0));
    EXPECT_EQ(true, resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch2, 2);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(211, resultfirstVec1->GetValue(0));
    EXPECT_EQ(true, resultValueSetVec1->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch1);
    VectorHelper::FreeVecBatch(vecBatch2);
    delete resultfirstVec1;
    delete resultValueSetVec1;
    delete firstIgnoreNullFactory;
}

//first basic function:  first include null
TEST(AggregatorTest, first_int_includenull_test)
{
    auto firstWithNullFactory = new FirstAggregatorFactory(OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL);
    std::vector<int32_t> channal0 = { 0 };
    auto firstWithNullIntAggPartial = firstWithNullFactory->CreateAggregator(
            AggregatorUtil::WrapWithDataTypes(IntType()),
            AggregatorUtil::WrapWithDataTypes(IntType()),
            channal0, true, true);
    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("first_int_includenull_test");

    auto* inputIntVec1 = new IntVector(vectorAllocator, 5);
    inputIntVec1->SetValueNull(0);
    inputIntVec1->SetValueNull(1);
    inputIntVec1->SetValue(2, 111);
    inputIntVec1->SetValue(3, 113);
    inputIntVec1->SetValueNull(4);

    auto *vecBatch1 = new VectorBatch(1);
    vecBatch1->SetVector(0, inputIntVec1);

    auto *resultfirstVec1 = new IntVector(vectorAllocator, 1);
    auto *resultValueSetVec1 = new BooleanVector(vectorAllocator, 1);
    std::vector<Vector*> extractVecs = {resultfirstVec1, resultValueSetVec1};

    AggregateState state { nullptr };
    firstWithNullIntAggPartial->InitiateGroup(state, vecBatch1, 0);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(true, resultValueSetVec1->GetValue(0));

    firstWithNullIntAggPartial->ProcessGroup(state, vecBatch1, 1);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(true, resultValueSetVec1->GetValue(0));

    firstWithNullIntAggPartial->ProcessGroup(state, vecBatch1, 2);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(true, resultValueSetVec1->GetValue(0));

    firstWithNullIntAggPartial->ProcessGroup(state, vecBatch1, 3);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(true, resultValueSetVec1->GetValue(0));

    firstWithNullIntAggPartial->ProcessGroup(state, vecBatch1, 4);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(true, resultValueSetVec1->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch1);
    delete resultfirstVec1;
    delete resultValueSetVec1;
    delete firstWithNullFactory;
}

//first agg function for 2 steps partial  + final
TEST(AggregatorTest, first_int_ignorenull_2steps_test)
{
    auto firstIgnoreNullFactory = new FirstAggregatorFactory(OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL);

    VectorAllocator *vectorAllocator =
            VectorAllocator::GetGlobalAllocator()->NewChildAllocator("first_int_ignorenull_2steps_test");

    std::vector<int32_t> channal0 = { 0 };
    auto firstIgnoreNullIntAggPartial = firstIgnoreNullFactory->CreateAggregator(
            AggregatorUtil::WrapWithDataTypes(LongType()),
            AggregatorUtil::WrapWithDataTypes(LongType()),
            channal0, true, true);

    auto* inputLongVec1 = new LongVector(vectorAllocator, 2);
    inputLongVec1->SetValueNull(0);
    inputLongVec1->SetValueNull(1);

    auto *vecBatch1 = new VectorBatch(1);
    vecBatch1->SetVector(0, inputLongVec1);

    auto *resultfirstVec1 = new LongVector(vectorAllocator, 1);
    auto *resultValueSetVec1 = new BooleanVector(vectorAllocator, 1);
    std::vector<Vector*> extractVecs1 = {resultfirstVec1, resultValueSetVec1};

    AggregateState state { nullptr };

    //add first VectorBatch
    firstIgnoreNullIntAggPartial->InitiateGroup(state, vecBatch1, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs1, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(false, resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 1);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs1, 0);
    EXPECT_EQ(true, resultfirstVec1->IsValueNull(0));
    EXPECT_EQ(false, resultValueSetVec1->GetValue(0));

    std::vector<DataTypePtr> vector;
    vector.push_back(LongType());
    vector.push_back(BooleanType());
    DataTypesPtr inputTypes = std::make_unique<DataTypes>(vector);

    std::vector<int32_t> channal2 = { 0, 1 };
    auto firstIgnoreNullIntAggFinal = firstIgnoreNullFactory->CreateAggregator(
            inputTypes,
            AggregatorUtil::WrapWithDataTypes(LongType()),
            channal2, false, false);

    auto* inputLongVec2 = new LongVector(vectorAllocator, 4);
    inputLongVec2->SetValueNull(0);
    inputLongVec2->SetValue(1, 111);
    inputLongVec2->SetValueNull(2);
    inputLongVec2->SetValueNull(3);

    auto* inputBooleanVec2 = new BooleanVector(vectorAllocator, 4);
    inputBooleanVec2->SetValue(0, false);
    inputBooleanVec2->SetValue(1, true);
    inputBooleanVec2->SetValue(2, false);
    inputBooleanVec2->SetValue(3, false);

    auto *vecBatch2 = new VectorBatch(2);
    vecBatch2->SetVector(0, inputLongVec2);
    vecBatch2->SetVector(1, inputBooleanVec2);

    auto *resultfirstVec2 = new LongVector(vectorAllocator, 1);
    std::vector<Vector*> extractVecs2 = {resultfirstVec2};

    firstIgnoreNullIntAggFinal->InitiateGroup(state, vecBatch2, 0);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_EQ(true, resultfirstVec2->IsValueNull(0));

    firstIgnoreNullIntAggFinal->ProcessGroup(state, vecBatch2, 1);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_EQ(false, resultfirstVec2->IsValueNull(0));
    EXPECT_EQ(111, resultfirstVec2->GetValue(0));

    firstIgnoreNullIntAggFinal->ProcessGroup(state, vecBatch2, 2);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_EQ(false, resultfirstVec2->IsValueNull(0));
    EXPECT_EQ(111, resultfirstVec2->GetValue(0));

    firstIgnoreNullIntAggFinal->ProcessGroup(state, vecBatch2, 3);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_EQ(false, resultfirstVec2->IsValueNull(0));
    EXPECT_EQ(111, resultfirstVec2->GetValue(0));


    firstIgnoreNullIntAggFinal->ProcessGroup(state, vecBatch2, 4);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_EQ(false, resultfirstVec2->IsValueNull(0));
    EXPECT_EQ(111, resultfirstVec2->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch1);
    VectorHelper::FreeVecBatch(vecBatch2);
    delete resultfirstVec1;
    delete resultValueSetVec1;
    delete resultfirstVec2;
    delete firstIgnoreNullFactory;
}


}