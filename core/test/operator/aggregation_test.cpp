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
#include "operator/aggregation/aggregator/aggregator_factory_impl.h"
#include "operator/aggregation/aggregator/all_aggregators.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "vector/vector_helper.h"
#include "util/perf_util.h"
#include "util/config_util.h"
#include "util/test_util.h"

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

VectorBatch *ConstructSimpleBuildData()
{
    const int32_t dataSize = 3;
    std::vector<DataTypePtr> types { LongType(),   LongType(), IntType(), ShortType(),
        DoubleType(), LongType(), LongType() };
    DataTypes outTypes(types);
    int64_t buildData0[dataSize] = {2, 1, 0};
    int64_t buildData1[dataSize] = {2, 1, 0};
    int32_t buildData2[dataSize] = {2, 1, 0};
    int16_t buildData3[dataSize] = {2, 1, 0};
    double buildData4[dataSize] = {2, 1, 0};
    int64_t buildData5[dataSize] = {60, 30, 0};
    int64_t buildData6[dataSize] = {60, 30, 0};
    return CreateVectorBatch(outTypes, dataSize, buildData0, buildData1, buildData2, buildData3, buildData4, buildData5,
        buildData6);
}

std::vector<VectorBatch *> ConstructSimpleBuildData(int32_t vecBatchCnt, int32_t rowPerBatch,
    VectorAllocator *allocator)
{
    const int32_t cols = 5;
    const int32_t mod = 3;
    std::vector<VectorBatch *> input(vecBatchCnt);
    for (int32_t i = 0; i < vecBatchCnt; ++i) {
        auto *vecBatch = new VectorBatch(cols);
        auto *col1 = new LongVector(allocator, rowPerBatch);
        auto *col2 = new IntVector(allocator, rowPerBatch);
        auto *col3 = new ShortVector(allocator, rowPerBatch);
        auto *col4 = new DoubleVector(allocator, rowPerBatch);

        for (int32_t j = 0; j < rowPerBatch; ++j) {
            col1->SetValue(j, j % mod);
            col2->SetValue(j, j % mod);
            col3->SetValue(j, j % mod);
            col4->SetValue(j, j % mod);
        }
        std::vector<Vector *> tmp { col1, col1->Slice(0, rowPerBatch), col2, col3, col4 };
        // set to vecBatch
        for (int32_t index = 0; index < cols; index++) {
            vecBatch->SetVector(index, tmp[index]);
        }
        input[i] = vecBatch;
    }
    return input;
}

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

std::unique_ptr<HashAggregationOperatorFactory> CreateHashAggregationOperatorFactory(
    const std::vector<uint32_t> &groupByColumns_,
    const std::vector<DataTypePtr> &groupTypes,
    const std::vector<uint32_t> &aggFuncTypes_,
    const std::vector<uint32_t> &aggInputCols,
    const std::vector<DataTypePtr> &aggInputTypes,
    const std::vector<DataTypePtr> &aggOutputTypes,
    const std::vector<uint32_t> &aggMask_,
    const bool inputRaw,
    const bool outputPartial,
    const bool nullWhenOverflow
)
{
    EXPECT_EQ(groupByColumns_.size(), groupTypes.size());
    auto numAgg = aggFuncTypes_.size();
    EXPECT_EQ(numAgg, aggInputCols.size());
    EXPECT_EQ(numAgg, aggInputTypes.size());
    EXPECT_EQ(numAgg, aggOutputTypes.size());
    if (aggMask_.size() != 0) {
        EXPECT_EQ(numAgg, aggMask_.size());
    }

    auto aggInputColsWrap = AggregatorUtil::WrapWithVector(aggInputCols);
    auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(aggInputTypes));
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(aggOutputTypes));
    std::vector<uint32_t> aggMask;
    if (aggMask_.size() == 0) {
        aggMask.reserve(numAgg);
        for (size_t i = 0; i < numAgg; ++i) {
            aggMask.push_back(static_cast<uint32_t>(-1));
        }
    } else {
        aggMask = std::vector<uint32_t>(aggMask_);
    }
    auto inputRawWrap = AggregatorUtil::WrapWithVector(inputRaw, numAgg);
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(outputPartial, numAgg);

    auto groupByColumns = std::vector<uint32_t>(groupByColumns_);
    auto aggFuncTypes = std::vector<uint32_t>(aggFuncTypes_);
    auto hashAggOpFactory = std::make_unique<HashAggregationOperatorFactory>(
        groupByColumns,
        DataTypes(groupTypes),
        aggInputColsWrap,
        aggInputTypesWrap,
        aggOutputTypesWrap,
        aggFuncTypes,
        aggMask,
        inputRawWrap,
        outputPartialWrap,
        nullWhenOverflow
    );
    hashAggOpFactory->Init();
    return hashAggOpFactory;
}

std::unique_ptr<AggregationOperatorFactory> CreateAggregationOperatorFactory(
    const std::vector<uint32_t> &aggFuncTypes_,
    const std::vector<uint32_t> &aggInputCols,
    const std::vector<DataTypePtr> &aggInputTypes,
    const std::vector<DataTypePtr> &aggOutputTypes,
    const std::vector<uint32_t> &aggMask_,
    const bool inputRaw,
    const bool outputPartial,
    const bool nullWhenOverflow
)
{
    auto numAgg = aggFuncTypes_.size();
    EXPECT_EQ(numAgg, aggInputCols.size());
    EXPECT_EQ(numAgg, aggInputTypes.size());
    EXPECT_EQ(numAgg, aggOutputTypes.size());
    if (aggMask_.size() != 0) {
        EXPECT_EQ(numAgg, aggMask_.size());
    }


    auto aggInputColsWrap = AggregatorUtil::WrapWithVector(aggInputCols);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(aggOutputTypes));
    std::vector<uint32_t> aggMask;
    if (aggMask_.size() == 0) {
        aggMask.reserve(numAgg);
        for (size_t i = 0; i < numAgg; ++i) {
            aggMask.push_back(static_cast<uint32_t>(-1));
        }
    } else {
        aggMask = std::vector<uint32_t>(aggMask_);
    }
    auto inputRawWrap = AggregatorUtil::WrapWithVector(inputRaw, numAgg);
    auto outputPartialWrap = AggregatorUtil::WrapWithVector(outputPartial, numAgg);

    auto aggFuncTypes = std::vector<uint32_t>(aggFuncTypes_);
    DataTypes inputTypes(aggInputTypes);
    auto aggOpFactory = std::make_unique<AggregationOperatorFactory>(
        inputTypes,
        aggFuncTypes,
        aggInputColsWrap,
        aggMask,
        aggOutputTypesWrap,
        inputRawWrap,
        outputPartialWrap,
        nullWhenOverflow
    );
    aggOpFactory->Init();
    return aggOpFactory;
}

TEST(HashAggregationOperatorTest, verify_correctness)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    // create 10 pages
    const int vecBatchNum = 10;
    const int rowSize = 2000;
    const int cardinality = 10;
    std::string aggNames[] = {"group", "group", "sum", "avg", "count", "min", "max"};
    std::vector<uint32_t> aggFuncTypes = {
        OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_MIN,
        OMNI_AGGREGATION_TYPE_MAX
    };
    std::vector<DataTypePtr> groupTypes = { LongType(), LongType() };
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType(), LongType(), LongType(), LongType() };
    VectorBatch **input1 = buildAggInput(vecBatchNum, rowSize, cardinality, 2, 5, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    // First stage (partial)
    auto aggPartialFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0, 1 }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        aggFuncTypes,
        std::vector<uint32_t>({ 2, 3, 4, 5, 6 }),
        std::vector<DataTypePtr>({ LongType(), LongType(), LongType(), LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }),
            LongType(), LongType(), LongType() }),
        std::vector<uint32_t>(),
        true,
        true,
        false
    );

    // operator 1 (partial)
    auto aggPartial1 = aggPartialFactory->CreateOperator();
    aggPartial1->Init();
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggPartial1->AddInput(input1[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t vecBatchCount = aggPartial1->GetOutput(result1);
    EXPECT_EQ(vecBatchCount, 1);
    Operator::DeleteOperator(aggPartial1);

    VectorBatch **input2 = buildAggInput(vecBatchNum, rowSize, cardinality, 2, 5, groupTypes, aggTypes);
    if (input2 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    // operator 2 (partial)
    auto aggPartial2 = aggPartialFactory->CreateOperator();
    aggPartial2->Init();
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggPartial2->AddInput(input2[i]);
    }

    std::vector<VectorBatch *> result2;
    int32_t tableCount2 = aggPartial2->GetOutput(result2);
    EXPECT_EQ(tableCount2, 1);
    Operator::DeleteOperator(aggPartial2);

    // Second stage (final)
    auto aggFinalFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0, 1 }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        aggFuncTypes,
        std::vector<uint32_t>({ 2, 3, 4, 5, 6 }),
        std::vector<DataTypePtr>({ LongType(), ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }),
            LongType(), LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), DoubleType(), LongType(), LongType(), LongType() }),
        std::vector<uint32_t>(),
        false,
        false,
        false
    );

    auto aggFinal = aggFinalFactory->CreateOperator();
    aggFinal->Init();

    for (uint32_t i = 0; i < result1.size(); ++i) {
        aggFinal->AddInput(result1[i]);
    }
    for (uint32_t i = 0; i < result2.size(); ++i) {
        aggFinal->AddInput(result2[i]);
    }

    std::vector<VectorBatch *> result3;
    aggFinal->GetOutput(result3);
    Operator::DeleteOperator(aggFinal);

    std::vector<DataTypePtr> expectFieldTypes {
        LongType(), LongType(),  LongType(),  DoubleType(), LongType(),  LongType(),  LongType()
    };
    // construct the output data
    DataTypes expectTypes(expectFieldTypes);
    int64_t expectData1[cardinality] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int64_t expectData2[cardinality] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int64_t expectData3[cardinality] = {4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000};
    double expectData4[cardinality] = {1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0};
    int64_t expectData5[cardinality] = {4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000};
    int64_t expectData6[cardinality] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    int64_t expectData7[cardinality] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    VectorBatch *expectVecBatch = CreateVectorBatch(
        expectTypes, cardinality,
        expectData1, expectData2, expectData3, expectData4, expectData5, expectData6, expectData7
    );

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(result3[0], expectVecBatch));
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
    std::vector<uint32_t> aggFuncTypes = {
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX
    };
    std::string aggNames[] = {"group", "count", "min", "max" };
    std::string data0[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data1[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data2[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data3[8] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1", "1.1", "1"};
    VectorBatch **input = BuildVarCharInput(vecBatchNum, columnCount, rowSize, data0, data1, data2, data3);

    // single stage
    auto aggFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0 }),
        std::vector<DataTypePtr>({ VarcharType(1) }),
        aggFuncTypes,
        std::vector<uint32_t>({ 1, 2, 3 }),
        std::vector<DataTypePtr>({ VarcharType(1), VarcharType(1), VarcharType(4) }),
        std::vector<DataTypePtr>({ LongType(), VarcharType(1), VarcharType(4) }),
        std::vector<uint32_t>(),
        true,
        false,
        false
    );

    auto groupByVarChar = aggFactory->CreateOperator();
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
    std::string expectData1[3] = {"0", "1", "2"};
    int64_t expectData2[3] = {3, 3, 2};
    std::string expectData3[3] = {"0", "1", "2"};
    std::string expectData4[3] = {"6.6", "5.5", "4.4"};
    std::vector<DataTypePtr> expectedFieldTypes { VarcharType(1), LongType(), VarcharType(1), VarcharType(3) };
    DataTypes expectedTypes(expectedFieldTypes);
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectedTypes, 3, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(resBatch, expectVecBatch));

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
    std::vector<uint32_t> aggFuncTypes = {
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX
    };
    std::string data0[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data1[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data2[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data3[8] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1", "1.1", "1"};
    VectorBatch **input = BuildVarCharInput(vecBatchNum, columnCount, rowSize, data0, data1, data2, data3);

    // single stage
    auto aggFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0 }),
        std::vector<DataTypePtr>({ CharType(1) }),
        aggFuncTypes,
        std::vector<uint32_t>({ 1, 2, 3 }),
        std::vector<DataTypePtr>({ CharType(1), CharType(1), CharType(4) }),
        std::vector<DataTypePtr>({ LongType(), CharType(1), CharType(4) }),
        std::vector<uint32_t>(),
        true,
        false,
        false
    );

    auto groupByVarChar = aggFactory->CreateOperator();
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
    std::string expectData1[3] = {"0", "1", "2"};
    int64_t expectData2[3] = {3, 3, 2};
    std::string expectData3[3] = {"0", "1", "2"};
    std::string expectData4[3] = {"6.6", "5.5", "4.4"};

    std::vector<DataTypePtr> expectedFieldTypes { CharType(1), LongType(), CharType(1), CharType(3) };
    DataTypes expectedTypes(expectedFieldTypes);
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectedTypes, 3, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(resBatch, expectVecBatch));

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
    std::vector<uint32_t> aggFuncTypes = {
        OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_MIN,
        OMNI_AGGREGATION_TYPE_MAX
    };
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

    // single stage
    auto aggFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0 }),
        std::vector<DataTypePtr>({ LongType() }),
        aggFuncTypes,
        std::vector<uint32_t>({ 1, 2, 3, 4, 5 }),
        std::vector<DataTypePtr>({ LongType(), LongType(), LongType(), LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), DoubleType(), LongType(), LongType(), LongType() }),
        std::vector<uint32_t>(),
        true,
        false,
        false
    );
    auto groupByNULL = aggFactory->CreateOperator();
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
    const int dataSize = 10;
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
        "aggregation_verfify_correctness_group_by_agg_same_cols");
    std::vector<VectorBatch *> input = ConstructSimpleBuildData(vecBatchNum, dataSize, vecAllocator);

    auto aggFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0, 1, 2, 3, 4 }),
        std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), ShortType(), DoubleType() }),
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM}),
        std::vector<uint32_t>({ 0, 1 }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<uint32_t>(),
        true,
        false,
        false
    );
    auto groupBy = aggFactory->CreateOperator();
    groupBy->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupBy->AddInput(input[i]);
    }

    std::vector<VectorBatch *> result;
    groupBy->GetOutput(result);
    std::vector<VectorBatch *> expected { ConstructSimpleBuildData() };
    EXPECT_TRUE(VecBatchesIgnoreOrderMatch(result, expected));

    Operator::DeleteOperator(groupBy);
    VectorHelper::FreeVecBatches(result);

    VectorHelper::FreeVecBatches(expected);
    delete vecAllocator;
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

    std::vector<DataTypePtr> types = {
        LongType(),    LongType(),    LongType(),    LongType(),
        LongType(),    LongType(),    BooleanType(), BooleanType(),
        BooleanType(), BooleanType(), BooleanType()
    };
    DataTypes sourceTypes(types);

    std::vector<DataTypePtr> inputTypes = { LongType(), LongType(), LongType(), LongType(), LongType() };
    DataTypes inputDataTypes(inputTypes);
    VectorBatch *vecBatch1 = CreateVectorBatch(
        sourceTypes, dataSize,
        dataHash,
        data0, data1, data2, data3, data4, data5, data6, data7, data8, data9
    );

    std::vector<uint32_t> aggFuncTypes = {
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_MAX,
        OMNI_AGGREGATION_TYPE_MIN
    };

    // STAGE1: (partial)
    auto aggPartialFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0 }),
        std::vector<DataTypePtr>({ LongType() }),
        aggFuncTypes,
        std::vector<uint32_t>({ 1, 2, 3, 4, 5 }),
        std::vector<DataTypePtr>({ LongType(), LongType(), LongType(), LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), LongType(),
            ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), LongType(), LongType() }),
        std::vector<uint32_t>({ 6, 7, 8, 9, 10 }),
        true,
        true,
        false
    );
    auto aggregatePartial = aggPartialFactory->CreateOperator();
    aggregatePartial->Init();
    aggregatePartial->AddInput(vecBatch1);

    std::vector<VectorBatch *> result;
    int32_t tableCount1 = aggregatePartial->GetOutput(result);
    EXPECT_EQ(tableCount1, 1);

    // STAGE2: (final)
    auto aggFinalFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0 }),
        std::vector<DataTypePtr>({ LongType() }),
        aggFuncTypes,
        std::vector<uint32_t>({ 1, 2, 3, 4, 5 }),
        std::vector<DataTypePtr>({ LongType(), LongType(),
            ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), LongType(), DoubleType(), LongType(), LongType() }),
        std::vector<uint32_t>(),
        false,
        false,
        false
    );
    auto aggregateFinal = aggFinalFactory->CreateOperator();
    aggregateFinal->Init();
    for (uint32_t i = 0; i < result.size(); ++i) {
        aggregateFinal->AddInput(result[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t tableCount = aggregateFinal->GetOutput(result1);
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
    VectorBatch *expVecBatch1 = CreateVectorBatch(
        resultTypes, resultDataSize, expHashData,  expData0, expData1, expData2, expData3, expData4
    );

    EXPECT_TRUE(VecBatchMatch(result1[0], expVecBatch1));

    omniruntime::op::Operator::DeleteOperator(aggregatePartial);
    omniruntime::op::Operator::DeleteOperator(aggregateFinal);
    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatches(result1);
}

TEST(HashAggregationOperatorTest, DISABLED_original_multiple_threads)
{
    using namespace std;
    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;

    std::vector<DataTypePtr> groupTypes { LongType(), LongType() };
    std::vector<DataTypePtr> inputTypes { LongType(), LongType() };
    VectorBatch **input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, inputTypes);
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    auto nativeOperatorFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0, 1 }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM }),
        std::vector<uint32_t>({ 2, 3 }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<uint32_t>(),
        true,
        false,
        false
    );
    uint64_t factoryObjAddr = reinterpret_cast<uint64_t>(nativeOperatorFactory.get());

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

TEST(AggregationOperatorTest, hmpp_min_max_varchar)
{
    ConfigUtil::SetEnableHMPP(true);
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

    std::vector<uint32_t> aggFuncTypes = {OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX};

    // STAGE1: (partial)
    std::vector<DataTypePtr> partialOutputTypes { VarcharType(100), VarcharType(100) };
    auto aggPartialFactory = CreateAggregationOperatorFactory(
        aggFuncTypes,
        std::vector<uint32_t>({ 0, 1 }),
        types,
        partialOutputTypes,
        std::vector<uint32_t>(),
        true,
        true,
        false
    );
    auto aggPartial = aggPartialFactory->CreateOperator();
    aggPartial->AddInput(input);
    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = aggPartial->GetOutput(result);
    EXPECT_EQ(vecBatchCount, 1);

    // STAGE2: (final)
    std::vector<DataTypePtr> finalOutputTypes { VarcharType(100), VarcharType(100) };
    auto aggFinalFactory = CreateAggregationOperatorFactory(
        aggFuncTypes,
        std::vector<uint32_t>({ 0, 1 }),
        partialOutputTypes,
        finalOutputTypes,
        std::vector<uint32_t>(),
        false,
        false,
        false
    );
    auto aggFinal = aggFinalFactory->CreateOperator();
    for (uint32_t i = 0; i < result.size(); i++) {
        aggFinal->AddInput(result[i]);
    }

    std::vector<VectorBatch *> finalResult;
    vecBatchCount = aggFinal->GetOutput(finalResult);
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

    omniruntime::op::Operator::DeleteOperator(aggFinal);
    omniruntime::op::Operator::DeleteOperator(aggPartial);
    VectorHelper::FreeVecBatches(finalResult);
    delete vectorAllocator;
    ConfigUtil::SetEnableHMPP(false);
}

TEST(AggregationOperatorTest, hmpp_min_max_varchar_without_nulls)
{
    ConfigUtil::SetEnableHMPP(true);
    std::string data0[] = {"Zulma.Carter@MfvjVN43Udd95KeZ.com", "*", "Aaron.Anderson@0CQ4QUkBY2Q.edu",
                           "Zulema.Ruiz@J2XvbX7.com", "**", "Aaron.Artis@bv.org"};
    std::string data1[] = {"**", "Zulma.Carter@MfvjVN43Udd95KeZ.com", "Aaron.Anderson@0CQ4QUkBY2Q.edu", "*",
                           "Zulema.Ruiz@J2XvbX7.com", "Aaron.Artis@bv.org"};
    std::vector<DataTypePtr> types = std::vector<DataTypePtr> { VarcharType(100), VarcharType(100) };
    int32_t rowCount = 6;
    auto vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("hmpp_min_varchar_test");
    auto vector1 = new VarcharVector(vectorAllocator,
        static_cast<VarcharDataType *>(types.at(0).get())->GetWidth() * rowCount, rowCount);
    auto vector2 = new VarcharVector(vectorAllocator,
        static_cast<VarcharDataType *>(types.at(1).get())->GetWidth() * rowCount, rowCount);
    for (int32_t i = 0; i < rowCount; i++) {
        vector1->SetValue(i, reinterpret_cast<const uint8_t *>(data0[i].c_str()), data0[i].size());
        vector2->SetValue(i, reinterpret_cast<const uint8_t *>(data1[i].c_str()), data1[i].size());
    }

    auto input = new VectorBatch(2, 6);
    input->SetVector(0, vector1);
    input->SetVector(1, vector2);

    std::vector<uint32_t> aggFuncTypes = {OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX};

    // STAGE1: (partial)
    std::vector<DataTypePtr> partialOutputTypes { VarcharType(100), VarcharType(100) };
    auto aggPartialFactory = CreateAggregationOperatorFactory(
        aggFuncTypes,
        std::vector<uint32_t>({ 0, 1 }),
        types,
        partialOutputTypes,
        std::vector<uint32_t>(),
        true,
        true,
        false
    );
    auto aggPartial = aggPartialFactory->CreateOperator();
    aggPartial->AddInput(input);
    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = aggPartial->GetOutput(result);
    EXPECT_EQ(vecBatchCount, 1);

    // STAGE2: (final)
    std::vector<DataTypePtr> finalOutputTypes { VarcharType(100), VarcharType(100) };
    auto aggFinalFactory = CreateAggregationOperatorFactory(
        aggFuncTypes,
        std::vector<uint32_t>({ 0, 1 }),
        partialOutputTypes,
        finalOutputTypes,
        std::vector<uint32_t>(),
        false,
        false,
        false
    );
    auto aggFinal = aggFinalFactory->CreateOperator();
    for (uint32_t i = 0; i < result.size(); i++) {
        aggFinal->AddInput(result[i]);
    }

    std::vector<VectorBatch *> finalResult;
    vecBatchCount = aggFinal->GetOutput(finalResult);
    EXPECT_EQ(vecBatchCount, 1);
    auto resultVec0 = static_cast<VarcharVector *>(finalResult[0]->GetVector(0));
    EXPECT_EQ(resultVec0->GetSize(), 1);
    uint8_t *minVal = nullptr;
    int32_t minValLen = resultVec0->GetValue(0, &minVal);
    std::string minStr(minVal, minVal + minValLen);
    EXPECT_EQ(minValLen, data0[1].size());
    EXPECT_EQ(data0[1].compare(minStr), 0);

    auto resultVec1 = static_cast<VarcharVector *>(finalResult[0]->GetVector(1));
    EXPECT_EQ(resultVec1->GetSize(), 1);
    uint8_t *maxVal = nullptr;
    int32_t maxValLen = resultVec1->GetValue(0, &maxVal);
    std::string maxStr(maxVal, maxVal + maxValLen);
    EXPECT_EQ(maxValLen, data1[1].size());
    EXPECT_EQ(data1[1].compare(maxStr), 0);

    omniruntime::op::Operator::DeleteOperator(aggFinal);
    omniruntime::op::Operator::DeleteOperator(aggPartial);
    VectorHelper::FreeVecBatches(finalResult);
    delete vectorAllocator;
    ConfigUtil::SetEnableHMPP(false);
}

TEST(AggregationOperatorTest, hmpp_sum_avg)
{
    ConfigUtil::SetEnableHMPP(true);
    const int32_t dataSize = 5;
    const int32_t resultDataSize = 1;

    int64_t data0[dataSize] = {3L, 10L, 2L, 7L, 3L};
    Decimal128 data1[dataSize] = {Decimal128(4000L, 0), Decimal128(2000L, 0), Decimal128(1000L, 0),
                                  Decimal128(3000L, 0), Decimal128(5000L, 0)};

    std::string aggNames[] = {"sum", "sum", "avg", "avg"};
    std::vector<uint32_t> aggFuncTypes = {
        OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG
    };
    std::vector<DataTypePtr> aggTypes = { LongType(), Decimal128Type(20, 5), LongType(), Decimal128Type(20, 5) };
    DataTypes sourceTypes(aggTypes);
    VectorBatch *input = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data0, data1);
    ASSERT(!(input == nullptr));

    // STAGE1: (partial)
    std::vector<DataTypePtr> partialOutputTypes {
        LongType(), SUM_IMMEDIATE_VARBINARY,
        ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), AVG_IMMEDIATE_VARBINARY
    };
    auto aggPartialFactory = CreateAggregationOperatorFactory(
        aggFuncTypes,
        std::vector<uint32_t>({ 0, 1, 2, 3 }),
        aggTypes,
        partialOutputTypes,
        std::vector<uint32_t>(),
        true,
        true,
        false
    );
    auto aggPartial = aggPartialFactory->CreateOperator();
    aggPartial->AddInput(input);

    std::vector<VectorBatch *> result;
    int32_t tableCount = aggPartial->GetOutput(result);
    EXPECT_EQ(tableCount, 1);

    // STAGE2: (final)
    std::vector<DataTypePtr> finalOutputTypes { LongType(), Decimal128Type(20, 5), DoubleType(), Decimal128Type(20, 5) };
    auto aggFinalFactory = CreateAggregationOperatorFactory(
        aggFuncTypes,
        std::vector<uint32_t>({ 0, 1, 2, 3 }),
        partialOutputTypes,
        finalOutputTypes,
        std::vector<uint32_t>(),
        false,
        false,
        false
    );
    auto aggFinal = aggFinalFactory->CreateOperator();
    for (uint32_t i = 0; i < result.size(); i++) {
        aggFinal->AddInput(result[i]);
    }

    std::vector<VectorBatch *> finalResult;
    tableCount = aggFinal->GetOutput(finalResult);
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

    omniruntime::op::Operator::DeleteOperator(aggFinal);
    omniruntime::op::Operator::DeleteOperator(aggPartial);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatches(finalResult);
    ConfigUtil::SetEnableHMPP(false);
}

TEST(AggregationOperatorTest, hmpp_decimal128)
{
    ConfigUtil::SetEnableHMPP(true);
    const int32_t dataSize = 5;
    const int32_t resultDataSize = 1;

    std::string value0[5] = {"1234567890123456789012345678901234567", "-55", "0",
                                 "9999999999999999999999999999999999999", "-9999999999999999999999999999999999999"};
    std::string value1[5] = {"1234567890123456789012", "-9223372036854775808", "0", "9999999999999999999999",
                                 "-9999999999999999999999"};

    Decimal128 data0[dataSize] = {Decimal128(value0[0]), Decimal128(value0[1]), Decimal128(value0[2]),
                                      Decimal128(value0[3]), Decimal128(value0[4])};
    Decimal128 data1[dataSize] = {Decimal128(value1[0]), Decimal128(value1[1]), Decimal128(value1[2]),
                                      Decimal128(value1[3]), Decimal128(value1[4])};

    std::string aggNames[] = {"min", "min", "max", "max", "sum", "sum", "avg", "avg"};
    std::vector<uint32_t> aggFuncTypes = {
        OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN,
        OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX,
        OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG
    };
    std::vector<DataTypePtr> groupTypes;
    std::vector<DataTypePtr> aggTypes = { Decimal128Type(38, 0), Decimal128Type(38, 0), Decimal128Type(38, 0),
        Decimal128Type(38, 0), Decimal128Type(38, 0), Decimal128Type(38, 0),
        Decimal128Type(38, 0), Decimal128Type(38, 0) };
    DataTypes sourceTypes(aggTypes);
    VectorBatch *input =
        CreateVectorBatch(sourceTypes, dataSize, data0, data1, data0, data1, data0, data1, data0, data1);

    ASSERT(!(input == nullptr));

    std::vector<DataTypePtr> partialOutputTypes { Decimal128Type(38, 0),   Decimal128Type(38, 0),
        Decimal128Type(38, 0),   Decimal128Type(38, 0),
        SUM_IMMEDIATE_VARBINARY, SUM_IMMEDIATE_VARBINARY,
        AVG_IMMEDIATE_VARBINARY, AVG_IMMEDIATE_VARBINARY };

    auto aggPartialFactory = CreateAggregationOperatorFactory(
        aggFuncTypes,
        std::vector<uint32_t>({ 0, 1, 2, 3, 4, 5, 6, 7 }),
        aggTypes,
        partialOutputTypes,
        std::vector<uint32_t>(),
        true,
        true,
        false
    );
    auto aggPartial = aggPartialFactory->CreateOperator();
    aggPartial->AddInput(input);
    std::vector<VectorBatch *> result;
    int32_t tableCount = aggPartial->GetOutput(result);
    EXPECT_EQ(tableCount, 1);

    // STAGE2:
    std::vector<DataTypePtr> finalOutputTypes { Decimal128Type(38, 0), Decimal128Type(38, 0), Decimal128Type(38, 0),
        Decimal128Type(38, 0), Decimal128Type(38, 0), Decimal128Type(38, 0),
        Decimal128Type(38, 0), Decimal128Type(38, 0) };
    auto aggFinalFactory = CreateAggregationOperatorFactory(
        aggFuncTypes,
        std::vector<uint32_t>({ 0, 1, 2, 3, 4, 5, 6, 7 }),
        partialOutputTypes,
        finalOutputTypes,
        std::vector<uint32_t>(),
        false,
        false,
        false
    );
    auto aggFinal = aggFinalFactory->CreateOperator();
    for (uint32_t i = 0; i < result.size(); i++) {
        aggFinal->AddInput(result[i]);
    }
    std::vector<VectorBatch *> finalResult;
    tableCount = aggFinal->GetOutput(finalResult);
    EXPECT_EQ(tableCount, 1);
    EXPECT_EQ(finalResult[0]->GetRowCount(), 1);
    EXPECT_EQ(finalResult[0]->GetVectorCount(), 8);

    Decimal128 expData0[resultDataSize] = {Decimal128("-9999999999999999999999999999999999999")};
    Decimal128 expData1[resultDataSize] = {Decimal128("-9999999999999999999999")};
    Decimal128 expData2[resultDataSize] = {Decimal128("9999999999999999999999999999999999999")};
    Decimal128 expData3[resultDataSize] = {Decimal128("9999999999999999999999")};
    Decimal128 expData4[resultDataSize] = {Decimal128("1234567890123456789012345678901234512")};
    Decimal128 expData5[resultDataSize] = {Decimal128("1225344518086602013204")};
    Decimal128 expData6[resultDataSize] = {Decimal128("246913578024691357802469135780246902")};
    Decimal128 expData7[resultDataSize] = {Decimal128("245068903617320402641")};
    std::vector<DataTypePtr> resultType = { Decimal128Type(38, 0), Decimal128Type(38, 0), Decimal128Type(38, 0),
        Decimal128Type(38, 0), Decimal128Type(38, 0), Decimal128Type(38, 0),
        Decimal128Type(38, 0), Decimal128Type(38, 0) };
    DataTypes resultTypes(resultType);
    VectorBatch *expVecBatch = CreateVectorBatch(resultTypes, resultDataSize, expData0, expData1, expData2, expData3,
        expData4, expData5, expData6, expData7);

    EXPECT_TRUE(VecBatchMatch(finalResult[0], expVecBatch));

    omniruntime::op::Operator::DeleteOperator(aggFinal);
    omniruntime::op::Operator::DeleteOperator(aggPartial);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatches(finalResult);
    ConfigUtil::SetEnableHMPP(false);
}

TEST(HashAggregationOperatorTest, hmpp_group_by_agg_same_cols)
{
    ConfigUtil::SetEnableHMPP(true);
    // create 10 vecBatches
    const int vecBatchNum = 10;
    const int dataSize = 10;
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("hmpp_group_by_same_cols");
    std::vector<VectorBatch *> input = ConstructSimpleBuildData(vecBatchNum, dataSize, vecAllocator);

    auto aggFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0, 1, 2, 3, 4 }),
        std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), ShortType(), DoubleType() }),
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM}),
        std::vector<uint32_t>({ 0, 1 }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<uint32_t>(),
        true,
        false,
        false
    );
    auto groupBy = aggFactory->CreateOperator();
    groupBy->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupBy->AddInput(input[i]);
    }

    std::vector<VectorBatch *> result;
    groupBy->GetOutput(result);
    std::vector<VectorBatch *> expected { ConstructSimpleBuildData() };
    EXPECT_TRUE(VecBatchesIgnoreOrderMatch(result, expected));
    Operator::DeleteOperator(groupBy);
    VectorHelper::FreeVecBatches(result);
    VectorHelper::FreeVecBatches(expected);
    delete vecAllocator;
    ConfigUtil::SetEnableHMPP(false);
}

TEST(HashAggregationOperatorTest, hmpp_varchar_vector_correctness)
{
    ConfigUtil::SetEnableHMPP(true);
    // create 10 pages
    const int vecBatchNum = 1;
    const int cardinality = 10;
    const int rowSize = 2000;
    // groupby + count + min + max
    std::string aggNames[] = {"group", "count", "min", "max" };
    std::vector<DataTypePtr> groupTypes = { VarcharType(10) };
    std::vector<DataTypePtr> aggTypes = { VarcharType(10), VarcharType(10), VarcharType(10) };
    VectorBatch **input = buildAggInput(vecBatchNum, rowSize, cardinality, 1, 3, groupTypes, aggTypes);

    std::vector<DataTypePtr> outputTypes { LongType(), VarcharType(10), VarcharType(10) };
    auto aggFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0 }),
        groupTypes,
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
            OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX}),
        std::vector<uint32_t>({ 1, 2, 3 }),
        aggTypes,
        outputTypes,
        std::vector<uint32_t>(),
        true,
        false,
        false
    );
    auto groupByVarChar = aggFactory->CreateOperator();
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

    std::vector<DataTypePtr> expectFieldTypes { VarcharType(10), LongType(), VarcharType(10), VarcharType(10) };
    // construct the output data
    DataTypes expectTypes(expectFieldTypes);
    std::string  expectData1[cardinality] = {"9", "8", "7", "6", "5", "4", "3", "2", "1", "0"};
    int64_t expectData2[cardinality] = {200, 200, 200, 200, 200, 200, 200, 200, 200, 200};
    std::string  expectData3[cardinality] = {"1009", "1008", "1007", "1006", "1005", "1004", "1003", "1002", "1", "0"};
    std::string  expectData4[cardinality] = {"999", "998", "997", "996", "995", "994", "993", "992", "991", "990"};

    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, cardinality, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(resBatch, expectVecBatch));

    delete[] input;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(resBatch);
    ConfigUtil::SetEnableHMPP(false);
}

TEST(AggregationOperatorTest, verify_correctness)
{
    // create 10 vecBatches
    const int vecBatchNum = 10;
    const int cardinality = 4;
    std::string aggNames[] = {"sum", "avg", "count", "min", "max"};
    std::vector<uint32_t> aggFuncTypes = {
        OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_MIN,
        OMNI_AGGREGATION_TYPE_MAX
    };
    std::vector<DataTypePtr> groupTypes;
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType(), LongType(), LongType(), LongType() };
    VectorBatch **input1 = buildAggInput(vecBatchNum, ROW_PER_VEC_BATCH, cardinality, 0, 5, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    // STAGE1: (partial)
    std::vector<DataTypePtr> partialOutputTypes {
        LongType(),
        ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }),
        LongType(),
        LongType(),
        LongType()
    };
    auto aggPartialFactory = CreateAggregationOperatorFactory(
        aggFuncTypes,
        std::vector<uint32_t>({ 0, 1, 2, 3, 4 }),
        aggTypes,
        partialOutputTypes,
        std::vector<uint32_t>(),
        true,
        true,
        false
    );

    // partial first operator
    auto aggPartial1 = aggPartialFactory->CreateOperator();
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggPartial1->AddInput(input1[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t tableCount1 = aggPartial1->GetOutput(result);
    EXPECT_EQ(tableCount1, 1);
    omniruntime::op::Operator::DeleteOperator(aggPartial1);

    VectorBatch **input2 = buildAggInput(vecBatchNum, ROW_PER_VEC_BATCH, cardinality, 0, 5, groupTypes, aggTypes);
    ASSERT(!(input2 == nullptr));

    // partial second operator
    auto aggPartial2 = aggPartialFactory->CreateOperator();
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggPartial2->AddInput(input2[i]);
    }
    int32_t tableCount2 = aggPartial2->GetOutput(result);
    EXPECT_EQ(tableCount2, 1);
    omniruntime::op::Operator::DeleteOperator(aggPartial2);

    // STAGE2: (final)
    std::vector<DataTypePtr> finalOutputTypes { LongType(), DoubleType(), LongType(), LongType(), LongType() };
    auto aggFinalFactory = CreateAggregationOperatorFactory(
        aggFuncTypes,
        std::vector<uint32_t>({ 0, 1, 2, 3, 4 }),
        partialOutputTypes,
        finalOutputTypes,
        std::vector<uint32_t>(),
        false,
        false,
        false
    );
    auto aggFinal = aggFinalFactory->CreateOperator();
    for (uint32_t i = 0; i < result.size(); ++i) {
        aggFinal->AddInput(result[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t tableCount3 = aggFinal->GetOutput(result1);
    EXPECT_EQ(tableCount3, 1);
    EXPECT_EQ(result1[0]->GetRowCount(), 1);
    EXPECT_EQ(result1[0]->GetVectorCount(), 5);

    delete[] input1;
    delete[] input2;
    omniruntime::op::Operator::DeleteOperator(aggFinal);
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

    std::vector<uint32_t> aggFuncTypes = {
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_MAX,
        OMNI_AGGREGATION_TYPE_MIN
    };
    std::vector<DataTypePtr> types = {
        LongType(),    LongType(),    LongType(),    LongType(),    LongType(),
        BooleanType(), BooleanType(), BooleanType(), BooleanType(), BooleanType()
    };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 = CreateVectorBatch(
        sourceTypes, dataSize, data0, data1, data2, data3, data4, data5, data6, data7, data8, data9
    );

    // STAGE1: (partial)
    std::vector<DataTypePtr> partialOutputTypes({
        LongType(),
        LongType(),
        ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }),
        LongType(),
        LongType()
    });
    auto aggPartialFactory = CreateAggregationOperatorFactory(
        aggFuncTypes,
        std::vector<uint32_t>({ 0, 1, 2, 3, 4 }),
        std::vector<DataTypePtr>({ LongType(), LongType(), LongType(), LongType(), LongType() }),
        partialOutputTypes,
        std::vector<uint32_t>({ 5, 6, 7, 8, 9 }),
        true,
        true,
        false
    );
    auto aggPartial = aggPartialFactory->CreateOperator();
    aggPartial->AddInput(vecBatch1);

    std::vector<VectorBatch *> result;
    int32_t tableCount1 = aggPartial->GetOutput(result);
    EXPECT_EQ(tableCount1, 1);

    // STAGE2: (final)
    std::vector<DataTypePtr> finalOutputTypes { LongType(), LongType(), DoubleType(), LongType(), LongType() };
    auto aggFinalFactory = CreateAggregationOperatorFactory(
        aggFuncTypes,
        std::vector<uint32_t>({ 0, 1, 2, 3, 4 }),
        partialOutputTypes,
        finalOutputTypes,
        std::vector<uint32_t>(),
        false,
        false,
        false
    );
    auto aggFinal = aggFinalFactory->CreateOperator();
    for (uint32_t i = 0; i < result.size(); ++i) {
        aggFinal->AddInput(result[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t tableCount = aggFinal->GetOutput(result1);
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
    VectorBatch *expVecBatch1 = CreateVectorBatch(
        resultTypes, resultDataSize, expData0, expData1, expData2, expData3, expData4
    );

    EXPECT_TRUE(VecBatchMatch(result1[0], expVecBatch1));

    omniruntime::op::Operator::DeleteOperator(aggPartial);
    omniruntime::op::Operator::DeleteOperator(aggFinal);
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

    // single stage
    auto aggFactory = CreateAggregationOperatorFactory(
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_AVG }),
        std::vector<uint32_t>({ 0 }),
        aggTypes,
        std::vector<DataTypePtr>({ DoubleType() }),
        std::vector<uint32_t>(),
        true,
        false,
        false
    );
    auto aggregate = aggFactory->CreateOperator();
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

    // single stage
    auto aggFactory = CreateAggregationOperatorFactory(
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX }),
        std::vector<uint32_t>({ 0, 1 }),
        types,
        std::vector<DataTypePtr>({ VarcharType(10), VarcharType(10) }),
        std::vector<uint32_t>(),
        true,
        false,
        false
    );
    auto aggOperator = aggFactory->CreateOperator();
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
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
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
        { LongType(), ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }),
            SUM_IMMEDIATE_VARBINARY, AVG_IMMEDIATE_VARBINARY },
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
        { LongType(), ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }),
            SUM_IMMEDIATE_VARBINARY, AVG_IMMEDIATE_VARBINARY },
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
        { LongType(), ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }),
            SUM_IMMEDIATE_VARBINARY, AVG_IMMEDIATE_VARBINARY },
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
    EXPECT_TRUE(VecBatchMatchIgnoreOrder(resultFromFinal[0], expectVecBatch));

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

    auto aggFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0, 1 }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM }),
        std::vector<uint32_t>({ 2, 3 }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), DoubleType() }),
        std::vector<uint32_t>(),
        true,
        false,
        false
    );
    auto groupBy = aggFactory->CreateOperator();
    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = groupBy->GetOutput(result);
    ASSERT_EQ(vecBatchCount, 1);
    Operator::DeleteOperator(groupBy);
    VectorHelper::FreeVecBatches(result);
    result.clear();
    delete[] input;

    groupTypes[0] = Date32Type();
    groupTypes[1] = Date32Type();
    aggTypes[0] = Date32Type();
    aggTypes[1] = Date32Type();
    input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);
    aggFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0, 1 }),
        std::vector<DataTypePtr>({ Date32DataType::Instance(), Date32DataType::Instance() }),
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM }),
        std::vector<uint32_t>({ 2, 3 }),
        std::vector<DataTypePtr>({ Date32DataType::Instance(), Date32DataType::Instance() }),
        std::vector<DataTypePtr>({ Date32DataType::Instance(), Date32DataType::Instance() }),
        std::vector<uint32_t>(),
        true,
        false,
        false
    );
    groupBy = aggFactory->CreateOperator();
    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }

    vecBatchCount = groupBy->GetOutput(result);
    Operator::DeleteOperator(groupBy);
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

    aggFactory = CreateHashAggregationOperatorFactory(
        std::vector<uint32_t>({ 0 }),
        std::vector<DataTypePtr>({ IntType() }),
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_SUM }),
        std::vector<uint32_t>({ 1 }),
        std::vector<DataTypePtr>({ LongType() }),
        std::vector<DataTypePtr>({ LongType() }),
        std::vector<uint32_t>(),
        true,
        false,
        false
    );
    groupBy = aggFactory->CreateOperator();
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
    result.clear();
}

TEST(AggregatorTest, sum_test)
{
    int32_t rowPerVecBatch = 200;
    auto sumFactory = new SumAggregatorFactory();
    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal3 = { 3 };
    // sum test types : long + decimal + dictionary + null
    auto sumLong = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, true, false, false);
    auto sumShortDecimal = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(SHORT_DECIMAL_TYPE).get()),
        *(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE).get()), channal0, true, false, false);
    auto sumNull = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal3, true, false, false);

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
    EXPECT_EQ(rowPerVecBatch, vecBatch->GetRowCount());

    // process long
    AggregateState state { nullptr };
    sumLong->ProcessGroup(state, vecBatch, 0, vecBatch->GetRowCount());
    EXPECT_EQ(200, *static_cast<int64_t *>(state.val));
    state.Reset();

    // process null
    sumNull->ProcessGroup(state, vecBatch, 0, vecBatch->GetRowCount());
    // use state.count = 0 to determine null in sum
    EXPECT_EQ(0, state.count);
    state.Reset();

    // process short decimal
    sumShortDecimal->ProcessGroup(state, vecBatch, 0, vecBatch->GetRowCount());

    Decimal128 expected = 200L;
    Decimal128 actual = *static_cast<Decimal128 *>(state.val);
    // use state.count < 0 to determine if sum overflow
    EXPECT_TRUE(state.count >= 0);
    EXPECT_EQ(expected, actual);

    state.Reset();

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
    auto countLong = countFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, true, false, false);
    auto countNull = countFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal2, true, false, false);

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
    countLong->ProcessGroup(state, vecBatch, 0, vecBatch->GetRowCount());
    EXPECT_EQ(200, state.count);
    state.Reset();

    // process null
    countNull->ProcessGroup(state, vecBatch, 0, vecBatch->GetRowCount());
    EXPECT_EQ(0, state.count);
    state.Reset();

    VectorHelper::FreeVecBatch(vecBatch);
    delete countFactory;
}

TEST(AggregatorTest, count_all_test)
{
    int32_t rowPerVecBatch = 200;
    auto countAllFactory = new CountAllAggregatorFactory();
    std::vector<int32_t> channal0 = { -1 };

    auto countLong = countAllFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(NoneType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, true, false, false);
    auto countNull = countAllFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(NoneType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, true, false, false);

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
    countLong->ProcessGroup(state, vecBatch, 0, vecBatch->GetRowCount());
    EXPECT_EQ(200, state.count);
    state.Reset();

    // process null
    countNull->ProcessGroup(state, vecBatch, 0, vecBatch->GetRowCount());
    EXPECT_EQ(200, state.count);
    state.Reset();

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
    auto minLong = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, true, false, false);
    auto minDecimal = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE).get()),
        *(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE).get()), channal3, true, false, false);
    auto minVarchar = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(VarcharType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(VarcharType()).get()), channal4, true, false, false);
    auto minNull = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal2, true, false, false);
    auto minIntLong = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(IntType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal5, true, false, false);
    auto minLongInt = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(IntType()).get()), channal0, true, false, false);
    auto minBoolean = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(BooleanType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(BooleanType()).get()), channal6, true, false, false);

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

    std::vector<Vector *> result(1);

    // process long
    AggregateState state { nullptr };
    minLong->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    LongVector longResult(vectorAllocator, 1);
    result.clear();
    result.push_back(&longResult);
    minLong->ExtractValues(state, result, 0);
    EXPECT_EQ(1, longResult.GetValue(0));
    state.Reset();

    // process null
    minNull->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    minNull->ExtractValues(state, result, 0);
    EXPECT_TRUE(longResult.IsValueNull(0));
    state.Reset();

    // process varchar
    minVarchar->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    std::string expectedStr = "1";
    EXPECT_EQ(1, state.count);
    EXPECT_EQ(0, std::memcmp(state.val, expectedStr.c_str(), 1));
    VarcharVector minVarcharOutput(vectorAllocator, 1, 1);
    std::vector<Vector *> minVarcharOutputVector;
    minVarcharOutputVector.push_back(&minVarcharOutput);
    minVarchar->ExtractValues(state, minVarcharOutputVector, 0);
    uint8_t *strRes = nullptr;
    auto len = minVarcharOutput.GetValue(0, &strRes);
    EXPECT_EQ(1, len);
    EXPECT_EQ(0, std::memcmp(strRes, expectedStr.c_str(), 1));
    state.Reset();

    // process int to long
    minIntLong->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    minIntLong->ExtractValues(state, result, 0);
    EXPECT_EQ(1, longResult.GetValue(0));
    state.Reset();

    // process long to int
    minLongInt->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    IntVector intResult(vectorAllocator, 1);
    result.clear();
    result.push_back(& intResult);
    minLongInt->ExtractValues(state, result, 0);
    EXPECT_EQ(1, intResult.GetValue(0));
    state.Reset();

    minBoolean->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    BooleanVector booleanResult(vectorAllocator, 1);
    result.clear();
    result.push_back(&booleanResult);
    minBoolean->ExtractValues(state, result, 0);
    EXPECT_EQ(true, booleanResult.GetValue(0));
    state.Reset();

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
    auto maxLong = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, true, false, false);
    auto maxDecimal = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE).get()),
        *(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE).get()), channal1, true, false, false);
    auto maxVarchar = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(VarcharType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(VarcharType()).get()), channal2, true, false, false);
    auto maxNull = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal4, true, false, false);
    auto maxIntLong = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(IntType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal5, true, false, false);
    auto maxLongInt = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(IntType()).get()), channal0, true, false, false);
    auto maxBoolean = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(BooleanType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(BooleanType()).get()), channal6, true, false, false);

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
    maxLong->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    EXPECT_EQ(1, *static_cast<int64_t *>(state.val));
    state.Reset();

    // process null
    maxNull->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    // use state.count = 0 to determine null
    EXPECT_EQ(0, state.count);
    state.Reset();

    // process varchar
    maxVarchar->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    std::string expectedStr = "1";
    EXPECT_EQ(1, state.count);
    EXPECT_EQ(0, std::memcmp(state.val, expectedStr.c_str(), 1));
    VarcharVector maxVarcharOutput(VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation_maxTest2"), 1,
        1);
    std::vector<Vector *> maxVarcharOutputVector;
    maxVarcharOutputVector.push_back(&maxVarcharOutput);
    maxVarchar->ExtractValues(state, maxVarcharOutputVector, 0);
    uint8_t *strRes = nullptr;
    auto len = maxVarcharOutput.GetValue(0, &strRes);
    EXPECT_EQ(1, len);
    EXPECT_EQ(0, std::memcmp(strRes, expectedStr.c_str(), 1));
    state.Reset();

    // process decimal
    maxDecimal->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    Decimal128 expected(1);
    EXPECT_EQ(expected, *static_cast<Decimal128 *>(state.val));
    state.Reset();

    // process int to long
    maxIntLong->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    EXPECT_EQ(1, *static_cast<int64_t *>(state.val));
    state.Reset();

    // process long to int
    maxLongInt->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    EXPECT_EQ(1, *static_cast<int32_t *>(state.val));
    state.Reset();

    maxBoolean->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    EXPECT_EQ(true, *static_cast<bool *>(state.val));
    state.Reset();

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
    auto avgLong = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal0, true, false, false);
    auto avgDecimal = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE).get()),
        *(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE).get()), channal1, true, false, false);
    auto avgNull = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal3, true, false, false);

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
    avgLong->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    DoubleVector avgLongOutput(VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation_avgTest"), 1);
    std::vector<Vector *> avgLongOutputVector;
    avgLongOutputVector.push_back(&avgLongOutput);
    avgLong->ExtractValues(state, avgLongOutputVector, 0);
    EXPECT_TRUE(avgLongOutput.GetValue(0) - 1 <= DBL_EPSILON);
    state.Reset();

    // process null
    avgNull->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    DoubleVector avgNullOutput(VectorAllocator::GetGlobalAllocator()->NewChildAllocator("aggregation_avgTest"), 1);
    std::vector<Vector *> avgNullOutputVector;
    avgNullOutputVector.push_back(&avgNullOutput);
    avgNull->ExtractValues(state, avgNullOutputVector, 0);
    EXPECT_TRUE(avgNullOutput.IsValueNull(0));
    state.Reset();

    VectorHelper::FreeVecBatch(vectorBatch);
    delete avgFactory;
}

TEST(AggregatorTest, spark_sum_decimal64_normal)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumDeciAggPartial = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal64Type(18, 6)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)).get()), channal0, true, true);

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

    std::vector<Vector *> extractVec = { resultVec, isOverflowVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci18_6Vec);
    vecBatch->SetVector(1, isOverflowVec);

    AggregateState state { nullptr };
    sumDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    sumDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128Wrapper expected1("999999999999.999999");

    EXPECT_EQ(expected1.ToDecimal128(), resultVec->GetValue(0));
    EXPECT_EQ(expected1.ToDecimal128(), resultVec->GetValue(0));
    EXPECT_FALSE(isOverflowVec->GetValue(0));

    sumDeciAggPartial->ProcessGroup(state, vecBatch, 1);
    sumDeciAggPartial->ProcessGroup(state, vecBatch, 2);
    auto sumDeciAggFinal = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)).get()), channal0, false, false);

    sumDeciAggFinal->ExtractValues(state, extractVec, 0);

    Decimal128Wrapper expected2("2999999999999.999997");
    EXPECT_EQ(expected2.ToDecimal128(), resultVec->GetValue(0));
    EXPECT_EQ(expected2.ToDecimal128(), resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete sumFactory;
}

TEST(AggregatorTest, spark_sum_decimal128_normal)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumDeciAggPartial = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(25, 8)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)).get()), channal0, true, true);

    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_decimal128_normal");
    Decimal128 deci("99999999999999999.99999999");
    auto *deci25_8Vec = new Decimal128Vector(vectorAllocator, 3);
    deci25_8Vec->SetValue(0, deci);
    deci25_8Vec->SetValue(1, deci);
    deci25_8Vec->SetValue(2, deci);

    auto *isOverflowVec = new BooleanVector(vectorAllocator, 3);
    isOverflowVec->SetValue(0, false);
    isOverflowVec->SetValue(1, false);
    isOverflowVec->SetValue(2, false);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector *> extractVec = { resultVec, isOverflowVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci25_8Vec);
    vecBatch->SetVector(1, isOverflowVec);

    AggregateState state { nullptr };
    sumDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    sumDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1("99999999999999999.99999999");

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_FALSE(emptyVec->GetValue(0));

    auto sumDeciAggFinal = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)).get()), channal0, false, false);

    sumDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    sumDeciAggFinal->ProcessGroup(state, vecBatch, 2);
    sumDeciAggFinal->ExtractValues(state, extractVec, 0);

    Decimal128Wrapper expected2("299999999999999999.99999997");
    EXPECT_EQ(expected2.ToDecimal128().ToString(), Decimal128Wrapper(resultVec->GetValue(0)).ToString());
    EXPECT_EQ(expected2.ToDecimal128(), resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete sumFactory;
}

TEST(AggregatorTest, spark_sum_decimal128_overflow_throw_exception_when_isOverflowAsNull_is_false)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumDeciAggPartial = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()), channal0, true, true, false);

    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
        "AggregatorTest_spark_sum_decimal128_overflow_throw_exception_when_isOverflowAsNull_is_false");
    Decimal128 deci("99999999999999999999999999999999999999");
    auto *deci38_0Vec = new Decimal128Vector(vectorAllocator, 3);
    deci38_0Vec->SetValue(0, deci);
    deci38_0Vec->SetValue(1, deci);
    deci38_0Vec->SetValue(2, deci);

    auto *isOverflowVec = new BooleanVector(vectorAllocator, 3);
    isOverflowVec->SetValue(0, false);
    isOverflowVec->SetValue(1, false);
    isOverflowVec->SetValue(2, false);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector *> extractVec = { resultVec, isOverflowVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci38_0Vec);
    vecBatch->SetVector(1, isOverflowVec);

    AggregateState state { nullptr };
    sumDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    sumDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1("99999999999999999999999999999999999999");

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));
    EXPECT_FALSE(isOverflowVec->GetValue(0));

    auto sumDeciAggFinal = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()), channal0, false, false, false);

    sumDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    sumDeciAggFinal->ProcessGroup(state, vecBatch, 2);

    bool isThrowException = false;
    try {
        sumDeciAggFinal->ExtractValues(state, extractVec, 0);
    } catch (OmniException &e) {
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
    auto sumDeciAggPartial = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()), channal0, true, true, true);

    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
        "AggregatorTest_spark_sum_decimal128_overflow_return_null_when_isOverflowAsNull_is_true");
    Decimal128 deci("99999999999999999999999999999999999999");
    auto *deci38_0Vec = new Decimal128Vector(vectorAllocator, 3);
    deci38_0Vec->SetValue(0, deci);
    deci38_0Vec->SetValue(1, deci);
    deci38_0Vec->SetValue(2, deci);

    auto *isOverflowVec = new BooleanVector(vectorAllocator, 3);
    isOverflowVec->SetValue(0, false);
    isOverflowVec->SetValue(1, false);
    isOverflowVec->SetValue(2, false);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector *> extractVec = { resultVec, isOverflowVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci38_0Vec);
    vecBatch->SetVector(1, isOverflowVec);

    AggregateState state { nullptr };
    sumDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    sumDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1("99999999999999999999999999999999999999");

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));
    EXPECT_FALSE(isOverflowVec->GetValue(0));

    auto sumDeciAggFinal = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()), channal0, false, false, true);

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
    auto avgDeciAggPartial = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal64Type(18, 6)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)).get()), channal0, true, true);

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

    std::vector<Vector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci18_6Vec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128Wrapper expected1("999999999999.999999");

    EXPECT_EQ(expected1.ToDecimal128().ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));

    avgDeciAggPartial->ProcessGroup(state, vecBatch, 1);
    avgDeciAggPartial->ProcessGroup(state, vecBatch, 2);
    auto avgDeciAggFinal = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(22, 10)).get()), channal0, false, false);

    EXPECT_EQ(3, static_cast<DecimalAverageState *>(state.val)->count);

    avgDeciAggFinal->ExtractValues(state, extractVec, 0);

    Decimal128Wrapper expected2 = Decimal128Wrapper("999999999999.9999990000");
    EXPECT_EQ(expected2.ToDecimal128().ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected2.ToDecimal128(), resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_avg_decimal128_normal)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgDeciAggPartial = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(25, 8)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)).get()), channal0, true, true);

    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_decimal128_normal");
    Decimal128Wrapper deci("99999999999999999.99999999");
    auto *deci25_8Vec = new Decimal128Vector(vectorAllocator, 3);
    deci25_8Vec->SetValue(0, deci.ToDecimal128());
    deci25_8Vec->SetValue(1, deci.ToDecimal128());
    deci25_8Vec->SetValue(2, deci.ToDecimal128());

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci25_8Vec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128Wrapper expected1("99999999999999999.99999999");
    EXPECT_EQ(expected1.ToDecimal128().ToString(), Decimal128Wrapper(resultVec->GetValue(0)).ToString());
    EXPECT_EQ(expected1.ToDecimal128(), resultVec->GetValue(0));

    auto avgDeciAggFinal = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(29, 12)).get()), channal0, false, false);

    avgDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    avgDeciAggFinal->ProcessGroup(state, vecBatch, 2);
    avgDeciAggFinal->ExtractValues(state, extractVec, 0);

    Decimal128Wrapper expected2("99999999999999999.999999990000");
    EXPECT_EQ(expected2.ToDecimal128().ToString(), Decimal128Wrapper(resultVec->GetValue(0)).ToString());
    EXPECT_EQ(expected2.ToDecimal128(), resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_avg_decimal128_overflow_throw_exception_when_isOverflowAsNull_is_false)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgDeciAggPartial = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()), channal0, true, true, false);

    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
        "AggregatorTest_spark_avg_decimal128_overflow_throw_exception_when_isOverflowAsNull_is_false");
    Decimal128 deci("99999999999999999999999999999999999999");
    auto *deci38_0Vec = new Decimal128Vector(vectorAllocator, 3);
    deci38_0Vec->SetValue(0, deci);
    deci38_0Vec->SetValue(1, deci);
    deci38_0Vec->SetValue(2, deci);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci38_0Vec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1("99999999999999999999999999999999999999");

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));

    auto avgDeciAggFinal = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 4)).get()), channal0, false, false, false);

    avgDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    avgDeciAggFinal->ProcessGroup(state, vecBatch, 2);

    bool isThrowException = false;
    try {
        avgDeciAggFinal->ExtractValues(state, extractVec, 0);
    } catch (OmniException &e) {
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
    auto avgDeciAggPartial = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()), channal0, true, true, true);

    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
        "AggregatorTest_spark_avg_decimal128_overflow_return_null_when_isOverflowAsNull_is_true");
    Decimal128 deci("99999999999999999999999999999999999999");
    auto *deci38_0Vec = new Decimal128Vector(vectorAllocator, 3);
    deci38_0Vec->SetValue(0, deci);
    deci38_0Vec->SetValue(1, deci);
    deci38_0Vec->SetValue(2, deci);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci38_0Vec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1("99999999999999999999999999999999999999");

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));

    auto avgDeciAggFinal = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 4)).get()), channal0, false, false, true);

    avgDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    avgDeciAggFinal->ProcessGroup(state, vecBatch, 2);
    avgDeciAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_TRUE(resultVec->IsValueNull(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_avg_decimal128_count_cast_to_wider_type_overflow_return_null_when_isOverflowAsNull_is_true)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgDeciAggPartial = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 38)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 38)).get()), channal0, true, true, true);

    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
        "spark_avg_decimal128_count_cast_to_wider_type_overflow_return_null_when_isOverflowAsNull_is_true");

    Decimal128Wrapper deci1("-0.99999999999999999999999999999999999999");
    Decimal128Wrapper deci2("0.14159265354378240000000000000000000000");
    Decimal128Wrapper deci3("0.00000000000000000000000000000000000000");

    auto *deci38_38Vec = new Decimal128Vector(vectorAllocator, 3);
    deci38_38Vec->SetValue(0, deci1.ToDecimal128());
    deci38_38Vec->SetValue(1, deci2.ToDecimal128());
    deci38_38Vec->SetValue(2, deci3.ToDecimal128());

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci38_38Vec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    auto avgDeciAggFinal = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 38)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 38)).get()), channal0, false, false, true);

    avgDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    avgDeciAggFinal->ProcessGroup(state, vecBatch, 2);
    avgDeciAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_TRUE(resultVec->IsValueNull(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_avg_decimal128_normal_when_inputRaw_is_true_and_outputPartial_is_false)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgDeciWindow = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(22, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(26, 4)).get()), channal0, true, false, true);

    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
        "spark_avg_decimal128_normal_when_inputRaw_is_true_and_outputPartial_is_false");

    Decimal128Wrapper deci1("1234567890123456789012");
    Decimal128Wrapper deci2("9999999999999999999999");

    auto *deci22_0Vec = new Decimal128Vector(vectorAllocator, 2);
    deci22_0Vec->SetValue(0, deci1.ToDecimal128());
    deci22_0Vec->SetValue(1, deci2.ToDecimal128());

    auto *avgCountVec = new LongVector(vectorAllocator, 2);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);

    auto *resultVec = new Decimal128Vector(vectorAllocator, 1);

    std::vector<Vector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, deci22_0Vec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgDeciWindow->ProcessGroup(state, vecBatch, 0);
    avgDeciWindow->ProcessGroup(state, vecBatch, 1);
    avgDeciWindow->ExtractValues(state, extractVec, 0);

    Decimal128 expected("5617283945061728394505.5000");

    EXPECT_EQ(expected.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_sum_short_normal)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumShortAggPartial = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(ShortType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, true, true);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_short_normal");

    auto *shortVec = new ShortVector(vectorAllocator, 3);
    shortVec->SetValue(0, 12345);
    shortVec->SetValue(1, 23451);
    shortVec->SetValue(2, 12345);

    auto *resultVec = new LongVector(vectorAllocator, 1);
    std::vector<Vector *> extractVec = { resultVec };

    auto *vecBatch = new VectorBatch(1);
    vecBatch->SetVector(0, shortVec);

    AggregateState state { nullptr };
    sumShortAggPartial->InitiateGroup(state, vecBatch, 0);
    sumShortAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(12345, resultVec->GetValue(0));

    sumShortAggPartial->ProcessGroup(state, vecBatch, 1);
    sumShortAggPartial->ProcessGroup(state, vecBatch, 2);

    auto sumShortAggFinal = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, false, false);
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
    auto sumIntAggPartial = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(IntType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, true, true);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_int_normal");

    auto *intVec = new IntVector(vectorAllocator, 3);
    intVec->SetValue(0, 1234567890);
    intVec->SetValue(1, 2045678901);
    intVec->SetValue(2, 1234567890);

    auto *resultVec = new LongVector(vectorAllocator, 1);
    std::vector<Vector *> extractVec = { resultVec };

    auto *vecBatch = new VectorBatch(1);
    vecBatch->SetVector(0, intVec);

    AggregateState state { nullptr };
    sumIntAggPartial->InitiateGroup(state, vecBatch, 0);
    sumIntAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(1234567890, resultVec->GetValue(0));

    sumIntAggPartial->ProcessGroup(state, vecBatch, 1);
    sumIntAggPartial->ProcessGroup(state, vecBatch, 2);

    auto sumIntAggFinal = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, false, false);
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
    auto sumLongAggPartial = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, true, true);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_long_normal");

    auto *longVec = new LongVector(vectorAllocator, 3);
    longVec->SetValue(0, 1234567890123456789);
    longVec->SetValue(1, 2345678901234567891);
    longVec->SetValue(2, 3456789012345678912);

    auto *resultVec = new LongVector(vectorAllocator, 1);
    std::vector<Vector *> extractVec = { resultVec };

    auto *vecBatch = new VectorBatch(1);
    vecBatch->SetVector(0, longVec);

    AggregateState state { nullptr };
    sumLongAggPartial->InitiateGroup(state, vecBatch, 0);
    sumLongAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(1234567890123456789, resultVec->GetValue(0));

    sumLongAggPartial->ProcessGroup(state, vecBatch, 1);
    sumLongAggPartial->ProcessGroup(state, vecBatch, 2);

    auto sumLongAggFinal = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, false, false);
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
    auto sumLongAggPartial = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, true, true);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_long_overflow");

    auto *longVec = new LongVector(vectorAllocator, 3);
    longVec->SetValue(0, 9223372036854774807);
    longVec->SetValue(1, 9223372036854774807);
    longVec->SetValue(2, 9223372036854774807);

    auto *resultVec = new LongVector(vectorAllocator, 1);
    std::vector<Vector *> extractVec = { resultVec };

    auto *vecBatch = new VectorBatch(1);
    vecBatch->SetVector(0, longVec);

    AggregateState state { nullptr };
    sumLongAggPartial->InitiateGroup(state, vecBatch, 0);
    sumLongAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(9223372036854774807, resultVec->GetValue(0));

    sumLongAggPartial->ProcessGroup(state, vecBatch, 1);
    sumLongAggPartial->ProcessGroup(state, vecBatch, 2);

    auto sumLongAggFinal = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, false, false);
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
    auto sumDoubleAggPartial = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal0, true, true);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_sum_double_normal");

    auto *doubleVec = new DoubleVector(vectorAllocator, 3);
    doubleVec->SetValue(0, 123456789012.3456789);
    doubleVec->SetValue(1, 234567890123.4567891);
    doubleVec->SetValue(2, 345678901234.5678912);

    auto *resultVec = new DoubleVector(vectorAllocator, 1);
    std::vector<Vector *> extractVec = { resultVec };

    auto *vecBatch = new VectorBatch(1);
    vecBatch->SetVector(0, doubleVec);

    AggregateState state { nullptr };
    sumDoubleAggPartial->InitiateGroup(state, vecBatch, 0);
    sumDoubleAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(123456789012.3456789, resultVec->GetValue(0));

    sumDoubleAggPartial->ProcessGroup(state, vecBatch, 1);
    sumDoubleAggPartial->ProcessGroup(state, vecBatch, 2);

    auto sumDoubleAggFinal = sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal0, false, false);
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
    auto avgShortAggPartial = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(ShortType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal0, true, true);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_short_normal");

    auto *shortVec = new ShortVector(vectorAllocator, 3);
    shortVec->SetValue(0, 12345);
    shortVec->SetValue(1, 23451);
    shortVec->SetValue(2, 12345);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new DoubleVector(vectorAllocator, 1);
    std::vector<Vector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, shortVec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgShortAggPartial->InitiateGroup(state, vecBatch, 0);
    avgShortAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(12345.0, resultVec->GetValue(0));

    avgShortAggPartial->ProcessGroup(state, vecBatch, 1);
    avgShortAggPartial->ProcessGroup(state, vecBatch, 2);

    auto avgShortAggFinal = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal0, false, false);
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
    auto avgIntAggPartial = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(IntType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal0, true, true);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_int_normal");

    auto *intVec = new IntVector(vectorAllocator, 3);
    intVec->SetValue(0, 1234567890);
    intVec->SetValue(1, 2045678901);
    intVec->SetValue(2, 1234567890);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new DoubleVector(vectorAllocator, 1);
    std::vector<Vector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, intVec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgIntAggPartial->InitiateGroup(state, vecBatch, 0);
    avgIntAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(1234567890.0, resultVec->GetValue(0));

    avgIntAggPartial->ProcessGroup(state, vecBatch, 1);
    avgIntAggPartial->ProcessGroup(state, vecBatch, 2);

    auto avgIntAggFinal = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal0, false, false);
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
    auto avgLongAggPartial = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal0, true, true);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_long_normal");

    auto *longVec = new LongVector(vectorAllocator, 3);
    longVec->SetValue(0, 9223372036854774807L);
    longVec->SetValue(1, 9223372036854774807L);
    longVec->SetValue(2, 9223372036854774807L);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new DoubleVector(vectorAllocator, 1);
    std::vector<Vector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, longVec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgLongAggPartial->InitiateGroup(state, vecBatch, 0);
    avgLongAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(9.2233720368547748E18, resultVec->GetValue(0));

    avgLongAggPartial->ProcessGroup(state, vecBatch, 1);
    avgLongAggPartial->ProcessGroup(state, vecBatch, 2);

    auto avgLongAggFinal = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal0, false, false);
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
    auto avgDoubleAggPartial = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal0, true, true);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("AggregatorTest_spark_avg_double_normal");

    auto *doubleVec = new DoubleVector(vectorAllocator, 3);
    doubleVec->SetValue(0, 123456789012.3456789);
    doubleVec->SetValue(1, 234567890123.4567891);
    doubleVec->SetValue(2, 345678901234.5678912);

    auto *avgCountVec = new LongVector(vectorAllocator, 3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new DoubleVector(vectorAllocator, 1);
    std::vector<Vector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, doubleVec);
    vecBatch->SetVector(1, avgCountVec);

    AggregateState state { nullptr };
    avgDoubleAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDoubleAggPartial->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(123456789012.3456789, resultVec->GetValue(0));

    avgDoubleAggPartial->ProcessGroup(state, vecBatch, 1);
    avgDoubleAggPartial->ProcessGroup(state, vecBatch, 2);

    auto avgDoubleAggFinal = avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal0, false, false);
    avgDoubleAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_EQ(2.345678601234568E11, resultVec->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

// first basic function:  first with ignore null
TEST(AggregatorTest, first_short_ignorenull_test)
{
    auto firstIgnoreNullFactory = new FirstAggregatorFactory(OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL);
    std::vector<int32_t> channal0 = { 0 };
    auto firstIgnoreNullIntAggPartial =
        firstIgnoreNullFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(ShortType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(ShortType()).get()), channal0, true, true);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("first_short_ignorenull_test");

    auto *inputShortVec1 = new ShortVector(vectorAllocator, 5);
    for (int i = 0; i < 5; i++) {
        inputShortVec1->SetValueNull(i);
    }

    auto *vecBatch1 = new VectorBatch(1);
    vecBatch1->SetVector(0, inputShortVec1);

    auto *resultfirstVec1 = new ShortVector(vectorAllocator, 1);
    auto *resultValueSetVec1 = new BooleanVector(vectorAllocator, 1);
    std::vector<Vector *> extractVecs = { resultfirstVec1, resultValueSetVec1 };

    AggregateState state { nullptr };

    // add first VectorBatch
    firstIgnoreNullIntAggPartial->InitiateGroup(state, vecBatch1, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 1);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 2);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 3);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 4);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    // add second VectorBatch,keep value not change
    auto *inputShortVec2 = new ShortVector(vectorAllocator, 3);
    inputShortVec2->SetValueNull(0);
    inputShortVec2->SetValue(1, 211);
    inputShortVec2->SetValueNull(2);
    auto *vecBatch2 = new VectorBatch(1);
    vecBatch2->SetVector(0, inputShortVec2);

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch2, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch2, 1);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(211, resultfirstVec1->GetValue(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch2, 2);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(211, resultfirstVec1->GetValue(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch1);
    VectorHelper::FreeVecBatch(vecBatch2);
    delete resultfirstVec1;
    delete resultValueSetVec1;
    delete firstIgnoreNullFactory;
}

// first basic function:  first with ignore null
TEST(AggregatorTest, first_int_ignorenull_test)
{
    auto firstIgnoreNullFactory = new FirstAggregatorFactory(OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL);
    std::vector<int32_t> channal0 = { 0 };
    auto firstIgnoreNullIntAggPartial =
        firstIgnoreNullFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(IntType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(IntType()).get()), channal0, true, true);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("first_int_ignorenull_test");

    auto *inputIntVec1 = new IntVector(vectorAllocator, 5);
    for (int i = 0; i < 5; i++) {
        inputIntVec1->SetValueNull(i);
    }

    auto *vecBatch1 = new VectorBatch(1);
    vecBatch1->SetVector(0, inputIntVec1);

    auto *resultfirstVec1 = new IntVector(vectorAllocator, 1);
    auto *resultValueSetVec1 = new BooleanVector(vectorAllocator, 1);
    std::vector<Vector *> extractVecs = { resultfirstVec1, resultValueSetVec1 };

    AggregateState state { nullptr };

    // add first VectorBatch
    firstIgnoreNullIntAggPartial->InitiateGroup(state, vecBatch1, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 1);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 2);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 3);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 4);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    // add second VectorBatch,keep value not change
    auto *inputIntVec2 = new IntVector(vectorAllocator, 3);
    inputIntVec2->SetValueNull(0);
    inputIntVec2->SetValue(1, 211);
    inputIntVec2->SetValueNull(2);
    auto *vecBatch2 = new VectorBatch(1);
    vecBatch2->SetVector(0, inputIntVec2);

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch2, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch2, 1);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(211, resultfirstVec1->GetValue(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch2, 2);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_EQ(211, resultfirstVec1->GetValue(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch1);
    VectorHelper::FreeVecBatch(vecBatch2);
    delete resultfirstVec1;
    delete resultValueSetVec1;
    delete firstIgnoreNullFactory;
}

// first basic function:  first include null
TEST(AggregatorTest, first_int_includenull_test)
{
    auto firstWithNullFactory = new FirstAggregatorFactory(OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL);
    std::vector<int32_t> channal0 = { 0 };
    auto firstWithNullIntAggPartial =
        firstWithNullFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(IntType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(IntType()).get()), channal0, true, true);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("first_int_includenull_test");

    auto *inputIntVec1 = new IntVector(vectorAllocator, 5);
    inputIntVec1->SetValueNull(0);
    inputIntVec1->SetValueNull(1);
    inputIntVec1->SetValue(2, 111);
    inputIntVec1->SetValue(3, 113);
    inputIntVec1->SetValueNull(4);

    auto *vecBatch1 = new VectorBatch(1);
    vecBatch1->SetVector(0, inputIntVec1);

    auto *resultfirstVec1 = new IntVector(vectorAllocator, 1);
    auto *resultValueSetVec1 = new BooleanVector(vectorAllocator, 1);
    std::vector<Vector *> extractVecs = { resultfirstVec1, resultValueSetVec1 };

    AggregateState state { nullptr };
    firstWithNullIntAggPartial->InitiateGroup(state, vecBatch1, 0);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    firstWithNullIntAggPartial->ProcessGroup(state, vecBatch1, 1);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    firstWithNullIntAggPartial->ProcessGroup(state, vecBatch1, 2);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    firstWithNullIntAggPartial->ProcessGroup(state, vecBatch1, 3);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    firstWithNullIntAggPartial->ProcessGroup(state, vecBatch1, 4);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch1);
    delete resultfirstVec1;
    delete resultValueSetVec1;
    delete firstWithNullFactory;
}

// first agg function for 2 steps partial  + final
TEST(AggregatorTest, first_int_ignorenull_2steps_test)
{
    auto firstIgnoreNullFactory = new FirstAggregatorFactory(OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL);

    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("first_int_ignorenull_2steps_test");

    std::vector<int32_t> channal0 = { 0 };
    auto firstIgnoreNullIntAggPartial =
        firstIgnoreNullFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, true, true);

    auto *inputLongVec1 = new LongVector(vectorAllocator, 2);
    inputLongVec1->SetValueNull(0);
    inputLongVec1->SetValueNull(1);

    auto *vecBatch1 = new VectorBatch(1);
    vecBatch1->SetVector(0, inputLongVec1);

    auto *resultfirstVec1 = new LongVector(vectorAllocator, 1);
    auto *resultValueSetVec1 = new BooleanVector(vectorAllocator, 1);
    std::vector<Vector *> extractVecs1 = { resultfirstVec1, resultValueSetVec1 };

    AggregateState state { nullptr };

    // add first VectorBatch
    firstIgnoreNullIntAggPartial->InitiateGroup(state, vecBatch1, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs1, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 1);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs1, 0);
    EXPECT_TRUE(resultfirstVec1->IsValueNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    std::vector<DataTypePtr> vector;
    vector.push_back(LongType());
    vector.push_back(BooleanType());
    DataTypesPtr inputTypes = std::make_unique<DataTypes>(vector);

    std::vector<int32_t> channal2 = { 0, 1 };
    auto firstIgnoreNullIntAggFinal = firstIgnoreNullFactory->CreateAggregator(*inputTypes.get(),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal2, false, false);

    auto *inputLongVec2 = new LongVector(vectorAllocator, 4);
    inputLongVec2->SetValueNull(0);
    inputLongVec2->SetValue(1, 111);
    inputLongVec2->SetValueNull(2);
    inputLongVec2->SetValueNull(3);

    auto *inputBooleanVec2 = new BooleanVector(vectorAllocator, 4);
    inputBooleanVec2->SetValue(0, false);
    inputBooleanVec2->SetValue(1, true);
    inputBooleanVec2->SetValue(2, false);
    inputBooleanVec2->SetValue(3, false);

    auto *vecBatch2 = new VectorBatch(2);
    vecBatch2->SetVector(0, inputLongVec2);
    vecBatch2->SetVector(1, inputBooleanVec2);

    auto *resultfirstVec2 = new LongVector(vectorAllocator, 1);
    std::vector<Vector *> extractVecs2 = { resultfirstVec2 };

    firstIgnoreNullIntAggFinal->InitiateGroup(state, vecBatch2, 0);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_TRUE(resultfirstVec2->IsValueNull(0));

    firstIgnoreNullIntAggFinal->ProcessGroup(state, vecBatch2, 1);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_FALSE(resultfirstVec2->IsValueNull(0));
    EXPECT_EQ(111, resultfirstVec2->GetValue(0));

    firstIgnoreNullIntAggFinal->ProcessGroup(state, vecBatch2, 2);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_FALSE(resultfirstVec2->IsValueNull(0));
    EXPECT_EQ(111, resultfirstVec2->GetValue(0));

    firstIgnoreNullIntAggFinal->ProcessGroup(state, vecBatch2, 3);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_FALSE(resultfirstVec2->IsValueNull(0));
    EXPECT_EQ(111, resultfirstVec2->GetValue(0));


    firstIgnoreNullIntAggFinal->ProcessGroup(state, vecBatch2, 4);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_FALSE(resultfirstVec2->IsValueNull(0));
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