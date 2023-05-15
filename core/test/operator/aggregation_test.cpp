/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2023. All rights reserved.
 */

#include <vector>
#include <iostream>
#include <thread>
#include <cstdlib>
#include <mutex>
#include <cstdarg>
#include <gtest/gtest.h>
#include "operator/aggregation/aggregator/all_aggregators.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "operator/operator.h"
#include "vector/vector_helper.h"
#include "util/perf_util.h"
#include "util/config_util.h"
#include "util/test_util.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"
#include "type/decimal128.h"

namespace omniruntime {
using namespace omniruntime::vec;
using namespace omniruntime::op;
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

#ifdef DISABLE_TEST_NO_NEED_OCCUPY_BRANCH_TEST
long lrand()
{
    const int CONST_VALUE_8 = 8;
    if (sizeof(int) < sizeof(long)) {
        return (static_cast<long>(rand())) << (sizeof(int) * CONST_VALUE_8) | rand();
    }
    return rand();
}
#endif

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

std::vector<VectorBatch *> ConstructSimpleBuildData(int32_t vecBatchCnt, int32_t rowPerBatch)
{
    const int32_t mod = 3;
    std::vector<VectorBatch *> input(vecBatchCnt);
    for (int32_t i = 0; i < vecBatchCnt; ++i) {
        auto *vecBatch = new VectorBatch(rowPerBatch);
        auto *col1 = new Vector<int64_t>(rowPerBatch);
        auto *col2 = new Vector<int32_t>(rowPerBatch);
        auto *col3 = new Vector<short>(rowPerBatch);
        auto *col4 = new Vector<double>(rowPerBatch);

        for (int32_t j = 0; j < rowPerBatch; ++j) {
            col1->SetValue(j, j % mod);
            col2->SetValue(j, j % mod);
            col3->SetValue(j, j % mod);
            col4->SetValue(j, j % mod);
        }

        vecBatch->Append(col1);
        vecBatch->Append(col1->Slice(0, rowPerBatch).release());
        vecBatch->Append(col2);
        vecBatch->Append(col3);
        vecBatch->Append(col4);

        input[i] = vecBatch;
    }
    return input;
}

BaseVector *BuildHashInput(const DataTypePtr groupType, int32_t rowPerVecBatch, int32_t cardinality)
{
    switch (groupType->GetId()) {
        case OMNI_INT:
        case OMNI_DATE32: {
            Vector<int32_t> *col = new Vector<int32_t>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                if (cardinality != 0) {
                    col->SetValue(j, j % cardinality);
                }
            }
            return col;
        }
        case OMNI_SHORT: {
            Vector<short> *col = new Vector<short>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                if (cardinality != 0) {
                    col->SetValue(j, j % cardinality);
                }
            }
            return col;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            Vector<int64_t> *col = new Vector<int64_t>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                if (cardinality != 0) {
                    col->SetValue(j, j % cardinality);
                }
            }
            return col;
        }
        case OMNI_DOUBLE: {
            Vector<double> *col = new Vector<double>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, j % cardinality);
            }
            return col;
        }
        case OMNI_BOOLEAN: {
            Vector<bool> *col = new Vector<bool>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, j % cardinality);
            }
            return col;
        }
        case OMNI_DECIMAL128: {
            Vector<Decimal128> *col = new Vector<Decimal128>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                Decimal128 val = Decimal128(0, j % cardinality);
                col->SetValue(j, val);
            }
            return col;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            Vector<LargeStringContainer<std::string_view>> *col =
                new Vector<LargeStringContainer<std::string_view>>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                std::string str = std::to_string(j % cardinality);
                std::string_view strV(str.c_str(), str.size());
                col->SetValue(j, strV);
            }
            return col;
        }
        default: {
            LogError("No such %d type support", groupType->GetId());
            return nullptr;
        }
    }
}

BaseVector *BuildAggregateInput(const DataTypePtr aggType, int32_t rowPerVecBatch)
{
    switch (aggType->GetId()) {
        case OMNI_NONE: {
            Vector<int64_t> *col = new Vector<int64_t>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetNull(j);
            }
            return col;
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            Vector<int32_t> *col = new Vector<int32_t>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_SHORT: {
            Vector<short> *col = new Vector<short>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            Vector<int64_t> *col = new Vector<int64_t>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_DOUBLE: {
            Vector<double> *col = new Vector<double>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_BOOLEAN: {
            Vector<bool> *col = new Vector<bool>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, true);
            }
            return col;
        }
        case OMNI_DECIMAL128: {
            Vector<Decimal128> *col = new Vector<Decimal128>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                Decimal128 val(0, 1);
                col->SetValue(j, val);
            }
            return col;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            Vector<LargeStringContainer<std::string_view>> *col =
                new Vector<LargeStringContainer<std::string_view>>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                std::string str = std::to_string(j);
                std::string_view strV(str.c_str(), str.size());
                col->SetValue(j, strV);
            }
            return col;
        }
        default: {
            LogError("No such %d type support", aggType->GetId());
            return nullptr;
        }
    }
}

VectorBatch **BuildAggInput(int32_t vecBatchNum, int32_t rowPerVecBatch, int32_t cardinality, int32_t groupColNum,
    int32_t aggColNum, const std::vector<DataTypePtr> &groupTypes, const std::vector<DataTypePtr> &aggTypes)
{
    VectorBatch **input = new VectorBatch *[vecBatchNum];
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        VectorBatch *vecBatch = new VectorBatch(rowPerVecBatch);
        for (int32_t index = 0; index < groupColNum; ++index) {
            BaseVector *vec = BuildHashInput(groupTypes[index], rowPerVecBatch, cardinality);
            vecBatch->Append(vec);
        }
        for (int32_t index = 0; index < aggColNum; ++index) {
            BaseVector *vec = BuildAggregateInput(aggTypes[index], rowPerVecBatch);
            vecBatch->Append(vec);
        }
        input[i] = vecBatch;
    }
    return input;
}

VectorBatch **BuildVarCharInput(int32_t vecBatchNum, int32_t colNum, int32_t rowPerVecBatch, ...)
{
    va_list args;
    va_start(args, rowPerVecBatch);
    VectorBatch **input = new VectorBatch *[vecBatchNum];
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        VectorBatch *vecBatch = new VectorBatch(rowPerVecBatch);
        for (int32_t c = 0; c < colNum; ++c) {
            Vector<LargeStringContainer<std::string_view>> *col =
                new Vector<LargeStringContainer<std::string_view>>(rowPerVecBatch);
            std::string *values = va_arg(args, std::string *);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                std::string_view strV(values[j].c_str(), values[j].length());
                col->SetValue(j, strV);
            }
            vecBatch->Append(col);
        }
        input[i] = vecBatch;
    }
    va_end(args);
    return input;
}

#ifdef DISABLE_TEST_NO_NEED_OCCUPY_BRANCH_TEST
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
#endif

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
#ifdef DISABLE_TEST_NO_NEED_OCCUPY_BRANCH_TEST
void PerfTestOriginal(int64_t moduleAddr, VectorBatch **input, std::vector<DataTypePtr> allTypes)
{
    // create operator
    HashAggregationOperatorFactory *nativeOperatorFactory =
        reinterpret_cast<HashAggregationOperatorFactory *>(moduleAddr);
    auto groupBy = nativeOperatorFactory->CreateOperator();

    // execution
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        auto copiedBatch = DuplicateVectorBatch(input[i], allTypes);
        groupBy->AddInput(copiedBatch);
    }
    VectorBatch *outputVecBatch = nullptr;
    int32_t vecBatchCount = groupBy->GetOutput(&outputVecBatch);
    EXPECT_EQ(vecBatchCount, 1);
    EXPECT_EQ(outputVecBatch->GetVectorCount(), 4);
    EXPECT_EQ(outputVecBatch->GetRowCount(), 4);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(groupBy);
}

void PerfTest(int64_t moduleAddr, VectorBatch **input, int32_t vecBatchNum, int32_t *rowCount,
    std::vector<DataTypePtr> allTypes)
{
    // create operatory
    HashAggregationOperatorFactory *nativeOperatorFactory =
        reinterpret_cast<HashAggregationOperatorFactory *>(moduleAddr);
    auto groupBy = nativeOperatorFactory->CreateOperator();

    // execution
    for (int pageIndex = 0; pageIndex < vecBatchNum; ++pageIndex) {
        auto copiedBatch = DuplicateVectorBatch(input[pageIndex], allTypes);
        auto errNo = groupBy->AddInput(copiedBatch);
        EXPECT_EQ(errNo, OMNI_STATUS_NORMAL);
    }
    VectorBatch *outputVecBatch = nullptr;
    int32_t vecBatchCount = groupBy->GetOutput(&outputVecBatch);
    EXPECT_EQ(vecBatchCount, 1);
    EXPECT_EQ(outputVecBatch->GetVectorCount(), 4);
    EXPECT_EQ(outputVecBatch->GetRowCount(), 4);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(groupBy);
}

void PerfTestNonGroup(int64_t moduleAddr, bool codegenMode, VectorBatch **input, int32_t vecBatchNum, int32_t *rowCount,
    std::vector<type::DataTypePtr> allTypes)
{
    // create operatory
    auto nativeOperatorFactory = reinterpret_cast<AggregationOperatorFactory *>(moduleAddr);
    omniruntime::op::Operator *aggregation = nullptr;
    if (codegenMode) {
        aggregation = CreateTestOperator(nativeOperatorFactory);
    } else {
        aggregation = nativeOperatorFactory->CreateOperator();
    }

    // execution
    for (int pageIndex = 0; pageIndex < vecBatchNum; ++pageIndex) {
        auto copiedBatch = DuplicateVectorBatch(input[pageIndex], allTypes);
        auto errNo = aggregation->AddInput(copiedBatch);
        EXPECT_EQ(errNo, OMNI_STATUS_NORMAL);
    }
    VectorBatch *outputVecBatch = nullptr;
    int32_t vecBatchCount = aggregation->GetOutput(&outputVecBatch);
    EXPECT_EQ(vecBatchCount, 1);
    EXPECT_EQ(outputVecBatch->GetVectorCount(), 4);
    EXPECT_EQ(outputVecBatch->GetRowCount(), 1);
    VectorHelper::FreeVecBatch(outputVecBatch);
}
#endif

std::unique_ptr<HashAggregationOperatorFactory> CreateHashAggregationOperatorFactory(
    const std::vector<uint32_t> &groupByColumns_, const std::vector<DataTypePtr> &groupTypes,
    const std::vector<uint32_t> &aggFuncTypes_, const std::vector<uint32_t> &aggInputCols,
    const std::vector<DataTypePtr> &aggInputTypes, const std::vector<DataTypePtr> &aggOutputTypes,
    const std::vector<uint32_t> &aggMask_, const bool inputRaw, const bool outputPartial, const bool nullWhenOverflow)
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
    auto hashAggOpFactory = std::make_unique<HashAggregationOperatorFactory>(groupByColumns, DataTypes(groupTypes),
        aggInputColsWrap, aggInputTypesWrap, aggOutputTypesWrap, aggFuncTypes, aggMask, inputRawWrap, outputPartialWrap,
        nullWhenOverflow);
    hashAggOpFactory->Init();
    return hashAggOpFactory;
}

std::unique_ptr<AggregationOperatorFactory> CreateAggregationOperatorFactory(const std::vector<uint32_t> &aggFuncTypes_,
    const std::vector<uint32_t> &aggInputCols, const std::vector<DataTypePtr> &aggInputTypes,
    const std::vector<DataTypePtr> &aggOutputTypes, const std::vector<uint32_t> &aggMask_, const bool inputRaw,
    const bool outputPartial, const bool nullWhenOverflow)
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
    auto aggOpFactory = std::make_unique<AggregationOperatorFactory>(inputTypes, aggFuncTypes, aggInputColsWrap,
        aggMask, aggOutputTypesWrap, inputRawWrap, outputPartialWrap, nullWhenOverflow);
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
    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX };
    std::vector<DataTypePtr> groupTypes = { LongType(), LongType() };
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType(), LongType(), LongType(), LongType() };
    VectorBatch **input1 = BuildAggInput(vecBatchNum, rowSize, cardinality, 2, 5, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    // First stage (partial)
    auto aggPartialFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0, 1 }),
        std::vector<DataTypePtr>({ LongType(), LongType() }), aggFuncTypes, std::vector<uint32_t>({ 2, 3, 4, 5, 6 }),
        std::vector<DataTypePtr>({ LongType(), LongType(), LongType(), LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }),
        LongType(), LongType(), LongType() }),
        std::vector<uint32_t>(), true, true, false);

    // operator 1 (partial)
    auto aggPartial1 = aggPartialFactory->CreateOperator();
    aggPartial1->Init();
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggPartial1->AddInput(input1[i]);
    }

    VectorBatch *outputVecBatch1 = nullptr;
    int32_t vecBatchCount = aggPartial1->GetOutput(&outputVecBatch1);
    EXPECT_EQ(vecBatchCount, 1);
    op::Operator::DeleteOperator(aggPartial1);

    VectorBatch **input2 = BuildAggInput(vecBatchNum, rowSize, cardinality, 2, 5, groupTypes, aggTypes);
    if (input2 == nullptr) {
        return;
    }

    // operator 2 (partial)
    auto aggPartial2 = aggPartialFactory->CreateOperator();
    aggPartial2->Init();
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggPartial2->AddInput(input2[i]);
    }

    VectorBatch *outputVecBatch2 = nullptr;
    int32_t tableCount2 = aggPartial2->GetOutput(&outputVecBatch2);
    EXPECT_EQ(tableCount2, 1);
    op::Operator::DeleteOperator(aggPartial2);

    // Second stage (final)
    auto aggFinalFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0, 1 }),
        std::vector<DataTypePtr>({ LongType(), LongType() }), aggFuncTypes, std::vector<uint32_t>({ 2, 3, 4, 5, 6 }),
        std::vector<DataTypePtr>({ LongType(), ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }),
        LongType(), LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), DoubleType(), LongType(), LongType(), LongType() }),
        std::vector<uint32_t>(), false, false, false);

    auto aggFinal = aggFinalFactory->CreateOperator();
    aggFinal->Init();

    aggFinal->AddInput(outputVecBatch1);
    aggFinal->AddInput(outputVecBatch2);

    VectorBatch *outputVecBatch3 = nullptr;
    aggFinal->GetOutput(&outputVecBatch3);
    op::Operator::DeleteOperator(aggFinal);

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

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch3, expectVecBatch, expectFieldTypes));
    EXPECT_EQ(outputVecBatch3->GetVectorCount(), 7);

    delete[] input1;
    delete[] input2;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch3);
}

TEST(HashAggregationOperatorTest, verify_varchar_vector_correctness)
{
    // create 10 pages
    const int vecBatchNum = 1;
    const int rowSize = 8;
    const int columnCount = 4; // groupby + count + min + max
    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_MIN,
        OMNI_AGGREGATION_TYPE_MAX };
    std::string aggNames[] = {"group", "count", "min", "max" };
    std::string data0[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data1[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data2[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data3[8] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1", "1.1", "1"};
    VectorBatch **input = BuildVarCharInput(vecBatchNum, columnCount, rowSize, data0, data1, data2, data3);

    // single stage
    auto aggFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0 }),
        std::vector<DataTypePtr>({ VarcharType(1) }), aggFuncTypes, std::vector<uint32_t>({ 1, 2, 3 }),
        std::vector<DataTypePtr>({ VarcharType(1), VarcharType(1), VarcharType(4) }),
        std::vector<DataTypePtr>({ LongType(), VarcharType(1), VarcharType(4) }), std::vector<uint32_t>(), true, false,
        false);
    auto resultTypes = std::vector<DataTypePtr>({ LongType(), VarcharType(1), VarcharType(4) });
    auto groupByVarChar = aggFactory->CreateOperator();
    groupByVarChar->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupByVarChar->AddInput(input[i]);
    }
    VectorBatch *outputVecBatch = nullptr;
    int32_t vecBatchCount = groupByVarChar->GetOutput(&outputVecBatch);
    EXPECT_EQ(vecBatchCount, 1);

    op::Operator::DeleteOperator(groupByVarChar);
    std::string expectData1[3] = {"0", "1", "2"};
    int64_t expectData2[3] = {3, 3, 2};
    std::string expectData3[3] = {"0", "1", "2"};
    std::string expectData4[3] = {"6.6", "5.5", "4.4"};
    std::vector<DataTypePtr> expectedFieldTypes { VarcharType(1), LongType(), VarcharType(1), VarcharType(3) };
    DataTypes expectedTypes(expectedFieldTypes);
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectedTypes, 3, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch, expectedFieldTypes));

    delete[] input;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(HashAggregationOperatorTest, verify_char_vector_correctness)
{
    // create 10 pages
    const int vecBatchNum = 1;
    const int rowSize = 8;
    const int columnCount = 4; // groupby + count + min + max
    std::string aggNames[] = {"group", "count", "min", "max" };
    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_MIN,
        OMNI_AGGREGATION_TYPE_MAX };
    std::string data0[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data1[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data2[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data3[8] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1", "1.1", "1"};
    VectorBatch **input = BuildVarCharInput(vecBatchNum, columnCount, rowSize, data0, data1, data2, data3);

    // single stage
    auto aggFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0 }),
        std::vector<DataTypePtr>({ CharType(1) }), aggFuncTypes, std::vector<uint32_t>({ 1, 2, 3 }),
        std::vector<DataTypePtr>({ CharType(1), CharType(1), CharType(4) }),
        std::vector<DataTypePtr>({ LongType(), CharType(1), CharType(4) }), std::vector<uint32_t>(), true, false,
        false);

    auto groupByVarChar = aggFactory->CreateOperator();
    groupByVarChar->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupByVarChar->AddInput(input[i]);
    }
    VectorBatch *outputVecBatch = nullptr;
    int32_t vecBatchCount = groupByVarChar->GetOutput(&outputVecBatch);
    EXPECT_EQ(vecBatchCount, 1);

    op::Operator::DeleteOperator(groupByVarChar);
    std::string expectData1[3] = {"0", "1", "2"};
    int64_t expectData2[3] = {3, 3, 2};
    std::string expectData3[3] = {"0", "1", "2"};
    std::string expectData4[3] = {"6.6", "5.5", "4.4"};

    std::vector<DataTypePtr> expectedFieldTypes { CharType(1), LongType(), CharType(1), CharType(3) };
    DataTypes expectedTypes(expectedFieldTypes);
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectedTypes, 3, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch, expectedFieldTypes));

    delete[] input;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(HashAggregationOperatorTest, verify_null_correctness)
{
    // create 10 pages
    const int vecBatchNum = 1;
    const int ROW_SIZE = 6;
    const int cardinality = 1;
    std::string aggNames[] = {"group", "sum", "avg", "count", "min", "max"};
    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX };
    std::vector<DataTypePtr> groupTypes = { LongType() };
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType(), LongType(), LongType(), LongType() };
    VectorBatch **input = BuildAggInput(vecBatchNum, ROW_SIZE, cardinality, 1, 5, groupTypes, aggTypes);

    input[0]->Get(2)->SetNull(0);
    input[0]->Get(2)->SetNull(1);
    input[0]->Get(2)->SetNull(2);
    input[0]->Get(2)->SetNull(3);
    input[0]->Get(2)->SetNull(4);

    input[0]->Get(3)->SetNull(1);
    input[0]->Get(4)->SetNull(1);
    input[0]->Get(5)->SetNull(1);

    // single stage
    auto aggFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0 }),
        std::vector<DataTypePtr>({ LongType() }), aggFuncTypes, std::vector<uint32_t>({ 1, 2, 3, 4, 5 }),
        std::vector<DataTypePtr>({ LongType(), LongType(), LongType(), LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), DoubleType(), LongType(), LongType(), LongType() }),
        std::vector<uint32_t>(), true, false, false);
    auto groupByNULL = aggFactory->CreateOperator();
    groupByNULL->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupByNULL->AddInput(input[i]);
    }
    VectorBatch *outputVecBatch = nullptr;
    int32_t vecBatchCount = groupByNULL->GetOutput(&outputVecBatch);
    EXPECT_EQ(vecBatchCount, 1);

    omniruntime::op::Operator::DeleteOperator(groupByNULL);

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

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectVecBatch, expectedFieldTypes));

    delete[] input;
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
}

TEST(HashAggregationOperatorTest, verfify_correctness_group_by_agg_same_cols)
{
    // create 10 vecBatches
    const int vecBatchNum = 10;
    const int dataSize = 10;
    std::vector<VectorBatch *> input = ConstructSimpleBuildData(vecBatchNum, dataSize);

    auto aggFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0, 1, 2, 3, 4 }),
        std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), ShortType(), DoubleType() }),
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM }),
        std::vector<uint32_t>({ 0, 1 }), std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), LongType() }), std::vector<uint32_t>(), true, false, false);
    auto groupBy = aggFactory->CreateOperator();
    groupBy->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupBy->AddInput(input[i]);
    }

    VectorBatch *outputVecBatch = nullptr;
    groupBy->GetOutput(&outputVecBatch);
    std::vector<DataTypePtr> expectedTypes { LongType(),   LongType(), IntType(), ShortType(),
        DoubleType(), LongType(), LongType() };
    VectorBatch *expected = ConstructSimpleBuildData();
    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expected, expectedTypes));

    op::Operator::DeleteOperator(groupBy);
    VectorHelper::FreeVecBatch(outputVecBatch);

    VectorHelper::FreeVecBatch(expected);
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

    std::vector<DataTypePtr> inputTypes = { LongType(), LongType(), LongType(), LongType(), LongType() };
    DataTypes inputDataTypes(inputTypes);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, dataHash, data0, data1, data2, data3, data4,
        data5, data6, data7, data8, data9);

    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MIN };

    // STAGE1: (partial)
    auto aggPartialFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0 }),
        std::vector<DataTypePtr>({ LongType() }), aggFuncTypes, std::vector<uint32_t>({ 1, 2, 3, 4, 5 }),
        std::vector<DataTypePtr>({ LongType(), LongType(), LongType(), LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), LongType(),
        ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), LongType(), LongType() }),
        std::vector<uint32_t>({ 6, 7, 8, 9, 10 }), true, true, false);
    auto aggregatePartial = aggPartialFactory->CreateOperator();
    aggregatePartial->Init();
    aggregatePartial->AddInput(vecBatch1);

    VectorBatch *outputVecBatch = nullptr;
    int32_t tableCount1 = aggregatePartial->GetOutput(&outputVecBatch);
    EXPECT_EQ(tableCount1, 1);

    // STAGE2: (final)
    auto aggFinalFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0 }),
        std::vector<DataTypePtr>({ LongType() }), aggFuncTypes, std::vector<uint32_t>({ 1, 2, 3, 4, 5 }),
        std::vector<DataTypePtr>({ LongType(), LongType(),
        ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), LongType(), DoubleType(), LongType(), LongType() }),
        std::vector<uint32_t>(), false, false, false);
    auto aggregateFinal = aggFinalFactory->CreateOperator();
    aggregateFinal->Init();
    aggregateFinal->AddInput(outputVecBatch);

    VectorBatch *outputVecBatch1 = nullptr;
    int32_t tableCount = aggregateFinal->GetOutput(&outputVecBatch1);
    EXPECT_EQ(tableCount, 1);
    EXPECT_EQ(outputVecBatch1->GetRowCount(), 1);
    EXPECT_EQ(outputVecBatch1->GetVectorCount(), 6);

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

    EXPECT_TRUE(VecBatchMatch(outputVecBatch1, expVecBatch1, resultTypes.Get()));

    omniruntime::op::Operator::DeleteOperator(aggregatePartial);
    omniruntime::op::Operator::DeleteOperator(aggregateFinal);
    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatch(outputVecBatch1);
}
#ifdef DISABLE_TEST_NO_NEED_OCCUPY_BRANCH_TEST
TEST(HashAggregationOperatorTest, DISABLED_original_multiple_threads)
{
    using namespace std;
    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;

    std::vector<DataTypePtr> groupTypes { LongType(), LongType() };
    std::vector<DataTypePtr> inputTypes { LongType(), LongType() };
    std::vector<DataTypePtr> allInputTypes { LongType(), LongType(), LongType(), LongType() };
    VectorBatch **input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, inputTypes);

    auto nativeOperatorFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0, 1 }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM }),
        std::vector<uint32_t>({ 2, 3 }), std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), LongType() }), std::vector<uint32_t>(), true, false, false);
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
            std::thread t(PerfTestOriginal, factoryObjAddr, input, std::move(allInputTypes));
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
    delete input;
}

TEST(HashAggregationOperatorTest, DISABLED_perf_via_API_multiple_threads)
{
    using namespace std;
    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;

    vector<DataTypePtr> groupTypes = { LongType(), LongType() };
    vector<DataTypePtr> aggTypes = { LongType(), LongType() };
    vector<DataTypePtr> allInputTypes = { LongType(), LongType(), LongType(), LongType() };
    VectorBatch **input = BuildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);

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
            std::thread t(PerfTest, factoryObjAddr, input, VEC_BATCH_NUM, rowCount, std::move(allInputTypes));
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
    delete reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(factoryObjAddr);
    delete[] rowCount;
    delete input;
}
#endif

TEST(AggregationOperatorTest, hmpp_min_max_varchar)
{
    ConfigUtil::SetEnableHMPP(true);
    int32_t rowCount = 6;
    std::string data0[] = {"Zulma.Carter@MfvjVN43Udd95KeZ.com", "*", "Aaron.Anderson@0CQ4QUkBY2Q.edu",
                           "Zulema.Ruiz@J2XvbX7.com", "*", "Aaron.Artis@bv.org"};
    std::string data1[] = {"*", "Zulma.Carter@MfvjVN43Udd95KeZ.com", "Aaron.Anderson@0CQ4QUkBY2Q.edu", "*",
                           "Zulema.Ruiz@J2XvbX7.com", "Aaron.Artis@bv.org"};
    std::string_view dataV0[rowCount];
    std::string_view dataV1[rowCount];
    for (int i = 0; i < rowCount; ++i) {
        dataV0[i] = std::string_view(data0[i].c_str(), data0[i].size());
        dataV1[i] = std::string_view(data1[i].c_str(), data1[i].size());
    }

    std::vector<DataTypePtr> types = std::vector<DataTypePtr> { VarcharType(100), VarcharType(100) };
    auto vector1 = new Vector<LargeStringContainer<std::string_view>>(rowCount);
    auto vector2 = new Vector<LargeStringContainer<std::string_view>>(rowCount);
    for (int32_t i = 0; i < rowCount; i++) {
        if (i == 1 || i == 4) {
            vector1->SetNull(i);
        } else {
            vector1->SetValue(i, dataV0[i]);
        }
        if (i == 0 || i == 3) {
            vector2->SetNull(i);
        } else {
            vector2->SetValue(i, dataV1[i]);
        }
    }

    auto input = new VectorBatch(6);
    input->Append(vector1);
    input->Append(vector2);

    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX };

    // STAGE1: (partial)
    std::vector<DataTypePtr> partialOutputTypes { VarcharType(100), VarcharType(100) };
    auto aggPartialFactory = CreateAggregationOperatorFactory(aggFuncTypes, std::vector<uint32_t>({ 0, 1 }), types,
        partialOutputTypes, std::vector<uint32_t>(), true, true, false);
    auto aggPartial = aggPartialFactory->CreateOperator();
    aggPartial->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    int32_t vecBatchCount = aggPartial->GetOutput(&outputVecBatch);
    EXPECT_EQ(vecBatchCount, 1);

    // STAGE2: (final)
    std::vector<DataTypePtr> finalOutputTypes { VarcharType(100), VarcharType(100) };
    auto aggFinalFactory = CreateAggregationOperatorFactory(aggFuncTypes, std::vector<uint32_t>({ 0, 1 }),
        partialOutputTypes, finalOutputTypes, std::vector<uint32_t>(), false, false, false);
    auto aggFinal = aggFinalFactory->CreateOperator();
    aggFinal->AddInput(outputVecBatch);

    VectorBatch *finalOutputVecBatch = nullptr;
    vecBatchCount = aggFinal->GetOutput(&finalOutputVecBatch);
    EXPECT_EQ(vecBatchCount, 1);
    auto resultVec0 = static_cast<Vector<LargeStringContainer<std::string_view>> *>(finalOutputVecBatch->Get(0));
    EXPECT_EQ(resultVec0->GetSize(), 1);
    std::string_view minVal = resultVec0->GetValue(0);
    std::string minStr(minVal.data(), minVal.data() + minVal.size());
    EXPECT_EQ(minVal.size(), data0[2].size());
    EXPECT_EQ(data0[2].compare(minStr), 0);

    auto resultVec1 = static_cast<Vector<LargeStringContainer<std::string_view>> *>(finalOutputVecBatch->Get(1));
    EXPECT_EQ(resultVec1->GetSize(), 1);
    std::string_view maxVal = resultVec1->GetValue(0);
    std::string maxStr(maxVal.data(), maxVal.data() + maxVal.size());
    EXPECT_EQ(maxVal.size(), data1[1].size());
    EXPECT_EQ(data1[1].compare(maxStr), 0);

    omniruntime::op::Operator::DeleteOperator(aggFinal);
    omniruntime::op::Operator::DeleteOperator(aggPartial);
    VectorHelper::FreeVecBatch(finalOutputVecBatch);
    ConfigUtil::SetEnableHMPP(false);
}

TEST(AggregationOperatorTest, hmpp_min_max_varchar_without_nulls)
{
    ConfigUtil::SetEnableHMPP(true);
    int32_t rowCount = 6;
    std::string data0[] = {"Zulma.Carter@MfvjVN43Udd95KeZ.com", "*", "Aaron.Anderson@0CQ4QUkBY2Q.edu",
                           "Zulema.Ruiz@J2XvbX7.com", "**", "Aaron.Artis@bv.org"};
    std::string data1[] = {"**", "Zulma.Carter@MfvjVN43Udd95KeZ.com", "Aaron.Anderson@0CQ4QUkBY2Q.edu", "*",
                           "Zulema.Ruiz@J2XvbX7.com", "Aaron.Artis@bv.org"};
    std::string_view dataV0[rowCount];
    std::string_view dataV1[rowCount];
    for (int i = 0; i < rowCount; ++i) {
        dataV0[i] = std::string_view(data0[i].c_str(), data0[i].size());
        dataV1[i] = std::string_view(data1[i].c_str(), data1[i].size());
    }

    std::vector<DataTypePtr> types = std::vector<DataTypePtr> { VarcharType(100), VarcharType(100) };
    auto vector1 = new Vector<LargeStringContainer<std::string_view>>(rowCount);
    auto vector2 = new Vector<LargeStringContainer<std::string_view>>(rowCount);
    for (int32_t i = 0; i < rowCount; i++) {
        vector1->SetValue(i, dataV0[i]);
        vector2->SetValue(i, dataV1[i]);
    }

    auto input = new VectorBatch(6);
    input->Append(vector1);
    input->Append(vector2);

    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX };

    // STAGE1: (partial)
    std::vector<DataTypePtr> partialOutputTypes { VarcharType(100), VarcharType(100) };
    auto aggPartialFactory = CreateAggregationOperatorFactory(aggFuncTypes, std::vector<uint32_t>({ 0, 1 }), types,
        partialOutputTypes, std::vector<uint32_t>(), true, true, false);
    auto aggPartial = aggPartialFactory->CreateOperator();
    aggPartial->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    int32_t vecBatchCount = aggPartial->GetOutput(&outputVecBatch);
    EXPECT_EQ(vecBatchCount, 1);

    // STAGE2: (final)
    std::vector<DataTypePtr> finalOutputTypes { VarcharType(100), VarcharType(100) };
    auto aggFinalFactory = CreateAggregationOperatorFactory(aggFuncTypes, std::vector<uint32_t>({ 0, 1 }),
        partialOutputTypes, finalOutputTypes, std::vector<uint32_t>(), false, false, false);
    auto aggFinal = aggFinalFactory->CreateOperator();
    aggFinal->AddInput(outputVecBatch);

    VectorBatch *finalOutputVecBatch = nullptr;
    vecBatchCount = aggFinal->GetOutput(&finalOutputVecBatch);
    EXPECT_EQ(vecBatchCount, 1);
    auto resultVec0 = static_cast<Vector<LargeStringContainer<std::string_view>> *>(finalOutputVecBatch->Get(0));
    EXPECT_EQ(resultVec0->GetSize(), 1);
    std::string_view minVal = resultVec0->GetValue(0);
    std::string minStr(minVal.data(), minVal.data() + minVal.size());
    EXPECT_EQ(minVal.size(), data0[1].size());
    EXPECT_EQ(data0[1].compare(minStr), 0);

    auto resultVec1 = static_cast<Vector<LargeStringContainer<std::string_view>> *>(finalOutputVecBatch->Get(1));
    EXPECT_EQ(resultVec1->GetSize(), 1);
    std::string_view maxVal = resultVec1->GetValue(0);
    std::string maxStr(maxVal.data(), maxVal.data() + maxVal.size());
    EXPECT_EQ(maxVal.size(), data1[1].size());
    EXPECT_EQ(data1[1].compare(maxStr), 0);

    omniruntime::op::Operator::DeleteOperator(aggFinal);
    omniruntime::op::Operator::DeleteOperator(aggPartial);
    VectorHelper::FreeVecBatch(finalOutputVecBatch);
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
    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG };
    std::vector<DataTypePtr> aggTypes = { LongType(), Decimal128Type(20, 5), LongType(), Decimal128Type(20, 5) };
    DataTypes sourceTypes(aggTypes);
    VectorBatch *input = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data0, data1);
    ASSERT(!(input == nullptr));

    // STAGE1: (partial)
    std::vector<DataTypePtr> partialOutputTypes { LongType(), SUM_IMMEDIATE_VARBINARY,
        ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), AVG_IMMEDIATE_VARBINARY };
    auto aggPartialFactory = CreateAggregationOperatorFactory(aggFuncTypes, std::vector<uint32_t>({ 0, 1, 2, 3 }),
        aggTypes, partialOutputTypes, std::vector<uint32_t>(), true, true, false);
    auto aggPartial = aggPartialFactory->CreateOperator();
    aggPartial->AddInput(input);

    VectorBatch *outputVecBatch = nullptr;
    int32_t tableCount = aggPartial->GetOutput(&outputVecBatch);
    EXPECT_EQ(tableCount, 1);

    // STAGE2: (final)
    std::vector<DataTypePtr> finalOutputTypes { LongType(), Decimal128Type(20, 5), DoubleType(),
        Decimal128Type(20, 5) };
    auto aggFinalFactory = CreateAggregationOperatorFactory(aggFuncTypes, std::vector<uint32_t>({ 0, 1, 2, 3 }),
        partialOutputTypes, finalOutputTypes, std::vector<uint32_t>(), false, false, false);
    auto aggFinal = aggFinalFactory->CreateOperator();
    aggFinal->AddInput(outputVecBatch);

    VectorBatch *finalOutputVecBatch = nullptr;
    tableCount = aggFinal->GetOutput(&finalOutputVecBatch);
    EXPECT_EQ(tableCount, 1);
    EXPECT_EQ(finalOutputVecBatch->GetRowCount(), 1);
    EXPECT_EQ(finalOutputVecBatch->GetVectorCount(), 4);

    int64_t expData0[resultDataSize] = {25L};
    Decimal128 expData1[resultDataSize] = {Decimal128(15000L, 0)};
    double expData2[resultDataSize] = {5.0};
    Decimal128 expData3[resultDataSize] = {Decimal128(3000L, 0)};
    std::vector<DataTypePtr> resultType = { LongType(), Decimal128Type(20, 5), DoubleType(), Decimal128Type(20, 5) };
    DataTypes resultTypes(resultType);
    VectorBatch *expVecBatch = CreateVectorBatch(resultTypes, resultDataSize, expData0, expData1, expData2, expData3);

    EXPECT_TRUE(VecBatchMatch(finalOutputVecBatch, expVecBatch, resultTypes.Get()));

    omniruntime::op::Operator::DeleteOperator(aggFinal);
    omniruntime::op::Operator::DeleteOperator(aggPartial);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(finalOutputVecBatch);
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
    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN,
        OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX,
        OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG };
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

    auto aggPartialFactory =
        CreateAggregationOperatorFactory(aggFuncTypes, std::vector<uint32_t>({ 0, 1, 2, 3, 4, 5, 6, 7 }), aggTypes,
        partialOutputTypes, std::vector<uint32_t>(), true, true, false);
    auto aggPartial = aggPartialFactory->CreateOperator();
    aggPartial->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    int32_t tableCount = aggPartial->GetOutput(&outputVecBatch);
    EXPECT_EQ(tableCount, 1);

    // STAGE2:
    std::vector<DataTypePtr> finalOutputTypes { Decimal128Type(38, 0), Decimal128Type(38, 0), Decimal128Type(38, 0),
        Decimal128Type(38, 0), Decimal128Type(38, 0), Decimal128Type(38, 0),
        Decimal128Type(38, 0), Decimal128Type(38, 0) };
    auto aggFinalFactory =
        CreateAggregationOperatorFactory(aggFuncTypes, std::vector<uint32_t>({ 0, 1, 2, 3, 4, 5, 6, 7 }),
        partialOutputTypes, finalOutputTypes, std::vector<uint32_t>(), false, false, false);
    auto aggFinal = aggFinalFactory->CreateOperator();
    aggFinal->AddInput(outputVecBatch);
    VectorBatch *finalOutputVecBatch = nullptr;
    tableCount = aggFinal->GetOutput(&finalOutputVecBatch);
    EXPECT_EQ(tableCount, 1);
    EXPECT_EQ(finalOutputVecBatch->GetRowCount(), 1);
    EXPECT_EQ(finalOutputVecBatch->GetVectorCount(), 8);

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

    EXPECT_TRUE(VecBatchMatch(finalOutputVecBatch, expVecBatch, resultTypes.Get()));

    omniruntime::op::Operator::DeleteOperator(aggFinal);
    omniruntime::op::Operator::DeleteOperator(aggPartial);
    VectorHelper::FreeVecBatch(expVecBatch);
    VectorHelper::FreeVecBatch(finalOutputVecBatch);
    ConfigUtil::SetEnableHMPP(false);
}

TEST(HashAggregationOperatorTest, hmpp_group_by_agg_same_cols)
{
    ConfigUtil::SetEnableHMPP(true);
    // create 10 vecBatches
    const int vecBatchNum = 10;
    const int dataSize = 10;
    std::vector<VectorBatch *> input = ConstructSimpleBuildData(vecBatchNum, dataSize);

    auto aggFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0, 1, 2, 3, 4 }),
        std::vector<DataTypePtr>({ LongType(), LongType(), IntType(), ShortType(), DoubleType() }),
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM }),
        std::vector<uint32_t>({ 0, 1 }), std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), LongType() }), std::vector<uint32_t>(), true, false, false);
    auto groupBy = aggFactory->CreateOperator();
    groupBy->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupBy->AddInput(input[i]);
    }

    VectorBatch *outputVecBatch = nullptr;
    groupBy->GetOutput(&outputVecBatch);
    std::vector<DataTypePtr> expectedTypes { LongType(),   LongType(), IntType(), ShortType(),
        DoubleType(), LongType(), LongType() };
    VectorBatch *expected = ConstructSimpleBuildData();
    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expected, expectedTypes));
    op::Operator::DeleteOperator(groupBy);
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expected);
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
    VectorBatch **input = BuildAggInput(vecBatchNum, rowSize, cardinality, 1, 3, groupTypes, aggTypes);

    std::vector<DataTypePtr> outputTypes { LongType(), VarcharType(10), VarcharType(10) };
    auto aggFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0 }), groupTypes,
        std::vector<uint32_t>(
        { OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX }),
        std::vector<uint32_t>({ 1, 2, 3 }), aggTypes, outputTypes, std::vector<uint32_t>(), true, false, false);
    auto groupByVarChar = aggFactory->CreateOperator();
    groupByVarChar->Init();

    for (int32_t i = 0; i < vecBatchNum; ++i) {
        groupByVarChar->AddInput(input[i]);
    }

    VectorBatch *outputVecBatch = nullptr;
    int32_t vecBatchCount = groupByVarChar->GetOutput(&outputVecBatch);
    EXPECT_EQ(vecBatchCount, 1);

    op::Operator::DeleteOperator(groupByVarChar);

    std::vector<DataTypePtr> expectFieldTypes { VarcharType(10), LongType(), VarcharType(10), VarcharType(10) };
    // construct the output data
    DataTypes expectTypes(expectFieldTypes);
    std::string  expectData1[cardinality] = {"9", "8", "7", "6", "5", "4", "3", "2", "1", "0"};
    int64_t expectData2[cardinality] = {200, 200, 200, 200, 200, 200, 200, 200, 200, 200};
    std::string  expectData3[cardinality] = {"1009", "1008", "1007", "1006", "1005", "1004", "1003", "1002", "1", "0"};
    std::string  expectData4[cardinality] = {"999", "998", "997", "996", "995", "994", "993", "992", "991", "990"};

    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, cardinality, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch, expectFieldTypes));

    delete[] input;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    ConfigUtil::SetEnableHMPP(false);
}

TEST(AggregationOperatorTest, verify_correctness)
{
    // create 10 vecBatches
    const int vecBatchNum = 10;
    const int cardinality = 4;
    std::string aggNames[] = {"sum", "avg", "count", "min", "max"};
    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MAX };
    std::vector<DataTypePtr> groupTypes;
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType(), LongType(), LongType(), LongType() };
    VectorBatch **input1 = BuildAggInput(vecBatchNum, ROW_PER_VEC_BATCH, cardinality, 0, 5, groupTypes, aggTypes);

    // STAGE1: (partial)
    std::vector<DataTypePtr> partialOutputTypes { LongType(),
        ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), LongType(), LongType(), LongType() };
    auto aggPartialFactory = CreateAggregationOperatorFactory(aggFuncTypes, std::vector<uint32_t>({ 0, 1, 2, 3, 4 }),
        aggTypes, partialOutputTypes, std::vector<uint32_t>(), true, true, false);

    // partial first operator
    auto aggPartial1 = aggPartialFactory->CreateOperator();
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggPartial1->AddInput(input1[i]);
    }

    VectorBatch *outputVecBatch1 = nullptr;
    int32_t tableCount1 = aggPartial1->GetOutput(&outputVecBatch1);
    EXPECT_EQ(tableCount1, 1);
    omniruntime::op::Operator::DeleteOperator(aggPartial1);

    VectorBatch **input2 = BuildAggInput(vecBatchNum, ROW_PER_VEC_BATCH, cardinality, 0, 5, groupTypes, aggTypes);
    ASSERT(!(input2 == nullptr));

    // partial second operator
    auto aggPartial2 = aggPartialFactory->CreateOperator();
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggPartial2->AddInput(input2[i]);
    }
    VectorBatch *outputVecBatch2 = nullptr;
    int32_t tableCount2 = aggPartial2->GetOutput(&outputVecBatch2);
    EXPECT_EQ(tableCount2, 1);
    omniruntime::op::Operator::DeleteOperator(aggPartial2);

    // STAGE2: (final)
    std::vector<DataTypePtr> finalOutputTypes { LongType(), DoubleType(), LongType(), LongType(), LongType() };
    auto aggFinalFactory = CreateAggregationOperatorFactory(aggFuncTypes, std::vector<uint32_t>({ 0, 1, 2, 3, 4 }),
        partialOutputTypes, finalOutputTypes, std::vector<uint32_t>(), false, false, false);
    auto aggFinal = aggFinalFactory->CreateOperator();
    aggFinal->AddInput(outputVecBatch1);
    aggFinal->AddInput(outputVecBatch2);

    VectorBatch *finalOutputVecBatch = nullptr;
    int32_t tableCount3 = aggFinal->GetOutput(&finalOutputVecBatch);
    EXPECT_EQ(tableCount3, 1);
    EXPECT_EQ(finalOutputVecBatch->GetRowCount(), 1);
    EXPECT_EQ(finalOutputVecBatch->GetVectorCount(), 5);

    delete[] input1;
    delete[] input2;
    omniruntime::op::Operator::DeleteOperator(aggFinal);
    VectorHelper::FreeVecBatch(finalOutputVecBatch);
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

    std::vector<uint32_t> aggFuncTypes = { OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MIN };
    std::vector<DataTypePtr> types = { LongType(),    LongType(),    LongType(),    LongType(),    LongType(),
        BooleanType(), BooleanType(), BooleanType(), BooleanType(), BooleanType() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 =
        CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2, data3, data4, data5, data6, data7, data8, data9);

    // STAGE1: (partial)
    std::vector<DataTypePtr> partialOutputTypes({ LongType(), LongType(),
        ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), LongType(), LongType() });
    auto aggPartialFactory = CreateAggregationOperatorFactory(aggFuncTypes, std::vector<uint32_t>({ 0, 1, 2, 3, 4 }),
        std::vector<DataTypePtr>({ LongType(), LongType(), LongType(), LongType(), LongType() }), partialOutputTypes,
        std::vector<uint32_t>({ 5, 6, 7, 8, 9 }), true, true, false);
    auto aggPartial = aggPartialFactory->CreateOperator();
    aggPartial->AddInput(vecBatch1);

    VectorBatch *outputVecBatch = nullptr;
    int32_t tableCount1 = aggPartial->GetOutput(&outputVecBatch);
    EXPECT_EQ(tableCount1, 1);

    // STAGE2: (final)
    std::vector<DataTypePtr> finalOutputTypes { LongType(), LongType(), DoubleType(), LongType(), LongType() };
    auto aggFinalFactory = CreateAggregationOperatorFactory(aggFuncTypes, std::vector<uint32_t>({ 0, 1, 2, 3, 4 }),
        partialOutputTypes, finalOutputTypes, std::vector<uint32_t>(), false, false, false);
    auto aggFinal = aggFinalFactory->CreateOperator();
    aggFinal->AddInput(outputVecBatch);

    VectorBatch *finalOutputVecBatch = nullptr;
    int32_t tableCount = aggFinal->GetOutput(&finalOutputVecBatch);
    EXPECT_EQ(tableCount, 1);
    EXPECT_EQ(finalOutputVecBatch->GetRowCount(), 1);
    EXPECT_EQ(finalOutputVecBatch->GetVectorCount(), 5);

    int64_t expData0[resultDataSize] = {3L};
    int64_t expData1[resultDataSize] = {6L};
    double expData2[resultDataSize] = {5.0};
    int64_t expData3[resultDataSize] = {4L};
    int64_t expData4[resultDataSize] = {1L};
    std::vector<DataTypePtr> resultType = { LongType(), LongType(), DoubleType(), LongType(), LongType() };
    DataTypes resultTypes(resultType);
    VectorBatch *expVecBatch1 =
        CreateVectorBatch(resultTypes, resultDataSize, expData0, expData1, expData2, expData3, expData4);

    EXPECT_TRUE(VecBatchMatch(finalOutputVecBatch, expVecBatch1, resultTypes.Get()));

    omniruntime::op::Operator::DeleteOperator(aggPartial);
    omniruntime::op::Operator::DeleteOperator(aggFinal);
    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatch(finalOutputVecBatch);
}

TEST(AggregationOperatorTest, avg_correctness_test)
{
    // create 10 pages
    const int vecBatchNum = 10;
    const int cardinality = 100;
    std::vector<DataTypePtr> groupTypes;
    std::vector<DataTypePtr> aggTypes = { LongType() };
    VectorBatch **input = BuildAggInput(vecBatchNum, ROW_PER_VEC_BATCH, cardinality, 0, 1, groupTypes, aggTypes);

    // single stage
    auto aggFactory = CreateAggregationOperatorFactory(std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_AVG }),
        std::vector<uint32_t>({ 0 }), aggTypes, std::vector<DataTypePtr>({ DoubleType() }), std::vector<uint32_t>(),
        true, false, false);
    auto aggregate = aggFactory->CreateOperator();
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        aggregate->AddInput(input[i]);
    }

    VectorBatch *outputVecBatch = nullptr;
    int32_t tableCount = aggregate->GetOutput(&outputVecBatch);

    EXPECT_EQ(tableCount, 1);
    EXPECT_EQ(outputVecBatch->GetVectorCount(), 1);
    EXPECT_EQ(outputVecBatch->GetRowCount(), 1);

    delete[] input;
    VectorHelper::FreeVecBatch(outputVecBatch);
    op::Operator::DeleteOperator(aggregate);
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
        std::vector<uint32_t>({ 0, 1 }), types, std::vector<DataTypePtr>({ VarcharType(10), VarcharType(10) }),
        std::vector<uint32_t>(), true, false, false);
    auto aggOperator = aggFactory->CreateOperator();
    aggOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    int32_t vecBatchCount = aggOperator->GetOutput(&outputVecBatch);
    EXPECT_EQ(vecBatchCount, 1);

    auto resultVec0 = static_cast<Vector<LargeStringContainer<std::string_view>> *>(outputVecBatch->Get(0));
    EXPECT_EQ(resultVec0->GetSize(), 1);
    std::string_view minVal = resultVec0->GetValue(0);
    std::string minStr(minVal.data(), minVal.data() + minVal.size());
    EXPECT_EQ(minVal.size(), data0[4].size());
    EXPECT_EQ(data0[4].compare(minStr), 0);

    auto resultVec1 = static_cast<Vector<LargeStringContainer<std::string_view>> *>(outputVecBatch->Get(1));
    EXPECT_EQ(resultVec1->GetSize(), 1);
    std::string_view maxVal = resultVec1->GetValue(0);
    std::string maxStr(maxVal.data(), maxVal.data() + maxVal.size());
    EXPECT_EQ(maxVal.size(), data1[4].size());
    EXPECT_EQ(data1[4].compare(maxStr), 0);

    omniruntime::op::Operator::DeleteOperator(aggOperator);
    VectorHelper::FreeVecBatch(outputVecBatch);
}
#ifdef DISABLE_TEST_NO_NEED_OCCUPY_BRANCH_TEST
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

    VectorBatch **input = BuildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 0, COLUMN_NUM,
        std::vector<DataTypePtr>(), sourceTypes.Get());

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
            std::thread t(PerfTestNonGroup, factoryAddr, false, input, VEC_BATCH_NUM, rowCount,
                std::vector<DataTypePtr> { LongType(), LongType(), LongType(), LongType() });
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
    delete nativeOperatorFactory;
    delete input;
}

TEST(AggregationOperatorTest, DISABLED_perf_codegen)
{
    using namespace std;

    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;

    std::vector<DataTypePtr> groupTypes;
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType(), LongType(), LongType() };
    VectorBatch **input = BuildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 0, 4, groupTypes, aggTypes);

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
            std::thread t(PerfTestNonGroup, factoryObjAddr, true, input, VEC_BATCH_NUM, rowCount,
                std::vector<DataTypePtr> { LongType(), LongType(), LongType(), LongType() });
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
    delete input;
}
#endif

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
        BuildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupInputTypes.Get(), aggInputTypes.Get());

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
    VectorBatch *jittedResult = nullptr;
    jitGroupBy->GetOutput(&jittedResult);

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
        BuildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupInputTypes.Get(), aggInputTypes.Get());
    if (input2 == nullptr) {
        return;
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

    VectorBatch *outputVecBatch = nullptr;
    groupBy->GetOutput(&outputVecBatch);

    timer.CalculateElapse();
    wallElapsed = timer.GetWallElapse();
    cpuElapsed = timer.GetCpuElapse();
    std::cout << "HashAgg without OmniJit, wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;

    delete perfUtil;
    delete[] input1;
    delete[] input2;
    op::Operator::DeleteOperator(jitGroupBy);
    op::Operator::DeleteOperator(groupBy);
    delete nativeOperatorFactory;
    delete nativeOperatorFactory2;

    std::vector<DataTypePtr> expectedTypes { LongType(), LongType(), LongType(), LongType() };
    EXPECT_TRUE(VecBatchMatch(jittedResult, outputVecBatch, expectedTypes));
    VectorHelper::FreeVecBatch(jittedResult);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(HashAggregationOperatorTest, multi_stage)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);
    std::vector<DataTypePtr> groupTypes = { LongType(), LongType() };
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType(), Decimal64Type(7, 2), Decimal64Type(7, 2) };
    VectorBatch **input1 = BuildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 4, groupTypes, aggTypes);
    HAFactoryParameters parameters1 = {
        true,
        true,
        { 0, 1 },
        { LongType(), LongType() },
        { 2, 3, 4, 5 },
        { LongType(), LongType(), SHORT_DECIMAL_TYPE, SHORT_DECIMAL_TYPE },
        { LongType(), ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), SUM_IMMEDIATE_VARBINARY,
        AVG_IMMEDIATE_VARBINARY },
        { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG },
        { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1) },
    };

    uintptr_t partialFactoryAddr1 = CreateHashFactoryWithoutJit(parameters1);
    auto partialFactory1 = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(partialFactoryAddr1);
    auto partialOperator1 = partialFactory1->CreateOperator();
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        partialOperator1->AddInput(input1[i]);
    }
    VectorBatch *resultFromPartial1 = nullptr;
    partialOperator1->GetOutput(&resultFromPartial1);

    VectorBatch **input2 = BuildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 4, groupTypes, aggTypes);
    if (input2 == nullptr) {
        return;
    }

    HAFactoryParameters parameters2 = { true,
        true,
        { 0, 1 },
        { LongType(), LongType() },
        { 2, 3, 4, 5 },
        { LongType(), LongType(), SHORT_DECIMAL_TYPE, SHORT_DECIMAL_TYPE },
        { LongType(), ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), SUM_IMMEDIATE_VARBINARY,
        AVG_IMMEDIATE_VARBINARY },
        { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG },
        { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1),
        static_cast<uint32_t>(-1) } };

    uintptr_t partialFactoryAddr2 = CreateHashFactoryWithoutJit(parameters2);
    auto partialFactory2 = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(partialFactoryAddr2);
    auto partialOperator2 = partialFactory2->CreateOperator();
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        partialOperator2->AddInput(input2[i]);
    }
    VectorBatch *resultFromPartial2 = nullptr;
    partialOperator2->GetOutput(&resultFromPartial2);

    HAFactoryParameters parameters3 = {
        false,
        false,
        { 0, 1 },
        { LongType(), LongType() },
        { 2, 3, 4, 5 },
        { LongType(), ContainerType(std::vector<DataTypePtr> { DoubleType(), LongType() }), SUM_IMMEDIATE_VARBINARY,
        AVG_IMMEDIATE_VARBINARY },
        { LongType(), DoubleType(), LONG_DECIMAL_TYPE, SHORT_DECIMAL_TYPE },
        { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG },
        { static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1), static_cast<uint32_t>(-1) },
    };

    uintptr_t finalFactoryAddr = CreateHashFactoryWithoutJit(parameters3);
    auto finalFactory = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(finalFactoryAddr);
    auto operator2 = finalFactory->CreateOperator();
    operator2->AddInput(resultFromPartial1);
    operator2->AddInput(resultFromPartial2);
    VectorBatch *resultFromFinal = nullptr;
    operator2->GetOutput(&resultFromFinal);

    op::Operator::DeleteOperator(partialOperator1);
    op::Operator::DeleteOperator(partialOperator2);
    op::Operator::DeleteOperator(operator2);
    delete partialFactory1;
    delete partialFactory2;
    delete finalFactory;

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
    EXPECT_TRUE(VecBatchMatchIgnoreOrder(resultFromFinal, expectVecBatch, expectFieldTypes));

    delete[] input1;
    delete[] input2;
    VectorHelper::FreeVecBatch(resultFromFinal);
    VectorHelper::FreeVecBatch(expectVecBatch);
}

TEST(HashAggregationOperatorTest, supported_type_test)
{
    std::vector<DataTypePtr> groupTypes = { LongType(), LongType() };
    std::vector<DataTypePtr> aggTypes = { LongType(), LongType() };
    VectorBatch **input = BuildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);

    auto aggFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0, 1 }),
        std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM }),
        std::vector<uint32_t>({ 2, 3 }), std::vector<DataTypePtr>({ LongType(), LongType() }),
        std::vector<DataTypePtr>({ LongType(), DoubleType() }), std::vector<uint32_t>(), true, false, false);
    auto groupBy = aggFactory->CreateOperator();
    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }

    VectorBatch *outputVecBatch = nullptr;
    int32_t vecBatchCount = groupBy->GetOutput(&outputVecBatch);
    ASSERT_EQ(vecBatchCount, 1);
    op::Operator::DeleteOperator(groupBy);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] input;

    groupTypes[0] = Date32Type();
    groupTypes[1] = Date32Type();
    aggTypes[0] = Date32Type();
    aggTypes[1] = Date32Type();
    input = BuildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);
    aggFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0, 1 }),
        std::vector<DataTypePtr>({ Date32DataType::Instance(), Date32DataType::Instance() }),
        std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM }),
        std::vector<uint32_t>({ 2, 3 }),
        std::vector<DataTypePtr>({ Date32DataType::Instance(), Date32DataType::Instance() }),
        std::vector<DataTypePtr>({ Date32DataType::Instance(), Date32DataType::Instance() }), std::vector<uint32_t>(),
        true, false, false);
    groupBy = aggFactory->CreateOperator();
    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }

    vecBatchCount = groupBy->GetOutput(&outputVecBatch);
    op::Operator::DeleteOperator(groupBy);
    VectorHelper::FreeVecBatch(outputVecBatch);
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
    VectorBatch *vectorBatch = new VectorBatch(dataSize);
    for (int32_t i = 0; i < 2; i++) {
        DataTypePtr dataType = sourceTypes.GetType(i);
        vectorBatch->Append(CreateDictionaryVector(*dataType, dataSize, ids, dataSize, datas[i]).release());
    }

    aggFactory = CreateHashAggregationOperatorFactory(std::vector<uint32_t>({ 0 }),
        std::vector<DataTypePtr>({ IntType() }), std::vector<uint32_t>({ OMNI_AGGREGATION_TYPE_SUM }),
        std::vector<uint32_t>({ 1 }), std::vector<DataTypePtr>({ LongType() }),
        std::vector<DataTypePtr>({ LongType() }), std::vector<uint32_t>(), true, false, false);
    groupBy = aggFactory->CreateOperator();
    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        // create a copy of input
        VectorBatch *copied = DuplicateVectorBatch(vectorBatch, sourceFieldTypes);
        groupBy->AddInput(copied);
    }

    vecBatchCount = groupBy->GetOutput(&outputVecBatch);
    op::Operator::DeleteOperator(groupBy);
    VectorHelper::FreeVecBatch(vectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
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

    auto longInputVec = BuildAggregateInput(LongType(), rowPerVecBatch);
    auto decimalInputVec = BuildAggregateInput(Decimal128Type(38, 2), rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(NoneType(), rowPerVecBatch);

    VectorBatch *vecBatch = new VectorBatch(rowPerVecBatch);
    vecBatch->Append(longInputVec);
    vecBatch->Append(decimalInputVec);
    vecBatch->Append(dictInputVec.release());
    vecBatch->Append(nullInputVec);
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

    auto longInputVec = BuildAggregateInput(LongType(), rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(NoneType(), rowPerVecBatch);

    VectorBatch *vecBatch = new VectorBatch(rowPerVecBatch);
    vecBatch->Append(longInputVec);
    vecBatch->Append(dictInputVec.release());
    vecBatch->Append(nullInputVec);

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

    auto longInputVec = BuildAggregateInput(LongType(), rowPerVecBatch);
    LongDataType longDataType;
    auto nullInputVec = BuildAggregateInput(NoneType(), ROW_PER_VEC_BATCH);

    VectorBatch *vecBatch = new VectorBatch(rowPerVecBatch);
    vecBatch->Append(longInputVec);
    vecBatch->Append(nullInputVec);

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

    auto longInputVec = BuildAggregateInput(LongType(), rowPerVecBatch);
    auto decimalInputVec = BuildAggregateInput(Decimal128Type(38, 0), rowPerVecBatch);
    VarcharDataType varcharDataType(1);
    std::string stringVals[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        stringVals[i] = "1";
    }
    auto varcharInputVec = CreateVarcharVector(varcharDataType, stringVals, rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(NoneType(), rowPerVecBatch);
    auto intInputVec = BuildAggregateInput(IntType(), rowPerVecBatch);
    auto BoolInputVec = BuildAggregateInput(BooleanType(), rowPerVecBatch);

    VectorBatch *vectorBatch = new VectorBatch(7);
    vectorBatch->Append(longInputVec);
    vectorBatch->Append(dictInputVec.release());
    vectorBatch->Append(nullInputVec);
    vectorBatch->Append(decimalInputVec);
    vectorBatch->Append(varcharInputVec.release());
    vectorBatch->Append(intInputVec);
    vectorBatch->Append(BoolInputVec);

    std::vector<BaseVector *> result(1);

    // process long
    AggregateState state { nullptr };
    minLong->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    Vector<int64_t> longResult(1);
    result.clear();
    result.push_back(&longResult);
    minLong->ExtractValues(state, result, 0);
    EXPECT_EQ(1, longResult.GetValue(0));
    state.Reset();

    // process null
    minNull->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    minNull->ExtractValues(state, result, 0);
    EXPECT_TRUE(longResult.IsNull(0));
    state.Reset();

    // process varchar
    minVarchar->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    std::string expectedStr = "1";
    EXPECT_EQ(1, state.count);
    EXPECT_EQ(0, std::memcmp(state.val, expectedStr.c_str(), 1));
    Vector<LargeStringContainer<std::string_view>> minVarcharOutput(1);
    std::vector<BaseVector *> minVarcharOutputVector;
    minVarcharOutputVector.push_back(&minVarcharOutput);
    minVarchar->ExtractValues(state, minVarcharOutputVector, 0);
    auto strRes = minVarcharOutput.GetValue(0);
    EXPECT_EQ(1, strRes.size());
    EXPECT_EQ(0, std::memcmp(strRes.data(), expectedStr.c_str(), 1));
    state.Reset();

    // process int to long
    minIntLong->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    minIntLong->ExtractValues(state, result, 0);
    EXPECT_EQ(1, longResult.GetValue(0));
    state.Reset();

    // process long to int
    minLongInt->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    Vector<int32_t> intResult(1);
    result.clear();
    result.push_back(&intResult);
    minLongInt->ExtractValues(state, result, 0);
    EXPECT_EQ(1, intResult.GetValue(0));
    state.Reset();

    minBoolean->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    Vector<bool> booleanResult(1);
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

    auto longInputVec = BuildAggregateInput(LongType(), rowPerVecBatch);
    auto decimalInputVec = BuildAggregateInput(Decimal128Type(38, 0), rowPerVecBatch);
    VarcharDataType varcharDataType(1);
    std::string stringVals[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        stringVals[i] = "1";
    }
    auto varcharInputVec = CreateVarcharVector(varcharDataType, stringVals, rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(NoneType(), rowPerVecBatch);
    auto intInputVec = BuildAggregateInput(IntType(), rowPerVecBatch);
    auto boolInputVec = BuildAggregateInput(BooleanType(), rowPerVecBatch);

    VectorBatch *vectorBatch = new VectorBatch(7);
    vectorBatch->Append(longInputVec);
    vectorBatch->Append(decimalInputVec);
    vectorBatch->Append(varcharInputVec.release());
    vectorBatch->Append(dictInputVec.release());
    vectorBatch->Append(nullInputVec);
    vectorBatch->Append(intInputVec);
    vectorBatch->Append(boolInputVec);

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
    Vector<LargeStringContainer<std::string_view>> maxVarcharOutput(1);
    std::vector<BaseVector *> maxVarcharOutputVector;
    maxVarcharOutputVector.push_back(&maxVarcharOutput);
    maxVarchar->ExtractValues(state, maxVarcharOutputVector, 0);
    std::string_view strRes = maxVarcharOutput.GetValue(0);
    EXPECT_EQ(1, strRes.size());
    EXPECT_EQ(0, std::memcmp(strRes.data(), expectedStr.c_str(), 1));
    state.Reset();

    // process decimal
    maxDecimal->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    Decimal128 expected(1);
    EXPECT_EQ(expected, *static_cast<Decimal128 *>(state.val));
    state.Reset();

    // process int to long
    maxIntLong->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    auto int64Value = *static_cast<int64_t *>(state.val);
    int64Value &= 0x0000FFFF;
    EXPECT_EQ(1, int64Value);
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

    auto longInputVec = BuildAggregateInput(LongType(), rowPerVecBatch);
    auto decimalInputVec = BuildAggregateInput(Decimal128Type(38, 0), rowPerVecBatch);
    LongDataType longDataType;
    int32_t ids[rowPerVecBatch];
    int64_t dict[rowPerVecBatch];
    for (int32_t i = 0; i < rowPerVecBatch; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longDataType, rowPerVecBatch, ids, rowPerVecBatch, dict);
    auto nullInputVec = BuildAggregateInput(NoneType(), rowPerVecBatch);

    VectorBatch *vectorBatch = new VectorBatch(4);
    vectorBatch->Append(longInputVec);
    vectorBatch->Append(decimalInputVec);
    vectorBatch->Append(dictInputVec.release());
    vectorBatch->Append(nullInputVec);

    // process long
    AggregateState state { nullptr };
    avgLong->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    Vector<double> avgLongOutput(1);
    std::vector<BaseVector *> avgLongOutputVector;
    avgLongOutputVector.push_back(&avgLongOutput);
    avgLong->ExtractValues(state, avgLongOutputVector, 0);
    EXPECT_TRUE(avgLongOutput.GetValue(0) - 1 <= DBL_EPSILON);
    state.Reset();

    // process null
    avgNull->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    Vector<double> avgNullOutput(1);
    std::vector<BaseVector *> avgNullOutputVector;
    avgNullOutputVector.push_back(&avgNullOutput);
    avgNull->ExtractValues(state, avgNullOutputVector, 0);
    EXPECT_TRUE(avgNullOutput.IsNull(0));
    state.Reset();

    VectorHelper::FreeVecBatch(vectorBatch);
    delete avgFactory;
}

TEST(AggregatorTest, spark_sum_decimal64_normal)
{
    auto sumFactory = new SumSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto sumDeciAggPartial =
        sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal64Type(18, 6)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)).get()), channal0, true, true);

    auto *deci18_6Vec = new Vector<int64_t>(3);
    deci18_6Vec->SetValue(0, 999999999999999999L);
    deci18_6Vec->SetValue(1, 999999999999999999L);
    deci18_6Vec->SetValue(2, 999999999999999999L);

    auto *isOverflowVec = new Vector<bool>(3);
    isOverflowVec->SetValue(0, false);
    isOverflowVec->SetValue(1, false);
    isOverflowVec->SetValue(2, false);

    auto *resultVec = new Vector<Decimal128>(1);

    std::vector<BaseVector *> extractVec = { resultVec, isOverflowVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(deci18_6Vec);
    vecBatch->Append(isOverflowVec);

    AggregateState state { nullptr };
    sumDeciAggPartial->InitState(state);
    sumDeciAggPartial->ProcessGroup(state, vecBatch, 0);
    sumDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128Wrapper expected1("999999999999.999999");

    EXPECT_EQ(expected1.ToDecimal128(), resultVec->GetValue(0));
    EXPECT_EQ(expected1.ToDecimal128(), resultVec->GetValue(0));
    EXPECT_FALSE(isOverflowVec->GetValue(0));

    sumDeciAggPartial->ProcessGroup(state, vecBatch, 1);
    sumDeciAggPartial->ProcessGroup(state, vecBatch, 2);
    auto sumDeciAggFinal =
        sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)).get()),
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
    auto sumDeciAggPartial =
        sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(25, 8)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)).get()), channal0, true, true);

    Decimal128 deci("99999999999999999.99999999");
    auto *deci25_8Vec = new Vector<Decimal128>(3);
    deci25_8Vec->SetValue(0, deci);
    deci25_8Vec->SetValue(1, deci);
    deci25_8Vec->SetValue(2, deci);

    auto *isOverflowVec = new Vector<bool>(3);
    isOverflowVec->SetValue(0, false);
    isOverflowVec->SetValue(1, false);
    isOverflowVec->SetValue(2, false);

    auto *resultVec = new Vector<Decimal128>(1);

    std::vector<BaseVector *> extractVec = { resultVec, isOverflowVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(deci25_8Vec);
    vecBatch->Append(isOverflowVec);

    AggregateState state { nullptr };
    sumDeciAggPartial->InitState(state);
    sumDeciAggPartial->ProcessGroup(state, vecBatch, 0);
    sumDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1("99999999999999999.99999999");

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_FALSE(isOverflowVec->GetValue(0));

    auto sumDeciAggFinal =
        sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)).get()),
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
    auto sumDeciAggPartial =
        sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()), channal0, true, true, false);

    Decimal128 deci("99999999999999999999999999999999999999");
    auto *deci38_0Vec = new Vector<Decimal128>(3);
    deci38_0Vec->SetValue(0, deci);
    deci38_0Vec->SetValue(1, deci);
    deci38_0Vec->SetValue(2, deci);

    auto *isOverflowVec = new Vector<bool>(3);
    isOverflowVec->SetValue(0, false);
    isOverflowVec->SetValue(1, false);
    isOverflowVec->SetValue(2, false);

    auto *resultVec = new Vector<Decimal128>(1);

    std::vector<BaseVector *> extractVec = { resultVec, isOverflowVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(deci38_0Vec);
    vecBatch->Append(isOverflowVec);

    AggregateState state { nullptr };
    sumDeciAggPartial->InitState(state);
    sumDeciAggPartial->ProcessGroup(state, vecBatch, 0);
    sumDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1("99999999999999999999999999999999999999");

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));
    EXPECT_FALSE(isOverflowVec->GetValue(0));

    auto sumDeciAggFinal =
        sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
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
    auto sumDeciAggPartial =
        sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()), channal0, true, true, true);

    Decimal128 deci("99999999999999999999999999999999999999");
    auto *deci38_0Vec = new Vector<Decimal128>(3);
    deci38_0Vec->SetValue(0, deci);
    deci38_0Vec->SetValue(1, deci);
    deci38_0Vec->SetValue(2, deci);

    auto *isOverflowVec = new Vector<bool>(3);
    isOverflowVec->SetValue(0, false);
    isOverflowVec->SetValue(1, false);
    isOverflowVec->SetValue(2, false);

    auto *resultVec = new Vector<Decimal128>(1);

    std::vector<BaseVector *> extractVec = { resultVec, isOverflowVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(deci38_0Vec);
    vecBatch->Append(isOverflowVec);

    AggregateState state { nullptr };
    sumDeciAggPartial->InitState(state);
    sumDeciAggPartial->ProcessGroup(state, vecBatch, 0);
    sumDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1("99999999999999999999999999999999999999");

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));
    EXPECT_FALSE(isOverflowVec->GetValue(0));

    auto sumDeciAggFinal =
        sumFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()), channal0, false, false, true);

    sumDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    sumDeciAggFinal->ProcessGroup(state, vecBatch, 2);
    sumDeciAggFinal->ExtractValues(state, extractVec, 0);

    EXPECT_TRUE(resultVec->IsNull(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete sumFactory;
}

TEST(AggregatorTest, spark_avg_decimal64_normal)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgDeciAggPartial =
        avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal64Type(18, 6)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)).get()), channal0, true, true);

    auto *deci18_6Vec = new Vector<int64_t>(3);
    deci18_6Vec->SetValue(0, 999999999999999999L);
    deci18_6Vec->SetValue(1, 999999999999999999L);
    deci18_6Vec->SetValue(2, 999999999999999999L);

    auto *avgCountVec = new Vector<int64_t>(3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Vector<Decimal128>(1);

    std::vector<BaseVector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(deci18_6Vec);
    vecBatch->Append(avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128Wrapper expected1("999999999999.999999");

    EXPECT_EQ(expected1.ToDecimal128().ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));

    avgDeciAggPartial->ProcessGroup(state, vecBatch, 1);
    avgDeciAggPartial->ProcessGroup(state, vecBatch, 2);
    auto avgDeciAggFinal =
        avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(28, 6)).get()),
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
    auto avgDeciAggPartial =
        avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(25, 8)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)).get()), channal0, true, true);

    Decimal128Wrapper deci("99999999999999999.99999999");
    Decimal128 decimal128 = deci.ToDecimal128();

    auto *deci25_8Vec = new Vector<Decimal128>(3);
    deci25_8Vec->SetValue(0, decimal128);
    deci25_8Vec->SetValue(1, decimal128);
    deci25_8Vec->SetValue(2, decimal128);

    auto *avgCountVec = new Vector<int64_t>(3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Vector<Decimal128>(1);

    std::vector<BaseVector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(deci25_8Vec);
    vecBatch->Append(avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128Wrapper expected1("99999999999999999.99999999");
    EXPECT_EQ(expected1.ToDecimal128().ToString(), Decimal128Wrapper(resultVec->GetValue(0)).ToString());
    EXPECT_EQ(expected1.ToDecimal128(), resultVec->GetValue(0));

    auto avgDeciAggFinal =
        avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(35, 8)).get()),
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
    auto avgDeciAggPartial =
        avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()), channal0, true, true, false);

    Decimal128 deci("99999999999999999999999999999999999999");
    auto *deci38_0Vec = new Vector<Decimal128>(3);
    deci38_0Vec->SetValue(0, deci);
    deci38_0Vec->SetValue(1, deci);
    deci38_0Vec->SetValue(2, deci);

    auto *avgCountVec = new Vector<int64_t>(3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Vector<Decimal128>(1);

    std::vector<BaseVector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(deci38_0Vec);
    vecBatch->Append(avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1("99999999999999999999999999999999999999");

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));

    auto avgDeciAggFinal =
        avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
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
    auto avgDeciAggPartial =
        avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()), channal0, true, true, true);

    Decimal128 deci("99999999999999999999999999999999999999");
    auto *deci38_0Vec = new Vector<Decimal128>(3);
    deci38_0Vec->SetValue(0, deci);
    deci38_0Vec->SetValue(1, deci);
    deci38_0Vec->SetValue(2, deci);

    auto *avgCountVec = new Vector<int64_t>(3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Vector<Decimal128>(1);

    std::vector<BaseVector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(deci38_0Vec);
    vecBatch->Append(avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    Decimal128 expected1("99999999999999999999999999999999999999");

    EXPECT_EQ(expected1.ToString(), resultVec->GetValue(0).ToString());
    EXPECT_EQ(expected1, resultVec->GetValue(0));

    auto avgDeciAggFinal =
        avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 0)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 4)).get()), channal0, false, false, true);

    avgDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    avgDeciAggFinal->ProcessGroup(state, vecBatch, 2);
    avgDeciAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_TRUE(resultVec->IsNull(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch);
    delete resultVec;
    delete avgFactory;
}

TEST(AggregatorTest, spark_avg_decimal128_count_cast_to_wider_type_overflow_return_null_when_isOverflowAsNull_is_true)
{
    auto avgFactory = new AverageSparkAggregatorFactory();
    std::vector<int32_t> channal0 = { 0, 1 };
    auto avgDeciAggPartial =
        avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 38)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 38)).get()), channal0, true, true, true);

    Decimal128Wrapper deci1("-0.99999999999999999999999999999999999999");
    Decimal128Wrapper deci2("0.14159265354378240000000000000000000000");
    Decimal128Wrapper deci3("0.00000000000000000000000000000000000000");

    Decimal128 decimal128_1 = deci1.ToDecimal128();
    Decimal128 decimal128_2 = deci2.ToDecimal128();
    Decimal128 decimal128_3 = deci3.ToDecimal128();

    auto *deci38_38Vec = new Vector<Decimal128>(3);
    deci38_38Vec->SetValue(0, decimal128_1);
    deci38_38Vec->SetValue(1, decimal128_2);
    deci38_38Vec->SetValue(2, decimal128_3);

    auto *avgCountVec = new Vector<int64_t>(3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Vector<Decimal128>(1);

    std::vector<BaseVector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(deci38_38Vec);
    vecBatch->Append(avgCountVec);

    AggregateState state { nullptr };
    avgDeciAggPartial->InitiateGroup(state, vecBatch, 0);
    avgDeciAggPartial->ExtractValues(state, extractVec, 0);

    auto avgDeciAggFinal =
        avgFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 38)).get()),
        *(AggregatorUtil::WrapWithDataTypes(Decimal128Type(38, 38)).get()), channal0, false, false, true);

    avgDeciAggFinal->ProcessGroup(state, vecBatch, 1);
    avgDeciAggFinal->ProcessGroup(state, vecBatch, 2);
    avgDeciAggFinal->ExtractValues(state, extractVec, 0);
    EXPECT_TRUE(resultVec->IsNull(0));

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

    Decimal128Wrapper deci1("1234567890123456789012");
    Decimal128Wrapper deci2("9999999999999999999999");

    Decimal128 decimal128_1 = deci1.ToDecimal128();
    Decimal128 decimal128_2 = deci2.ToDecimal128();

    auto *deci22_0Vec = new Vector<Decimal128>(2);
    deci22_0Vec->SetValue(0, decimal128_1);
    deci22_0Vec->SetValue(1, decimal128_2);

    auto *avgCountVec = new Vector<int64_t>(2);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);

    auto *resultVec = new Vector<Decimal128>(1);

    std::vector<BaseVector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(deci22_0Vec);
    vecBatch->Append(avgCountVec);

    AggregateState state { nullptr };
    avgDeciWindow->InitState(state);
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

    auto *shortVec = new Vector<short>(3);
    shortVec->SetValue(0, 12345);
    shortVec->SetValue(1, 23451);
    shortVec->SetValue(2, 12345);

    auto *resultVec = new Vector<int64_t>(1);
    std::vector<BaseVector *> extractVec = { resultVec };

    auto *vecBatch = new VectorBatch(1);
    vecBatch->Append(shortVec);

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

    auto *intVec = new Vector<int32_t>(3);
    intVec->SetValue(0, 1234567890);
    intVec->SetValue(1, 2045678901);
    intVec->SetValue(2, 1234567890);

    auto *resultVec = new Vector<int64_t>(1);
    std::vector<BaseVector *> extractVec = { resultVec };

    auto *vecBatch = new VectorBatch(1);
    vecBatch->Append(intVec);

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

    auto *longVec = new Vector<int64_t>(3);
    longVec->SetValue(0, 1234567890123456789);
    longVec->SetValue(1, 2345678901234567891);
    longVec->SetValue(2, 3456789012345678912);

    auto *resultVec = new Vector<int64_t>(1);
    std::vector<BaseVector *> extractVec = { resultVec };

    auto *vecBatch = new VectorBatch(1);
    vecBatch->Append(longVec);

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

    auto *longVec = new Vector<int64_t>(3);
    longVec->SetValue(0, 9223372036854774807);
    longVec->SetValue(1, 9223372036854774807);
    longVec->SetValue(2, 9223372036854774807);

    auto *resultVec = new Vector<int64_t>(1);
    std::vector<BaseVector *> extractVec = { resultVec };

    auto *vecBatch = new VectorBatch(1);
    vecBatch->Append(longVec);

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

    auto *doubleVec = new Vector<double>(3);
    doubleVec->SetValue(0, 123456789012.3456789);
    doubleVec->SetValue(1, 234567890123.4567891);
    doubleVec->SetValue(2, 345678901234.5678912);

    auto *resultVec = new Vector<double>(1);
    std::vector<BaseVector *> extractVec = { resultVec };

    auto *vecBatch = new VectorBatch(1);
    vecBatch->Append(doubleVec);

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

    auto *shortVec = new Vector<short>(3);
    shortVec->SetValue(0, 12345);
    shortVec->SetValue(1, 23451);
    shortVec->SetValue(2, 12345);

    auto *avgCountVec = new Vector<int64_t>(3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Vector<double>(1);
    std::vector<BaseVector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(shortVec);
    vecBatch->Append(avgCountVec);

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

    auto *intVec = new Vector<int32_t>(3);
    intVec->SetValue(0, 1234567890);
    intVec->SetValue(1, 2045678901);
    intVec->SetValue(2, 1234567890);

    auto *avgCountVec = new Vector<int64_t>(3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Vector<double>(1);
    std::vector<BaseVector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(intVec);
    vecBatch->Append(avgCountVec);

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

    auto *longVec = new Vector<int64_t>(3);
    longVec->SetValue(0, 9223372036854774807L);
    longVec->SetValue(1, 9223372036854774807L);
    longVec->SetValue(2, 9223372036854774807L);

    auto *avgCountVec = new Vector<int64_t>(3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Vector<double>(1);
    std::vector<BaseVector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(longVec);
    vecBatch->Append(avgCountVec);

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

    auto *doubleVec = new Vector<double>(3);
    doubleVec->SetValue(0, 123456789012.3456789);
    doubleVec->SetValue(1, 234567890123.4567891);
    doubleVec->SetValue(2, 345678901234.5678912);

    auto *avgCountVec = new Vector<int64_t>(3);
    avgCountVec->SetValue(0, 1);
    avgCountVec->SetValue(1, 1);
    avgCountVec->SetValue(2, 1);

    auto *resultVec = new Vector<double>(1);
    std::vector<BaseVector *> extractVec = { resultVec, avgCountVec };

    auto *vecBatch = new VectorBatch(2);
    vecBatch->Append(doubleVec);
    vecBatch->Append(avgCountVec);

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

    auto *inputShortVec1 = new Vector<short>(5);
    for (int i = 0; i < 5; i++) {
        inputShortVec1->SetNull(i);
    }

    auto *vecBatch1 = new VectorBatch(1);
    vecBatch1->Append(inputShortVec1);

    auto *resultfirstVec1 = new Vector<short>(1);
    auto *resultValueSetVec1 = new Vector<bool>(1);
    std::vector<BaseVector *> extractVecs = { resultfirstVec1, resultValueSetVec1 };

    AggregateState state { nullptr };

    // add first VectorBatch
    firstIgnoreNullIntAggPartial->InitiateGroup(state, vecBatch1, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 1);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 2);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 3);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 4);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    // add second VectorBatch,keep value not change
    auto *inputShortVec2 = new Vector<short>(3);
    inputShortVec2->SetNull(0);
    inputShortVec2->SetValue(1, 211);
    inputShortVec2->SetNull(2);
    auto *vecBatch2 = new VectorBatch(1);
    vecBatch2->Append(inputShortVec2);

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch2, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
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

    auto *inputIntVec1 = new Vector<int32_t>(5);
    for (int i = 0; i < 5; i++) {
        inputIntVec1->SetNull(i);
    }

    auto *vecBatch1 = new VectorBatch(1);
    vecBatch1->Append(inputIntVec1);

    auto *resultfirstVec1 = new Vector<int32_t>(1);
    auto *resultValueSetVec1 = new Vector<bool>(1);
    std::vector<BaseVector *> extractVecs = { resultfirstVec1, resultValueSetVec1 };

    AggregateState state { nullptr };

    // add first VectorBatch
    firstIgnoreNullIntAggPartial->InitiateGroup(state, vecBatch1, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 1);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 2);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 3);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 4);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    // add second VectorBatch,keep value not change
    auto *inputIntVec2 = new Vector<int32_t>(3);
    inputIntVec2->SetNull(0);
    inputIntVec2->SetValue(1, 211);
    inputIntVec2->SetNull(2);
    auto *vecBatch2 = new VectorBatch(1);
    vecBatch2->Append(inputIntVec2);

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch2, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
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

    auto *inputIntVec1 = new Vector<int32_t>(5);
    inputIntVec1->SetNull(0);
    inputIntVec1->SetNull(1);
    inputIntVec1->SetValue(2, 111);
    inputIntVec1->SetValue(3, 113);
    inputIntVec1->SetNull(4);

    auto *vecBatch1 = new VectorBatch(1);
    vecBatch1->Append(inputIntVec1);

    auto *resultfirstVec1 = new Vector<int32_t>(1);
    auto *resultValueSetVec1 = new Vector<bool>(1);
    std::vector<BaseVector *> extractVecs = { resultfirstVec1, resultValueSetVec1 };

    AggregateState state { nullptr };
    firstWithNullIntAggPartial->InitiateGroup(state, vecBatch1, 0);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    firstWithNullIntAggPartial->ProcessGroup(state, vecBatch1, 1);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    firstWithNullIntAggPartial->ProcessGroup(state, vecBatch1, 2);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    firstWithNullIntAggPartial->ProcessGroup(state, vecBatch1, 3);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_TRUE(resultValueSetVec1->GetValue(0));

    firstWithNullIntAggPartial->ProcessGroup(state, vecBatch1, 4);
    firstWithNullIntAggPartial->ExtractValues(state, extractVecs, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
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

    std::vector<int32_t> channal0 = { 0 };
    auto firstIgnoreNullIntAggPartial =
        firstIgnoreNullFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal0, true, true);

    auto *inputLongVec1 = new Vector<int64_t>(2);
    inputLongVec1->SetNull(0);
    inputLongVec1->SetNull(1);

    auto *vecBatch1 = new VectorBatch(1);
    vecBatch1->Append(inputLongVec1);

    auto *resultfirstVec1 = new Vector<int64_t>(1);
    auto *resultValueSetVec1 = new Vector<bool>(1);
    std::vector<BaseVector *> extractVecs1 = { resultfirstVec1, resultValueSetVec1 };

    AggregateState state { nullptr };

    // add first VectorBatch
    firstIgnoreNullIntAggPartial->InitiateGroup(state, vecBatch1, 0);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs1, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    firstIgnoreNullIntAggPartial->ProcessGroup(state, vecBatch1, 1);
    firstIgnoreNullIntAggPartial->ExtractValues(state, extractVecs1, 0);
    EXPECT_TRUE(resultfirstVec1->IsNull(0));
    EXPECT_FALSE(resultValueSetVec1->GetValue(0));

    std::vector<DataTypePtr> vector;
    vector.push_back(LongType());
    vector.push_back(BooleanType());
    DataTypesPtr inputTypes = std::make_unique<DataTypes>(vector);

    std::vector<int32_t> channal2 = { 0, 1 };
    auto firstIgnoreNullIntAggFinal = firstIgnoreNullFactory->CreateAggregator(*inputTypes.get(),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal2, false, false);

    auto *inputLongVec2 = new Vector<int64_t>(4);
    inputLongVec2->SetNull(0);
    inputLongVec2->SetValue(1, 111);
    inputLongVec2->SetNull(2);
    inputLongVec2->SetNull(3);

    auto *inputBooleanVec2 = new Vector<bool>(4);
    inputBooleanVec2->SetValue(0, false);
    inputBooleanVec2->SetValue(1, true);
    inputBooleanVec2->SetValue(2, false);
    inputBooleanVec2->SetValue(3, false);

    auto *vecBatch2 = new VectorBatch(2);
    vecBatch2->Append(inputLongVec2);
    vecBatch2->Append(inputBooleanVec2);

    auto *resultfirstVec2 = new Vector<int64_t>(1);
    std::vector<BaseVector *> extractVecs2 = { resultfirstVec2 };

    firstIgnoreNullIntAggFinal->InitiateGroup(state, vecBatch2, 0);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_TRUE(resultfirstVec2->IsNull(0));

    firstIgnoreNullIntAggFinal->ProcessGroup(state, vecBatch2, 1);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_FALSE(resultfirstVec2->IsNull(0));
    EXPECT_EQ(111, resultfirstVec2->GetValue(0));

    firstIgnoreNullIntAggFinal->ProcessGroup(state, vecBatch2, 2);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_FALSE(resultfirstVec2->IsNull(0));
    EXPECT_EQ(111, resultfirstVec2->GetValue(0));

    firstIgnoreNullIntAggFinal->ProcessGroup(state, vecBatch2, 3);
    firstIgnoreNullIntAggFinal->ExtractValues(state, extractVecs2, 0);
    EXPECT_FALSE(resultfirstVec2->IsNull(0));
    EXPECT_EQ(111, resultfirstVec2->GetValue(0));

    state.val = nullptr;
    VectorHelper::FreeVecBatch(vecBatch1);
    VectorHelper::FreeVecBatch(vecBatch2);
    delete resultfirstVec1;
    delete resultValueSetVec1;
    delete resultfirstVec2;
    delete firstIgnoreNullFactory;
}

TEST(AggregatorTest, typed_aggregator_test)
{
    class TestTypeAggregator : public TypedAggregator {
    public:
        TestTypeAggregator(const FunctionType aggregateType, const DataTypes &inputTypes, const DataTypes &outputTypes,
            const std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial,
            const bool isOverflowAsNull)
            : TypedAggregator(aggregateType, inputTypes, outputTypes, channels, inputRaw, outputPartial,
            isOverflowAsNull)
        {}

        BaseVector *GetVector(VectorBatch *vectorBatch, const int32_t rowOffset, const int32_t rowCount,
            uint8_t **nullMap, AggregatorBuffer<int32_t> &indexMap, const size_t channelIdx)
        {
            return TypedAggregator::GetVector(vectorBatch, rowOffset, rowCount, nullMap, indexMap, channelIdx);
        }
        void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, const int32_t rowIndex)
        {
            return;
        }

        virtual void ProcessSingleInternal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
            const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap)
        {
            return;
        }

        virtual void ProcessSingleInternalFilter(AggregateState &state, BaseVector *vector, Vector<bool> *booleanVector,
            const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap)
        {
            return;
        }

        virtual void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
            BaseVector *vector, const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap)
        {
            return;
        }
        virtual void ProcessGroupInternalFilter(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
            BaseVector *vector, Vector<bool> *booleanVector, const int32_t rowOffset, const uint8_t *nullMap,
            const int32_t *indexMap)
        {
            return;
        }
    };
    // just used to produce vector
    auto inputType = DoubleType();
    DataTypes inputTypes({ inputType }), outputTypes({ inputType });
    std::vector<int32_t> channels { 0 };
    bool rawIn = false, partialOut = false, isOverflowAsNull = false;

    const int dataSize = 6;
    double data0[dataSize] = {0.0f, 1.0f, 2.0f, 0.0f, 1.0f, 2.0f};
    void *datas[1] = {data0};
    std::vector<DataTypePtr> sourceFieldTypes { IntType() };
    DataTypes sourceTypes(sourceFieldTypes);
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vectorBatch = new VectorBatch(dataSize);
    DataType type(inputType->GetId());
    Vector<double> *doubleVector = new Vector<double>(dataSize);
    for (int i = 0; i < dataSize; i++) {
        doubleVector->SetValue(i, data0[i]);
    }
    std::unique_ptr<BaseVector> uniqueV =
        VectorHelper::CreateDictionaryVector(ids, dataSize, doubleVector, inputType->GetId());
    vectorBatch->Append(uniqueV.release());
    delete doubleVector;
    AggregateState state;
    auto rawVectorBatch = CreateVectorBatch(inputTypes, dataSize, datas[0]);
    std::vector<DataTypePtr> typesPtr = {
        nullptr,         IntType(),        LongType(), DoubleType(),    BooleanType(),   ShortType(),
        Decimal64Type(), Decimal128Type(), IntType(),  LongType(),      IntType(),       LongType(),
        LongType(),      LongType(),       LongType(), VarcharType(15), VarcharType(16), VarcharType(15),
    };
    {
        TestTypeAggregator agg(omniruntime::op::FunctionType::OMNI_AGGREGATION_TYPE_SUM,
            DataTypes({ typesPtr.at(OMNI_SHORT) }), DataTypes({ typesPtr.at(OMNI_SHORT) }), channels, rawIn, partialOut,
            isOverflowAsNull);

        uint8_t *nullMap = nullptr;
        AggregatorBuffer<int32_t> indexMap;
        agg.GetVector(vectorBatch, 0, dataSize, &nullMap, indexMap, 0);
        indexMap.Release();
        agg.GetVector(rawVectorBatch, 0, dataSize, &nullMap, indexMap, 0);
        indexMap.Release();
        rawVectorBatch->Get(0)->SetNull(1);
        agg.GetVector(vectorBatch, 0, dataSize, &nullMap, indexMap, 0);
    }
    VectorHelper::FreeVecBatch(vectorBatch);
    VectorHelper::FreeVecBatch(rawVectorBatch);
}

template <typename T> class ExtremumValueCreator {
public:
    static constexpr T MinValue = std::numeric_limits<T>::lowest();
    static constexpr T MaxValue = std::numeric_limits<T>::max();
    // result will be (extrame_min_value, rand_data... extrame_max_value)
    std::vector<T> ProduceExtremumData(int32_t totoalNum)
    {
        std::vector<T> ret;
        ret.reserve(totoalNum);
        ret.emplace_back(MinValue);
        if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
            for (int i = 1; i < totoalNum - 1; ++i) {
                float d = float(i) * 0.01f;
                ret.emplace_back(d);
            }
        } else if constexpr (std::is_same_v<T, Decimal128>) {
            for (int i = 1; i < totoalNum - 1; ++i) {
                Decimal128 d(i);
                ret.emplace_back(d);
            }
        } else {
            for (int i = 1; i < totoalNum - 1; ++i) {
                T d = (i % MaxValue);
                ret.emplace_back(d);
            }
        }
        ret.emplace_back(MaxValue);
        return ret;
    }
    std::vector<T> ProduceOnlyMinimum(int32_t totoalNum)
    {
        return std::vector<T>(totoalNum, MinValue);
    }
    std::vector<T> ProduceOnlyMaximum(int32_t totoalNum)
    {
        return std::vector<T>(totoalNum, MaxValue);
    }

    std::unique_ptr<vec::BaseVector> ProduceVec(int32_t totoalNum)
    {
        auto value = ProduceExtremumData(totoalNum);
        auto dataPtr = const_cast<T *>(value.data());
        return TestUtil::CreateVector<T>(totoalNum, dataPtr);
    }
    std::unique_ptr<vec::BaseVector> ProduceVecButOnlyMinimum(int32_t totoalNum)
    {
        auto value = ProduceOnlyMinimum(totoalNum);
        auto dataPtr = const_cast<T *>(value.data());
        return TestUtil::CreateVector<T>(totoalNum, dataPtr);
    }
    std::unique_ptr<vec::BaseVector> ProduceVecButOnlyMaximum(int32_t totoalNum)
    {
        auto value = ProduceOnlyMaximum(totoalNum);
        auto dataPtr = const_cast<T *>(value.data());
        return TestUtil::CreateVector<T>(totoalNum, dataPtr);
    }
};

class DecimalCreator {
public:
    static Decimal128 MinValue;
    static Decimal128 MaxValue;
    // result will be (extrame_min_value, rand_data... extrame_max_value)
    std::vector<Decimal128> ProduceExtremumData(int32_t totoalNum)
    {
        std::vector<Decimal128> ret;
        ret.reserve(totoalNum);
        ret.emplace_back(MinValue);
        for (int i = 1; i < totoalNum - 1; ++i) {
            Decimal128 d(i);
            ret.emplace_back(d);
        }
        ret.emplace_back(MaxValue);
        return ret;
    }
    std::vector<Decimal128> ProduceOnlyMinimum(int32_t totoalNum)
    {
        return std::vector<Decimal128>(totoalNum, MinValue);
    }
    std::vector<Decimal128> ProduceOnlyMaximum(int32_t totoalNum)
    {
        return std::vector<Decimal128>(totoalNum, MaxValue);
    }

    std::unique_ptr<vec::BaseVector> ProduceVec(int32_t totoalNum)
    {
        auto value = ProduceExtremumData(totoalNum);
        auto dataPtr = const_cast<type::Decimal128 *>(value.data());
        return TestUtil::CreateVector<Decimal128>(totoalNum, dataPtr);
    }

    std::unique_ptr<vec::BaseVector> ProduceVecButOnlyMinimum(int32_t totoalNum)
    {
        auto value = ProduceOnlyMinimum(totoalNum);
        auto dataPtr = const_cast<type::Decimal128 *>(value.data());
        return TestUtil::CreateVector<Decimal128>(totoalNum, dataPtr);
    }

    std::unique_ptr<vec::BaseVector> ProduceVecButOnlyMaximum(int32_t totoalNum)
    {
        auto value = ProduceOnlyMaximum(totoalNum);
        auto dataPtr = const_cast<type::Decimal128 *>(value.data());
        return TestUtil::CreateVector<Decimal128>(totoalNum, dataPtr);
    }
};

Decimal128 DecimalCreator::MinValue = Decimal128(type::DECIMAL128_MIN_VALUE);
Decimal128 DecimalCreator::MaxValue = Decimal128(type::DECIMAL128_MAX_VALUE);
TEST(AggregatorTest, max_agg_extrame_value_test)
{
    int32_t rowPerVecBatch = 200;
    auto maxFactory = new MaxAggregatorFactory();
    auto minFactory = new MinAggregatorFactory();
    std::vector<int32_t> channal0 = { 0 };
    std::vector<int32_t> channal1 = { 1 };
    std::vector<int32_t> channal2 = { 2 };
    std::vector<int32_t> channal3 = { 3 };
    std::vector<int32_t> channal4 = { 4 };
    std::vector<int32_t> channal5 = { 5 };
    // max test types : long + decimal + varchar + dictionary + null
    auto maxBoolean = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(BooleanType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(BooleanType()).get()), channal0, true, false, false);
    auto maxShort = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(ShortType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(ShortType()).get()), channal1, true, false, false);
    auto maxInt = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(IntType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(IntType()).get()), channal2, true, false, false);
    auto maxLong = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal3, true, false, false);
    auto maxDouble = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal4, true, false, false);
    auto maxDecimal = maxFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE).get()),
        *(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE).get()), channal5, true, false, false);

    auto minBoolean = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(BooleanType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(BooleanType()).get()), channal0, true, false, false);
    auto minShort = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(ShortType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(ShortType()).get()), channal1, true, false, false);
    auto minInt = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(IntType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(IntType()).get()), channal2, true, false, false);
    auto minLong = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LongType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(LongType()).get()), channal3, true, false, false);
    auto minDouble = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()),
        *(AggregatorUtil::WrapWithDataTypes(DoubleType()).get()), channal4, true, false, false);
    auto minDecimal = minFactory->CreateAggregator(*(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE).get()),
        *(AggregatorUtil::WrapWithDataTypes(LONG_DECIMAL_TYPE).get()), channal5, true, false, false);

    ExtremumValueCreator<int8_t> int8Creator;
    auto boolVector = int8Creator.ProduceVec(rowPerVecBatch);
    auto minBoolVector = int8Creator.ProduceVecButOnlyMinimum(rowPerVecBatch);
    auto maxBoolVector = int8Creator.ProduceVecButOnlyMaximum(rowPerVecBatch);

    ExtremumValueCreator<int16_t> int16Creator;
    auto shortVector = int16Creator.ProduceVec(rowPerVecBatch);
    auto minShortVector = int16Creator.ProduceVecButOnlyMinimum(rowPerVecBatch);
    auto maxShortVector = int16Creator.ProduceVecButOnlyMaximum(rowPerVecBatch);

    ExtremumValueCreator<int32_t> int32Creator;
    auto intVector = int32Creator.ProduceVec(rowPerVecBatch);
    auto minIntVector = int32Creator.ProduceVecButOnlyMinimum(rowPerVecBatch);
    auto maxIntVector = int32Creator.ProduceVecButOnlyMaximum(rowPerVecBatch);

    ExtremumValueCreator<int64_t> int64Creator;
    auto longVector = int64Creator.ProduceVec(rowPerVecBatch);
    auto minLongVector = int64Creator.ProduceVecButOnlyMinimum(rowPerVecBatch);
    auto maxLongVector = int64Creator.ProduceVecButOnlyMaximum(rowPerVecBatch);

    ExtremumValueCreator<double> doubleCreator;
    auto doubleVector = doubleCreator.ProduceVec(rowPerVecBatch);
    auto minDoubleVector = doubleCreator.ProduceVecButOnlyMinimum(rowPerVecBatch);
    auto maxDoubleVector = doubleCreator.ProduceVecButOnlyMaximum(rowPerVecBatch);

    DecimalCreator decimalCreator;
    auto decimalVector = decimalCreator.ProduceVec(rowPerVecBatch);
    auto minDecimalVector = decimalCreator.ProduceVecButOnlyMinimum(rowPerVecBatch);
    auto maxDecimalVector = decimalCreator.ProduceVecButOnlyMaximum(rowPerVecBatch);

    VectorBatch *vectorBatch = new VectorBatch(rowPerVecBatch);
    vectorBatch->Append(boolVector.release());
    vectorBatch->Append(shortVector.release());
    vectorBatch->Append(intVector.release());
    vectorBatch->Append(longVector.release());
    vectorBatch->Append(doubleVector.release());
    vectorBatch->Append(decimalVector.release());

    VectorBatch *minVectorBatch = new VectorBatch(rowPerVecBatch);
    minVectorBatch->Append(minBoolVector.release());
    minVectorBatch->Append(minShortVector.release());
    minVectorBatch->Append(minIntVector.release());
    minVectorBatch->Append(minLongVector.release());
    minVectorBatch->Append(minDoubleVector.release());
    minVectorBatch->Append(minDecimalVector.release());

    VectorBatch *maxVectorBatch = new VectorBatch(rowPerVecBatch);
    maxVectorBatch->Append(maxBoolVector.release());
    maxVectorBatch->Append(maxShortVector.release());
    maxVectorBatch->Append(maxIntVector.release());
    maxVectorBatch->Append(maxLongVector.release());
    maxVectorBatch->Append(maxDoubleVector.release());
    maxVectorBatch->Append(maxDecimalVector.release());

    {
        AggregateState state{ nullptr };
        maxBoolean->InitState(state);
        // process bool
        maxBoolean->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        auto maxValue = *static_cast<int8_t *>(state.val);
        EXPECT_EQ(int8Creator.MaxValue, maxValue);
    }
    {
        // process short
        AggregateState state{ nullptr };
        maxShort->InitState(state);
        maxShort->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        auto maxValue = *static_cast<int16_t *>(state.val);
        EXPECT_EQ(int16Creator.MaxValue, maxValue);
    }
    {
        // process short
        AggregateState state{ nullptr };
        maxShort->InitState(state);
        maxShort->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        auto maxValue = *static_cast<int16_t *>(state.val);
        EXPECT_EQ(int16Creator.MaxValue, maxValue);
    }
    {
        // process int
        AggregateState state{ nullptr };
        maxInt->InitState(state);
        maxInt->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        auto int64Value = *static_cast<int64_t *>(state.val);
        int32_t value = (int32_t)(int64Value);
        EXPECT_EQ(int32Creator.MaxValue, value);
    }
    {
        // process int64
        AggregateState state{ nullptr };
        maxLong->InitState(state);
        maxLong->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        auto int64Value = *static_cast<int64_t *>(state.val);
        EXPECT_EQ(int64Creator.MaxValue, int64Value);
    }
    {
        // process double
        AggregateState state{ nullptr };
        maxDouble->InitState(state);
        maxDouble->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        auto maxValue = *static_cast<double *>(state.val);
        EXPECT_EQ(doubleCreator.MaxValue, maxValue);
    }
    {
        // process decimal
        AggregateState state{ nullptr };
        maxDecimal->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        EXPECT_EQ(decimalCreator.MaxValue, *static_cast<Decimal128 *>(state.val));
    }
    // test maxAggregator but only min value
    {
        AggregateState state{ nullptr };
        maxBoolean->InitState(state);
        // process bool
        maxBoolean->ProcessGroup(state, minVectorBatch, 0, minVectorBatch->GetRowCount());
        auto maxValue = *static_cast<int8_t *>(state.val);
        EXPECT_EQ(int8Creator.MinValue, maxValue);
    }
    {
        AggregateState state{ nullptr };
        maxShort->InitState(state);
        maxShort->ProcessGroup(state, minVectorBatch, 0, minVectorBatch->GetRowCount());
        auto maxValue = *static_cast<int16_t *>(state.val);
        EXPECT_EQ(int16Creator.MinValue, maxValue);
    }
    {
        AggregateState state{ nullptr };
        maxInt->InitState(state);
        // process bool
        maxInt->ProcessGroup(state, minVectorBatch, 0, minVectorBatch->GetRowCount());
        auto int64Value = *static_cast<int64_t *>(state.val);
        int32_t value = (int32_t)(int64Value);
        EXPECT_EQ(int32Creator.MinValue, value);
    }
    {
        AggregateState state{ nullptr };
        maxLong->InitState(state);
        maxLong->ProcessGroup(state, minVectorBatch, 0, minVectorBatch->GetRowCount());
        auto maxValue = *static_cast<int64_t *>(state.val);
        EXPECT_EQ(int64Creator.MinValue, maxValue);
    }
    {
        AggregateState state{ nullptr };
        maxDouble->InitState(state);
        maxDouble->ProcessGroup(state, minVectorBatch, 0, minVectorBatch->GetRowCount());
        auto maxValue = *static_cast<double *>(state.val);
        EXPECT_EQ(doubleCreator.MinValue, maxValue);
    }
    {
        AggregateState state{ nullptr };
        maxDecimal->InitState(state);
        maxDecimal->ProcessGroup(state, minVectorBatch, 0, minVectorBatch->GetRowCount());
        auto maxValue = *static_cast<Decimal128 *>(state.val);
        EXPECT_EQ(decimalCreator.MinValue, maxValue);
    }


    // test min aggregator
    {
        AggregateState state{ nullptr };
        minBoolean->InitState(state);
        minBoolean->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        auto minValue = *static_cast<int8_t *>(state.val);
        EXPECT_EQ(int8Creator.MinValue, minValue);
    }
    {
        // process short
        AggregateState state{ nullptr };
        minShort->InitState(state);
        minShort->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        auto minValue = *static_cast<int16_t *>(state.val);
        EXPECT_EQ(int16Creator.MinValue, minValue);
    }
    {
        // process short
        AggregateState state{ nullptr };
        minShort->InitState(state);
        minShort->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        auto minValue = *static_cast<int16_t *>(state.val);
        EXPECT_EQ(int16Creator.MinValue, minValue);
    }
    {
        // process int
        AggregateState state{ nullptr };
        minInt->InitState(state);
        minInt->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        auto int64Value = *static_cast<int64_t *>(state.val);
        int32_t value = (int32_t)(int64Value);
        EXPECT_EQ(int32Creator.MinValue, value);
    }
    {
        // process int64
        AggregateState state{ nullptr };
        minLong->InitState(state);
        minLong->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        auto int64Value = *static_cast<int64_t *>(state.val);
        EXPECT_EQ(int64Creator.MinValue, int64Value);
    }
    {
        // process double
        AggregateState state{ nullptr };
        minDouble->InitState(state);
        minDouble->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        auto minValue = *static_cast<double *>(state.val);
        EXPECT_EQ(doubleCreator.MinValue, minValue);
    }
    {
        // process decimal
        AggregateState state{ nullptr };
        minDecimal->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
        EXPECT_EQ(decimalCreator.MinValue, *static_cast<Decimal128 *>(state.val));
    }

    // test minAggregator but only max value
    {
        AggregateState state{ nullptr };
        minBoolean->InitState(state);
        // process bool
        minBoolean->ProcessGroup(state, maxVectorBatch, 0, maxVectorBatch->GetRowCount());
        auto minValue = *static_cast<int8_t *>(state.val);
        EXPECT_EQ(int8Creator.MaxValue, minValue);
    }
    {
        AggregateState state{ nullptr };
        minShort->InitState(state);
        // process bool
        minShort->ProcessGroup(state, maxVectorBatch, 0, maxVectorBatch->GetRowCount());
        auto minValue = *static_cast<int16_t *>(state.val);
        EXPECT_EQ(int16Creator.MaxValue, minValue);
    }
    {
        AggregateState state{ nullptr };
        minInt->InitState(state);
        // process bool
        minInt->ProcessGroup(state, maxVectorBatch, 0, maxVectorBatch->GetRowCount());
        auto int64Value = *static_cast<int64_t *>(state.val);
        int32_t value = (int32_t)(int64Value);
        EXPECT_EQ(int32Creator.MaxValue, value);
    }
    {
        AggregateState state{ nullptr };
        minLong->InitState(state);
        minLong->ProcessGroup(state, maxVectorBatch, 0, maxVectorBatch->GetRowCount());
        auto minValue = *static_cast<int64_t *>(state.val);
        EXPECT_EQ(int64Creator.MaxValue, minValue);
    }
    {
        AggregateState state{ nullptr };
        minDouble->InitState(state);
        minDouble->ProcessGroup(state, maxVectorBatch, 0, maxVectorBatch->GetRowCount());
        auto minValue = *static_cast<double *>(state.val);
        EXPECT_EQ(doubleCreator.MaxValue, minValue);
    }
    {
        AggregateState state{ nullptr };
        minDecimal->InitState(state);
        minDecimal->ProcessGroup(state, maxVectorBatch, 0, maxVectorBatch->GetRowCount());
        auto minValue = *static_cast<Decimal128 *>(state.val);
        EXPECT_EQ(decimalCreator.MaxValue, minValue);
    }

    VectorHelper::FreeVecBatch(maxVectorBatch);
    VectorHelper::FreeVecBatch(minVectorBatch);
    VectorHelper::FreeVecBatch(vectorBatch);
    delete maxFactory;
    delete minFactory;
}

TEST(AggregatorTest, count_aggregator_exception)
{
    // just used to produce vector
    auto inputType = DoubleType();
    DataTypes inputTypes({ inputType }), outputTypes({ inputType });
    std::vector<int32_t> channels { 0 };
    bool rawIn = false, partialOut = false, isOverflowAsNull = false;

    auto agg = CountColumnAggregator<OMNI_NONE, OMNI_LONG>::Create(inputTypes, outputTypes, channels, rawIn, partialOut,
        isOverflowAsNull);
    EXPECT_TRUE(agg == nullptr);
#define TestColumnAggregator(IN, OUT)                                                                                \
    do {                                                                                                             \
        auto countAgg = CountColumnAggregator<IN, OUT>::Create(inputTypes, outputTypes, channels, rawIn, partialOut, \
            isOverflowAsNull);                                                                                       \
        EXPECT_TRUE(countAgg == nullptr);                                                                            \
        auto countAllAgg = CountAllAggregator<IN, OUT>::Create(inputTypes, outputTypes, channels, rawIn, partialOut, \
            isOverflowAsNull);                                                                                       \
        EXPECT_TRUE(countAllAgg == nullptr);                                                                         \
    } while (0)

    TestColumnAggregator(OMNI_NONE, OMNI_LONG);

    TestColumnAggregator(OMNI_BOOLEAN, OMNI_LONG);

    TestColumnAggregator(OMNI_SHORT, OMNI_LONG);

    TestColumnAggregator(OMNI_DATE32, OMNI_LONG);

    TestColumnAggregator(OMNI_TIME32, OMNI_LONG);

    TestColumnAggregator(OMNI_INT, OMNI_LONG);

    TestColumnAggregator(OMNI_LONG, OMNI_LONG);

    TestColumnAggregator(OMNI_DATE64, OMNI_LONG);

    TestColumnAggregator(OMNI_TIME64, OMNI_LONG);

    TestColumnAggregator(OMNI_TIMESTAMP, OMNI_LONG);

    TestColumnAggregator(OMNI_DOUBLE, OMNI_LONG);

    TestColumnAggregator(OMNI_DECIMAL64, OMNI_LONG);

    TestColumnAggregator(OMNI_DECIMAL128, OMNI_LONG);

    TestColumnAggregator(OMNI_CONTAINER, OMNI_LONG);

    TestColumnAggregator(OMNI_VARCHAR, OMNI_LONG);

    TestColumnAggregator(OMNI_CHAR, OMNI_LONG);

    TestColumnAggregator(OMNI_INVALID, OMNI_LONG);
}

#ifdef ENABLE_HMPP
TEST(AggregatorTest, hmpp_sum_aggregator_exceptions)
{
    ConfigUtil::SetEnableHMPP(true);
    auto inputType = DoubleType();
    DataTypes inputTypes({ inputType }), outputTypes({ inputType });
    std::vector<int32_t> channels { 0 };
    bool rawIn = false, partialOut = false, isOverflowAsNull = false;

    const int dataSize = 6;
    double data0[dataSize] = {0.0f, 1.0f, 2.0f, 0.0f, 1.0f, 2.0f};
    void *datas[1] = {data0};
    std::vector<DataTypePtr> sourceFieldTypes { IntType() };
    DataTypes sourceTypes(sourceFieldTypes);
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vectorBatch = new VectorBatch(dataSize);
    DataType type(inputType->GetId());
    vectorBatch->Append(CreateDictionaryVector(type, dataSize, ids, dataSize, datas[0]).release());
    AggregateState state;
    auto rawVectorBatch = CreateVectorBatch(inputTypes, dataSize, datas[0]);
    std::vector<DataTypePtr> typesPtr = {
        nullptr,         IntType(),        LongType(), DoubleType(),    BooleanType(),   ShortType(),
        Decimal64Type(), Decimal128Type(), IntType(),  LongType(),      IntType(),       LongType(),
        LongType(),      LongType(),       LongType(), VarcharType(15), VarcharType(16), ContainerType(),
    };
    {
        auto agg = SumAggregator<OMNI_SHORT, OMNI_SHORT>::Create(DataTypes({ typesPtr.at(OMNI_SHORT) }),
            DataTypes({ typesPtr.at(OMNI_SHORT) }), channels, rawIn, partialOut, isOverflowAsNull);
        auto aggPtr = reinterpret_cast<SumAggregator<OMNI_SHORT, OMNI_SHORT> *>(agg.get());
        EXPECT_ANY_THROW(aggPtr->ProcessGroupWithHMPP(state, vectorBatch));
        EXPECT_FALSE(agg->CanProcessWithHMPP(state, vectorBatch));
        EXPECT_FALSE(agg->CanProcessWithHMPP(state, rawVectorBatch));
    }
#define TestSumAggregator(IN, OUT)                                                                                 \
    do {                                                                                                           \
        auto agg = SumAggregator<IN, OUT>::Create(DataTypes({ typesPtr.at(IN) }), DataTypes({ typesPtr.at(OUT) }), \
            channels, rawIn, partialOut, isOverflowAsNull);                                                        \
        if (agg != nullptr) {                                                                                      \
            EXPECT_ANY_THROW(agg->ProcessGroupWithHMPP(state, vectorBatch));                                       \
            EXPECT_FALSE(agg->CanProcessWithHMPP(state, vectorBatch));                                             \
            EXPECT_FALSE(agg->CanProcessWithHMPP(state, rawVectorBatch));                                          \
        }                                                                                                          \
    } while (0)

    TestSumAggregator(OMNI_SHORT, OMNI_LONG);

    TestSumAggregator(OMNI_SHORT, OMNI_DOUBLE);

    TestSumAggregator(OMNI_SHORT, OMNI_DECIMAL128);

    TestSumAggregator(OMNI_SHORT, OMNI_DECIMAL64);

    TestSumAggregator(OMNI_SHORT, OMNI_VARCHAR);

    TestSumAggregator(OMNI_SHORT, OMNI_CONTAINER);

    TestSumAggregator(OMNI_INT, OMNI_SHORT);

    TestSumAggregator(OMNI_INT, OMNI_INT);

    TestSumAggregator(OMNI_INT, OMNI_LONG);

    TestSumAggregator(OMNI_INT, OMNI_DOUBLE);

    TestSumAggregator(OMNI_INT, OMNI_DECIMAL128);

    TestSumAggregator(OMNI_INT, OMNI_DECIMAL64);

    TestSumAggregator(OMNI_INT, OMNI_VARCHAR);

    TestSumAggregator(OMNI_INT, OMNI_CONTAINER);

    TestSumAggregator(OMNI_DOUBLE, OMNI_SHORT);

    TestSumAggregator(OMNI_DOUBLE, OMNI_INT);

    TestSumAggregator(OMNI_DOUBLE, OMNI_LONG);

    TestSumAggregator(OMNI_DOUBLE, OMNI_DOUBLE);

    TestSumAggregator(OMNI_DOUBLE, OMNI_DECIMAL128);

    TestSumAggregator(OMNI_DOUBLE, OMNI_DECIMAL64);

    TestSumAggregator(OMNI_DOUBLE, OMNI_VARCHAR);

    TestSumAggregator(OMNI_DOUBLE, OMNI_CONTAINER);

    TestSumAggregator(OMNI_DECIMAL64, OMNI_SHORT);

    TestSumAggregator(OMNI_DECIMAL64, OMNI_INT);

    TestSumAggregator(OMNI_DECIMAL64, OMNI_LONG);

    TestSumAggregator(OMNI_DECIMAL64, OMNI_DOUBLE);

    TestSumAggregator(OMNI_DECIMAL64, OMNI_DECIMAL128);

    TestSumAggregator(OMNI_DECIMAL64, OMNI_DECIMAL64);

    TestSumAggregator(OMNI_DECIMAL64, OMNI_VARCHAR);

    TestSumAggregator(OMNI_DECIMAL64, OMNI_CONTAINER);

    TestSumAggregator(OMNI_VARCHAR, OMNI_SHORT);

    TestSumAggregator(OMNI_VARCHAR, OMNI_INT);

    TestSumAggregator(OMNI_VARCHAR, OMNI_LONG);

    TestSumAggregator(OMNI_VARCHAR, OMNI_DOUBLE);

    TestSumAggregator(OMNI_VARCHAR, OMNI_DECIMAL128);

    TestSumAggregator(OMNI_VARCHAR, OMNI_DECIMAL64);

    TestSumAggregator(OMNI_VARCHAR, OMNI_VARCHAR);

    TestSumAggregator(OMNI_VARCHAR, OMNI_CONTAINER);

    TestSumAggregator(OMNI_CONTAINER, OMNI_SHORT);

    TestSumAggregator(OMNI_CONTAINER, OMNI_INT);

    TestSumAggregator(OMNI_CONTAINER, OMNI_LONG);

    TestSumAggregator(OMNI_CONTAINER, OMNI_DOUBLE);

    TestSumAggregator(OMNI_CONTAINER, OMNI_DECIMAL128);

    TestSumAggregator(OMNI_CONTAINER, OMNI_DECIMAL64);

    TestSumAggregator(OMNI_CONTAINER, OMNI_VARCHAR);

    TestSumAggregator(OMNI_CONTAINER, OMNI_CONTAINER);
    ConfigUtil::SetEnableHMPP(false);
    VectorHelper::FreeVecBatch(vectorBatch);
    VectorHelper::FreeVecBatch(rawVectorBatch);
}

TEST(AggregatorTest, hmpp_avg_aggregator_exceptions)
{
    ConfigUtil::SetEnableHMPP(true);
    auto inputType = DoubleType();
    DataTypes inputTypes({ inputType }), outputTypes({ inputType });
    std::vector<int32_t> channels { 0 };
    bool rawIn = false, partialOut = false, isOverflowAsNull = false;

    const int dataSize = 6;
    double data0[dataSize] = {0.0f, 1.0f, 2.0f, 0.0f, 1.0f, 2.0f};
    void *datas[1] = {data0};
    std::vector<DataTypePtr> sourceFieldTypes { IntType() };
    DataTypes sourceTypes(sourceFieldTypes);
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vectorBatch = new VectorBatch(dataSize);
    DataType type(inputType->GetId());
    vectorBatch->Append(CreateDictionaryVector(type, dataSize, ids, dataSize, datas[0]).release());
    AggregateState state;
    auto rawVectorBatch = CreateVectorBatch(inputTypes, dataSize, datas[0]);
    std::vector<DataTypePtr> typesPtr = {
        nullptr,         IntType(),        LongType(), DoubleType(),    BooleanType(),   ShortType(),
        Decimal64Type(), Decimal128Type(), IntType(),  LongType(),      IntType(),       LongType(),
        LongType(),      LongType(),       LongType(), VarcharType(15), VarcharType(16), ContainerType(),
    };

#define TestAggregator(IN, OUT)                                                                                        \
    do {                                                                                                               \
        auto agg = AverageAggregator<IN, OUT>::Create(DataTypes({ typesPtr.at(IN) }), DataTypes({ typesPtr.at(OUT) }), \
            channels, rawIn, partialOut, isOverflowAsNull);                                                            \
        if (agg != nullptr) {                                                                                          \
            EXPECT_ANY_THROW(agg->ProcessGroupWithHMPP(state, vectorBatch));                                           \
            EXPECT_FALSE(agg->CanProcessWithHMPP(state, vectorBatch));                                                 \
            EXPECT_FALSE(agg->CanProcessWithHMPP(state, rawVectorBatch));                                              \
        }                                                                                                              \
    } while (0)

    TestAggregator(OMNI_SHORT, OMNI_LONG);

    TestAggregator(OMNI_SHORT, OMNI_DOUBLE);

    TestAggregator(OMNI_SHORT, OMNI_DECIMAL128);

    TestAggregator(OMNI_SHORT, OMNI_DECIMAL64);

    TestAggregator(OMNI_SHORT, OMNI_VARCHAR);

    TestAggregator(OMNI_SHORT, OMNI_CONTAINER);

    TestAggregator(OMNI_INT, OMNI_SHORT);

    TestAggregator(OMNI_INT, OMNI_INT);

    TestAggregator(OMNI_INT, OMNI_LONG);

    TestAggregator(OMNI_INT, OMNI_DOUBLE);

    TestAggregator(OMNI_INT, OMNI_DECIMAL128);

    TestAggregator(OMNI_INT, OMNI_DECIMAL64);

    TestAggregator(OMNI_INT, OMNI_VARCHAR);

    TestAggregator(OMNI_INT, OMNI_CONTAINER);

    TestAggregator(OMNI_DOUBLE, OMNI_SHORT);

    TestAggregator(OMNI_DOUBLE, OMNI_INT);

    TestAggregator(OMNI_DOUBLE, OMNI_LONG);

    TestAggregator(OMNI_DOUBLE, OMNI_DOUBLE);

    TestAggregator(OMNI_DOUBLE, OMNI_DECIMAL128);

    TestAggregator(OMNI_DOUBLE, OMNI_DECIMAL64);

    TestAggregator(OMNI_DOUBLE, OMNI_VARCHAR);

    TestAggregator(OMNI_DOUBLE, OMNI_CONTAINER);

    TestAggregator(OMNI_DECIMAL64, OMNI_SHORT);

    TestAggregator(OMNI_DECIMAL64, OMNI_INT);

    TestAggregator(OMNI_DECIMAL64, OMNI_LONG);

    TestAggregator(OMNI_DECIMAL64, OMNI_DOUBLE);

    TestAggregator(OMNI_DECIMAL64, OMNI_DECIMAL128);

    TestAggregator(OMNI_DECIMAL64, OMNI_DECIMAL64);

    TestAggregator(OMNI_DECIMAL64, OMNI_VARCHAR);

    TestAggregator(OMNI_DECIMAL64, OMNI_CONTAINER);

    TestAggregator(OMNI_VARCHAR, OMNI_SHORT);

    TestAggregator(OMNI_VARCHAR, OMNI_INT);

    TestAggregator(OMNI_VARCHAR, OMNI_LONG);

    TestAggregator(OMNI_VARCHAR, OMNI_DOUBLE);

    TestAggregator(OMNI_VARCHAR, OMNI_DECIMAL128);

    TestAggregator(OMNI_VARCHAR, OMNI_DECIMAL64);

    TestAggregator(OMNI_VARCHAR, OMNI_VARCHAR);

    TestAggregator(OMNI_VARCHAR, OMNI_CONTAINER);

    TestAggregator(OMNI_CONTAINER, OMNI_SHORT);

    TestAggregator(OMNI_CONTAINER, OMNI_INT);

    TestAggregator(OMNI_CONTAINER, OMNI_LONG);

    TestAggregator(OMNI_CONTAINER, OMNI_DOUBLE);

    TestAggregator(OMNI_CONTAINER, OMNI_DECIMAL128);

    TestAggregator(OMNI_CONTAINER, OMNI_DECIMAL64);

    TestAggregator(OMNI_CONTAINER, OMNI_VARCHAR);

    TestAggregator(OMNI_CONTAINER, OMNI_CONTAINER);
    ConfigUtil::SetEnableHMPP(false);
    VectorHelper::FreeVecBatch(vectorBatch);
    VectorHelper::FreeVecBatch(rawVectorBatch);
}
#endif
}