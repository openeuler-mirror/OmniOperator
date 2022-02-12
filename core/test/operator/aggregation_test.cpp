#include <gtest/gtest.h>
#include "operator/aggregation/aggregator/aggregator_factory.h"
#include "operator/aggregation/aggregator/all_aggregators.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "test/util/test_util.h"
#include "operator/jit_context/jit_context.h"
#include "vector/vector_helper.h"
#include "util/perf_util.h"
#include "../../libconfig.h"

#include <vector>
#include <iostream>
#include <thread>
#include <cstdlib>
#include <mutex>
#include <stdarg.h>

const int32_t VEC_BATCH_NUM = 10;
const int32_t ROW_PER_VEC_BATCH = 2000000;
const int32_t CARDINALITY = 4;
const int32_t COLUMN_NUM = 4;
const bool INPUT_MODE = true;
const bool OUTPUT_MODE = false;

static Decimal64VecType SHORT_DECIMAL_TYPE(7, 2);
static VarcharVecType IMMEDIATE_VARBINARY(24);
static Decimal128VecType LONG_DECIMAL_TYPE(38, 0);

long lrand()
{
    if (sizeof(int) < sizeof(long)) {
        return (static_cast<long>(rand())) << (sizeof(int) * 8) | rand();
    }
    return rand();
}

using namespace omniruntime::vec;
using namespace omniruntime::op;

Vector *buildHashInput(const VecType &groupType, int32_t rowPerVecBatch, int32_t cardinality)
{
    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    switch (groupType.GetId()) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            IntVector *col = new IntVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, j % cardinality);
            }
            return col;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            LongVector *col = new LongVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, j % cardinality);
            }
            return col;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            DoubleVector *col = new DoubleVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, j % cardinality);
            }
            return col;
        }
        case OMNI_VEC_TYPE_BOOLEAN: {
            BooleanVector *col = new BooleanVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, j % cardinality);
            }
            return col;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128Vector *col = new Decimal128Vector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, Decimal128(0, j % cardinality));
            }
            return col;
        }
        case OMNI_VEC_TYPE_VARCHAR:
        case OMNI_VEC_TYPE_CHAR: {
            VarcharVecType charType = (VarcharVecType &)groupType;
            VarcharVector *col = new VarcharVector(vecAllocator, charType.GetWidth() * rowPerVecBatch, rowPerVecBatch);
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

Vector *buildAggregateInput(const VecType &aggType, int32_t rowPerVecBatch)
{
    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    switch (aggType.GetId()) {
        case OMNI_VEC_TYPE_NONE: {
            LongVector *col = new LongVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValueNull(j);
            }
            return col;
        }
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            IntVector *col = new IntVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            LongVector *col = new LongVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            DoubleVector *col = new DoubleVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_VEC_TYPE_BOOLEAN: {
            BooleanVector *col = new BooleanVector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128Vector *col = new Decimal128Vector(vecAllocator, rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, Decimal128(0, 1));
            }
            return col;
        }
        case OMNI_VEC_TYPE_VARCHAR:
        case OMNI_VEC_TYPE_CHAR: {
            VarcharVecType charType = (VarcharVecType &)aggType;
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
    int32_t aggColNum, const std::vector<VecType> &groupTypes, const std::vector<VecType> &aggTypes)
{
    VectorBatch **input = new VectorBatch *[vecBatchNum];
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        VectorBatch *vecBatch = new VectorBatch(groupColNum + aggColNum);
        for (int32_t index = 0; index < groupColNum; ++index) {
            Vector *vec = buildHashInput(groupTypes[index], rowPerVecBatch, cardinality);
            vecBatch->SetVector(index, vec);
        }
        for (int32_t index = 0; index < aggColNum; ++index) {
            Vector *vec = buildAggregateInput(aggTypes[index], rowPerVecBatch);
            vecBatch->SetVector(groupColNum + index, vec);
        }
        input[i] = vecBatch;
    }
    return input;
}

VectorBatch **buildVarCharInput(int32_t vecBatchNum, int32_t colNum, int32_t rowPerVecBatch, ...)
{
    va_list args;
    va_start(args, rowPerVecBatch);
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    VectorBatch **input = new VectorBatch *[vecBatchNum];
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        VectorBatch *vecBatch = new VectorBatch(colNum);
        for (int32_t c = 0; c < colNum; ++c) {
            VarcharVector *col =
                std::make_unique<VarcharVector>(vecAllocator, rowPerVecBatch * 10, rowPerVecBatch).release();
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
    std::vector<VecType> groupByTypeVec = { LongVecType::Instance(), LongVecType::Instance() };
    VecTypes groupByTypes(groupByTypeVec);
    uint32_t aggCols[2] = {2, 3};
    std::vector<VecType> aggInputTypeVec = { LongVecType::Instance(), LongVecType::Instance() };
    VecTypes aggInputTypes(aggInputTypeVec);
    std::vector<VecType> aggOutputTypeVec = { LongVecType::Instance(), LongVecType::Instance() };
    VecTypes aggOutputTypes(aggOutputTypeVec);
    // uint32_t aggFunType[] = {0, 0};
    uint32_t aggFunType[2] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG};
    uint32_t retTypes[] = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG};
    PrepareContext groupByColContext = { groupCols, 2 };
    PrepareContext groupByTypeContext = { reinterpret_cast<uint32_t *>(const_cast<int32_t *>(groupByTypes.GetIds())),
                                          2 };
    PrepareContext aggColContext = { aggCols, 2 };
    PrepareContext aggInputTypeContext = { reinterpret_cast<uint32_t *>(const_cast<int32_t *>(aggInputTypes.GetIds())),
                                           2 };
    PrepareContext aggOutputTypeContext = {
        reinterpret_cast<uint32_t *>(const_cast<int32_t *>(aggOutputTypes.GetIds())), 2
    };
    PrepareContext aggFuncTypeContext = { aggFunType, 2 };
    PrepareContext retTypesContext = { retTypes, 4 };

    auto jitContext = CreateHashAggregationJitContext(groupByTypes, (int32_t *)groupCols, aggInputTypes,
        (int32_t *)aggCols, (int32_t *)aggFunType, 2, aggOutputTypes);
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
    VecTypes sourceTypes(
        { LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance() });
    uint32_t aggFuncTypes[4] = {0, 0, 0, 0};
    omniruntime::op::PrepareContext aggFuncTypeContext = { aggFuncTypes, 4 };
    uint32_t aggInputCols[4] = {0, 1, 2, 3};
    omniruntime::op::PrepareContext aggInputColsContext = { aggInputCols, 4 };
    uint32_t maskCols[4] = {(uint32_t)(-1), (uint32_t)(-1), (uint32_t)(-1), (uint32_t)(-1)};
    omniruntime::op::PrepareContext maskColsContext = { maskCols, 4 };

    auto jitContext = CreateAggregationJitContext(sourceTypes, (int32_t *)aggInputCols, (int32_t *)maskCols,
        (int32_t *)aggFuncTypes, 4, sourceTypes);
    std::cout << "after jit" << std::endl;
    auto nativeOperatorFactory = new AggregationOperatorFactory(sourceTypes, aggFuncTypeContext, aggInputColsContext,
        maskColsContext, sourceTypes, true, false);
    nativeOperatorFactory->Init();
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->SetJitContext(jitContext);
    return reinterpret_cast<uintptr_t>(nativeOperatorFactory);
}

using HAOFactoryParameters = struct HashAggregationFactoryParameters {
public:
    bool inputRaw;
    bool outputPartial;
    std::vector<uint32_t> groupCols;
    std::vector<VecType> groupByTypes;
    std::vector<uint32_t> aggCols;
    std::vector<VecType> aggInputTypes;
    std::vector<VecType> aggOutputTypes;
    std::vector<uint32_t> aggFuncTypes;
};

uintptr_t CreateHashFactoryWithoutJit(HAOFactoryParameters &parameters)
{
    VecTypes groupByTypes(parameters.groupByTypes);
    VecTypes aggInputTypes(parameters.aggInputTypes);
    VecTypes aggOutputTypes(parameters.aggOutputTypes);

    PrepareContext groupByColContext = { parameters.groupCols.data(), parameters.groupCols.size() };
    PrepareContext aggColContext = { parameters.aggCols.data(), parameters.aggCols.size() };
    PrepareContext aggFuncTypeContext = { parameters.aggFuncTypes.data(), parameters.aggFuncTypes.size() };

    omniruntime::op::HashAggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByTypes, aggColContext,
        aggInputTypes, aggOutputTypes, aggFuncTypeContext, parameters.inputRaw, parameters.outputPartial);
    nativeOperatorFactory->Init();
    return reinterpret_cast<uintptr_t>(nativeOperatorFactory);
}

double total_cpu_time;
double total_wall_time;

void perfTestOriginal(int64_t moduleAddr, VectorBatch **input)
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
    EXPECT_EQ(result[0]->GetVectorCount(), 4);
    EXPECT_EQ(result[0]->GetRowCount(), 4);
    for (auto res : result) {
        VectorHelper::FreeVecBatch(res);
    }
    Operator::DeleteOperator(groupBy);
}

void perfTest(int64_t moduleAddr, VectorBatch **input, int32_t vecBatchNum, int32_t *rowCount)
{
    // create operatory
    HashAggregationOperatorFactory *nativeOperatorFactory =
        reinterpret_cast<HashAggregationOperatorFactory *>(moduleAddr);
    auto groupBy = reinterpret_cast<HashAggModule>(nativeOperatorFactory->GetJitContext()->func)(nativeOperatorFactory);

    // execution
    for (int pageIndex = 0; pageIndex < vecBatchNum; ++pageIndex) {
        auto copiedBatch = DuplicateVectorBatch(input[pageIndex]);
        auto errNo = groupBy->AddInput(copiedBatch);
    }
    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = groupBy->GetOutput(result);
    EXPECT_EQ(result[0]->GetVectorCount(), 4);
    EXPECT_EQ(result[0]->GetRowCount(), 4);
    for (auto res : result) {
        VectorHelper::FreeVecBatch(res);
    }
    Operator::DeleteOperator(groupBy);
}

void perfTestNonGroup(int64_t moduleAddr, bool codegenMode, VectorBatch **input, int32_t vecBatchNum, int32_t *rowCount)
{
    // create operatory
    auto nativeOperatorFactory = reinterpret_cast<AggregationOperatorFactory *>(moduleAddr);
    Operator *aggregation;
    if (codegenMode) {
        aggregation = reinterpret_cast<opt_module>(nativeOperatorFactory->GetJitContext()->func)(nativeOperatorFactory);
    } else {
        aggregation = nativeOperatorFactory->CreateOperator();
    }

    // execution
    for (int pageIndex = 0; pageIndex < vecBatchNum; ++pageIndex) {
        auto copiedBatch = DuplicateVectorBatch(input[pageIndex]);
        auto errNo = aggregation->AddInput(copiedBatch);
    }
    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = aggregation->GetOutput(result);
    EXPECT_EQ(result[0]->GetVectorCount(), 4);
    EXPECT_EQ(result[0]->GetRowCount(), 1);
    for (auto res : result) {
        VectorHelper::FreeVecBatch(res);
    }
}

TEST(HashAggregationOperatorTest, verify_correctness)
{
    // create 10 pages
    const int VEC_BATCH_NUM = 10;
    const int ROW_SIZE = 2000;
    const int CARDINALITY = 10;
    const int COLUMN_COUNT = 7; // groupby*2 + sum + avg + count + min + max

    std::string aggNames[] = {"group", "group", "sum", "avg", "count", "min", "max"};
    std::vector<VecType> groupTypes = { LongVecType(), LongVecType() };
    std::vector<VecType> aggTypes = { LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType() };
    VectorBatch **input1 = buildAggInput(VEC_BATCH_NUM, ROW_SIZE, CARDINALITY, 2, 5, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    // First stage
    ColumnIndex c0 = { 0, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c1 = { 1, LongVecType::Instance(), LongVecType::Instance() };
    std::vector<int32_t> aggInputCols1 = { 2, 3, 4, 5, 6 };
    VecTypes aggInputTypes1(std::vector<VecType> { LongVecType::Instance(), LongVecType::Instance(),
        LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance() });
    VecTypes aggOutputTypes1(std::vector<VecType> { LongVecType::Instance(), ContainerVecType::Instance(),
        LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance() });
    std::vector<ColumnIndex> groupByColumns1 = { c0, c1 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(),
        LongVecType::Instance(), 2, true, true));
    aggs1.push_back(
        std::make_unique<AverageAggregator<LongVector>>(LongVecType::Instance(), ContainerVecType::Instance(), 3, true, true));
    aggs1.push_back(std::make_unique<CountColumnAggregator>(LongVecType::Instance(), LongVecType::Instance(), 4, true, true));
    aggs1.push_back(
        std::make_unique<MinAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(), 5, true, true));
    aggs1.push_back(
            std::make_unique<MaxAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(), 6, true, true));
    HashAggregationOperator *groupBy1 = new HashAggregationOperator(groupByColumns1, aggInputCols1, aggInputTypes1,
                                                                    aggOutputTypes1, std::move(aggs1), true, true);
    groupBy1->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy1->AddInput(input1[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t vecBatchCount = groupBy1->GetOutput(result1);
    Operator::DeleteOperator(groupBy1);

    VectorBatch **input2 = buildAggInput(VEC_BATCH_NUM, ROW_SIZE, CARDINALITY, 2, 5, groupTypes, aggTypes);
    if (input2 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    ColumnIndex c2 = { 0, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c3 = { 1, LongVecType::Instance(), LongVecType::Instance() };
    std::vector<int32_t> aggInputCols2 = { 2, 3, 4, 5, 6 };
    VecTypes aggInputTypes2(std::vector<VecType> { LongVecType::Instance(), LongVecType::Instance(),
        LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance() });
    VecTypes aggOutputTypes2(std::vector<VecType> { LongVecType::Instance(), ContainerVecType::Instance(),
        LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance() });
    groupByColumns1 = { c2, c3 };

    aggs1.clear();
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(),
        LongVecType::Instance(), 2, true, true));
    aggs1.push_back(
        std::make_unique<AverageAggregator<LongVector>>(LongVecType::Instance(), ContainerVecType::Instance(), 3, true, true));
    aggs1.push_back(std::make_unique<CountColumnAggregator>(LongVecType::Instance(), LongVecType::Instance(), 4, true, true));
    aggs1.push_back(
        std::make_unique<MinAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(), 5, true, true));
    aggs1.push_back(
        std::make_unique<MaxAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(), 6, true, true));
    HashAggregationOperator *groupBy2 = new HashAggregationOperator(groupByColumns1, aggInputCols2, aggInputTypes2,
                                                                    aggOutputTypes2, std::move(aggs1), true, true);
    groupBy2->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy2->AddInput(input2[i]);
    }

    std::vector<VectorBatch *> result2;
    int32_t tableCount2 = groupBy2->GetOutput(result2);
    Operator::DeleteOperator(groupBy2);

    // Second stage
    ColumnIndex c4 = { 0, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c5 = { 1, LongVecType::Instance(), LongVecType::Instance() };
    std::vector<int32_t> aggInputCols3 = { 2, 3, 4, 5, 6 };
    VecTypes aggInputTypes3(std::vector<VecType> { LongVecType::Instance(), LongVecType::Instance(),
        LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance() });
    VecTypes aggOutputTypes3(std::vector<VecType> { LongVecType::Instance(), DoubleVecType::Instance(),
        LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance() });

    std::vector<ColumnIndex> groupByColumns2 = { c4, c5 };
    std::vector<std::unique_ptr<Aggregator>> aggs2;
    aggs2.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(),
        LongVecType::Instance(), 2, false, false));
    aggs2.push_back(
        std::make_unique<AverageAggregator<LongVector>>(LongVecType::Instance(), ContainerVecType::Instance(), 3, false, false));
    aggs2.push_back(std::make_unique<CountColumnAggregator>(LongVecType::Instance(), LongVecType::Instance(), 4, false, false));
    aggs2.push_back(
        std::make_unique<MinAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(), 5, false, false));
    aggs2.push_back(
        std::make_unique<MaxAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(), 6, false, false));
    HashAggregationOperator *groupBy3 = new HashAggregationOperator(groupByColumns2, aggInputCols3, aggInputTypes3,
                                                                    aggOutputTypes3, std::move(aggs2), false, false);
    groupBy3->Init();

    for (int32_t i = 0; i < result1.size(); ++i) {
        groupBy3->AddInput(result1[i]);
    }
    for (int32_t i = 0; i < result2.size(); ++i) {
        groupBy3->AddInput(result2[i]);
    }

    std::vector<VectorBatch *> result3;
    groupBy3->GetOutput(result3);
    Operator::DeleteOperator(groupBy3);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>(
        { LongVecType(), LongVecType(), LongVecType(), DoubleVecType(), LongVecType(), LongVecType(), LongVecType() }));
    int64_t expectData1[CARDINALITY] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int64_t expectData2[CARDINALITY] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int64_t expectData3[CARDINALITY] = {4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000};
    double expectData4[CARDINALITY] = {1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0};
    int64_t expectData5[CARDINALITY] = {4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000};
    int64_t expectData6[CARDINALITY] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    int64_t expectData7[CARDINALITY] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, CARDINALITY, expectData1, expectData2, expectData3,
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
    const int VEC_BATCH_NUM = 1;
    const int ROW_SIZE = 8;
    const int COLUMN_COUNT = 4; // groupby + count + min + max
    std::string aggNames[] = {"group", "count","min","max" };
    std::string data0[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data1[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data2[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data3[8] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1", "1.1", "1"};
    VectorBatch **input = buildVarCharInput(VEC_BATCH_NUM, COLUMN_COUNT, ROW_SIZE, data0, data1, data2, data3);
    // First stage
    VarcharVecType type1(1);
    VarcharVecType type2(1);
    VarcharVecType type3(1);
    VarcharVecType type4(4);
    ColumnIndex c0 = { 0, type1, type1 };
    std::vector<int32_t> aggInputCols1 = { 1, 2, 3 };
    VecTypes aggInputTypes1(std::vector<VecType> { type2, type3, type4 });
    VecTypes aggOutputTypes1(std::vector<VecType> { LongVecType(), type3, type4 });

    std::vector<ColumnIndex> groupByColumns1 = { c0 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(
        std::make_unique<CountColumnAggregator>(VarcharVecType::Instance(), LongVecType::Instance(), 1, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MinVarcharAggregator>(VarcharVecType::Instance(), VarcharVecType::Instance(), 2, INPUT_MODE,
        OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MaxVarcharAggregator>(VarcharVecType::Instance(), VarcharVecType::Instance(), 3, INPUT_MODE,
        OUTPUT_MODE));

    HashAggregationOperator *groupByVarChar = new HashAggregationOperator(groupByColumns1, aggInputCols1,
        aggInputTypes1, aggOutputTypes1, std::move(aggs1), true, false);
    groupByVarChar->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupByVarChar->AddInput(input[i]);
    }
    std::vector<VectorBatch *> result1;
    int32_t vecBatchCount = groupByVarChar->GetOutput(result1);
    auto resBatch = VectorHelper::ConcatVectorBatches(result1);
    for (auto res : result1) {
        VectorHelper::FreeVecBatch(res);
    }

    Operator::DeleteOperator(groupByVarChar);
    std::string expectData1[3] = {"2", "1","0"};
    int64_t expectData2[3] = {2,3,3};
    std::string expectData3[3] = {"2", "1","0"};
    std::string expectData4[3] = {"4.4", "5.5","6.6"};
    VecTypes expectedTypes(
        std::vector<VecType>({ VarcharVecType(1), LongVecType(), VarcharVecType(1), VarcharVecType(3) }));
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectedTypes, 3, expectData1, expectData2, expectData3, expectData4);
    VectorHelper::PrintVecBatch(resBatch);
    VectorHelper::PrintVecBatch(expectVecBatch);
    EXPECT_TRUE(VecBatchMatch(resBatch, expectVecBatch));
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(resBatch);
}

TEST(HashAggregationOperatorTest, verify_char_vector_correctness)
{
    // create 10 pages
    const int VEC_BATCH_NUM = 1;
    const int ROW_SIZE = 8;
    const int COLUMN_COUNT = 4; // groupby + count + min + max
    std::string aggNames[] = {"group", "count","min","max" };
    std::string data0[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data1[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data2[8] = {"0", "1", "2", "0", "1", "2", "0", "1"};
    std::string data3[8] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1", "1.1", "1"};
    VectorBatch **input = buildVarCharInput(VEC_BATCH_NUM, COLUMN_COUNT, ROW_SIZE, data0, data1, data2, data3);
    // First stage
    CharVecType type1(1);
    CharVecType type2(1);
    CharVecType type3(1);
    CharVecType type4(4);
    ColumnIndex c0 = { 0, type1, type1 };
    std::vector<int32_t> aggInputCols1 = { 1, 2, 3 };
    VecTypes aggInputTypes1(std::vector<VecType> { type2, type3, type4 });
    VecTypes aggOutputTypes1(std::vector<VecType> { LongVecType(), type3, type4 });

    std::vector<ColumnIndex> groupByColumns1 = { c0 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(
        std::make_unique<CountColumnAggregator>(CharVecType::Instance(), LongVecType::Instance(), 1, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(
        std::make_unique<MinVarcharAggregator>(CharVecType::Instance(), CharVecType::Instance(), 2, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(
        std::make_unique<MaxVarcharAggregator>(CharVecType::Instance(), CharVecType::Instance(), 3, INPUT_MODE, OUTPUT_MODE));
    HashAggregationOperator *groupByVarChar = new HashAggregationOperator(groupByColumns1, aggInputCols1,
                                                                          aggInputTypes1, aggOutputTypes1, std::move(aggs1), true, false);
    groupByVarChar->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupByVarChar->AddInput(input[i]);
    }
    std::vector<VectorBatch *> result1;
    int32_t vecBatchCount = groupByVarChar->GetOutput(result1);
    auto resBatch = VectorHelper::ConcatVectorBatches(result1);
    for (auto res : result1) {
        VectorHelper::FreeVecBatch(res);
    }

    Operator::DeleteOperator(groupByVarChar);
    std::string expectData1[3] = {"2", "1","0"};
    int64_t expectData2[3] = {2,3,3};
    std::string expectData3[3] = {"2", "1","0"};
    std::string expectData4[3] = {"4.4", "5.5","6.6"};
    VecTypes expectedTypes(std::vector<VecType>({ CharVecType(1), LongVecType(), CharVecType(1), CharVecType(3) }));
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectedTypes, 3, expectData1, expectData2, expectData3, expectData4);
    VectorHelper::PrintVecBatch(resBatch);
    VectorHelper::PrintVecBatch(expectVecBatch);
    EXPECT_TRUE(VecBatchMatch(resBatch, expectVecBatch));
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(resBatch);
}

TEST(HashAggregationOperatorTest, verify_null_correctness)
{
    // create 10 pages
    const int VEC_BATCH_NUM = 1;
    const int ROW_SIZE = 6;
    const int CARDINALITY = 1;
    const int COLUMN_COUNT = 6; // groupby + sum + avg + count + min + max
    std::string aggNames[] = {"group", "sum", "avg", "count", "min", "max"};
    std::vector<VecType> groupTypes = { LongVecType() };
    std::vector<VecType> aggTypes = { LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType() };
    VectorBatch **input = buildAggInput(VEC_BATCH_NUM, ROW_SIZE, CARDINALITY, 1, 5, groupTypes, aggTypes);
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
    ColumnIndex c0 = { 0, LongVecType::Instance(), LongVecType::Instance() };
    std::vector<int32_t> aggInputCols1 = { 1, 2, 3, 4, 5 };
    VecTypes aggInputTypes1(std::vector<VecType> { LongVecType::Instance(), LongVecType::Instance(),
        LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance() });
    VecTypes aggOutputTypes1(std::vector<VecType> { LongVecType::Instance(), DoubleVecType::Instance(),
        LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance() });

    std::vector<ColumnIndex> groupByColumns1 = { c0 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(),
        LongVecType::Instance(), 1, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(LongVecType::Instance(), DoubleVecType::Instance(), 2,
        INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(
        std::make_unique<CountColumnAggregator>(LongVecType::Instance(), LongVecType::Instance(), 3, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MinAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(), 4,
        INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MaxAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(), 5,
        INPUT_MODE, OUTPUT_MODE));

    HashAggregationOperator *groupByNULL = new HashAggregationOperator(groupByColumns1, aggInputCols1, aggInputTypes1,
        aggOutputTypes1, std::move(aggs1), true, false);
    groupByNULL->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupByNULL->AddInput(input[i]);
    }
    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = groupByNULL->GetOutput(result);

    for (auto &i : aggs1) {
        delete i.release();
    }
    Operator::DeleteOperator(groupByNULL);

    int64_t expectData1[1] = {0};
    int64_t expectData2[1] = {6};
    double expectData3[1] = {1};
    int64_t expectData4[1] = {5};
    int64_t expectData5[1] = {1};
    int64_t expectData6[1] = {1};
    VecTypes expectedTypes(std::vector<VecType>(
        { LongVecType(), LongVecType(), DoubleVecType(), LongVecType(), LongVecType(), LongVecType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, 1, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);
    VectorHelper::PrintVecBatch(expectVecBatch);
    VectorHelper::PrintVecBatch(result[0]);
    EXPECT_TRUE(VecBatchMatch(result[0], expectVecBatch));
    VectorHelper::FreeVecBatches(result);
    VectorHelper::FreeVecBatch(expectVecBatch);
}

TEST(HashAggregationOperatorTest, verfify_correctness_group_by_agg_same_cols)
{
    // FIXME INT32+INT64
    // create 10 vecBatches
    const int VEC_BATCH_NUM = 10;
    VectorBatch **input = new VectorBatch *[VEC_BATCH_NUM];
    const int DATA_SIZE = 10;
    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        VectorBatch *vecBatch = new VectorBatch(2);
        LongVector *col1 = new LongVector(vecAllocator, DATA_SIZE);
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            col1->SetValue(i, i % 3);
        }

        LongVector *col2 = new LongVector(vecAllocator, DATA_SIZE);
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            col2->SetValue(i, i % 3);
        }
        vecBatch->SetVector(0, col1);
        vecBatch->SetVector(1, col2);
        input[i] = vecBatch;
    }
    ColumnIndex c0 = { 0, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c1 = { 1, LongVecType::Instance(), LongVecType::Instance() };
    std::vector<int32_t> aggInputCols = { 0, 1 };
    VecTypes aggInputTypes(std::vector<VecType> { LongVecType::Instance(), LongVecType::Instance() });
    VecTypes aggOutputTypes(std::vector<VecType> { LongVecType::Instance(), LongVecType::Instance() });
    std::vector<ColumnIndex> groupByColumns = { c0, c1 };
    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(IntVecType::Instance(), IntVecType::Instance(), 0,
        INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(), LongVecType::Instance(),
        1, INPUT_MODE, OUTPUT_MODE));
    HashAggregationOperator *groupBy = new HashAggregationOperator(groupByColumns, aggInputCols, aggInputTypes,
        aggOutputTypes, std::move(aggs), true, false);
    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = groupBy->GetOutput(result);
    Operator::DeleteOperator(groupBy);

    EXPECT_EQ(result[0]->GetVectorCount(), 4);

    for (int32_t i = 0; i < result[0]->GetVectorCount(); ++i) {
        Vector *col = result[0]->GetVector(i);
        // TODO: print data;
        // col->printColumn();
    }
    VectorHelper::FreeVecBatches(result);
}

TEST(HashAggregationOperatorTest, DISABLED_original_multiple_threads)
{
    using namespace std;
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;


    FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    VecTypes groupTypes({ LongVecType::Instance(), LongVecType::Instance() });
    VecTypes aggInputTypes({ LongVecType::Instance(), LongVecType::Instance() });
    VecTypes aggOutputTypes({ LongVecType::Instance(), LongVecType::Instance() });
    VectorBatch **input =
        buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes.Get(), aggInputTypes.Get());
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    uint32_t groupCols[2] = {0, 1};
    uint32_t aggCols[2] = {2, 3};
    uint32_t retTypes[] = {1,1,1,1};
    PrepareContext groupByColContext = { groupCols, 2 };
    PrepareContext aggColContext = { aggCols, 2 };
    PrepareContext aggFuncTypeContext = { reinterpret_cast<uint32_t *>(aggFunType), 2 };
    PrepareContext retTypesContext = { retTypes, 4 };
    HashAggregationOperatorFactory *nativeOperatorFactory = new HashAggregationOperatorFactory(groupByColContext,
        groupTypes, aggColContext, aggInputTypes, aggOutputTypes, aggFuncTypeContext, true, false);
    nativeOperatorFactory->Init();
    uint64_t factoryObjAddr = reinterpret_cast<uint64_t>(nativeOperatorFactory);

    int threadNums[] = {1, 2, 4, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t j = 0; j < threadNum; ++j) {
            // same stage Id
            std::thread t(perfTestOriginal, factoryObjAddr, input);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    DeleteOperatorFactory(nativeOperatorFactory);
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(HashAggregationOperatorTest, DISABLED_perf_via_API_multiple_threads)
{
    using namespace std;
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;

    vector<VecType> groupTypes = { LongVecType(), LongVecType() };
    vector<VecType> aggTypes = { LongVecType(), LongVecType() };
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
    int threadNums[] = {1, 2, 4, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t j = 0; j < threadNum; ++j) {
            // same stage Id
            std::thread t(perfTest, factoryObjAddr, input, VEC_BATCH_NUM, rowCount);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    DeleteOperatorFactory(reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(factoryObjAddr));
    delete[] rowCount;
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(AggregationOperatorTest, verify_correctness)
{
    // create 10 vecBatches
    const int VEC_BATCH_NUM = 10;
    const int ROW_SIZE = 100;
    const int CARDINALITY = 4;
    const int COLUMN_COUNT = 5; // groupby*2 + sum + avg + count + min + max
    std::string aggNames[] = {"sum", "avg", "count", "min", "max"};
    std::vector<VecType> groupTypes;
    std::vector<VecType> aggTypes = { LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType() };
    VectorBatch **input1 = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 0, 5, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(), LongVecType::Instance(),
        0, true, true));
    aggs.push_back(
        std::make_unique<AverageAggregator<LongVector>>(LongVecType::Instance(), ContainerVecType::Instance(), 1, true, true));
    aggs.push_back(std::make_unique<CountColumnAggregator>(LongVecType::Instance(), LongVecType::Instance(), 2, true, true));
    aggs.push_back(
        std::make_unique<MinAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(), 3, true, true));
    aggs.push_back(
        std::make_unique<MaxAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(), 4, true, true));
    std::vector<int32_t> aggInputCols = { 0, 1, 2, 3, 4 };
    std::vector<int32_t> maskCols = { -1, -1, -1, -1, -1 };
    VecTypes aggPartialOutputTypes(
        std::vector<VecType> { LongVecType(), ContainerVecType(), LongVecType(), LongVecType(), LongVecType() });
    auto aggregate1 =
        new AggregationOperator(std::move(aggs), aggInputCols, maskCols, aggPartialOutputTypes, true, true);

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        aggregate1->AddInput(input1[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t tableCount1 = aggregate1->GetOutput(result);
    omniruntime::op::Operator::DeleteOperator(aggregate1);

    VectorBatch **input2 = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 0, 5, groupTypes, aggTypes);
    ASSERT(!(input2 == nullptr));
    aggs.clear();
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(), LongVecType::Instance(),
        0, true, true));
    aggs.push_back(
        std::make_unique<AverageAggregator<LongVector>>(LongVecType::Instance(), ContainerVecType::Instance(), 1, true, true));
    aggs.push_back(std::make_unique<CountColumnAggregator>(LongVecType::Instance(), LongVecType::Instance(), 2, true, true));
    aggs.push_back(
        std::make_unique<MinAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(), 3, true, true));
    aggs.push_back(
        std::make_unique<MaxAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(), 4, true, true));
    auto aggregate2 =
        new AggregationOperator(std::move(aggs), aggInputCols, maskCols, aggPartialOutputTypes, true, true);

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        aggregate2->AddInput(input2[i]);
    }
    int32_t tableCount2 = aggregate2->GetOutput(result);
    omniruntime::op::Operator::DeleteOperator(aggregate2);

    // Second stage
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(),
        LongVecType::Instance(), 0, false, false));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(LongVecType::Instance(), DoubleVecType::Instance(),
        1, false, false));
    aggs1.push_back(
        std::make_unique<CountColumnAggregator>(LongVecType::Instance(), LongVecType::Instance(), 2, false, false));
    aggs1.push_back(std::make_unique<MinAggregator<LongVector, int64_t>>(LongVecType::Instance(),
        LongVecType::Instance(), 3, false, false));
    aggs1.push_back(std::make_unique<MaxAggregator<LongVector, int64_t>>(LongVecType::Instance(),
        LongVecType::Instance(), 4, false, false));
    VecTypes aggFinalOutputTypes(
        std::vector<VecType> { LongVecType(), DoubleVecType(), LongVecType(), LongVecType(), LongVecType() });
    auto aggregate3 =
        new AggregationOperator(std::move(aggs1), aggInputCols, maskCols, aggFinalOutputTypes, false, false);

    for (int32_t i = 0; i < result.size(); ++i) {
        aggregate3->AddInput(result[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t tableCount3 = aggregate3->GetOutput(result1);
    omniruntime::op::Operator::DeleteOperator(aggregate3);
    EXPECT_EQ(result1[0]->GetRowCount(), 1);
    EXPECT_EQ(result1[0]->GetVectorCount(), 5);

    for (auto &aggType : aggNames) {
        std::cout << aggType << "\t";
    }
    std::cout << std::endl;
    for (auto vecBatch : result1) {
        VectorHelper::PrintVecBatch(vecBatch);
    }
    VectorHelper::FreeVecBatches(result1);
}

TEST(AggregationOperatorTest, verify_agg_distinct)
{
    // construct data
    const int32_t DATA_SIZE = 4;
    const int32_t RESULT_DATA_SIZE = 1;

    // table1
    int64_t data0[DATA_SIZE] = {10L, 20L, 10L, 30L};
    int64_t data1[DATA_SIZE] = {1L, 1L, 2L, 3L};
    int64_t data2[DATA_SIZE] = {3L, 5L, 5L, 7L};
    int64_t data3[DATA_SIZE] = {4L, 2L, 1L, 2L};
    int64_t data4[DATA_SIZE] = {4L, 2L, 1L, 2L};
    bool data5[DATA_SIZE] = {true, true, false, true};
    bool data6[DATA_SIZE] = {true, false, true, true};
    bool data7[DATA_SIZE] = {true, true, false, true};
    bool data8[DATA_SIZE] = {true, true, true, false};
    bool data9[DATA_SIZE] = {true, true, true, false};

    std::vector<VecType> types = { LongVecType::Instance(),    LongVecType::Instance(),    LongVecType::Instance(),
        LongVecType::Instance(),    LongVecType::Instance(),    BooleanVecType::Instance(),
        BooleanVecType::Instance(), BooleanVecType::Instance(), BooleanVecType::Instance(),
        BooleanVecType::Instance() };
    VecTypes sourceTypes(types);
    VectorBatch *vecBatch1 =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8, data9);

    std::vector<int32_t> aggInputCols = { 0, 1, 2, 3, 4 };
    std::vector<int32_t> maskCols = { 5, 6, 7, 8, 9 };

    // STAGE1:
    std::vector<std::unique_ptr<Aggregator>> aggs;
    std::unique_ptr<Aggregator> aggregator =
        std::make_unique<CountColumnAggregator>(LongVecType::Instance(), LongVecType::Instance(), 0, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(5, std::move(aggregator)));
    aggregator = std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(),
        LongVecType::Instance(), 1, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(6, std::move(aggregator)));
    aggregator = std::make_unique<AverageAggregator<LongVector>>(LongVecType::Instance(), ContainerVecType::Instance(),
        2, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(7, std::move(aggregator)));
    aggregator = std::make_unique<MaxAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(),
        3, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(8, std::move(aggregator)));
    aggregator = std::make_unique<MinAggregator<LongVector, int64_t>>(LongVecType::Instance(), LongVecType::Instance(),
        4, true, true);
    aggs.push_back(std::make_unique<MaskColAggregator>(9, std::move(aggregator)));

    VecTypes aggPartialOutputTypes(
        std::vector<VecType> { LongVecType(), LongVecType(), ContainerVecType(), LongVecType(), LongVecType() });
    auto aggregate1 =
        new AggregationOperator(std::move(aggs), aggInputCols, maskCols, aggPartialOutputTypes, true, true);

    aggregate1->AddInput(vecBatch1);

    std::vector<VectorBatch *> result;
    int32_t tableCount1 = aggregate1->GetOutput(result);

    // STAGE2:
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<CountColumnAggregator>(LongVecType::Instance(), LongVecType::Instance(), 0, false, false));
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(),
        LongVecType::Instance(), 1, false, false));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(LongVecType::Instance(), DoubleVecType::Instance(),
        2, false, false));
    aggs1.push_back(std::make_unique<MaxAggregator<LongVector, int64_t>>(LongVecType::Instance(),
        LongVecType::Instance(), 3, false, false));
    aggs1.push_back(std::make_unique<MinAggregator<LongVector, int64_t>>(LongVecType::Instance(),
        LongVecType::Instance(), 4, false, false));
    VecTypes aggFinalOutputTypes(
        std::vector<VecType> { LongVecType(), LongVecType(), DoubleVecType(), LongVecType(), LongVecType() });
    std::vector<int32_t> maskCols1 = { -1, -1, -1, -1, -1 }; // no mask cols at final stage
    auto aggregate2 =
        new AggregationOperator(std::move(aggs1), aggInputCols, maskCols1, aggFinalOutputTypes, false, false);

    for (int32_t i = 0; i < result.size(); ++i) {
        aggregate2->AddInput(result[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t tableCount = aggregate2->GetOutput(result1);
    EXPECT_EQ(result1[0]->GetRowCount(), 1);
    EXPECT_EQ(result1[0]->GetVectorCount(), 5);

    int64_t expData0[RESULT_DATA_SIZE] = {3L};
    int64_t expData1[RESULT_DATA_SIZE] = {6L};
    double expData2[RESULT_DATA_SIZE] = {5.0};
    int64_t expData3[RESULT_DATA_SIZE] = {4L};
    int64_t expData4[RESULT_DATA_SIZE] = {1L};
    std::vector<VecType> resultType = { LongVecType::Instance(), LongVecType::Instance(), DoubleVecType::Instance(),
        LongVecType::Instance(), LongVecType::Instance() };
    VecTypes resultTypes(resultType);
    VectorBatch *expVecBatch1 =
        CreateVectorBatch(resultTypes, RESULT_DATA_SIZE, expData0, expData1, expData2, expData3, expData4);

    std::cout << "Expected result:" << std::endl;
    VectorHelper::PrintVecBatch(expVecBatch1);

    std::cout << "Actual result:" << std::endl;
    for (auto vecBatch : result1) {
        VectorHelper::PrintVecBatch(vecBatch);
    }
    EXPECT_TRUE(VecBatchMatch(result1[0], expVecBatch1));

    omniruntime::op::Operator::DeleteOperator(aggregate1);
    omniruntime::op::Operator::DeleteOperator(aggregate2);
    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatches(result1);
}

TEST(AggregationOperatorTest, avg_correctness_test)
{
    // create 10 pages
    const int VEC_BATCH_NUM = 10;
    const int ROW_SIZE = 100;
    const int CARDINALITY = 100;
    const int COLUMN_COUNT = 1;
    std::vector<VecType> groupTypes;
    std::vector<VecType> aggTypes = { LongVecType() };
    VectorBatch **input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 0, 1, groupTypes, aggTypes);
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    ColumnIndex c0 = { 0, LongVecType::Instance(), LongVecType::Instance() };
    std::vector<ColumnIndex> aggregateColumns = { c0 };
    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(
        std::make_unique<AverageAggregator<LongVector>>(LongVecType::Instance(), DoubleVecType::Instance(), 0));
    std::vector<int32_t> aggInputCols = { 0 };
    std::vector<int32_t> maskCols = { -1 };
    VecTypes aggOutputTypes(std::vector<VecType> { DoubleVecType() });
    auto aggregate = new AggregationOperator(std::move(aggs), aggInputCols, maskCols, aggOutputTypes, true, false);

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        aggregate->AddInput(input[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t tableCount = aggregate->GetOutput(result);

    EXPECT_EQ(result[0]->GetVectorCount(), 1);
    EXPECT_EQ(result[0]->GetRowCount(), 1);

    std::string aggNames[] = {"avg"};

    for (int32_t i = 0; i < result[0]->GetVectorCount(); ++i) {
        Vector *col = result[0]->GetVector(i);
        std::cout << aggNames[i] << " ";
        // col->printColumn();
    }

    VectorHelper::FreeVecBatches(result);
    Operator::DeleteOperator(aggregate);
}

TEST(AggregationOperatorTest, DISABLED_perf_original)
{
    VecTypes sourceTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType(), LongVecType() }));
    VecTypes aggOutputTypes(
        { LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance() });
    FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    uint32_t aggInputCols[] = {0, 1, 2, 3};
    PrepareContext aggFuncTypesContext = { reinterpret_cast<uint32_t *>(aggFunType), 4 };
    PrepareContext aggInputColsContext = { aggInputCols, 4 };
    uint32_t maskCols[4] = {(uint32_t)(-1), (uint32_t)(-1), (uint32_t)(-1), (uint32_t)(-1)};
    omniruntime::op::PrepareContext maskColsContext = { maskCols, 4 };

    auto nativeOperatorFactory = new AggregationOperatorFactory(sourceTypes, aggFuncTypesContext, aggInputColsContext,
        maskColsContext, aggOutputTypes, true, false);
    nativeOperatorFactory->Init();
    int64_t factoryAddr = reinterpret_cast<int64_t>(nativeOperatorFactory);
    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;

    VectorBatch **input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 0, COLUMN_NUM,
        std::vector<VecType>(), sourceTypes.Get());
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    int32_t *rowCount = new int32_t[VEC_BATCH_NUM];
    for (int32_t i = 0; i < VEC_BATCH_NUM; i++) {
        rowCount[i] = ROW_PER_VEC_BATCH;
    }
    std::cout << "after prepare" << std::endl;
    int threadNums[] = {1, 2, 4, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t j = 0; j < threadNum; ++j) {
            // same stage Id
            std::thread t(perfTestNonGroup, factoryAddr, false, input, VEC_BATCH_NUM, rowCount);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    delete[] rowCount;
    DeleteOperatorFactory(nativeOperatorFactory);
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(AggregationOperatorTest, DISABLED_perf_codegen)
{
    using namespace std;

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;

    std::vector<VecType> groupTypes;
    std::vector<VecType> aggTypes = { LongVecType(), LongVecType(), LongVecType(), LongVecType() };
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
    int threadNums[] = {1, 2, 4, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        total_wall_time = 0;
        total_cpu_time = 0;
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t j = 0; j < threadNum; ++j) {
            // same stage Id
            std::thread t(perfTestNonGroup, factoryObjAddr, true, input, VEC_BATCH_NUM, rowCount);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(100ms);
    }
    delete[] rowCount;
    VectorHelper::FreeVecBatches(input, VEC_BATCH_NUM);
}

TEST(HashAggregationOperatorTest, compare_perf)
{
    uint32_t groupCols[] = {0, 1};

    VecTypes groupInputTypes({ LongVecType::Instance(), LongVecType::Instance() });
    uint32_t aggCols[] = {2, 3};
    VecTypes aggInput({ LongVecType::Instance(), LongVecType::Instance() });
    VecTypes aggOutput({ LongVecType::Instance(), LongVecType::Instance() });
    FunctionType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    uint32_t retTypes[] = {1,1,1,1};
    PrepareContext groupByColContext = { groupCols, 2 };
    PrepareContext groupByTypeContext = { reinterpret_cast<uint32_t *>(const_cast<int32_t *>(groupInputTypes.GetIds())),
        2 };
    PrepareContext aggColContext = { aggCols, 2 };
    PrepareContext aggTypeContext = { reinterpret_cast<uint32_t *>(const_cast<int32_t *>(aggInput.GetIds())), 2 };
    PrepareContext aggFuncTypeContext = { reinterpret_cast<uint32_t *>(aggFunType), 2 };
    PrepareContext retTypesContext = { retTypes, 4 };

    // ------------------------------------------Create operator--------------------------------------------
    auto jitContext = CreateHashAggregationJitContext(groupInputTypes, (int32_t *)groupCols, aggInput,
        (int32_t *)aggCols, (int32_t *)aggFunType, 2, aggOutput);
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
    timer.setStart();

    auto *perfUtil = new PerfUtil();
    perfUtil->Init();

    perfUtil->Reset();
    perfUtil->Start();

    for (int pageIndex = 0; pageIndex < VEC_BATCH_NUM; ++pageIndex) {
        auto errNo = jitGroupBy->AddInput(input1[pageIndex]);
    }

    perfUtil->Stop();
    long instCount = perfUtil->GetData();
    if (instCount != -1) {
        printf("HashAgg with OmniJit, used %lld instructions\n", perfUtil->GetData());
    }

    timer.calculateElapse();
    double wall_elapsed = timer.getWallElapse();
    double cpu_elapsed = timer.getCpuElapse();
    std::cout << "wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;
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
    timer.reset();

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

    timer.calculateElapse();
    wall_elapsed = timer.getWallElapse();
    cpu_elapsed = timer.getCpuElapse();

    std::cout << "wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;
    groupBy->GetOutput(result);

    Operator::DeleteOperator(jitGroupBy);
    Operator::DeleteOperator(groupBy);
    DeleteOperatorFactory(nativeOperatorFactory);
    DeleteOperatorFactory(nativeOperatorFactory2);

    EXPECT_EQ(jittedResult.size(), result.size());
    for (int32_t i = 0; i < jittedResult.size(); ++i) {
        EXPECT_TRUE(VecBatchMatch(jittedResult[i], result[i]));
        VectorHelper::PrintVecBatch(result[i]);
    }
    VectorHelper::FreeVecBatches(jittedResult);
    VectorHelper::FreeVecBatches(result);
}

TEST(HashAggregationOperatorTest, multi_stage)
{
    std::vector<VecType> groupTypes = { LongVecType(), LongVecType() };
    std::vector<VecType> aggTypes = { LongVecType(), LongVecType(), Decimal64VecType(7, 2) };
    VectorBatch **input1 = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 3, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }
    HAOFactoryParameters parameters1 = { true,
        true,
        { 0, 1 },
        { LongVecType::Instance(), LongVecType::Instance() },
        { 2, 3, 4 },
        { LongVecType::Instance(), LongVecType::Instance(), SHORT_DECIMAL_TYPE },
        { LongVecType::Instance(), ContainerVecType::Instance(), IMMEDIATE_VARBINARY },
        { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM } };

    uintptr_t partialFactoryAddr1 = CreateHashFactoryWithoutJit(parameters1);
    auto partialFactory1 = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(partialFactoryAddr1);
    auto partialOperator1 = partialFactory1->CreateOperator();
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        partialOperator1->AddInput(input1[i]);
    }
    std::vector<VectorBatch *> resultFromPartial1;
    partialOperator1->GetOutput(resultFromPartial1);

    VectorBatch **input2 = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 3, groupTypes, aggTypes);
    if (input2 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    HAOFactoryParameters parameters2 = { true,
        true,
        { 0, 1 },
        { LongVecType::Instance(), LongVecType::Instance() },
        { 2, 3, 4 },
        { LongVecType::Instance(), LongVecType::Instance(), SHORT_DECIMAL_TYPE },
        { LongVecType::Instance(), ContainerVecType::Instance(), IMMEDIATE_VARBINARY },
        { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM } };

    uintptr_t partialFactoryAddr2 = CreateHashFactoryWithoutJit(parameters2);
    auto partialFactory2 = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(partialFactoryAddr2);
    auto partialOperator2 = partialFactory2->CreateOperator();
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        partialOperator2->AddInput(input2[i]);
    }
    std::vector<VectorBatch *> resultFromPartial2;
    partialOperator2->GetOutput(resultFromPartial2);

    HAOFactoryParameters parameters3 = { false,
        false,
        { 0, 1 },
        { LongVecType::Instance(), LongVecType::Instance() },
        { 2, 3, 4 },
        { LongVecType::Instance(), ContainerVecType::Instance(), IMMEDIATE_VARBINARY },
        { LongVecType::Instance(), DoubleVecType::Instance(), LONG_DECIMAL_TYPE },
        { OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_SUM } };

    uintptr_t finalFactoryAddr = CreateHashFactoryWithoutJit(parameters3);
    auto finalFactory = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(finalFactoryAddr);
    auto operator2 = finalFactory->CreateOperator();
    for (int32_t i = 0; i < resultFromPartial1.size(); ++i) {
        operator2->AddInput(resultFromPartial1[i]);
    }
    for (int32_t i = 0; i < resultFromPartial2.size(); ++i) {
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

    for (auto vecBatch : resultFromFinal) {
        VectorHelper::PrintVecBatch(vecBatch);
    }

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType(), DoubleVecType() }));
    int64_t expectData1[CARDINALITY] = {0, 1, 2, 3};
    int64_t expectData2[CARDINALITY] = {0, 1, 2, 3};
    int64_t expectData3[CARDINALITY] = {10000000, 10000000, 10000000, 10000000};
    double expectData4[CARDINALITY] = {1.0, 1.0, 1.0, 1.0};
    Decimal128 expectedDecimal(10000000L);
    Decimal128 expectData5[CARDINALITY] = {expectedDecimal, expectedDecimal, expectedDecimal, expectedDecimal};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, CARDINALITY, expectData1, expectData2, expectData3, expectData4, expectData5);
    EXPECT_TRUE(VecBatchMatch(resultFromFinal[0], expectVecBatch));
    VectorHelper::FreeVecBatches(resultFromFinal);
}

TEST(HashAggregationOperatorTest, supported_type_test)
{
    std::vector<VecType> groupTypes = { LongVecType(), LongVecType() };
    std::vector<VecType> aggTypes = { LongVecType(), LongVecType() };
    VectorBatch **input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);
    if (input == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    ColumnIndex c0 = { 0, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c1 = { 1, LongVecType::Instance(), LongVecType::Instance() };
    std::vector<int32_t> aggInputCols = { 2, 3 };
    VecTypes aggInputTypes(std::vector<VecType> { LongVecType::Instance(), LongVecType::Instance() });
    VecTypes aggOutputTypes(std::vector<VecType> { LongVecType::Instance(), LongVecType::Instance() });
    std::vector<ColumnIndex> groupByColumns = { c0, c1 };
    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(), LongVecType::Instance(),
        2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(), LongVecType::Instance(),
        3, INPUT_MODE, OUTPUT_MODE));

    HashAggregationOperator *groupBy = new HashAggregationOperator(groupByColumns, aggInputCols, aggInputTypes,
        aggOutputTypes, std::move(aggs), true, false);
    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = groupBy->GetOutput(result);
    Operator::DeleteOperator(groupBy);
    for (auto batch : result) {
        VectorHelper::PrintVecBatch(batch);
        VectorHelper::FreeVecBatch(batch);
    }
    groupByColumns.clear();
    aggInputCols.clear();
    aggs.clear();
    result.clear();
    delete[] input;

    groupTypes[0] = Date32VecType(DAY);
    groupTypes[1] = Date32VecType(DAY);
    aggTypes[0] = Date32VecType(DAY);
    aggTypes[1] = Date32VecType(DAY);
    input = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);
    c0 = { 0, Date32VecType::Instance(), Date32VecType::Instance() };
    c1 = { 1, Date32VecType::Instance(), Date32VecType::Instance() };
    aggInputCols = { 2, 3 };
    VecTypes aggInputTypes1(std::vector<VecType> { Date32VecType::Instance(), Date32VecType::Instance() });
    VecTypes aggOutputTypes1(std::vector<VecType> { Date32VecType::Instance(), Date32VecType::Instance() });

    groupByColumns = { c0, c1 };
    aggs.push_back(std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(Date32VecType::Instance(),
                                                                                Date32VecType::Instance(), 2, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(Date32VecType::Instance(),
                                                                                Date32VecType::Instance(), 3, INPUT_MODE, OUTPUT_MODE));

    groupBy = new HashAggregationOperator(groupByColumns, aggInputCols, aggInputTypes1, aggOutputTypes1,
        std::move(aggs), true, false);
    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }

    vecBatchCount = groupBy->GetOutput(result);
    Operator::DeleteOperator(groupBy);
    for (auto batch : result) {
        VectorHelper::PrintVecBatch(batch);
        VectorHelper::FreeVecBatch(batch);
    }
    groupByColumns.clear();
    aggInputCols.clear();
    aggs.clear();
    result.clear();
    delete[] input;

    // dictionary test
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    void *datas[2] = {data0, data1};
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType() }));
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vectorBatch = new VectorBatch(2, dataSize);
    for (int32_t i = 0; i < 2; i++) {
        VecType vecType = sourceTypes.Get()[i];
        vectorBatch->SetVector(i, CreateDictionaryVector(vecType, dataSize, ids, dataSize, datas[i]));
    }

    groupTypes[0] = IntVecType();
    aggTypes[0] = LongVecType();
    c0 = { 0, IntVecType::Instance(), IntVecType::Instance() };
    aggInputCols = { 1 };
    omniruntime::vec::VecTypes aggInputTypes2(std::vector<VecType> { LongVecType::Instance() });
    omniruntime::vec::VecTypes aggOutputTypes2(std::vector<VecType> { LongVecType::Instance() });
    groupByColumns = { c0 };
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(LongVecType::Instance(), LongVecType::Instance(),
        1, INPUT_MODE, OUTPUT_MODE));

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

    for (auto batch : result) {
        VectorHelper::PrintVecBatch(batch);
    }
    VectorHelper::FreeVecBatches(result);
    groupByColumns.clear();
    aggInputCols.clear();
    aggs.clear();
    result.clear();
}

TEST(AggregatorTest, sum_test)
{
    int32_t ROW_PER_VEC_BATCH = 200;
    auto sumFactory = new SumAggregatorFactory();
    // sum test types : long + decimal + dictionary + null
    auto sumLong = sumFactory->CreateAggregator(LongVecType::Instance(), LongVecType::Instance(), 0, true, false);
    auto sumShortDecimal = sumFactory->CreateAggregator(SHORT_DECIMAL_TYPE, LONG_DECIMAL_TYPE, 0, true, false);
    auto sumNull = sumFactory->CreateAggregator(LongVecType::Instance(), LongVecType::Instance(), 3, true, false);

    auto longInputVec = buildAggregateInput(LongVecType(), ROW_PER_VEC_BATCH);
    auto decimalInputVec = buildAggregateInput(Decimal128VecType(38, 2), ROW_PER_VEC_BATCH);
    LongVecType longVecType;
    int32_t ids[ROW_PER_VEC_BATCH];
    int64_t dict[ROW_PER_VEC_BATCH];
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longVecType, ROW_PER_VEC_BATCH, ids, ROW_PER_VEC_BATCH, dict);
    auto nullInputVec = buildAggregateInput(VecType(OMNI_VEC_TYPE_NONE), ROW_PER_VEC_BATCH);

    VectorBatch *vecBatch = new VectorBatch(4);
    vecBatch->SetVector(0, longInputVec);
    vecBatch->SetVector(1, decimalInputVec);
    vecBatch->SetVector(2, dictInputVec);
    vecBatch->SetVector(3, nullInputVec);
    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            sumLong->InitiateGroup(state, vecBatch, i);
        } else {
            sumLong->ProcessGroup(state, vecBatch, i);
        }
    }
    EXPECT_EQ(200, *static_cast<int64_t *>(state.val));
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            sumNull->InitiateGroup(state, vecBatch, i);
        } else {
            sumNull->ProcessGroup(state, vecBatch, i);
        }
    }
    EXPECT_EQ(nullptr, state.val);
    state.val = nullptr;

    // process short decimal
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
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
    int32_t ROW_PER_VEC_BATCH = 200;
    auto countFactory = new CountColumnAggregatorFactory();
    // count test types : long + dictionary + null
    auto countLong = countFactory->CreateAggregator(LongVecType::Instance(), LongVecType::Instance(), 0, true, false);
    auto countNull = countFactory->CreateAggregator(LongVecType::Instance(), LongVecType::Instance(), 2, true, false);

    auto longInputVec = buildAggregateInput(LongVecType(), ROW_PER_VEC_BATCH);
    LongVecType longVecType;
    int32_t ids[ROW_PER_VEC_BATCH];
    int64_t dict[ROW_PER_VEC_BATCH];
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longVecType, ROW_PER_VEC_BATCH, ids, ROW_PER_VEC_BATCH, dict);
    auto nullInputVec = buildAggregateInput(VecType(OMNI_VEC_TYPE_NONE), ROW_PER_VEC_BATCH);

    VectorBatch *vecBatch = new VectorBatch(3);
    vecBatch->SetVector(0, longInputVec);
    vecBatch->SetVector(1, dictInputVec);
    vecBatch->SetVector(2, nullInputVec);

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            countLong->InitiateGroup(state, vecBatch, i);
        } else {
            countLong->ProcessGroup(state, vecBatch, i);
        }
    }
    EXPECT_EQ(200, state.count);
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
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
    int32_t ROW_PER_VEC_BATCH = 200;
    auto countAllFactory = new CountAllAggregatorFactory();
    auto countLong = countAllFactory->CreateAggregator(OMNI_VEC_TYPE_NONE, OMNI_VEC_TYPE_LONG,
                                                       Aggregator::INVALID_INPUT_COL, true, false);
    auto countNull = countAllFactory->CreateAggregator(OMNI_VEC_TYPE_NONE, OMNI_VEC_TYPE_LONG,
                                                       Aggregator::INVALID_INPUT_COL, true, false);

    auto longInputVec = buildAggregateInput(LongVecType(), ROW_PER_VEC_BATCH);
    LongVecType longVecType;
    int32_t ids[ROW_PER_VEC_BATCH];
    int64_t dict[ROW_PER_VEC_BATCH];
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto nullInputVec = buildAggregateInput(VecType(OMNI_VEC_TYPE_NONE), ROW_PER_VEC_BATCH);

    VectorBatch *vecBatch = new VectorBatch(2, ROW_PER_VEC_BATCH);
    vecBatch->SetVector(0, longInputVec);
    vecBatch->SetVector(1, nullInputVec);

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            countLong->InitiateGroup(state, vecBatch, i);
        } else {
            countLong->ProcessGroup(state, vecBatch, i);
        }
    }
    EXPECT_EQ(200, state.count);
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
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
    int32_t ROW_PER_VEC_BATCH = 200;
    auto minFactory = new MinAggregatorFactory();
    // min test types : long + decimal + varchar + dictionary + null
    auto minLong = minFactory->CreateAggregator(LongVecType::Instance(), LongVecType::Instance(), 0, true, false);
    auto minDecimal = minFactory->CreateAggregator(LONG_DECIMAL_TYPE, LONG_DECIMAL_TYPE, 3, true, false);
    auto minVarchar =
        minFactory->CreateAggregator(VarcharVecType::Instance(), VarcharVecType::Instance(), 4, true, false);
    auto minNull = minFactory->CreateAggregator(LongVecType::Instance(), LongVecType::Instance(), 2, true, false);

    auto longInputVec = buildAggregateInput(LongVecType(), ROW_PER_VEC_BATCH);
    auto decimalInputVec = buildAggregateInput(Decimal128VecType(38, 0), ROW_PER_VEC_BATCH);
    VarcharVecType varcharVecType(1);
    std::string stringVals[ROW_PER_VEC_BATCH];
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        stringVals[i] = "1";
    }
    Vector *varcharInputVec = CreateVarcharVector(varcharVecType, stringVals, ROW_PER_VEC_BATCH);
    LongVecType longVecType;
    int32_t ids[ROW_PER_VEC_BATCH];
    int64_t dict[ROW_PER_VEC_BATCH];
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longVecType, ROW_PER_VEC_BATCH, ids, ROW_PER_VEC_BATCH, dict);
    auto nullInputVec = buildAggregateInput(VecType(OMNI_VEC_TYPE_NONE), ROW_PER_VEC_BATCH);

    VectorBatch *vectorBatch = new VectorBatch(5);
    vectorBatch->SetVector(0, longInputVec);
    vectorBatch->SetVector(1, dictInputVec);
    vectorBatch->SetVector(2, nullInputVec);
    vectorBatch->SetVector(3, decimalInputVec);
    vectorBatch->SetVector(4, varcharInputVec);

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            minLong->InitiateGroup(state, vectorBatch, i);
        } else {
            minLong->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(1, *static_cast<int64_t *>(state.val));
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            minNull->InitiateGroup(state, vectorBatch, i);
        } else {
            minNull->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(nullptr, state.val);
    state.val = nullptr;

    // process varchar
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            minVarchar->InitiateGroup(state, vectorBatch, i);
        } else {
            minVarchar->ProcessGroup(state, vectorBatch, i);
        }
    }
    std::string expectedStr = "1";
    EXPECT_EQ(0, std::memcmp(state.val, expectedStr.c_str(), 1));
    VarcharVector minVarcharOutput(VectorAllocatorFactory::GetGlobalAllocator(), 1, 1);
    minVarchar->ExtractValue(state, &minVarcharOutput, 0);
    uint8_t *strRes;
    minVarcharOutput.GetValue(0, &strRes);
    EXPECT_EQ(0, std::memcmp(strRes, expectedStr.c_str(), 1));
    state.val = nullptr;

    VectorHelper::FreeVecBatch(vectorBatch);
    delete minFactory;
}

TEST(AggregatorTest, max_test)
{
    int32_t ROW_PER_VEC_BATCH = 200;
    auto maxFactory = new MaxAggregatorFactory();
    // max test types : long + decimal + varchar + dictionary + null
    auto maxLong = maxFactory->CreateAggregator(LongVecType::Instance(), LongVecType::Instance(), 0, true, false);
    auto maxDecimal = maxFactory->CreateAggregator(LONG_DECIMAL_TYPE, LONG_DECIMAL_TYPE, 1, true, false);
    auto maxVarchar =
        maxFactory->CreateAggregator(VarcharVecType::Instance(), VarcharVecType::Instance(), 2, true, false);
    auto maxNull = maxFactory->CreateAggregator(LongVecType::Instance(), LongVecType::Instance(), 4, true, false);

    auto longInputVec = buildAggregateInput(LongVecType(), ROW_PER_VEC_BATCH);
    auto decimalInputVec = buildAggregateInput(Decimal128VecType(38, 0), ROW_PER_VEC_BATCH);
    VarcharVecType varcharVecType(1);
    std::string stringVals[ROW_PER_VEC_BATCH];
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        stringVals[i] = "1";
    }
    Vector *varcharInputVec = CreateVarcharVector(varcharVecType, stringVals, ROW_PER_VEC_BATCH);
    LongVecType longVecType;
    int32_t ids[ROW_PER_VEC_BATCH];
    int64_t dict[ROW_PER_VEC_BATCH];
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longVecType, ROW_PER_VEC_BATCH, ids, ROW_PER_VEC_BATCH, dict);
    auto nullInputVec = buildAggregateInput(VecType(OMNI_VEC_TYPE_NONE), ROW_PER_VEC_BATCH);

    VectorBatch *vectorBatch = new VectorBatch(5);
    vectorBatch->SetVector(0, longInputVec);
    vectorBatch->SetVector(1, decimalInputVec);
    vectorBatch->SetVector(2, varcharInputVec);
    vectorBatch->SetVector(3, dictInputVec);
    vectorBatch->SetVector(4, nullInputVec);

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            maxLong->InitiateGroup(state, vectorBatch, i);
        } else {
            maxLong->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(1, *static_cast<int64_t *>(state.val));
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            maxNull->InitiateGroup(state, vectorBatch, i);
        } else {
            maxNull->ProcessGroup(state, vectorBatch, i);
        }
    }
    EXPECT_EQ(nullptr, state.val);
    state.val = nullptr;

    // process varchar
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            maxVarchar->InitiateGroup(state, vectorBatch, i);
        } else {
            maxVarchar->ProcessGroup(state, vectorBatch, i);
        }
    }
    std::string expectedStr = "1";
    EXPECT_EQ(0, std::memcmp(state.val, expectedStr.c_str(), 1));
    VarcharVector maxVarcharOutput(VectorAllocatorFactory::GetGlobalAllocator(), 1, 1);
    maxVarchar->ExtractValue(state, &maxVarcharOutput, 0);
    uint8_t *strRes;
    maxVarcharOutput.GetValue(0, &strRes);
    EXPECT_EQ(0, std::memcmp(strRes, expectedStr.c_str(), 1));
    state.val = nullptr;

    // process decimal
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            maxDecimal->InitiateGroup(state, vectorBatch, i);
        } else {
            maxDecimal->ProcessGroup(state, vectorBatch, i);
        }
    }
    Decimal128 expected(1);
    EXPECT_EQ(expected, *static_cast<Decimal128 *>(state.val));
    state.val = nullptr;

    VectorHelper::FreeVecBatch(vectorBatch);
    delete maxFactory;
}

TEST(AggregatorTest, avg_test)
{
    int32_t ROW_PER_VEC_BATCH = 200;

    auto avgFactory = new AverageAggregatorFactory();
    // avg test types : long + decimal + dictionary + null
    auto avgLong = avgFactory->CreateAggregator(LongVecType::Instance(), DoubleVecType::Instance(), 0, true, false);
    auto avgDecimal = avgFactory->CreateAggregator(LONG_DECIMAL_TYPE, LONG_DECIMAL_TYPE, 1, true, false);
    auto avgNull = avgFactory->CreateAggregator(LongVecType::Instance(), DoubleVecType::Instance(), 3, true, false);

    auto longInputVec = buildAggregateInput(LongVecType(), ROW_PER_VEC_BATCH);
    auto decimalInputVec = buildAggregateInput(Decimal128VecType(38, 0), ROW_PER_VEC_BATCH);
    LongVecType longVecType;
    int32_t ids[ROW_PER_VEC_BATCH];
    int64_t dict[ROW_PER_VEC_BATCH];
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longVecType, ROW_PER_VEC_BATCH, ids, ROW_PER_VEC_BATCH, dict);
    auto nullInputVec = buildAggregateInput(VecType(OMNI_VEC_TYPE_NONE), ROW_PER_VEC_BATCH);

    VectorBatch *vectorBatch = new VectorBatch(4);
    vectorBatch->SetVector(0, longInputVec);
    vectorBatch->SetVector(1, decimalInputVec);
    vectorBatch->SetVector(2, dictInputVec);
    vectorBatch->SetVector(3, nullInputVec);

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            avgLong->InitiateGroup(state, vectorBatch, i);
        } else {
            avgLong->ProcessGroup(state, vectorBatch, i);
        }
    }
    DoubleVector avgLongOutput(VectorAllocatorFactory::GetGlobalAllocator(), 1);
    avgLong->ExtractValue(state, &avgLongOutput, 0);
    EXPECT_TRUE(avgLongOutput.GetValue(0) - 1 <= DBL_EPSILON);
    state.val = nullptr;

    //    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
    //        if (i == 0) {
    //            avgDecimal->InitiateGroup(state, decimalInputVec, i);
    //            avgDecimal->InitiateNonGroup(decimalInputVec, i);
    //        } else {
    //            avgDecimal->ProcessGroup(state, decimalInputVec, i);
    //            avgDecimal->ProcessNonGroup(decimalInputVec, i);
    //        }
    //    }
    //    Decimal128 expected(1);
    //    EXPECT_EQ(expected, *static_cast<Decimal128 *>(avgDecimal->Evaluate(state)));
    //    EXPECT_EQ(expected, *static_cast<Decimal128 *>(avgDecimal->Evaluate(avgDecimal->GetNonGroupState())));
    //    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            avgNull->InitiateGroup(state, vectorBatch, i);
        } else {
            avgNull->ProcessGroup(state, vectorBatch, i);
        }
    }
    DoubleVector avgNullOutput(VectorAllocatorFactory::GetGlobalAllocator(), 1);
    avgNull->ExtractValue(state, &avgNullOutput, 0);
    EXPECT_TRUE(avgNullOutput.IsValueNull(0));
    state.val = nullptr;

    VectorHelper::FreeVecBatch(vectorBatch);
    delete avgFactory;
}
