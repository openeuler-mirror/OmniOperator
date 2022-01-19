#include <gtest/gtest.h>
#include "operator/aggregation/aggregator/aggregator_factory.h"
#include "operator/aggregation/aggregator/all_aggregators.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "test/util/test_util.h"
#include "jit/jit.h"
#include "jit/specialization.h"
#include "operator/optimization.h"
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

void destroyInput(VectorBatch **input, int32_t vecBatchNum, int32_t colNum)
{
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        input[i]->ReleaseAllVectors();
        delete input[i];
    }
}

// create a factory and make it optimized
uintptr_t CreateHashFactoryWithJit(bool inputRaw, bool outputPartial)
{
    using namespace omniruntime::jit;
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

    int32_t groupColNum = groupByColContext.len;
    int32_t aggColNum = aggColContext.len;
    int32_t colNum = groupByColContext.len + aggColContext.len;
    int32_t *colTypes = new int32_t[colNum];

    for (int i = 0; i < groupColNum; ++i) {
        colTypes[groupByColContext.context[i]] = groupByTypeContext.context[i];
    }
    for (int i = 0; i < aggColNum; ++i) {
        colTypes[aggColContext.context[i]] = aggInputTypeContext.context[i];
    }

    ParamValue p_col_type = ParamValue(colTypes, colNum);
    ParamValue p_col_count = ParamValue(&colNum);
    ParamValue p_groupByColIdx = ParamValue((int32_t *)groupByColContext.context, groupColNum);
    ParamValue p_group_num = ParamValue(&groupColNum);
    ParamValue p_aggColIdx = ParamValue((int32_t *)aggColContext.context, aggColNum);
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_data_type = ParamValue((int32_t *)aggInputTypeContext.context, aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t *)aggFuncTypeContext.context, aggColNum);

    Specialization *inloopSp = new Specialization();
    inloopSp->AddSpecializedParam(3, &p_col_type);
    inloopSp->AddSpecializedParam(4, &p_col_count);
    inloopSp->AddSpecializedParam(5, &p_groupByColIdx);
    inloopSp->AddSpecializedParam(6, &p_group_num);
    inloopSp->AddSpecializedParam(7, &p_aggColIdx);
    inloopSp->AddSpecializedParam(8, &p_agg_num);
    inloopSp->AddSpecializedParam(9, &p_agg_types);

    Specialization *hashColumnSp = new Specialization();
    hashColumnSp->AddSpecializedParam(2, &p_col_type);
    hashColumnSp->AddSpecializedParam(3, &p_group_num);

    Specialization *aggColumnSp = new Specialization();
    aggColumnSp->AddSpecializedParam(2, &p_col_type);
    aggColumnSp->AddSpecializedParam(3, &p_agg_num);

    std::map<std::string, Specialization> hashGroupbySps = {
        { OMNIJIT_HASH_GROUPBY_INLOOP, *inloopSp },
    };

    omniruntime::jit::Context groupAggregationContext(GenerateOperatorTemplatePath("group_aggregation"),
        hashGroupbySps);
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { groupAggregationContext });
    jit->Specialize(std::vector<Optimization>());

    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(jit->GetJitedFunction("CreateOperator"));
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
    using namespace omniruntime::jit;
    VecTypes sourceTypes(
        { LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance(), LongVecType::Instance() });
    uint32_t aggFuncTypes[4] = {0, 0, 0, 0};
    omniruntime::op::PrepareContext aggFuncTypeContext = { aggFuncTypes, 4 };
    uint32_t aggInputCols[4] = {0, 1, 2, 3};
    omniruntime::op::PrepareContext aggInputColsContext = { aggInputCols, 4 };


    int32_t aggColNum = aggFuncTypeContext.len;

    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_data_type = ParamValue(sourceTypes.GetIds(), aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t *)aggFuncTypes, aggColNum);

    auto *inloopSp = new Specialization();
    std::map<std::string, Specialization> nonGroupSps = { { OMNIJIT_NON_GROUP_INLOOP, *inloopSp } };

    auto *groupAggregationContext =
        new omniruntime::jit::Context(GenerateOperatorTemplatePath("non_group_aggregation"), nonGroupSps);
    auto jit = new Jit(std::vector<omniruntime::jit::Context> { *groupAggregationContext });
    jit->Specialize(std::vector<Optimization>());

    auto jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(jit->GetJitedFunction("CreateOperator"));
    std::cout << "after jit" << std::endl;
    auto nativeOperatorFactory =
        new AggregationOperatorFactory(sourceTypes, aggFuncTypeContext, aggInputColsContext, sourceTypes, true, false);
    nativeOperatorFactory->Init();
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->SetJitContext(jitContext);
    return reinterpret_cast<uintptr_t>(nativeOperatorFactory);
}

uintptr_t CreateHashFactoryWithoutJit(bool inputRaw, bool outputPartial)
{
    using namespace omniruntime::jit;
    uint32_t groupCols[2] = {0, 1};
    std::vector<VecType> groupByTypeVec = { LongVecType::Instance(), LongVecType::Instance() };
    VecTypes groupByTypes(groupByTypeVec);
    uint32_t aggCols[2] = {2, 3};
    std::vector<VecType> aggInputTypeVec = { LongVecType::Instance(), LongVecType::Instance() };
    VecTypes aggInputTypes(aggInputTypeVec);
    std::vector<VecType> aggOutputTypeVec;
    if (outputPartial == true) {
        aggOutputTypeVec = { LongVecType::Instance(), ContainerVecType::Instance() };
    } else {
        aggOutputTypeVec = { LongVecType::Instance(), DoubleVecType::Instance() };
    }
    VecTypes aggOutputTypes(aggOutputTypeVec);
    uint32_t aggFunType[2] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_AVG};
    uint32_t retTypes[] = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_DOUBLE};
    PrepareContext groupByColContext = { groupCols, 2 };
    PrepareContext aggColContext = { aggCols, 2 };
    PrepareContext aggFuncTypeContext = { aggFunType, 2 };
    PrepareContext retTypesContext = { retTypes, 4 };

    int32_t groupColNum = groupByColContext.len;
    int32_t aggColNum = aggColContext.len;
    int32_t colNum = groupByColContext.len + aggColContext.len;

    omniruntime::op::HashAggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupByTypes, aggColContext,
        aggInputTypes, aggOutputTypes, aggFuncTypeContext, inputRaw, outputPartial);
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
    ColumnIndex c2 = { 2, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c3 = { 3, LongVecType::Instance(), ContainerVecType::Instance() };
    ColumnIndex c4 = { 4, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c5 = { 5, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c6 = { 6, LongVecType::Instance(), LongVecType::Instance() };
    std::vector<ColumnIndex> groupByColumns1 = { c0, c1 };
    std::vector<ColumnIndex> aggregateColumns1 = { c2, c3, c4, c5, c6 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(OMNI_VEC_TYPE_LONG,
        OMNI_VEC_TYPE_LONG, true, true));
    aggs1.push_back(
        std::make_unique<AverageAggregator<LongVector>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_CONTAINER, true, true));
    aggs1.push_back(std::make_unique<CountAggregator>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, true));
    aggs1.push_back(
        std::make_unique<MinAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, true));
    aggs1.push_back(
        std::make_unique<MaxAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, true));
    HashAggregationOperator *groupBy1 =
        new HashAggregationOperator(groupByColumns1, aggregateColumns1, std::move(aggs1), true, true);
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
    ColumnIndex c7 = { 0, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c8 = { 1, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c9 = { 2, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c10 = { 3, LongVecType::Instance(), ContainerVecType::Instance() };
    ColumnIndex c11 = { 4, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c12 = { 5, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c13 = { 6, LongVecType::Instance(), LongVecType::Instance() };
    groupByColumns1 = { c7, c8 };
    aggregateColumns1 = { c9, c10, c11, c12, c13 };
    aggs1.clear();
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(OMNI_VEC_TYPE_LONG,
        OMNI_VEC_TYPE_LONG, true, true));
    aggs1.push_back(
        std::make_unique<AverageAggregator<LongVector>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_CONTAINER, true, true));
    aggs1.push_back(std::make_unique<CountAggregator>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, true));
    aggs1.push_back(
        std::make_unique<MinAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, true));
    aggs1.push_back(
        std::make_unique<MaxAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, true));
    HashAggregationOperator *groupBy2 =
        new HashAggregationOperator(groupByColumns1, aggregateColumns1, std::move(aggs1), true, true);
    groupBy2->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy2->AddInput(input2[i]);
    }

    std::vector<VectorBatch *> result2;
    int32_t tableCount2 = groupBy2->GetOutput(result2);
    Operator::DeleteOperator(groupBy2);

    // Second stage
    ColumnIndex c14 = { 0, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c15 = { 1, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c16 = { 2, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c17 = { 3, LongVecType::Instance(), DoubleVecType::Instance() };
    ColumnIndex c18 = { 4, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c19 = { 5, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c20 = { 6, LongVecType::Instance(), LongVecType::Instance() };
    std::vector<ColumnIndex> groupByColumns2 = { c14, c15 };
    std::vector<ColumnIndex> aggregateColumns2 = { c16, c17, c18, c19, c20 };
    std::vector<std::unique_ptr<Aggregator>> aggs2;
    aggs2.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(OMNI_VEC_TYPE_LONG,
        OMNI_VEC_TYPE_LONG, false, false));
    aggs2.push_back(
        std::make_unique<AverageAggregator<LongVector>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_CONTAINER, false, false));
    aggs2.push_back(std::make_unique<CountAggregator>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, false, false));
    aggs2.push_back(
        std::make_unique<MinAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, false, false));
    aggs2.push_back(
        std::make_unique<MaxAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, false, false));
    HashAggregationOperator *groupBy3 =
        new HashAggregationOperator(groupByColumns2, aggregateColumns2, std::move(aggs2), false, false);
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
    ColumnIndex c0 = { 0, type1, type1 };
    VarcharVecType type2(1);
    ColumnIndex c1 = { 1, type2, LongVecType() };
    VarcharVecType type3(1);
    ColumnIndex c2 = { 2, type3, type3 };
    VarcharVecType type4(4);
    ColumnIndex c3 = { 3, type4, type4 };

    std::vector<ColumnIndex> groupByColumns1 = { c0 };
    std::vector<ColumnIndex> aggregateColumns1 = { c1, c2, c3 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(
        std::make_unique<CountAggregator>(OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_LONG, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(
        std::make_unique<MinVarcharAggregator>(OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(
        std::make_unique<MaxVarcharAggregator>(OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR, INPUT_MODE, OUTPUT_MODE));
    HashAggregationOperator *groupByVarChar =
        new HashAggregationOperator(groupByColumns1, aggregateColumns1, std::move(aggs1), true, false);
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
    ColumnIndex c0 = { 0, type1, type1 };
    CharVecType type2(1);
    ColumnIndex c1 = { 1, type2, LongVecType() };
    CharVecType type3(1);
    ColumnIndex c2 = { 2, type3, type3 };
    CharVecType type4(4);
    ColumnIndex c3 = { 3, type4, type4 };

    std::vector<ColumnIndex> groupByColumns1 = { c0 };
    std::vector<ColumnIndex> aggregateColumns1 = { c1, c2, c3 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<CountAggregator>(OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_LONG, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(
        std::make_unique<MinVarcharAggregator>(OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_CHAR, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(
        std::make_unique<MaxVarcharAggregator>(OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_CHAR, INPUT_MODE, OUTPUT_MODE));
    HashAggregationOperator *groupByVarChar =
        new HashAggregationOperator(groupByColumns1, aggregateColumns1, std::move(aggs1), true, false);
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
    ColumnIndex c1 = { 1, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c2 = { 2, LongVecType::Instance(), DoubleVecType::Instance() };
    ColumnIndex c3 = { 3, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c4 = { 4, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c5 = { 5, LongVecType::Instance(), LongVecType::Instance() };

    std::vector<ColumnIndex> groupByColumns1 = { c0 };
    std::vector<ColumnIndex> aggregateColumns1 = { c1, c2, c3, c4, c5 };
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(OMNI_VEC_TYPE_LONG,
        OMNI_VEC_TYPE_LONG, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<AverageAggregator<LongVector>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_DOUBLE,
        INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<CountAggregator>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MinAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG,
        INPUT_MODE, OUTPUT_MODE));
    aggs1.push_back(std::make_unique<MaxAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG,
        INPUT_MODE, OUTPUT_MODE));
    HashAggregationOperator *groupByNULL =
        new HashAggregationOperator(groupByColumns1, aggregateColumns1, std::move(aggs1), true, false);
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
    std::vector<ColumnIndex> v1 = { c0, c1 };
    std::vector<ColumnIndex> v2 = { c0, c1 };
    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT,
        INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG,
        INPUT_MODE, OUTPUT_MODE));
    HashAggregationOperator *groupBy = new HashAggregationOperator(v1, v2, std::move(aggs), true, false);
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


    AggregateType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
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
    delete nativeOperatorFactory;
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
    delete reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(factoryObjAddr);
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
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG,
        true, true));
    aggs.push_back(
        std::make_unique<AverageAggregator<LongVector>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_CONTAINER, true, true));
    aggs.push_back(std::make_unique<CountAggregator>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, true));
    aggs.push_back(
        std::make_unique<MinAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, true));
    aggs.push_back(
        std::make_unique<MaxAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, true));
    std::vector<int32_t> aggInputCols = {0, 1, 2, 3, 4};
    VecTypes aggPartialOutputTypes(
        std::vector<VecType> { LongVecType(), ContainerVecType(), LongVecType(), LongVecType(), LongVecType() });
    auto aggregate1 = new AggregationOperator(std::move(aggs), aggInputCols, aggPartialOutputTypes, true, true);

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        aggregate1->AddInput(input1[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t tableCount1 = aggregate1->GetOutput(result);
    delete aggregate1;

    VectorBatch **input2 = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 0, 5, groupTypes, aggTypes);
    ASSERT(!(input2 == nullptr));
    aggs.clear();
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG,
        true, true));
    aggs.push_back(
        std::make_unique<AverageAggregator<LongVector>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_CONTAINER, true, true));
    aggs.push_back(std::make_unique<CountAggregator>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, true));
    aggs.push_back(
        std::make_unique<MinAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, true));
    aggs.push_back(
        std::make_unique<MaxAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, true));
    auto aggregate2 = new AggregationOperator(std::move(aggs), aggInputCols, aggPartialOutputTypes, true, true);

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        aggregate2->AddInput(input2[i]);
    }
    int32_t tableCount2 = aggregate2->GetOutput(result);
    delete aggregate2;

    // Second stage
    std::vector<std::unique_ptr<Aggregator>> aggs1;
    aggs1.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(OMNI_VEC_TYPE_LONG,
        OMNI_VEC_TYPE_LONG, false, false));
    aggs1.push_back(
        std::make_unique<AverageAggregator<LongVector>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_DOUBLE, false, false));
    aggs1.push_back(std::make_unique<CountAggregator>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, false, false));
    aggs1.push_back(
        std::make_unique<MinAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, false, false));
    aggs1.push_back(
        std::make_unique<MaxAggregator<LongVector, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, false, false));
    VecTypes aggFinalOutputTypes(
        std::vector<VecType> { LongVecType(), DoubleVecType(), LongVecType(), LongVecType(), LongVecType() });
    auto aggregate3 = new AggregationOperator(std::move(aggs1), aggInputCols, aggFinalOutputTypes, false, false);

    for (int32_t i = 0; i < result.size(); ++i) {
        aggregate3->AddInput(result[i]);
    }

    std::vector<VectorBatch *> result1;
    int32_t tableCount3 = aggregate3->GetOutput(result1);
    delete aggregate3;
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
    aggs.push_back(std::make_unique<AverageAggregator<LongVector>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_DOUBLE));
    std::vector<int32_t> aggInputCols = {0};
    VecTypes aggOutputTypes(std::vector<VecType> { DoubleVecType() });
    auto aggregate = new AggregationOperator(std::move(aggs), aggInputCols, aggOutputTypes, true, false);

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
    AggregateType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    uint32_t aggInputCols[] = {0, 1, 2, 3};
    PrepareContext aggFuncTypesContext = { reinterpret_cast<uint32_t *>(aggFunType), 4 };
    PrepareContext aggInputColsContext = { aggInputCols, 4 };

    auto nativeOperatorFactory = new AggregationOperatorFactory(sourceTypes, aggFuncTypesContext, aggInputColsContext,
        aggOutputTypes, true, false);
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
    nativeOperatorFactory->Close();
    delete nativeOperatorFactory;
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
    using namespace omniruntime::jit;
    uint32_t groupCols[] = {0, 1};

    VecTypes groupInputTypes({ LongVecType::Instance(), LongVecType::Instance() });
    uint32_t aggCols[] = {2, 3};
    VecTypes aggInput({ LongVecType::Instance(), LongVecType::Instance() });
    VecTypes aggOutput({ LongVecType::Instance(), LongVecType::Instance() });
    AggregateType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    uint32_t retTypes[] = {1,1,1,1};
    PrepareContext groupByColContext = { groupCols, 2 };
    PrepareContext groupByTypeContext = { reinterpret_cast<uint32_t *>(const_cast<int32_t *>(groupInputTypes.GetIds())),
        2 };
    PrepareContext aggColContext = { aggCols, 2 };
    PrepareContext aggTypeContext = { reinterpret_cast<uint32_t *>(const_cast<int32_t *>(aggInput.GetIds())), 2 };
    PrepareContext aggFuncTypeContext = { reinterpret_cast<uint32_t *>(aggFunType), 2 };
    PrepareContext retTypesContext = { retTypes, 4 };

    int32_t groupColNum = groupByColContext.len;
    int32_t aggColNum = aggColContext.len;
    int32_t colNum = groupByColContext.len + aggColContext.len;
    int32_t colTypes[colNum];

    for (int i = 0; i < groupColNum; ++i) {
        colTypes[groupByColContext.context[i]] = groupByTypeContext.context[i];
    }
    for (int i = 0; i < aggColNum; ++i) {
        colTypes[aggColContext.context[i]] = aggTypeContext.context[i];
    }
    // ------------------------------------------JIT Optimization --------------------------------------------
    ParamValue p_col_type = ParamValue(colTypes, colNum);
    ParamValue p_col_count = ParamValue(&colNum);
    ParamValue p_groupByColIdx = ParamValue((int32_t *)groupByColContext.context, groupColNum);
    ParamValue p_group_num = ParamValue(&groupColNum);
    ParamValue p_aggColIdx = ParamValue((int32_t *)aggColContext.context, aggColNum);
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_data_type = ParamValue((int32_t *)aggTypeContext.context, aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t *)aggFuncTypeContext.context, aggColNum);

    Specialization inloopSp;
    inloopSp.AddSpecializedParam(3, &p_col_type);
    inloopSp.AddSpecializedParam(4, &p_col_count);
    inloopSp.AddSpecializedParam(5, &p_groupByColIdx);
    inloopSp.AddSpecializedParam(6, &p_group_num);
    inloopSp.AddSpecializedParam(7, &p_aggColIdx);
    inloopSp.AddSpecializedParam(8, &p_agg_num);
    inloopSp.AddSpecializedParam(9, &p_agg_types);

    std::map<std::string, Specialization> hashGroupbySps = {
        { OMNIJIT_HASH_GROUPBY_INLOOP, inloopSp },
    };

    //    auto *groupAggregationContext = new
    //    omniruntime::jit::Context(GenerateOperatorTemplatePath("group_aggregation"), hashGroupbySps);
    omniruntime::jit::Context groupAggregationContext(GenerateOperatorTemplatePath("group_aggregation"),
        hashGroupbySps);
    Jit jit(std::vector<omniruntime::jit::Context> { groupAggregationContext });
    jit.Specialize(std::vector<Optimization> { Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
        Optimization::SROA, Optimization::AGGRESIVE_DCE },
        std::vector<ModuleOptimization> { ModuleOptimization::PRUNE_EH });

    // ------------------------------------------Create operator--------------------------------------------
    JitContext jitContext;
    jitContext.func = reinterpret_cast<uintptr_t>(jit.GetJitedFunction("CreateOperator"));
    std::cout << "after JIT" << std::endl;
    omniruntime::op::HashAggregationOperatorFactory *nativeOperatorFactory =
        new omniruntime::op::HashAggregationOperatorFactory(groupByColContext, groupInputTypes, aggColContext, aggInput,
        aggOutput, aggFuncTypeContext, true, false);
    nativeOperatorFactory->Init();
    std::cout << "after create factory" << std::endl;
    nativeOperatorFactory->SetJitContext(&jitContext);
    // create operator
    auto jitGroupBy =
        reinterpret_cast<HashAggModule>(nativeOperatorFactory->GetJitContext()->func)(nativeOperatorFactory);

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
        printf("HashAgg with OmniJit,    used %lld instructions\n", perfUtil->GetData());
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
    delete nativeOperatorFactory;
    delete nativeOperatorFactory2;

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
    std::vector<VecType> aggTypes = { LongVecType(), LongVecType() };
    VectorBatch **input1 = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);
    if (input1 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    uintptr_t partialFactoryAddr1 = CreateHashFactoryWithoutJit(true, true);
    auto partialFactory1 = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(partialFactoryAddr1);
    auto partialOperator1 = partialFactory1->CreateOperator();
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        partialOperator1->AddInput(input1[i]);
    }
    std::vector<VectorBatch *> resultFromPartial1;
    partialOperator1->GetOutput(resultFromPartial1);

    VectorBatch **input2 = buildAggInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, CARDINALITY, 2, 2, groupTypes, aggTypes);
    if (input2 == nullptr) {
        std::cerr << "Building input data failed!" << std::endl;
    }

    uintptr_t partialFactoryAddr2 = CreateHashFactoryWithoutJit(true, true);
    auto partialFactory2 = reinterpret_cast<omniruntime::op::HashAggregationOperatorFactory *>(partialFactoryAddr2);
    auto partialOperator2 = partialFactory2->CreateOperator();
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        partialOperator2->AddInput(input2[i]);
    }
    std::vector<VectorBatch *> resultFromPartial2;
    partialOperator2->GetOutput(resultFromPartial2);

    uintptr_t finalFactoryAddr = CreateHashFactoryWithoutJit(false, false);
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
    delete partialFactory1;
    delete partialFactory2;
    delete finalFactory;

    for (auto vecBatch : resultFromFinal) {
        VectorHelper::PrintVecBatch(vecBatch);
    }

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType(), DoubleVecType() }));
    int64_t expectData1[CARDINALITY] = {0, 1, 2, 3};
    int64_t expectData2[CARDINALITY] = {0, 1, 2, 3};
    int64_t expectData3[CARDINALITY] = {10000000, 10000000, 10000000, 10000000};
    double expectData4[CARDINALITY] = {1.0, 1.0, 1.0, 1.0};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, CARDINALITY, expectData1, expectData2, expectData3, expectData4);
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
    ColumnIndex c2 = { 2, LongVecType::Instance(), LongVecType::Instance() };
    ColumnIndex c3 = { 3, LongVecType::Instance(), LongVecType::Instance() };
    std::vector<ColumnIndex> groupByColumns = { c0, c1 };
    std::vector<ColumnIndex> aggregateColumns = { c2, c3 };
    std::vector<std::unique_ptr<Aggregator>> aggs;
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG,
        INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG,
        INPUT_MODE, OUTPUT_MODE));
    HashAggregationOperator *groupBy =
        new HashAggregationOperator(groupByColumns, aggregateColumns, std::move(aggs), true, false);
    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }

    std::vector<VectorBatch *> result;
    int32_t vecBatchCount = groupBy->GetOutput(result);
    Operator::DeleteOperator(groupBy);
    for (auto batch : result) {
        VectorHelper::PrintVecBatch(batch);
        batch->ReleaseAllVectors();
        delete batch;
    }
    groupByColumns.clear();
    aggregateColumns.clear();
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
    c2 = { 2, Date32VecType::Instance(), Date32VecType::Instance() };
    c3 = { 3, Date32VecType::Instance(), Date32VecType::Instance() };
    groupByColumns = { c0, c1 };
    aggregateColumns = { c2, c3 };
    aggs.push_back(std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(OMNI_VEC_TYPE_DATE32,
        OMNI_VEC_TYPE_DATE32, INPUT_MODE, OUTPUT_MODE));
    aggs.push_back(std::make_unique<SumAggregator<IntVector, int32_t, int64_t>>(OMNI_VEC_TYPE_DATE32,
        OMNI_VEC_TYPE_DATE32, INPUT_MODE, OUTPUT_MODE));
    groupBy = new HashAggregationOperator(groupByColumns, aggregateColumns, std::move(aggs), true, false);
    groupBy->Init();

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->AddInput(input[i]);
    }

    vecBatchCount = groupBy->GetOutput(result);
    Operator::DeleteOperator(groupBy);
    for (auto batch : result) {
        VectorHelper::PrintVecBatch(batch);
        batch->ReleaseAllVectors();
        delete batch;
    }
    groupByColumns.clear();
    aggregateColumns.clear();
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
    c1 = { 1, LongVecType::Instance(), LongVecType::Instance() };
    groupByColumns = { c0 };
    aggregateColumns = { c1 };
    aggs.push_back(std::make_unique<SumAggregator<LongVector, int64_t, int64_t>>(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG,
        INPUT_MODE, OUTPUT_MODE));
    groupBy = new HashAggregationOperator(groupByColumns, aggregateColumns, std::move(aggs), true, false);
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
    aggregateColumns.clear();
    aggs.clear();
    result.clear();
}

TEST(AggregatorTest, sum_test)
{
    int32_t ROW_PER_VEC_BATCH = 200;
    auto sumFactory = new SumAggregatorFactory();
    // sum test types : long + decimal + dictionary + null
    auto sumLong = sumFactory->CreateAggregator(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, false);
    auto sumDecimal = sumFactory->CreateAggregator(OMNI_VEC_TYPE_DECIMAL128, OMNI_VEC_TYPE_DECIMAL128, true, false);
    auto sumNull = sumFactory->CreateAggregator(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, false);

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

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            sumLong->InitiateGroup(state, longInputVec, i);
        } else {
            sumLong->ProcessGroup(state, longInputVec, i);
        }
    }
    EXPECT_EQ(200, *static_cast<int64_t *>(state.val));
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            sumNull->InitiateGroup(state, nullInputVec, i);
        } else {
            sumNull->ProcessGroup(state, nullInputVec, i);
        }
    }
    EXPECT_EQ(nullptr, state.val);
    state.val = nullptr;

    delete longInputVec;
    delete decimalInputVec;
    delete dictInputVec;
    delete nullInputVec;
    delete sumFactory;
}

TEST(AggregatorTest, count_column_test)
{
    int32_t ROW_PER_VEC_BATCH = 200;
    auto countFactory = new CountAggregatorFactory();
    // count test types : long + dictionary + null
    auto countLong = countFactory->CreateAggregator(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, false);
    auto countNull = countFactory->CreateAggregator(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, false);

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

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            countLong->InitiateGroup(state, longInputVec, i);
        } else {
            countLong->ProcessGroup(state, longInputVec, i);
        }
    }
    EXPECT_EQ(200, state.count);
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            countNull->InitiateGroup(state, nullInputVec, i);
        } else {
            countNull->ProcessGroup(state, nullInputVec, i);
        }
    }
    EXPECT_EQ(0, state.count);
    state.val = nullptr;

    delete longInputVec;
    delete dictInputVec;
    delete nullInputVec;
    delete countFactory;
}

TEST(AggregatorTest, min_test)
{
    int32_t ROW_PER_VEC_BATCH = 200;
    auto minFactory = new MinAggregatorFactory();
    // min test types : long + decimal + varchar + dictionary + null
    auto minLong = minFactory->CreateAggregator(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, false);
    auto minDecimal = minFactory->CreateAggregator(OMNI_VEC_TYPE_DECIMAL128, OMNI_VEC_TYPE_DECIMAL128, true, false);
    auto minVarchar = minFactory->CreateAggregator(OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR, true, false);
    auto minNull = minFactory->CreateAggregator(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, false);

    auto longInputVec = buildAggregateInput(LongVecType(), ROW_PER_VEC_BATCH);
    auto decimalInputVec = buildAggregateInput(Decimal128VecType(38, 0), ROW_PER_VEC_BATCH);
    VarcharVecType varcharVecType(1);
    std::string stringVals[ROW_PER_VEC_BATCH];
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        stringVals[i] = "1";
    }
    auto varcharInputVec = CreateVarcharVector(varcharVecType, stringVals, ROW_PER_VEC_BATCH);
    LongVecType longVecType;
    int32_t ids[ROW_PER_VEC_BATCH];
    int64_t dict[ROW_PER_VEC_BATCH];
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longVecType, ROW_PER_VEC_BATCH, ids, ROW_PER_VEC_BATCH, dict);
    auto nullInputVec = buildAggregateInput(VecType(OMNI_VEC_TYPE_NONE), ROW_PER_VEC_BATCH);

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            minLong->InitiateGroup(state, longInputVec, i);
        } else {
            minLong->ProcessGroup(state, longInputVec, i);
        }
    }
    EXPECT_EQ(1, *static_cast<int64_t *>(state.val));
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            minNull->InitiateGroup(state, nullInputVec, i);
        } else {
            minNull->ProcessGroup(state, nullInputVec, i);
        }
    }
    EXPECT_EQ(nullptr, state.val);
    state.val = nullptr;

    // process varchar
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            minVarchar->InitiateGroup(state, varcharInputVec, i);
        } else {
            minVarchar->ProcessGroup(state, varcharInputVec, i);
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

    delete longInputVec;
    delete dictInputVec;
    delete nullInputVec;
    delete decimalInputVec;
    delete varcharInputVec;
    delete minFactory;
}

TEST(AggregatorTest, max_test)
{
    int32_t ROW_PER_VEC_BATCH = 200;
    auto maxFactory = new MaxAggregatorFactory();
    // max test types : long + decimal + varchar + dictionary + null
    auto maxLong = maxFactory->CreateAggregator(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, false);
    auto maxDecimal = maxFactory->CreateAggregator(OMNI_VEC_TYPE_DECIMAL128, OMNI_VEC_TYPE_DECIMAL128, true, false);
    auto maxVarchar = maxFactory->CreateAggregator(OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR, true, false);
    auto maxNull = maxFactory->CreateAggregator(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, true, false);

    auto longInputVec = buildAggregateInput(LongVecType(), ROW_PER_VEC_BATCH);
    auto decimalInputVec = buildAggregateInput(Decimal128VecType(38, 0), ROW_PER_VEC_BATCH);
    VarcharVecType varcharVecType(1);
    std::string stringVals[ROW_PER_VEC_BATCH];
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        stringVals[i] = "1";
    }
    auto varcharInputVec = CreateVarcharVector(varcharVecType, stringVals, ROW_PER_VEC_BATCH);
    LongVecType longVecType;
    int32_t ids[ROW_PER_VEC_BATCH];
    int64_t dict[ROW_PER_VEC_BATCH];
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        ids[i] = 0;
        dict[i] = 1;
    }
    auto dictInputVec = CreateDictionaryVector(longVecType, ROW_PER_VEC_BATCH, ids, ROW_PER_VEC_BATCH, dict);
    auto nullInputVec = buildAggregateInput(VecType(OMNI_VEC_TYPE_NONE), ROW_PER_VEC_BATCH);

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            maxLong->InitiateGroup(state, longInputVec, i);
        } else {
            maxLong->ProcessGroup(state, longInputVec, i);
        }
    }
    EXPECT_EQ(1, *static_cast<int64_t *>(state.val));
    state.val = nullptr;

    // process null
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            maxNull->InitiateGroup(state, nullInputVec, i);
        } else {
            maxNull->ProcessGroup(state, nullInputVec, i);
        }
    }
    EXPECT_EQ(nullptr, state.val);
    state.val = nullptr;

    // process varchar
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            maxVarchar->InitiateGroup(state, varcharInputVec, i);
        } else {
            maxVarchar->ProcessGroup(state, varcharInputVec, i);
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
            maxDecimal->InitiateGroup(state, decimalInputVec, i);
        } else {
            maxDecimal->ProcessGroup(state, decimalInputVec, i);
        }
    }
    Decimal128 expected(1);
    EXPECT_EQ(expected, *static_cast<Decimal128 *>(state.val));
    state.val = nullptr;

    delete longInputVec;
    delete dictInputVec;
    delete nullInputVec;
    delete decimalInputVec;
    delete varcharInputVec;
    delete maxFactory;
}

TEST(AggregatorTest, avg_test)
{
    int32_t ROW_PER_VEC_BATCH = 200;

    auto avgFactory = new AverageAggregatorFactory();
    // avg test types : long + decimal + dictionary + null
    auto avgLong = avgFactory->CreateAggregator(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_DOUBLE, true, false);
    auto avgDecimal = avgFactory->CreateAggregator(OMNI_VEC_TYPE_DECIMAL128, OMNI_VEC_TYPE_DECIMAL128, true, false);
    auto avgNull = avgFactory->CreateAggregator(OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_DOUBLE, true, false);

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

    // process long
    AggregateState state { nullptr };
    for (int32_t i = 0; i < ROW_PER_VEC_BATCH; ++i) {
        if (i == 0) {
            avgLong->InitiateGroup(state, longInputVec, i);
        } else {
            avgLong->ProcessGroup(state, longInputVec, i);
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
            avgNull->InitiateGroup(state, nullInputVec, i);
        } else {
            avgNull->ProcessGroup(state, nullInputVec, i);
        }
    }
    DoubleVector avgNullOutput(VectorAllocatorFactory::GetGlobalAllocator(), 1);
    avgNull->ExtractValue(state, &avgNullOutput, 0);
    EXPECT_TRUE(avgNullOutput.IsValueNull(0));
    state.val = nullptr;

    delete longInputVec;
    delete dictInputVec;
    delete nullInputVec;
    delete decimalInputVec;
    delete avgFactory;
}