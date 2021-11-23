/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include <gtest/gtest.h>
#include "../../src/operator/aggregation/aggregation.h"
#include "../../src/operator/aggregation/group_aggregation_expr.h"
#include "../../src/vector/vector_helper.h"
#include "../../src/jit/jit.h"
#include "../../src/operator/optimization.h"
#include "../util/test_util.h"

using namespace omniruntime::vec;
using namespace omniruntime::jit;
using namespace omniruntime::op;

JitContext *CreateHashAggregationWithExprJitContext(int32_t colNum, int32_t groupColNum, int32_t aggColNum,
    const int32_t* aggFuncTypes, int32_t* projectTypes)
{
    using namespace std;

    PrepareContext aggFuncTypeContext = { (uint32_t *)aggFuncTypes, static_cast<size_t>(aggColNum) };

    ParamValue pColType = ParamValue(projectTypes, colNum);
    ParamValue pColCount = ParamValue(&colNum);
    ParamValue pGroupNum = ParamValue(&groupColNum);
    ParamValue pAggNum = ParamValue(&aggColNum);
    ParamValue pAggTypes = ParamValue((int32_t *)aggFuncTypeContext.context, aggColNum);

    auto *inloopSp = new Specialization();
    inloopSp->AddSpecializedParam(3, &pColType);
    inloopSp->AddSpecializedParam(4, &pColCount);
    inloopSp->AddSpecializedParam(6, &pGroupNum);
    inloopSp->AddSpecializedParam(8, &pAggNum);
    inloopSp->AddSpecializedParam(9, &pAggTypes);

    auto *processAggSp = new Specialization();
    processAggSp->AddSpecializedParam(2, &pAggNum);
    processAggSp->AddSpecializedParam(3, &pColType);

    map<string, Specialization> hashGroupbySps = { { OMNIJIT_HASH_GROUPBY_INLOOP, *inloopSp },
        { OMNIJIT_HASH_GROUPBY_PROCESS_AGG, *processAggSp } };

    auto *groupAggWithExprContext = new omniruntime::jit::Context(
        GenerateOperatorTemplatePath("group_aggregation_expr"),
        map<string, Specialization>());
    auto *groupAggContext = new omniruntime::jit::Context(
        GenerateOperatorTemplatePath("group_aggregation"), hashGroupbySps);
    Jit *jit = new Jit(vector<omniruntime::jit::Context> { *groupAggWithExprContext, *groupAggContext });
    jit->Specialize(vector<Optimization> { Optimization::LOOP_UNROLL, Optimization::SCCP,
        Optimization::EARLY_CSE, Optimization::SROA, Optimization::AGGRESIVE_DCE },
        vector<ModuleOptimization> { ModuleOptimization::PRUNE_EH });
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");

    auto *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete inloopSp;
    delete processAggSp;
    delete groupAggWithExprContext;
    delete groupAggContext;
    delete jit;

    return jitContext;
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_partial_expr)
{
    using namespace omniruntime::op;

    const int32_t dataSize = 8;
    const int32_t groupByNum = 2;
    const int32_t aggNum = 2;
    const int32_t expectDataSize = 1;

    // prepare data
    int64_t data1[] = {2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L};
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};

    VecTypes sourceTypes(std::vector<VecType>({ LongVecType(), LongVecType(), IntVecType(), IntVecType() }));
    VecTypes aggOutputTypes(std::vector<VecType>({ LongVecType(), IntVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    std::string groupByKeys[] = {"MODULUS:2(#0, 3)", "#2"};
    std::string aggKeys[] = {"MULTIPLY:2(#1, 5)", "#3"};

    AggregateType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};

    int32_t colNum = 4;
    int32_t aggFuncTypes[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    int32_t projectTypes[] = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT,
        OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG};

    JitContext *jitContext = CreateHashAggregationWithExprJitContext(colNum, groupByNum, aggNum, aggFuncTypes,
        projectTypes);
    auto *hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggKeys, aggNum, sourceTypes,
        aggOutputTypes, (uint32_t *) aggFunType, true, false);
    hashAggWithExprOperatorFactory->SetJitContext(jitContext);
    auto *hashAggWithExprOperator = dynamic_cast<HashAggregationWithExprOperator *>(
        CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {5};
    int64_t expData3[] = {180};
    int32_t expData4[] = {36};
    VecTypes expectTypes(std::vector<VecType>({ LongVecType(), IntVecType(), LongVecType(),
        IntVecType() }));
    VectorBatch *expectVecorBatch = CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3,
        expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    delete hashAggWithExprOperator;
    DeleteOperatorFactory(hashAggWithExprOperatorFactory);

    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_full_expr)
{
    using namespace omniruntime::op;

    const int32_t dataSize = 8;
    const int32_t groupByNum = 2;
    const int32_t aggNum = 2;
    const int32_t expectDataSize = 1;

    // prepare data
    int64_t data1[] = {2L, 5L, 8L, 11L, 14L, 17L, 20L, 23L};
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};

    VecTypes sourceTypes(std::vector<VecType>({ LongVecType(), LongVecType(), IntVecType(), IntVecType() }));
    VecTypes aggOutputTypes(std::vector<VecType>({ LongVecType(), IntVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    std::string groupByKeys[] = {"MODULUS:2(#0, 3)", "ADD:1(#2, 5)"};
    std::string aggKeys[] = {"MULTIPLY:2(#1, 5)", "ADD:1(#3, 5)"};

    AggregateType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};

    int32_t colNum = 4;
    int32_t groupColNum = 2;
    int32_t aggColNum = 2;
    int32_t aggFuncTypes[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    int32_t projectTypes[] = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT,
        OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_INT};

    JitContext *jitContext = CreateHashAggregationWithExprJitContext(colNum, groupColNum, aggColNum, aggFuncTypes,
        projectTypes);
    auto *hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggKeys, aggNum, sourceTypes,
        aggOutputTypes, (uint32_t *) aggFunType, true, false);
    hashAggWithExprOperatorFactory->SetJitContext(jitContext);
    auto *hashAggWithExprOperator = dynamic_cast<HashAggregationWithExprOperator *>(
        CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {10};
    int64_t expData3[] = {180};
    int32_t expData4[] = {76};
    VecTypes expectTypes(std::vector<VecType>({ LongVecType(), IntVecType(), LongVecType(),
        IntVecType() }));
    VectorBatch *expectVecorBatch = CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3,
        expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    delete hashAggWithExprOperator;
    DeleteOperatorFactory(hashAggWithExprOperatorFactory);

    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}

TEST(HashAggregationWithExprOperatorTest, test_hashagg_no_expr)
{
    using namespace omniruntime::op;

    const int32_t dataSize = 8;
    const int32_t groupByNum = 2;
    const int32_t aggNum = 2;
    const int32_t expectDataSize = 1;

    // prepare data
    int64_t data1[] = {2L, 2L, 2L, 2L, 2L, 2L, 2L, 2L};
    int64_t data2[] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};
    int32_t data3[] = {5, 5, 5, 5, 5, 5, 5, 5};
    int32_t data4[] = {5, 3, 2, 6, 1, 4, 7, 8};

    VecTypes sourceTypes(std::vector<VecType>({ LongVecType(), LongVecType(), IntVecType(), IntVecType() }));
    VecTypes aggOutputTypes(std::vector<VecType>({ LongVecType(), IntVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);

    std::string groupByKeys[] = {"#0", "#2"};
    std::string aggKeys[] = {"#1", "#3"};

    AggregateType aggFunType[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};

    int32_t colNum = 4;
    int32_t groupColNum = 2;
    int32_t aggColNum = 2;
    int32_t aggFuncTypes[] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    int32_t projectTypes[] = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT};

    JitContext *jitContext = CreateHashAggregationWithExprJitContext(colNum, groupColNum, aggColNum, aggFuncTypes,
        projectTypes);
    auto *hashAggWithExprOperatorFactory =
        new HashAggregationWithExprOperatorFactory(groupByKeys, groupByNum, aggKeys, aggNum, sourceTypes,
        aggOutputTypes, (uint32_t *) aggFunType, true, false);
    hashAggWithExprOperatorFactory->SetJitContext(jitContext);
    auto *hashAggWithExprOperator = dynamic_cast<HashAggregationWithExprOperator *>(
        CreateTestOperator(hashAggWithExprOperatorFactory));

    hashAggWithExprOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatchs;
    hashAggWithExprOperator->GetOutput(outputVecBatchs);

    int64_t expData1[] = {2};
    int32_t expData2[] = {5};
    int64_t expData3[] = {36};
    int32_t expData4[] = {36};
    VecTypes expectTypes(std::vector<VecType>({ LongVecType(), IntVecType(), LongVecType(),
        IntVecType() }));
    VectorBatch *expectVecorBatch = CreateVectorBatch(expectTypes, expectDataSize, expData1, expData2, expData3,
        expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    delete hashAggWithExprOperator;
    DeleteOperatorFactory(hashAggWithExprOperatorFactory);

    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}
