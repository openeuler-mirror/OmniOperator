/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort operator test implementations
 */

#include "gtest/gtest.h"
#include "../../src/operator/sort/sort_expr.h"
#include "../../src/vector/vector_helper.h"
#include "../util/test_util.h"
#include "../../src/jit/jit.h"
#include "../../src/operator/optimization.h"

using omniruntime::jit::Context;
using omniruntime::jit::Jit;
using omniruntime::jit::ModuleOptimization;
using omniruntime::jit::Optimization;
using omniruntime::jit::ParamValue;
using omniruntime::jit::Specialization;

using namespace omniruntime::vec;
using namespace omniruntime::op;

JitContext *CreateTestSortExprJitContext(VecTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount,
    std::string *sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortKeysCount)
{
    std::vector<int32_t> newTypeIds;
    int32_t sortCols[sortKeysCount];
    GetTestTypeIds(sourceTypes, sortKeys, sortKeysCount, newTypeIds, sortCols);

    int sortColTypes[sortKeysCount];
    for (int32_t i = 0; i < sortKeysCount; ++i) {
        sortColTypes[i] = newTypeIds[sortCols[i]];
    }

    int32_t newTypeIdsCount = newTypeIds.size();
    ParamValue pSourceTypes = ParamValue(newTypeIds.data(), newTypeIdsCount);
    ParamValue pTypeCount = ParamValue(&newTypeIdsCount);
    ParamValue pOutputCols = ParamValue(outputCols, outputColsCount);
    ParamValue pOutputColCount = ParamValue(&outputColsCount);
    ParamValue pSortCols = ParamValue(sortCols, sortKeysCount);
    ParamValue pSortColTypes = ParamValue(sortColTypes, sortKeysCount);
    ParamValue pSortAscendings = ParamValue(sortAscendings, sortKeysCount);
    ParamValue pSortNullFirsts = ParamValue(sortNullFirsts, sortKeysCount);
    ParamValue pSortColCount = ParamValue(&sortKeysCount);

    Specialization *compareToSp = std::make_unique<Specialization>().release();
    compareToSp->AddSpecializedParam(PARAM_OFFSET_0, &pSortCols);
    compareToSp->AddSpecializedParam(PARAM_OFFSET_1, &pSortColTypes);
    compareToSp->AddSpecializedParam(PARAM_OFFSET_2, &pSortAscendings);
    compareToSp->AddSpecializedParam(PARAM_OFFSET_3, &pSortNullFirsts);
    compareToSp->AddSpecializedParam(PARAM_OFFSET_4, &pSortColCount);

    Specialization *getOutputSp = std::make_unique<Specialization>().release();
    getOutputSp->AddSpecializedParam(PARAM_OFFSET_1, &pOutputCols);
    getOutputSp->AddSpecializedParam(PARAM_OFFSET_2, &pOutputColCount);
    getOutputSp->AddSpecializedParam(PARAM_OFFSET_4, &pSourceTypes);

    std::map<std::string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp } };

    auto sortExprContext =
        new Context(GenerateOperatorTemplatePath("sort_expr"), std::map<std::string, Specialization>());
    auto sortContext = new Context(GenerateOperatorTemplatePath("sort"), std::map<std::string, Specialization>());
    auto pagesIndexContext = new Context(GenerateOperatorTemplatePath("pages_index"), pagesIndexSps);

    Jit *jit = new Jit(std::vector<Context> { *sortExprContext, *sortContext, *pagesIndexContext });
    jit->Specialize(std::vector<Optimization> { Optimization::LOOP_UNROLL, Optimization::SCCP, Optimization::EARLY_CSE,
        Optimization::SROA, Optimization::AGGRESIVE_DCE },
        std::vector<ModuleOptimization> { ModuleOptimization::FUNCTION_INLINING, ModuleOptimization::PRUNE_EH,
        ModuleOptimization::CONSTANT_MERGE });
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");

    JitContext *jitContext = new JitContext;
    jitContext->func = static_cast<uintptr_t>(createOperatorFunc);

    delete compareToSp;
    delete getOutputSp;
    delete sortExprContext;
    delete sortContext;
    delete pagesIndexContext;
    delete jit;

    return jitContext;
}

TEST(SortWithExprTest, TestOrderByZeroExprColumns)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4};
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int outputCols[2] = {0, 1};
    std::string sortKeys[2] = {"#0", "#1"};
    int ascendings[2] = {true, false};
    int nullFirsts[2] = {true, true};

    SortWithExprOperatorFactory *operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(
        sourceTypes, outputCols, 2, sortKeys, ascendings, nullFirsts, 2);
    JitContext *jitContext =
        CreateTestSortExprJitContext(sourceTypes, outputCols, 2, sortKeys, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);
    SortWithExprOperator *sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    VectorBatch *expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    delete sortOperator;
    DeleteOperatorFactory(operatorFactory);
}

TEST(SortWithExprTest, TestOrderByOneExprColumns)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4};
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int outputCols[2] = {0, 1};
    std::string sortKeys[2] = {"#0", "ADD:long(#1, 50)"};
    int ascendings[2] = {true, false};
    int nullFirsts[2] = {true, true};

    SortWithExprOperatorFactory *operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(
        sourceTypes, outputCols, 2, sortKeys, ascendings, nullFirsts, 2);
    JitContext *jitContext =
        CreateTestSortExprJitContext(sourceTypes, outputCols, 2, sortKeys, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);
    SortWithExprOperator *sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    VectorBatch *expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    delete sortOperator;
    DeleteOperatorFactory(operatorFactory);
}

TEST(SortWithExprTest, TestOrderByTwoExprColumns)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4};
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int outputCols[2] = {0, 1};
    std::string sortKeys[2] = {"ADD:int(#0, 50)", "ADD:long(#1, 50)"};
    int ascendings[2] = {true, false};
    int nullFirsts[2] = {true, true};

    SortWithExprOperatorFactory *operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(
        sourceTypes, outputCols, 2, sortKeys, ascendings, nullFirsts, 2);
    JitContext *jitContext =
        CreateTestSortExprJitContext(sourceTypes, outputCols, 2, sortKeys, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);
    SortWithExprOperator *sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    VectorBatch *expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    delete sortOperator;
    DeleteOperatorFactory(operatorFactory);
}

TEST(SortWithExprTest, TestOrderByTwoExprDictionaryColumns)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int64_t data2[dataSize] = {66, 55, 44, 33, 22, 11};
    void *datas[3] = {data0, data1, data2};
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), LongVecType() }));
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vecBatch = new VectorBatch(3, dataSize);
    for (int32_t i = 0; i < 3; i++) {
        VecType vecType = sourceTypes.Get()[i];
        vecBatch->SetVector(i, CreateDictionaryVector(vecType, dataSize, ids, dataSize, datas[i]));
    }

    int32_t outputCols[2] = {1, 2};
    std::string sortKeys[2] = {"ADD:int(#0, 50)", "ADD:long(50, #2)"};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    SortWithExprOperatorFactory *operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(
        sourceTypes, outputCols, 2, sortKeys, ascendings, nullFirsts, 2);
    JitContext *jitContext =
        CreateTestSortExprJitContext(sourceTypes, outputCols, 2, sortKeys, ascendings, nullFirsts, 2);
    operatorFactory->SetJitContext(jitContext);
    SortWithExprOperator *sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int64_t expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    VecTypes expectedTypes(std::vector<VecType> { LongVecType(), LongVecType() });
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    delete sortOperator;
    DeleteOperatorFactory(operatorFactory);
}

TEST(SortWithExprTest, TestOrderByOneVarcharExprColumn)
{
    VarcharVecType type(10);
    const int32_t dataSize = 4;
    const int32_t vecCount = 1;
    std::string values[dataSize] = {"hello", "world", "omni", "runtime"};
    VarcharVector *vector = CreateVarcharVector(type, values, dataSize);
    VectorBatch *vecBatch = new VectorBatch(vecCount, dataSize);
    vecBatch->SetVector(0, vector);

    VecTypes sourceTypes(std::vector<VecType>({type}));
    int32_t outputCols[vecCount] = {0};
    std::string sortKeys[vecCount] = {"substr:varchar(#0, 1, 4)"};
    int32_t ascendings[vecCount] = {true};
    int32_t nullFirsts[vecCount] = {true};

    SortWithExprOperatorFactory *operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(
            sourceTypes, outputCols, vecCount, sortKeys, ascendings, nullFirsts, vecCount);
    JitContext *jitContext =
            CreateTestSortExprJitContext(sourceTypes, outputCols, vecCount, sortKeys, ascendings, nullFirsts, vecCount);
    operatorFactory->SetJitContext(jitContext);
    SortWithExprOperator *sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    std::string expectValues[dataSize] = {"hello", "omni", "runtime", "world"};
    VarcharVector *expectVector = CreateVarcharVector(type, expectValues, dataSize);
    VectorBatch *expectVecBatch = new VectorBatch(vecCount, dataSize);
    expectVecBatch->SetVector(0, expectVector);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    delete sortOperator;
    DeleteOperatorFactory(operatorFactory);
}
