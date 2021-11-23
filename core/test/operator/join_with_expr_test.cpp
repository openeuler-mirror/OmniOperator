/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort operator test implementations
 */

#include "gtest/gtest.h"
#include "../../src/operator/join/hash_builder_expr.h"
#include "../../src/operator/join/lookup_join_expr.h"
#include "../util/test_util.h"
#include "../../src/vector/vector_helper.h"
#include "../../src/jit/jit.h"
#include "../../src/operator/optimization.h"

using omniruntime::jit::Context;
using omniruntime::jit::Jit;
using omniruntime::jit::ModuleOptimization;
using omniruntime::jit::Optimization;
using omniruntime::jit::ParamValue;
using omniruntime::jit::Specialization;

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;

void DeleteJoinExprOperatorFactory(HashBuilderWithExprOperatorFactory *hashBuilderOperatorFactory,
    LookupJoinWithExprOperatorFactory *lookupJoinOperatorFactory)
{
    if (hashBuilderOperatorFactory != nullptr) {
        if (hashBuilderOperatorFactory->GetJitContext() != nullptr) {
            delete hashBuilderOperatorFactory->GetJitContext();
        }
        delete hashBuilderOperatorFactory;
    }

    if (lookupJoinOperatorFactory != nullptr) {
        if (lookupJoinOperatorFactory->GetJitContext() != nullptr) {
            delete lookupJoinOperatorFactory->GetJitContext();
        }
        delete lookupJoinOperatorFactory;
    }
}

JitContext *CreateTestHashBuilderExprJitContext(VecTypes &buildVecTypes, string *buildHashKeys,
    int32_t buildHashKeysCount, int32_t operatorCount)
{
    vector<int32_t> buildTypes;
    int32_t buildHashCols[buildHashKeysCount];
    GetTestTypeIds(buildVecTypes, buildHashKeys, buildHashKeysCount, buildTypes, buildHashCols);

    int32_t hashColTypes[buildHashKeysCount];
    for (int32_t i = 0; i < buildHashKeysCount; i++) {
        hashColTypes[i] = buildTypes[buildHashCols[i]];
    }

    ParamValue pHashColTypes = ParamValue(hashColTypes, buildHashKeysCount);
    ParamValue pHashColCount = ParamValue(&buildHashKeysCount);

    auto hashPositionSp = new Specialization();
    hashPositionSp->AddSpecializedParam(3, &pHashColTypes);
    hashPositionSp->AddSpecializedParam(4, &pHashColCount);
    map<string, Specialization> joinHashTableSps = { { OMNIJIT_HASH_STRATEGY_HASH_POSITION, *hashPositionSp } };

    auto positionEqualsPositionIgnoreNullsSp = new Specialization();
    positionEqualsPositionIgnoreNullsSp->AddSpecializedParam(5, &pHashColTypes);
    positionEqualsPositionIgnoreNullsSp->AddSpecializedParam(6, &pHashColCount);

    map<string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS,
        *positionEqualsPositionIgnoreNullsSp } };

    auto hashBuilderWithExprContext =
        new Context(GenerateOperatorTemplatePath("hash_builder_expr"), map<string, Specialization>());
    auto hashBuilderContext = new Context(GenerateOperatorTemplatePath("hash_builder"), map<string, Specialization>());
    auto joinHashTableContext = new Context(GenerateOperatorTemplatePath("join_hash_table"), joinHashTableSps);
    auto pagesHashStrategyContext = new Context(GenerateOperatorTemplatePath("pages_hash_strategy"), hashStrategySps);

    Jit *jit = new Jit(vector<Context> { *hashBuilderWithExprContext, *hashBuilderContext, *joinHashTableContext,
        *pagesHashStrategyContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete hashPositionSp;
    delete positionEqualsPositionIgnoreNullsSp;
    delete hashBuilderWithExprContext;
    delete hashBuilderContext;
    delete joinHashTableContext;
    delete pagesHashStrategyContext;
    delete jit;

    return jitContext;
}

JitContext *CreateTestLookupJoinExprJitContext(VecTypes &probeVecTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, string *probeHashKeys, int32_t probeHashKeysCount, int32_t *buildOutputCols,
    const int32_t *buildOutputTypes, int32_t buildOutputColsCount)
{
    vector<int32_t> probeTypes;
    int32_t probeHashCols[probeHashKeysCount];
    GetTestTypeIds(probeVecTypes, probeHashKeys, probeHashKeysCount, probeTypes, probeHashCols);

    int32_t hashColTypes[probeHashKeysCount];
    for (int32_t i = 0; i < probeHashKeysCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }

    ParamValue pProbeOutputColsCount = ParamValue(&probeOutputColsCount);
    ParamValue pBuildOutputTypes = ParamValue(buildOutputTypes, buildOutputColsCount);
    ParamValue pBuildOutputCols = ParamValue(buildOutputCols, buildOutputColsCount);
    ParamValue pBuildOutputColsCount = ParamValue(&buildOutputColsCount);
    ParamValue pHashColTypes = ParamValue(hashColTypes, probeHashKeysCount);
    ParamValue pHashColCount = ParamValue(&probeHashKeysCount);

    auto buildBuildColumnsSp = new Specialization();
    buildBuildColumnsSp->AddSpecializedParam(3, &pBuildOutputTypes);
    buildBuildColumnsSp->AddSpecializedParam(4, &pBuildOutputCols);
    buildBuildColumnsSp->AddSpecializedParam(5, &pBuildOutputColsCount);
    buildBuildColumnsSp->AddSpecializedParam(6, &pProbeOutputColsCount);

    auto populateHashesSp = new Specialization();
    populateHashesSp->AddSpecializedParam(2, &pHashColTypes);
    populateHashesSp->AddSpecializedParam(3, &pHashColCount);
    map<string, Specialization> lookupJoinSps = { { OMNIJIT_CONSTRUCT_BUILD_COLUMNS, *buildBuildColumnsSp },
        { OMNIJIT_HASH_LOOKUP_JOIN_POPULATE_HASHES, *populateHashesSp } };

    auto hashRowSp = new Specialization();
    hashRowSp->AddSpecializedParam(2, &pHashColTypes);
    hashRowSp->AddSpecializedParam(3, &pHashColCount);
    map<string, Specialization> joinHashTableSps = { { OMNIJIT_HASH_ROW, *hashRowSp } };

    auto positionEqualsRowIgnoreNullsSp = new Specialization();
    positionEqualsRowIgnoreNullsSp->AddSpecializedParam(5, &pHashColTypes);
    positionEqualsRowIgnoreNullsSp->AddSpecializedParam(6, &pHashColCount);
    map<string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS,
        *positionEqualsRowIgnoreNullsSp } };

    auto lookupJoinWithExprContext =
        new Context(GenerateOperatorTemplatePath("lookup_join_expr"), map<string, Specialization>());
    auto lookupJoinContext = new Context(GenerateOperatorTemplatePath("lookup_join"), lookupJoinSps);
    auto joinHashTableContext = new Context(GenerateOperatorTemplatePath("join_hash_table"), joinHashTableSps);
    auto pagesHashStrategyContext = new Context(GenerateOperatorTemplatePath("pages_hash_strategy"), hashStrategySps);

    Jit *jit = new Jit(vector<Context> { *lookupJoinWithExprContext, *lookupJoinContext, *joinHashTableContext,
        *pagesHashStrategyContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete buildBuildColumnsSp;
    delete populateHashesSp;
    delete hashRowSp;
    delete positionEqualsRowIgnoreNullsSp;
    delete lookupJoinWithExprContext;
    delete lookupJoinContext;
    delete joinHashTableContext;
    delete pagesHashStrategyContext;
    delete jit;

    return jitContext;
}

TEST(JoinWithExprTest, TestEqualityJoinOnDictionary)
{
    // construct input data
    const int32_t DATA_SIZE = 4;
    VecTypes buildTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t buildData0[] = {1, 2, 3, 4};
    int64_t buildData1[] = {111, 11, 333, 33};
    VectorBatch *buildVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    buildVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(buildData0, DATA_SIZE));
    VecType vecType = buildTypes.Get()[1];
    int32_t ids[] = {0, 1, 2, 3};
    buildVecBatch->SetVector(1, CreateDictionaryVector(vecType, DATA_SIZE, ids, DATA_SIZE, buildData1));

    std::string buildHashKeys[1] = {"ADD:2(#1, 50)"};
    int32_t hashKeysCount = 1;
    std::string filter = "";
    int32_t hashTableCount = 1;
    HashBuilderWithExprOperatorFactory *hashBuilderWithExprOperatorFactory =
        HashBuilderWithExprOperatorFactory::CreateHashBuilderWithExprOperatorFactory(buildTypes, buildHashKeys,
        hashKeysCount, filter, hashTableCount);
    JitContext *buildContext =
        CreateTestHashBuilderExprJitContext(buildTypes, buildHashKeys, hashKeysCount, hashTableCount);
    hashBuilderWithExprOperatorFactory->SetJitContext(buildContext);
    Operator *hashBuilderWithExprOperator = CreateTestOperator(hashBuilderWithExprOperatorFactory);
    hashBuilderWithExprOperator->AddInput(buildVecBatch);
    std::vector<VectorBatch *> hashBuilderOutput;
    hashBuilderWithExprOperator->GetOutput(hashBuilderOutput);

    VecTypes probeTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t probeData0[] = {1, 2, 3, 4};
    int64_t probeData1[] = {11, 22, 33, 44};
    VectorBatch *probeVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    probeVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(probeData0, DATA_SIZE));
    VecType probeVecType = probeTypes.Get()[1];
    probeVecBatch->SetVector(1, CreateDictionaryVector(probeVecType, DATA_SIZE, ids, DATA_SIZE, probeData1));

    int32_t probeOutputCols[2]= {0, 1};
    int32_t probeOutputColsCount = 2;
    std::string probeHashKeys[1] = {"ADD:2(50, #1)"};
    int32_t probeHashKeysCount = 1;
    int32_t buildOutputCols[2] = {0, 1};
    int32_t buildOutputColsCount = 2;
    VecTypes buildOutputTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t hashBuilderFactoryAddr = (int64_t)hashBuilderWithExprOperatorFactory;
    LookupJoinWithExprOperatorFactory *lookupJoinWithExprOperatorFactory =
        LookupJoinWithExprOperatorFactory::CreateLookupJoinWithExprOperatorFactory(probeTypes, probeOutputCols,
        probeOutputColsCount, probeHashKeys, probeHashKeysCount, buildOutputCols, buildOutputTypes,
        OMNI_JOIN_TYPE_INNER, hashBuilderFactoryAddr);
    JitContext *probeContext = CreateTestLookupJoinExprJitContext(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashKeys, probeHashKeysCount, buildOutputCols, buildOutputTypes.GetIds(), buildOutputTypes.GetSize());
    lookupJoinWithExprOperatorFactory->SetJitContext(probeContext);
    Operator *lookupJoinWithExprOperator = CreateTestOperator(lookupJoinWithExprOperatorFactory);
    lookupJoinWithExprOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> lookupJoinOutput;
    lookupJoinWithExprOperator->GetOutput(lookupJoinOutput);

    EXPECT_EQ(lookupJoinOutput.size(), 1);
    VectorHelper::PrintVecBatch(lookupJoinOutput[0]);
    const int32_t EXPECTED_DATA_SIZE = 2;
    int64_t expectedDatas[4][EXPECTED_DATA_SIZE] = {
            {1, 3},
            {11, 33},
            {2, 4},
            {11, 33}};
    AssertVecBatchEquals(lookupJoinOutput[0], probeTypes.GetSize() + buildOutputColsCount, EXPECTED_DATA_SIZE,
        expectedDatas[0], expectedDatas[1], expectedDatas[2], expectedDatas[3]);
    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatch(buildVecBatch);
    VectorHelper::FreeVecBatches(lookupJoinOutput);
    delete hashBuilderWithExprOperator;
    delete lookupJoinWithExprOperator;
    DeleteJoinExprOperatorFactory(hashBuilderWithExprOperatorFactory, lookupJoinWithExprOperatorFactory);
}
