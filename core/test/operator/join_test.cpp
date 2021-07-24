/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#include "gtest/gtest.h"
#include "../../src/operator/join/hash_builder.h"
#include "../../src/operator/join/lookup_join.h"
#include "../../src/operator/hash_util.h"
#include "../util/test_util.h"
#include "../../src/jit/param_value.h"
#include "../../src/jit/jit.h"
#include "../../src/operator/optimization.h"
#include <src/operator/sort/sort.h>
#include <vector>
#include <chrono>
#include <thread>
#include <src/vector/vector_helper.h>
#include <src/vector/dictionary_vector.h>

using namespace omniruntime::op;
using namespace omniruntime::jit;
using namespace omniruntime::vec;

const int32_t COLUMN_COUNT_2 = 2;

void DeleteJoinOperatorFactory(HashBuilderOperatorFactory *hashBuilderOperatorFactory,
    LookupJoinOperatorFactory *lookupJoinOperatorFactory)
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

JitContext *CreateTestHashBuilderJitContext(int32_t *buildTypes, int32_t buildTypesCount, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, const int32_t *buildHashCols, int32_t buildHashColsCount, int32_t operatorCount)
{
    if (buildHashColsCount <= 0) {
        return nullptr;
    }
    int32_t hashColTypes[buildHashColsCount];
    for (int32_t i = 0; i < buildHashColsCount; i++) {
        hashColTypes[i] = buildTypes[buildHashCols[i]];
    }

    using namespace omniruntime::jit;
    ParamValue pHashColTypes = ParamValue(hashColTypes, buildHashColsCount);
    ParamValue pHashColCount = ParamValue(&buildHashColsCount);

    auto *hashPositionSp = new Specialization();
    hashPositionSp->addSpecializedParam(PARAM_OFFSET_3, &pHashColTypes);
    hashPositionSp->addSpecializedParam(PARAM_OFFSET_4, &pHashColCount);

    auto *positionEqualsPositionIgnoreNullsSp = new Specialization();
    positionEqualsPositionIgnoreNullsSp->addSpecializedParam(PARAM_OFFSET_5, &pHashColTypes);
    positionEqualsPositionIgnoreNullsSp->addSpecializedParam(PARAM_OFFSET_6, &pHashColCount);

    std::map<std::string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_HASH_POSITION, *hashPositionSp },
        { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS, *positionEqualsPositionIgnoreNullsSp } };

    auto *hashBuilderContext = new omniruntime::jit::Context("hash_builder", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>(), true);
    auto *pagesIndexContext = new omniruntime::jit::Context("pages_index", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());
    auto *joinHashTableContext = new omniruntime::jit::Context("join_hash_table",
        std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *pagesHashStrategyContext = new omniruntime::jit::Context("pages_hash_strategy", hashStrategySps,
        std::vector<std::string>(), std::vector<std::string>());
    auto *hashUtilContext = new omniruntime::jit::Context("hash_util", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *hashBuilderContext, *pagesIndexContext,
        *joinHashTableContext, *pagesHashStrategyContext, *hashUtilContext });
    auto createOperatorFunc = jit->specialize();
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    return jitContext;
}

Context *CreateTestHashStrategyContext(ParamValue &hashColTypes, ParamValue &hashColCount);
Context *CreateTestHashTableContext(ParamValue &hashColTypes, ParamValue &hashColCount);
Context *CreateTestLookupJoinContext(ParamValue &probeTypes, ParamValue &probeOutputCols,
    ParamValue &probeOutputColsCount, ParamValue &buildOutputTypes, ParamValue &buildOutputCols,
    ParamValue &buildOutputColsCount);

JitContext *CreateTestLookupJoinJitContext(int32_t *probeTypes, int32_t probeTypesCount, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    int32_t *buildOutputTypes, int32_t buildOutputColsCount, int64_t hashBuilderFactoryAddr)
{
    ParamValue pProbeTypes = ParamValue(probeTypes, probeTypesCount);
    ParamValue pProbeOutputCols = ParamValue(probeOutputCols, probeOutputColsCount);
    ParamValue pProbeOutputColsCount = ParamValue(&probeOutputColsCount);
    ParamValue pBuildOutputTypes = ParamValue(buildOutputTypes, buildOutputColsCount);
    ParamValue pBuildOutputCols = ParamValue(buildOutputCols, buildOutputColsCount);
    ParamValue pBuildOutputColsCount = ParamValue(&buildOutputColsCount);
    auto *lookupJoinContext = CreateTestLookupJoinContext(pProbeTypes, pProbeOutputCols, pProbeOutputColsCount,
        pBuildOutputTypes, pBuildOutputCols, pBuildOutputColsCount);

    int32_t hashColTypes[probeHashColsCount];
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }
    ParamValue pHashColTypes = ParamValue(hashColTypes, probeHashColsCount);
    ParamValue pHashColCount = ParamValue(&probeHashColsCount);
    auto *joinHashTableContext = CreateTestHashTableContext(pHashColTypes, pHashColCount);
    auto *pagesHashStrategyContext = CreateTestHashStrategyContext(pHashColTypes, pHashColCount);
    auto *pagesIndexContext = new omniruntime::jit::Context("pages_index", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());
    auto *hashUtilContext = new omniruntime::jit::Context("hash_util", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *lookupJoinContext, *joinHashTableContext,
        *pagesIndexContext, *pagesHashStrategyContext, *hashUtilContext, *memoryPoolContext });
    auto createOperatorFunc = jit->specialize();
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
}

Context *CreateTestLookupJoinContext(ParamValue &probeTypes, ParamValue &probeOutputCols,
    ParamValue &probeOutputColsCount, ParamValue &buildOutputTypes, ParamValue &buildOutputCols,
    ParamValue &buildOutputColsCount)
{
    auto *buildProbeColumnsSp = new Specialization();
    buildProbeColumnsSp->addSpecializedParam(PARAM_OFFSET_2, &probeTypes);
    buildProbeColumnsSp->addSpecializedParam(PARAM_OFFSET_3, &probeOutputCols);
    buildProbeColumnsSp->addSpecializedParam(PARAM_OFFSET_4, &probeOutputColsCount);

    auto *buildBuildColumnsSp = new Specialization();
    buildBuildColumnsSp->addSpecializedParam(PARAM_OFFSET_2, &buildOutputTypes);
    buildBuildColumnsSp->addSpecializedParam(PARAM_OFFSET_3, &buildOutputCols);
    buildBuildColumnsSp->addSpecializedParam(PARAM_OFFSET_4, &buildOutputColsCount);
    buildBuildColumnsSp->addSpecializedParam(PARAM_OFFSET_5, &probeOutputColsCount);

    map<string, Specialization> lookupJoinSps = { { OMNIJIT_CONSTRUCT_PROBE_COLUMNS_FROM_COPY, *buildProbeColumnsSp },
        { OMNIJIT_CONSTRUCT_BUILD_COLUMNS, *buildBuildColumnsSp } };
    auto *lookupJoinContext = new Context("lookup_join", lookupJoinSps, vector<string>(), vector<string>(), true);
    return lookupJoinContext;
}

Context *CreateTestHashTableContext(ParamValue &hashColTypes, ParamValue &hashColCount)
{
    auto *hashRowSp = new Specialization();
    hashRowSp->addSpecializedParam(PARAM_OFFSET_2, &hashColTypes);
    hashRowSp->addSpecializedParam(PARAM_OFFSET_3, &hashColCount);
    map<string, Specialization> joinHashTableSps = { { OMNIJIT_HASH_ROW, *hashRowSp } };
    auto *joinHashTableContext = new Context("join_hash_table", joinHashTableSps, vector<string>(), vector<string>());
    return joinHashTableContext;
}

Context *CreateTestHashStrategyContext(ParamValue &hashColTypes, ParamValue &hashColCount)
{
    auto *positionEqualsRowIgnoreNullsSp = new Specialization();
    positionEqualsRowIgnoreNullsSp->addSpecializedParam(PARAM_OFFSET_5, &hashColTypes);
    positionEqualsRowIgnoreNullsSp->addSpecializedParam(PARAM_OFFSET_6, &hashColCount);

    map<string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS,
        *positionEqualsRowIgnoreNullsSp } };
    auto *pagesHashStrategyContext =
        new Context("pages_hash_strategy", hashStrategySps, vector<string>(), vector<string>());
    return pagesHashStrategyContext;
}

VectorBatch *ConstructSimpleBuildData()
{
    const int32_t dataSize = 10;
    int64_t buildData0[dataSize] = {1, 2, 1, 2, 3, 4, 5, 6, 7, 1};
    int64_t buildData1[dataSize] = {79, 79, 70, 70, 70, 70, 70, 70, 70, 70};
    LongVector *buildColumn0 = new LongVector(nullptr, dataSize);
    buildColumn0->SetValues(0, buildData0, dataSize);
    LongVector *buildColumn1 = new LongVector(nullptr, dataSize);
    buildColumn1->SetValues(0, buildData1, dataSize);

    VectorBatch *vecBatch = new VectorBatch(COLUMN_COUNT_2);
    vecBatch->SetVector(0, buildColumn0);
    vecBatch->SetVector(1, buildColumn1);

    return vecBatch;
}

VectorBatch **ConstructSimpleBuildData2()
{
    const int32_t dataSize1 = 6;
    const int32_t dataSize2 = 4;

    int64_t buildData00[dataSize1] = {1, 1, 3, 6, 7, 1};
    int64_t buildData01[dataSize1] = {79, 70, 70, 70, 70, 70};
    LongVector *buildColumn00 = new LongVector(nullptr, dataSize1);
    buildColumn00->SetValues(0, buildData00, dataSize1);
    LongVector *buildColumn01 = new LongVector(nullptr, dataSize1);
    buildColumn01->SetValues(0, buildData01, dataSize1);
    VectorBatch *vecBatch0 = new VectorBatch(COLUMN_COUNT_2);
    vecBatch0->SetVector(0, buildColumn00);
    vecBatch0->SetVector(1, buildColumn01);

    int64_t buildData10[dataSize2] = {2, 2, 4, 5};
    int64_t buildData11[dataSize2] = {79, 70, 70, 70};
    LongVector *buildColumn10 = new LongVector(nullptr, dataSize2);
    buildColumn10->SetValues(0, buildData10, dataSize2);
    LongVector *buildColumn11 = new LongVector(nullptr, dataSize2);
    buildColumn11->SetValues(0, buildData11, dataSize2);
    VectorBatch *vecBatch1 = new VectorBatch(COLUMN_COUNT_2);
    vecBatch1->SetVector(0, buildColumn10);
    vecBatch1->SetVector(1, buildColumn11);

    VectorBatch **vectorBatches = new VectorBatch *[2];
    vectorBatches[0] = vecBatch0;
    vectorBatches[1] = vecBatch1;
    return vectorBatches;
}

VectorBatch *ConstructSimpleProbeData()
{
    const int32_t dataSize = 10;
    int64_t probeData0[] = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
    int64_t probeData1[] = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
    LongVector *probeColumn0 = new LongVector(nullptr, dataSize);
    probeColumn0->SetValues(0, probeData0, dataSize);
    LongVector *probeColumn1 = new LongVector(nullptr, dataSize);
    probeColumn1->SetValues(0, probeData1, dataSize);

    VectorBatch *probeVecBatch = new VectorBatch(COLUMN_COUNT_2);
    probeVecBatch->SetVector(0, probeColumn0);
    probeVecBatch->SetVector(1, probeColumn1);
    return probeVecBatch;
}

VectorBatch *ConstructSimpleExpectedData()
{
    const int32_t expectedDataSize = 18;
    int64_t expectedData0[expectedDataSize] = {78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 82, 82, 82, 82, 82, 65};
    int64_t expectedData1[expectedDataSize] = {70, 70, 79, 70, 79, 70, 70, 70, 70, 70, 70, 79, 70, 70, 79, 70, 79, 70};
    LongVector *expectedCol0 = new LongVector(nullptr, expectedDataSize);
    expectedCol0->SetValues(0, expectedData0, expectedDataSize);
    int32_t ids[expectedDataSize] = {0, 0, 0, 1, 1, 2, 3, 4, 5, 6, 6, 6, 7, 7, 7, 8, 8, 9};
    DictionaryVector *expectedVec0 = new DictionaryVector(expectedCol0, ids, expectedDataSize);

    LongVector *expectedCol1 = new LongVector(nullptr, expectedDataSize);
    expectedCol1->SetValues(0, expectedData1, expectedDataSize);

    VectorBatch *expectVecBatch = new VectorBatch(COLUMN_COUNT_2);
    expectVecBatch->SetVector(0, expectedVec0);
    expectVecBatch->SetVector(1, expectedCol1);

    return expectVecBatch;
}

HashBuilderOperatorFactory *CreateSimpleBuildFactory(int32_t operatorCount)
{
    int32_t buildTypes[2] = {2, 2};
    int32_t typesCount = 2;
    int32_t buildOutputCols[1] = {1};
    int32_t buildOutputColsCount = 1;
    int32_t buildJoinCols[1] = {0};
    int32_t joinColsCount = 1;

    HashBuilderOperatorFactory *hashBuilderFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(
        buildTypes, typesCount, buildOutputCols, buildOutputColsCount, buildJoinCols, joinColsCount, operatorCount);
    JitContext *hashBuilderJitContext = CreateTestHashBuilderJitContext(buildTypes, typesCount, buildOutputCols,
        buildOutputColsCount, buildJoinCols, joinColsCount, operatorCount);
    hashBuilderFactory->SetJitContext(hashBuilderJitContext);
    return hashBuilderFactory;
}

LookupJoinOperatorFactory *CreateSimpleProbeFactory(const HashBuilderOperatorFactory *hashBuilderFactory)
{
    int32_t probeTypes[2] = {2, 2};
    int32_t probeTypesCount = 2;
    int32_t probeOutputCols[1] = {1};
    int32_t probeOutputColsCount = 1;
    int32_t probeHashCols[1] = {0};
    int32_t probeHashColsCount = 1;
    int32_t buildOutputTypes[1] = {2};
    int32_t buildOutputCols[1] = {1};
    int32_t buildOutputColsCount = 1;

    int64_t hashBuilderFactoryAddr = reinterpret_cast<int64_t>(hashBuilderFactory);
    LookupJoinOperatorFactory *lookupJoinFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
        probeTypes, probeTypesCount, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
        buildOutputCols, buildOutputTypes, buildOutputColsCount, hashBuilderFactoryAddr);
    JitContext *lookupJoinJitContext = CreateTestLookupJoinJitContext(probeTypes, probeTypesCount, probeOutputCols,
        probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols, buildOutputTypes,
        buildOutputColsCount, hashBuilderFactoryAddr);
    lookupJoinFactory->SetJitContext(lookupJoinJitContext);
    return lookupJoinFactory;
}

TEST(NativeOmniJoinTest, TestOneHashBuilderOneColumn)
{
    VectorBatch *vecBatch = ConstructSimpleBuildData();
    HashBuilderOperatorFactory *hashBuilderFactory = CreateSimpleBuildFactory(1);
    HashBuilderOperator *hashBuilderOperator =
        dynamic_cast<HashBuilderOperator *>(CreateTestOperator(hashBuilderFactory));
    hashBuilderOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator->GetOutput(hashBuildOutput);

    VectorBatch *probeVecBatch = ConstructSimpleProbeData();
    LookupJoinOperatorFactory *lookupJoinFactory = CreateSimpleProbeFactory(hashBuilderFactory);
    LookupJoinOperator *lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(CreateTestOperator(lookupJoinFactory));
    lookupJoinOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->GetOutput(output);
    EXPECT_EQ(output.size(), 1);

    VectorBatch *expectVecBatch = ConstructSimpleExpectedData();
    EXPECT_EQ(output[0]->GetRowCount(), expectVecBatch->GetRowCount());
    EXPECT_TRUE(VecBatchMatch(output[0], expectVecBatch));

    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    delete hashBuilderOperator;
    delete lookupJoinOperator;
    DeleteJoinOperatorFactory(hashBuilderFactory, lookupJoinFactory);
}

TEST(NativeOmniJoinTest, TestTwoHashBuilderOneColumn)
{
    VectorBatch **vectorBatches = ConstructSimpleBuildData2();
    HashBuilderOperatorFactory *hashBuilderFactory = CreateSimpleBuildFactory(2);
    HashBuilderOperator *hashBuilderOperator0 =
        dynamic_cast<HashBuilderOperator *>(CreateTestOperator(hashBuilderFactory));
    hashBuilderOperator0->AddInput(vectorBatches[0]);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator0->GetOutput(hashBuildOutput);

    HashBuilderOperator *hashBuilderOperator1 =
        dynamic_cast<HashBuilderOperator *>(CreateTestOperator(hashBuilderFactory));
    hashBuilderOperator1->AddInput(vectorBatches[1]);
    hashBuilderOperator1->GetOutput(hashBuildOutput);

    VectorBatch *probeVecBatch = ConstructSimpleProbeData();
    LookupJoinOperatorFactory *lookupJoinFactory = CreateSimpleProbeFactory(hashBuilderFactory);
    LookupJoinOperator *lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(CreateTestOperator(lookupJoinFactory));
    lookupJoinOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->GetOutput(output);
    EXPECT_EQ(output.size(), 1);

    VectorBatch *expectVecBatch = ConstructSimpleExpectedData();
    EXPECT_EQ(output[0]->GetRowCount(), expectVecBatch->GetRowCount());
    EXPECT_TRUE(VecBatchMatch(output[0], expectVecBatch));

    VectorHelper::FreeVecBatches(vectorBatches, 2);
    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    delete hashBuilderOperator0;
    delete hashBuilderOperator1;
    delete lookupJoinOperator;
    DeleteJoinOperatorFactory(hashBuilderFactory, lookupJoinFactory);
}

const int32_t VEC_BATCH_COUNT_10 = 10;
const int32_t VEC_BATCH_COUNT_1 = 1;
const int32_t BUILD_POSITION_COUNT = 1000000;
const int32_t PROBE_POSITION_COUNT = 10000;
const int32_t COLUMN_COUNT_4 = 4;
const int32_t TIME_TO_SLEEP = 100;

struct HashJoinThreadArgs {
    bool isOriginal;
    int64_t hashBuilderFactoryAddr;
    VectorBatch **buildVecBatches;
    int32_t buildVecBatchCount;
    int32_t partitionIndex;
    int64_t lookupJoinFactoryAddr;
    VectorBatch **probeVecBatches;
    int32_t probeVecBatchCount;
};

VectorBatch **ConstructHashBuilderTestData(int32_t tableCount, int32_t columnCount)
{
    int32_t numbers[] = {1, 2, 3, 4, 6, 7, 8, 9, 12, 13, 75, 27, 28, 38, 36, 32, 20, 50, 37};
    int32_t numberCount = 19;
    VectorBatch **vectorBatches = new VectorBatch *[tableCount];
    int32_t positionCount = BUILD_POSITION_COUNT;

    for (int32_t vecBatchIdx = 0; vecBatchIdx < tableCount; vecBatchIdx++) {
        VectorBatch *vecBatch = new VectorBatch(columnCount);
        for (int32_t vecIdx = 0; vecIdx < columnCount; vecIdx++) {
            LongVector *vector = new LongVector(nullptr, positionCount);
            for (int32_t position = 0; position < positionCount; position++) {
                int64_t value = numbers[position % numberCount];
                vector->SetValue(position, value);
            }
            vecBatch->SetVector(vecIdx, vector);
        }
        vectorBatches[vecBatchIdx] = vecBatch;
    }

    return vectorBatches;
}

HashBuilderOperatorFactory *PrepareHashBuilder(int32_t operatorCount, bool isOriginal)
{
    int32_t buildTypes[] = {2, 2, 2, 2};
    int32_t buildTypesCount = 4;
    int32_t buildOutputCols[] = {0, 1};
    int32_t buildOutputColsCount = 2;
    int32_t buildHashCols[] = {2, 3};
    int32_t buildHashColsCount = 2;
    HashBuilderOperatorFactory *hashBuilderOperatorFactory =
        HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(buildTypes, buildTypesCount, buildOutputCols,
        buildOutputColsCount, buildHashCols, buildHashColsCount, operatorCount);
    if (isOriginal) {
        hashBuilderOperatorFactory->SetJitContext(nullptr);
    } else {
        JitContext *hashBuilderJitContext = CreateTestHashBuilderJitContext(buildTypes, buildTypesCount,
            buildOutputCols, buildOutputColsCount, buildHashCols, buildHashColsCount, operatorCount);
        hashBuilderOperatorFactory->SetJitContext(hashBuilderJitContext);
    }
    return hashBuilderOperatorFactory;
}

LookupJoinOperatorFactory *PrepareLookupJoin(const HashBuilderOperatorFactory *hashBuilderOperatorFactory,
    bool isOriginal)
{
    int32_t probeTypes[] = {2, 2, 2, 2};
    int32_t probeTypesCount = 4;
    int32_t probeOutputCols[] = {0, 1};
    int32_t probeOutputColsCount = 2;
    int32_t probeHashCols[] = {2, 3};
    int32_t probeHashColsCount = 2;
    int32_t buildOutputCols[] = {0, 1};
    int32_t buildOutputColsCount = 2;
    int32_t buildOutputTypes[] = {2, 2};
    int64_t hashBuilderFactoryAddr = reinterpret_cast<int64_t>(hashBuilderOperatorFactory);
    LookupJoinOperatorFactory *lookupJoinOperatorFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
        probeTypes, probeTypesCount, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
        buildOutputCols, buildOutputTypes, buildOutputColsCount, hashBuilderFactoryAddr);
    if (isOriginal) {
        lookupJoinOperatorFactory->SetJitContext(nullptr);
    } else {
        JitContext *lookupJoinJitContext = CreateTestLookupJoinJitContext(probeTypes, probeTypesCount,
           probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols, buildOutputTypes,
            buildOutputColsCount, hashBuilderFactoryAddr);
        lookupJoinOperatorFactory->SetJitContext(lookupJoinJitContext);
    }
    return lookupJoinOperatorFactory;
}

void SetHashJoinThreadArgs(struct HashJoinThreadArgs *hashJoinThreadArgs, bool isOriginal,
    int64_t hashBuilderFactoryAddr, VectorBatch **buildVecBatches, int32_t buildVecBatchCount,
    int64_t lookupJoinFactoryAddr, VectorBatch **probeVecBatches, int32_t probeVecBatchCount)
{
    hashJoinThreadArgs->isOriginal = isOriginal;
    hashJoinThreadArgs->hashBuilderFactoryAddr = hashBuilderFactoryAddr;
    hashJoinThreadArgs->buildVecBatches = buildVecBatches;
    hashJoinThreadArgs->buildVecBatchCount = buildVecBatchCount;
    hashJoinThreadArgs->lookupJoinFactoryAddr = lookupJoinFactoryAddr;
    hashJoinThreadArgs->probeVecBatches = probeVecBatches;
    hashJoinThreadArgs->probeVecBatchCount = probeVecBatchCount;
    hashJoinThreadArgs->partitionIndex = -1;
}

void TestHashBuilder(struct HashJoinThreadArgs *hashJoinThreadArgs)
{
    HashBuilderOperatorFactory *hashBuilderOperatorFactory =
        reinterpret_cast<HashBuilderOperatorFactory *>(hashJoinThreadArgs->hashBuilderFactoryAddr);
    HashBuilderOperator *hashBuilderOperator;
    if (hashJoinThreadArgs->isOriginal) {
        hashBuilderOperator = dynamic_cast<HashBuilderOperator *>(hashBuilderOperatorFactory->CreateOperator());
    } else {
        opt_module hashBuilderModule = reinterpret_cast<opt_module>(hashBuilderOperatorFactory->GetJitContext()->func);
        hashBuilderOperator = dynamic_cast<HashBuilderOperator *>(hashBuilderModule(hashBuilderOperatorFactory));
    }
    if (hashJoinThreadArgs->partitionIndex != -1) {
        hashBuilderOperator->AddInput(hashJoinThreadArgs->buildVecBatches[hashJoinThreadArgs->partitionIndex]);
    } else {
        for (int i = 0; i < hashJoinThreadArgs->buildVecBatchCount; ++i) {
            hashBuilderOperator->AddInput(hashJoinThreadArgs->buildVecBatches[i]);
        }
    }
    std::vector<VectorBatch *> buildOutputTables;
    hashBuilderOperator->GetOutput(buildOutputTables);
}

void TestLookupJoin(struct HashJoinThreadArgs *hashJoinThreadArgs)
{
    LookupJoinOperatorFactory *lookupJoinOperatorFactory =
        reinterpret_cast<LookupJoinOperatorFactory *>(hashJoinThreadArgs->lookupJoinFactoryAddr);
    LookupJoinOperator *lookupJoinOperator = nullptr;
    if (hashJoinThreadArgs->isOriginal) {
        lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(lookupJoinOperatorFactory->CreateOperator());
    } else {
        opt_module lookupJoinModule = reinterpret_cast<opt_module>(lookupJoinOperatorFactory->GetJitContext()->func);
        lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(lookupJoinModule(lookupJoinOperatorFactory));
    }
    const int32_t maxLoopCount = 1000;
    for (int loop = 0; loop < maxLoopCount; loop++) {
        for (int i = 0; i < hashJoinThreadArgs->probeVecBatchCount; ++i) {
            lookupJoinOperator->AddInput(hashJoinThreadArgs->probeVecBatches[i]);
            std::vector<VectorBatch *> probeOutputTables;
            lookupJoinOperator->GetOutput(probeOutputTables);
        }
    }
    delete lookupJoinOperator;
}

TEST(NativeOmniJoinTest, TestHashBuilderOriginalMultiThreads)
{
    VectorBatch **buildVecBatches = ConstructHashBuilderTestData(VEC_BATCH_COUNT_10, COLUMN_COUNT_4);
    std::cout << "finish build hash builder data" << std::endl;

    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto t = threadNums[i] < processorCount ? processorCount / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        HashBuilderOperatorFactory *hashBuilderOperatorFactory = PrepareHashBuilder(threadNum, true);
        struct HashJoinThreadArgs hashJoinThreadArgs;
        SetHashJoinThreadArgs(&hashJoinThreadArgs, true, reinterpret_cast<int64_t>(hashBuilderOperatorFactory),
            buildVecBatches, VEC_BATCH_COUNT_10, 0, nullptr, 0);

        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
            std::thread th(TestHashBuilder, &hashJoinThreadArgs);
            vecOfThreads.push_back(std::move(th));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wallElapsed = timer.getWallElapse();
        double cpuElapsed = timer.getCpuElapse();
        std::cout << "testHashBuilderOriginalMultiThreads " << threadNum << " wallElapsed time: " << wallElapsed <<
            "s" << std::endl;
        std::cout << "testHashBuilderOriginalMultiThreads " << threadNum << " cpuElapsed time: " <<
            cpuElapsed / processorCount * t << "s" << std::endl;

        DeleteJoinOperatorFactory(hashBuilderOperatorFactory, nullptr);
        std::this_thread::sleep_for(std::chrono::milliseconds(TIME_TO_SLEEP));
    }

    VectorHelper::FreeVecBatches(buildVecBatches, VEC_BATCH_COUNT_10);
}

TEST(NativeOmniJoinTest, TestHashBuilderJITMultiThreads)
{
    VectorBatch **buildVecBatches = ConstructHashBuilderTestData(VEC_BATCH_COUNT_10, COLUMN_COUNT_4);
    std::cout << "finish build hash builder data" << std::endl;

    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto t = threadNums[i] < processorCount ? processorCount / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        HashBuilderOperatorFactory *hashBuilderOperatorFactory = PrepareHashBuilder(threadNum, false);
        struct HashJoinThreadArgs hashJoinThreadArgs;
        SetHashJoinThreadArgs(&hashJoinThreadArgs, false, reinterpret_cast<int64_t>(hashBuilderOperatorFactory),
            buildVecBatches, VEC_BATCH_COUNT_10, 0, nullptr, 0);

        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
            std::thread th(TestHashBuilder, &hashJoinThreadArgs);
            vecOfThreads.push_back(std::move(th));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wallElapsed = timer.getWallElapse();
        double cpuElapsed = timer.getCpuElapse();
        std::cout << "testHashBuilderJITMultiThreads " << threadNum << " wallElapsed time: " << wallElapsed << "s" <<
            std::endl;
        std::cout << "testHashBuilderJITMultiThreads " << threadNum << " cpuElapsed time: " <<
            cpuElapsed / processorCount * t << "s" << std::endl;

        DeleteJoinOperatorFactory(hashBuilderOperatorFactory, nullptr);
        std::this_thread::sleep_for(std::chrono::milliseconds(TIME_TO_SLEEP));
    }
    VectorHelper::FreeVecBatches(buildVecBatches, VEC_BATCH_COUNT_10);
}


// this test data is used for testLookupJoin*MultiThreads.
// there is one row for a thread, and the thread i will handle the vector batch i
// the numbers[i] belongs to partition i
VectorBatch **ConstructBuilderTestData(int32_t *numbers, int32_t numberCount)
{
    VectorBatch **vectorBatches = new VectorBatch *[numberCount];
    for (int32_t vecBatchIdx = 0; vecBatchIdx < numberCount; vecBatchIdx++) {
        VectorBatch *vectorBatch = new VectorBatch(COLUMN_COUNT_4);
        for (int32_t colIdx = 0; colIdx < COLUMN_COUNT_4; colIdx++) {
            LongVector *column = new LongVector(nullptr, 1);
            column->SetValue(0, numbers[vecBatchIdx]);
            vectorBatch->SetVector(colIdx, column);
        }
        vectorBatches[vecBatchIdx] = vectorBatch;
    }
    return vectorBatches;
}

// this test data is used for testLookupJoin*MultiThreads.
// it will output one vector batches, each vector batches has all data
VectorBatch **ConstructProbeTestData(const int32_t *numbers, int32_t numberCount)
{
    if (numberCount <= 0) {
        return nullptr;
    }
    int32_t positionCount = PROBE_POSITION_COUNT;
    VectorBatch **vecBatches = new VectorBatch *[VEC_BATCH_COUNT_1];
    for (int32_t vecBatchIdx = 0; vecBatchIdx < VEC_BATCH_COUNT_1; vecBatchIdx++) {
        VectorBatch *vecBatch = new VectorBatch(COLUMN_COUNT_4);
        for (int32_t colIdx = 0; colIdx < COLUMN_COUNT_4; colIdx++) {
            LongVector *vector = new LongVector(nullptr, positionCount);
            for (int32_t posIdx = 0; posIdx < positionCount; posIdx++) {
                int64_t value = numbers[(posIdx % numberCount)];
                vector->SetValue(posIdx, value);
            }
            vecBatch->SetVector(colIdx, vector);
        }
        vecBatches[vecBatchIdx] = vecBatch;
    }

    return vecBatches;
}

HashBuilderOperatorFactory *TestHashBuilderMultiThreads(VectorBatch ***buildVecBatches, int32_t threadNum,
    int32_t index, bool isOriginal)
{
    HashBuilderOperatorFactory *hashBuilderOperatorFactory = PrepareHashBuilder(threadNum, isOriginal);
    struct HashJoinThreadArgs hashBuilderThreadArgs;
    SetHashJoinThreadArgs(&hashBuilderThreadArgs, isOriginal, reinterpret_cast<int64_t>(hashBuilderOperatorFactory),
        buildVecBatches[index], threadNum, 0, nullptr, 0);
    std::vector<std::thread> vecOfThreads;
    for (int32_t i = 0; i < threadNum; ++i) {
        hashBuilderThreadArgs.partitionIndex = i;
        std::thread th(TestHashBuilder, &hashBuilderThreadArgs);
        vecOfThreads.push_back(std::move(th));
    }
    for (auto &th : vecOfThreads) {
        if (th.joinable()) {
            th.join();
        }
    }
    return hashBuilderOperatorFactory;
}

LookupJoinOperatorFactory *TestLookupJoinMultiThreads(VectorBatch ***probeVecBatches, int32_t threadNum, int32_t index,
    bool isOriginal, const HashBuilderOperatorFactory *hashBuilderOperatorFactory, double &wallElapsed,
    double &cpuElapsed)
{
    LookupJoinOperatorFactory *lookupJoinOperatorFactory = PrepareLookupJoin(hashBuilderOperatorFactory, isOriginal);
    struct HashJoinThreadArgs lookupJoinThreadArgs;
    SetHashJoinThreadArgs(&lookupJoinThreadArgs, isOriginal, 0, nullptr, 0,
        reinterpret_cast<int64_t>(lookupJoinOperatorFactory), probeVecBatches[index], VEC_BATCH_COUNT_1);

    std::vector<std::thread> vecOfThreads;
    Timer timer;
    timer.setStart();
    for (int32_t i = 0; i < threadNum; ++i) {
        std::thread t(TestLookupJoin, &lookupJoinThreadArgs);
        vecOfThreads.push_back(std::move(t));
    }
    for (auto &th : vecOfThreads) {
        if (th.joinable()) {
            th.join();
        }
    }
    timer.calculateElapse();
    wallElapsed = timer.getWallElapse();
    cpuElapsed = timer.getCpuElapse();
    return lookupJoinOperatorFactory;
}

TEST(NativeOmniJoinTest, TestLookupJoinOriginalMultiThreads)
{
    int32_t numbers[3][16] = {
        {6},
        {6, 13, 7, 4, 1, 9, 3, 2},
        {8, 13, 75, 12, 27, 28, 3, 2, 6, 38, 36, 32, 1, 20, 50, 37}
        };
    int32_t threadNums[] = {1, 8, 16};
    int32_t groupCount = sizeof(threadNums) / sizeof(int32_t);
    VectorBatch ***buildVecBatches = new VectorBatch **[groupCount];
    for (int32_t i = 0; i < groupCount; i++) {
        buildVecBatches[i] = ConstructBuilderTestData(numbers[i], threadNums[i]);
    }
    VectorBatch ***probeVecBatches = new VectorBatch **[groupCount];
    for (int32_t i = 0; i < groupCount; i++) {
        probeVecBatches[i] = ConstructProbeTestData(numbers[i], threadNums[i]);
    }

    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;
    for (int32_t i = 0; i < groupCount; ++i) {
        auto t = threadNums[i] < processorCount ? processorCount / threadNums[i] : 1;
        int32_t threadNum = threadNums[i];

        HashBuilderOperatorFactory *hashBuilderOperatorFactory =
            TestHashBuilderMultiThreads(buildVecBatches, threadNum, i, true);
        double wallElapsed = 0;
        double cpuElapsed = 0;
        LookupJoinOperatorFactory *lookupJoinOperatorFactory = TestLookupJoinMultiThreads(probeVecBatches, threadNum, i,
            true, hashBuilderOperatorFactory, wallElapsed, cpuElapsed);

        std::cout << "testLookupJoinOriginalMultiThreads " << threadNum << " wallElapsed time: " << wallElapsed <<
            "s" << std::endl;
        std::cout << "testLookupJoinOriginalMultiThreads " << threadNum << " cpuElapsed time: " <<
            cpuElapsed / processorCount * t << "s" << std::endl;
        DeleteJoinOperatorFactory(hashBuilderOperatorFactory, lookupJoinOperatorFactory);
        std::this_thread::sleep_for(std::chrono::milliseconds(TIME_TO_SLEEP));
    }

    for (int32_t i = 0; i < groupCount; i++) {
        VectorHelper::FreeVecBatches(buildVecBatches[i], threadNums[i]);
        VectorHelper::FreeVecBatches(probeVecBatches[i], VEC_BATCH_COUNT_1);
    }
    delete[] buildVecBatches;
    delete[] probeVecBatches;
}

TEST(NativeOmniJoinTest, TestLookupJoinJITMultiThreads)
{
    int32_t numbers[3][16] = {
            {6},
            {6, 13, 7, 4, 1, 9, 3, 2},
            {8, 13, 75, 12, 27, 28, 3, 2, 6, 38, 36, 32, 1, 20, 50, 37}
    };
    int32_t threadNums[] = {1, 8, 16};
    int32_t groupCount = sizeof(threadNums) / sizeof(int32_t);
    VectorBatch ***buildVecBatches = new VectorBatch **[groupCount];
    for (int32_t i = 0; i < groupCount; i++) {
        buildVecBatches[i] = ConstructBuilderTestData(numbers[i], threadNums[i]);
    }
    VectorBatch ***probeVecBatches = new VectorBatch **[groupCount];
    for (int32_t i = 0; i < groupCount; i++) {
        probeVecBatches[i] = ConstructProbeTestData(numbers[i], threadNums[i]);
    }

    const auto processorCount = std::thread::hardware_concurrency();
    std::cout << "core number: " << processorCount << std::endl;
    for (int32_t i = 0; i < groupCount; ++i) {
        auto t = threadNums[i] < processorCount ? processorCount / threadNums[i] : 1;
        int32_t threadNum = threadNums[i];

        HashBuilderOperatorFactory *hashBuilderOperatorFactory =
            TestHashBuilderMultiThreads(buildVecBatches, threadNum, i, false);
        double wallElapsed = 0;
        double cpuElapsed = 0;
        LookupJoinOperatorFactory *lookupJoinOperatorFactory = TestLookupJoinMultiThreads(probeVecBatches, threadNum, i,
            false, hashBuilderOperatorFactory, wallElapsed, cpuElapsed);
        std::cout << "testLookupJoinOriginalMultiThreads " << threadNum << " wallElapsed time: " << wallElapsed <<
            "s" << std::endl;
        std::cout << "testLookupJoinOriginalMultiThreads " << threadNum << " cpuElapsed time: " <<
            cpuElapsed / processorCount * t << "s" << std::endl;
        DeleteJoinOperatorFactory(hashBuilderOperatorFactory, lookupJoinOperatorFactory);
        std::this_thread::sleep_for(std::chrono::milliseconds(TIME_TO_SLEEP));
    }

    for (int32_t i = 0; i < groupCount; i++) {
        VectorHelper::FreeVecBatches(buildVecBatches[i], threadNums[i]);
        VectorHelper::FreeVecBatches(probeVecBatches[i], VEC_BATCH_COUNT_1);
    }
    delete[] buildVecBatches;
    delete[] probeVecBatches;
}
