/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: lookup join operator test implementations
 */
#include "gtest/gtest.h"
#include "../../src/operator/join/hash_builder.h"
#include "../../src/operator/join/lookup_join.h"
#include "../../src/operator/hash_util.h"
#include "../util/test_util.h"
#include "../../src/jit/param_value.h"
#include "../../src/jit/jit.h"
#include "../../src/operator/optimization.h"
#include "../../libconfig.h"
#include <vector>
#include <chrono>
#include <thread>
#include <src/vector/vector_helper.h>
#include <src/vector/dictionary_vector.h>

using namespace omniruntime::op;
using namespace omniruntime::jit;
using namespace omniruntime::vec;
using std::map;
using std::string;
using std::vector;

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

JitContext *CreateTestHashBuilderJitContext(const int32_t *buildTypes, int32_t buildTypesCount,
    const int32_t *buildHashCols, int32_t buildHashColsCount, int32_t operatorCount)
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
    hashPositionSp->AddSpecializedParam(PARAM_OFFSET_3, &pHashColTypes);
    hashPositionSp->AddSpecializedParam(PARAM_OFFSET_4, &pHashColCount);
    std::map<std::string, Specialization> joinHashTableSps = { { OMNIJIT_HASH_STRATEGY_HASH_POSITION,
        *hashPositionSp } };

    auto *positionEqualsPositionIgnoreNullsSp = new Specialization();
    positionEqualsPositionIgnoreNullsSp->AddSpecializedParam(PARAM_OFFSET_5, &pHashColTypes);
    positionEqualsPositionIgnoreNullsSp->AddSpecializedParam(PARAM_OFFSET_6, &pHashColCount);

    std::map<std::string, Specialization> hashStrategySps = {
        { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS, *positionEqualsPositionIgnoreNullsSp }
    };

    auto *hashBuilderContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("hash_builder"), std::map<std::string, Specialization>());
    auto *joinHashTableContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("join_hash_table"), joinHashTableSps);
    auto *pagesHashStrategyContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("pages_hash_strategy"), hashStrategySps);
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *hashBuilderContext, *joinHashTableContext, *pagesHashStrategyContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    return jitContext;
}

Context *CreateTestHashStrategyContext(ParamValue &hashColTypes, ParamValue &hashColCount);
Context *CreateTestHashTableContext(ParamValue &hashColTypes, ParamValue &hashColCount);
Context *CreateTestLookupJoinContext(ParamValue &probeTypes, ParamValue &probeOutputCols,
    ParamValue &probeOutputColsCount, ParamValue &buildOutputTypes, ParamValue &buildOutputCols,
    ParamValue &buildOutputColsCount, ParamValue &pHashColTypes, ParamValue &pHashColCount);

JitContext *CreateTestLookupJoinJitContext(const int32_t *probeTypes, int32_t probeTypesCount, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    const int32_t *buildOutputTypes, int32_t buildOutputColsCount, int64_t hashBuilderFactoryAddr)
{
    ParamValue pProbeTypes = ParamValue(probeTypes, probeTypesCount);
    ParamValue pProbeOutputCols = ParamValue(probeOutputCols, probeOutputColsCount);
    ParamValue pProbeOutputColsCount = ParamValue(&probeOutputColsCount);
    ParamValue pBuildOutputTypes = ParamValue(buildOutputTypes, buildOutputColsCount);
    ParamValue pBuildOutputCols = ParamValue(buildOutputCols, buildOutputColsCount);
    ParamValue pBuildOutputColsCount = ParamValue(&buildOutputColsCount);

    int32_t hashColTypes[probeHashColsCount];
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }
    ParamValue pHashColTypes = ParamValue(hashColTypes, probeHashColsCount);
    ParamValue pHashColCount = ParamValue(&probeHashColsCount);

    auto *lookupJoinContext = CreateTestLookupJoinContext(pProbeTypes, pProbeOutputCols, pProbeOutputColsCount,
        pBuildOutputTypes, pBuildOutputCols, pBuildOutputColsCount, pHashColTypes, pHashColCount);

    auto *joinHashTableContext = CreateTestHashTableContext(pHashColTypes, pHashColCount);
    auto *pagesHashStrategyContext = CreateTestHashStrategyContext(pHashColTypes, pHashColCount);

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *lookupJoinContext, *joinHashTableContext,
        *pagesHashStrategyContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
}

Context *CreateTestLookupJoinContext(ParamValue &probeTypes, ParamValue &probeOutputCols,
    ParamValue &probeOutputColsCount, ParamValue &buildOutputTypes, ParamValue &buildOutputCols,
    ParamValue &buildOutputColsCount, ParamValue &pHashColTypes, ParamValue &pHashColCount)
{
    auto *buildBuildColumnsSp = new Specialization();
    buildBuildColumnsSp->AddSpecializedParam(PARAM_OFFSET_3, &buildOutputTypes);
    buildBuildColumnsSp->AddSpecializedParam(PARAM_OFFSET_4, &buildOutputCols);
    buildBuildColumnsSp->AddSpecializedParam(PARAM_OFFSET_5, &buildOutputColsCount);
    buildBuildColumnsSp->AddSpecializedParam(PARAM_OFFSET_6, &probeOutputColsCount);

    auto *populateHashesSp = new Specialization();
    populateHashesSp->AddSpecializedParam(PARAM_OFFSET_2, &pHashColTypes);
    populateHashesSp->AddSpecializedParam(PARAM_OFFSET_3, &pHashColCount);

    map<string, Specialization> lookupJoinSps = { { OMNIJIT_CONSTRUCT_BUILD_COLUMNS, *buildBuildColumnsSp },
        { OMNIJIT_HASH_LOOKUP_JOIN_POPULATE_HASHES, *populateHashesSp } };
    auto *lookupJoinContext = new Context(GenerateOperatorTemplatePath("lookup_join"), lookupJoinSps);
    return lookupJoinContext;
}

Context *CreateTestHashTableContext(ParamValue &hashColTypes, ParamValue &hashColCount)
{
    auto *hashRowSp = new Specialization();
    hashRowSp->AddSpecializedParam(PARAM_OFFSET_2, &hashColTypes);
    hashRowSp->AddSpecializedParam(PARAM_OFFSET_3, &hashColCount);

    map<string, Specialization> joinHashTableSps = { { OMNIJIT_HASH_ROW, *hashRowSp } };

    auto *joinHashTableContext = new Context(GenerateOperatorTemplatePath("join_hash_table"), joinHashTableSps);
    return joinHashTableContext;
}

Context *CreateTestHashStrategyContext(ParamValue &hashColTypes, ParamValue &hashColCount)
{
    auto *positionEqualsRowIgnoreNullsSp = new Specialization();
    positionEqualsRowIgnoreNullsSp->AddSpecializedParam(PARAM_OFFSET_5, &hashColTypes);
    positionEqualsRowIgnoreNullsSp->AddSpecializedParam(PARAM_OFFSET_6, &hashColCount);

    map<string, Specialization> hashStrategySps = { { OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS,
        *positionEqualsRowIgnoreNullsSp } };

    auto *pagesHashStrategyContext =
        new Context(GenerateOperatorTemplatePath("pages_hash_strategy"), hashStrategySps);
    return pagesHashStrategyContext;
}

VectorBatch *ConstructSimpleBuildData()
{
    const int32_t dataSize = 10;
    VecTypes buildTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t buildData0[dataSize] = {1, 2, 1, 2, 3, 4, 5, 6, 7, 1};
    int64_t buildData1[dataSize] = {79, 79, 70, 70, 70, 70, 70, 70, 70, 70};
    return CreateVectorBatch(buildTypes, dataSize, buildData0, buildData1);
}

VectorBatch **ConstructSimpleBuildData2()
{
    const int32_t dataSize1 = 6;
    const int32_t dataSize2 = 4;

    VecTypes buildTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t buildData00[dataSize1] = {1, 1, 3, 6, 7, 1};
    int64_t buildData01[dataSize1] = {79, 70, 70, 70, 70, 70};
    VectorBatch *vecBatch0 = CreateVectorBatch(buildTypes, dataSize1, buildData00, buildData01);

    int64_t buildData10[dataSize2] = {2, 2, 4, 5};
    int64_t buildData11[dataSize2] = {79, 70, 70, 70};
    VectorBatch *vecBatch1 = CreateVectorBatch(buildTypes, dataSize2, buildData10, buildData11);

    VectorBatch **vectorBatches = new VectorBatch *[2];
    vectorBatches[0] = vecBatch0;
    vectorBatches[1] = vecBatch1;
    return vectorBatches;
}

VectorBatch *ConstructSimpleProbeData()
{
    const int32_t dataSize = 10;
    VecTypes probeTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t probeData0[] = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
    int64_t probeData1[] = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
    return CreateVectorBatch(probeTypes, dataSize, probeData0, probeData1);
}

VectorBatch *ConstructSimpleExpectedData()
{
    const uint32_t expectedDataSize = 18;
    VecTypes expectedTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t expectedData0[expectedDataSize] = {78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 82, 82, 82, 82, 82, 65};
    int64_t expectedData1[expectedDataSize] = {70, 70, 79, 70, 79, 70, 70, 70, 70, 70, 70, 79, 70, 70, 79, 70, 79, 70};
    VectorBatch *vectorBatch = CreateVectorBatch(expectedTypes, expectedDataSize, expectedData0, expectedData1);

    int32_t ids[expectedDataSize] = {0, 0, 0, 1, 1, 2, 3, 4, 5, 6, 6, 6, 7, 7, 7, 8, 8, 9};
    DictionaryVector *expectedVec0 = new DictionaryVector(vectorBatch->GetVector(0), ids, expectedDataSize);
    vectorBatch->SetVector(0, expectedVec0);
    return vectorBatch;
}

HashBuilderOperatorFactory *CreateSimpleBuildFactory(int32_t operatorCount)
{
    VecTypes buildTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int32_t buildJoinCols[1] = {0};
    int32_t joinColsCount = 1;

    HashBuilderOperatorFactory *hashBuilderFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(
        buildTypes, buildJoinCols, joinColsCount, operatorCount);
    JitContext *hashBuilderJitContext = CreateTestHashBuilderJitContext(buildTypes.GetIds(), buildTypes.GetSize(),
        buildJoinCols, joinColsCount, operatorCount);
    hashBuilderFactory->SetJitContext(hashBuilderJitContext);
    return hashBuilderFactory;
}

LookupJoinOperatorFactory *CreateSimpleProbeFactory(const HashBuilderOperatorFactory *hashBuilderFactory)
{
    VecTypes probeTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int32_t probeOutputCols[1] = {1};
    int32_t probeOutputColsCount = 1;
    int32_t probeHashCols[1] = {0};
    int32_t probeHashColsCount = 1;
    VecTypes buildOutputTypes(std::vector<VecType>({ LongVecType() }));
    int32_t buildOutputCols[1] = {1};

    int64_t hashBuilderFactoryAddr = reinterpret_cast<int64_t>(hashBuilderFactory);
    LookupJoinOperatorFactory *lookupJoinFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
        probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes, OMNI_JOIN_TYPE_INNER, hashBuilderFactoryAddr);
    JitContext *lookupJoinJitContext = CreateTestLookupJoinJitContext(probeTypes.GetIds(), probeTypes.GetSize(),
        probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes.GetIds(), buildOutputTypes.GetSize(), hashBuilderFactoryAddr);
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

    VectorHelper::FreeVecBatches(output);
    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatch(vecBatch);
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

    VectorHelper::FreeVecBatches(output);
    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatches(vectorBatches, 2);
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

    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    for (int32_t vecBatchIdx = 0; vecBatchIdx < tableCount; vecBatchIdx++) {
        VectorBatch *vecBatch = new VectorBatch(columnCount);
        for (int32_t vecIdx = 0; vecIdx < columnCount; vecIdx++) {
            LongVector *vector = new LongVector(vecAllocator, positionCount);
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
    VecTypes buildTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType(), LongVecType() }));
    int32_t buildHashCols[] = {2, 3};
    int32_t buildHashColsCount = 2;
    HashBuilderOperatorFactory *hashBuilderOperatorFactory =
        HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(buildTypes, buildHashCols, buildHashColsCount,
        operatorCount);
    if (isOriginal) {
        hashBuilderOperatorFactory->SetJitContext(nullptr);
    } else {
        JitContext *hashBuilderJitContext = CreateTestHashBuilderJitContext(buildTypes.GetIds(), buildTypes.GetSize(),
            buildHashCols, buildHashColsCount, operatorCount);
        hashBuilderOperatorFactory->SetJitContext(hashBuilderJitContext);
    }
    return hashBuilderOperatorFactory;
}

LookupJoinOperatorFactory *PrepareLookupJoin(const HashBuilderOperatorFactory *hashBuilderOperatorFactory,
    bool isOriginal)
{
    VecTypes probeTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType(), LongVecType() }));
    int32_t probeOutputCols[] = {0, 1};
    int32_t probeOutputColsCount = 2;
    int32_t probeHashCols[] = {2, 3};
    int32_t probeHashColsCount = 2;
    int32_t buildOutputCols[] = {0, 1};
    VecTypes buildOutputTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t hashBuilderFactoryAddr = reinterpret_cast<int64_t>(hashBuilderOperatorFactory);
    LookupJoinOperatorFactory *lookupJoinOperatorFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
        probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes, OMNI_JOIN_TYPE_INNER, hashBuilderFactoryAddr);
    if (isOriginal) {
        lookupJoinOperatorFactory->SetJitContext(nullptr);
    } else {
        JitContext *lookupJoinJitContext = CreateTestLookupJoinJitContext(probeTypes.GetIds(), probeTypes.GetSize(),
            probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
            buildOutputTypes.GetIds(), buildOutputTypes.GetSize(), hashBuilderFactoryAddr);
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
        hashBuilderOperator = dynamic_cast<HashBuilderOperator *>(CreateTestOperator(hashBuilderOperatorFactory));
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
    VectorHelper::FreeVecBatches(buildOutputTables);
}

void TestLookupJoin(struct HashJoinThreadArgs *hashJoinThreadArgs)
{
    LookupJoinOperatorFactory *lookupJoinOperatorFactory =
        reinterpret_cast<LookupJoinOperatorFactory *>(hashJoinThreadArgs->lookupJoinFactoryAddr);
    LookupJoinOperator *lookupJoinOperator = nullptr;
    if (hashJoinThreadArgs->isOriginal) {
        lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(lookupJoinOperatorFactory->CreateOperator());
    } else {
        lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(CreateTestOperator(lookupJoinOperatorFactory));;
    }
    const int32_t maxLoopCount = 1000;
    for (int loop = 0; loop < maxLoopCount; loop++) {
        for (int i = 0; i < hashJoinThreadArgs->probeVecBatchCount; ++i) {
            lookupJoinOperator->AddInput(hashJoinThreadArgs->probeVecBatches[i]);
            std::vector<VectorBatch *> probeOutputTables;
            lookupJoinOperator->GetOutput(probeOutputTables);
            VectorHelper::FreeVecBatches(probeOutputTables);
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
    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    for (int32_t vecBatchIdx = 0; vecBatchIdx < numberCount; vecBatchIdx++) {
        VectorBatch *vectorBatch = new VectorBatch(COLUMN_COUNT_4);
        for (int32_t colIdx = 0; colIdx < COLUMN_COUNT_4; colIdx++) {
            LongVector *column = new LongVector(vecAllocator, 1);
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
    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    VectorBatch **vecBatches = new VectorBatch *[VEC_BATCH_COUNT_1];
    for (int32_t vecBatchIdx = 0; vecBatchIdx < VEC_BATCH_COUNT_1; vecBatchIdx++) {
        VectorBatch *vecBatch = new VectorBatch(COLUMN_COUNT_4);
        for (int32_t colIdx = 0; colIdx < COLUMN_COUNT_4; colIdx++) {
            LongVector *vector = new LongVector(vecAllocator, positionCount);
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

TEST(NativeOmniJoinTest, TestLeftLookupEqualityJoin)
{
    // construct input data
    const int32_t DATA_SIZE = 4;
    VecTypes buildTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t buildData0[] = {1, 2, 3, 4};
    int64_t buildData1[] = {111, 11, 333, 33};
    VectorBatch *buildVecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData0, buildData1);

    int32_t buildJoinCols[1] = {1};
    int32_t joinColsCount = 1;
    int32_t operatorCount = 1;
    HashBuilderOperatorFactory *hashBuilderFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(
        buildTypes, buildJoinCols, joinColsCount, operatorCount);
    JitContext *hashBuilderJitContext = CreateTestHashBuilderJitContext(buildTypes.GetIds(), buildTypes.GetSize(),
        buildJoinCols, joinColsCount, operatorCount);
    hashBuilderFactory->SetJitContext(hashBuilderJitContext);
    HashBuilderOperator *hashBuilderOperator =
        dynamic_cast<HashBuilderOperator *>(CreateTestOperator(hashBuilderFactory));
    hashBuilderOperator->AddInput(buildVecBatch);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator->GetOutput(hashBuildOutput);

    VecTypes probeTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t probeData0[] = {1, 2, 3, 4};
    int64_t probeData1[] = {11, 22, 33, 44};
    VectorBatch *probeVecBatch = CreateVectorBatch(probeTypes, DATA_SIZE, probeData0, probeData1);

    int32_t probeOutputCols[2]= {0, 1};
    int32_t probeOutputColsCount = 2;
    int32_t probeHashCols[1] = {1};
    int32_t probeHashColsCount = 1;
    int32_t buildOutputCols[2] = {0, 1};
    int32_t buildOutputColsCount = 2;
    VecTypes buildOutputTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t hashBuilderFactoryAddr = (int64_t)hashBuilderFactory;
    LookupJoinOperatorFactory *lookupJoinFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
        probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes, OMNI_JOIN_TYPE_LEFT, hashBuilderFactoryAddr);
    JitContext *lookupJoinJitContext = CreateTestLookupJoinJitContext(probeTypes.GetIds(), probeTypes.GetSize(),
        probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes.GetIds(), buildOutputColsCount, hashBuilderFactoryAddr);
    lookupJoinFactory->SetJitContext(lookupJoinJitContext);
    LookupJoinOperator *lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(CreateTestOperator(lookupJoinFactory));
    lookupJoinOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->GetOutput(output);

    EXPECT_EQ(output.size(), 1);
    VectorHelper::PrintVecBatch(output[0]);

    const int32_t EXPECTED_DATA_SIZE = 4;
    int64_t expectedDatas[4][EXPECTED_DATA_SIZE] = {
            {1, 2, 3, 4},
            {11, 22, 33, 44},
            {2, 0, 4, 0},
            {11, 0, 33, 0}};
    AssertVecBatchEquals(output[0], probeTypes.GetSize() + buildOutputColsCount, EXPECTED_DATA_SIZE, expectedDatas[0],
        expectedDatas[1], expectedDatas[2], expectedDatas[3]);

    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatch(buildVecBatch);
    VectorHelper::FreeVecBatches(output);
    delete hashBuilderOperator;
    delete lookupJoinOperator;
    DeleteJoinOperatorFactory(hashBuilderFactory, lookupJoinFactory);
}

TEST(NativeOmniJoinTest, TestLeftLookupEqualityJoinChar)
{
    // construct input data
    const int32_t DATA_SIZE = 4;
    VecTypes buildTypes(std::vector<VecType>({ LongVecType(), VarcharVecType(3) }));
    int64_t buildData0[DATA_SIZE] = {1, 2, 3, 4};
    std::string buildData1[DATA_SIZE] = {"aaa", "11", "ccc", "33"};
    VectorBatch *buildVecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData0, buildData1);

    int32_t buildJoinCols[1] = {1};
    int32_t joinColsCount = 1;
    int32_t operatorCount = 1;
    HashBuilderOperatorFactory *hashBuilderFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(
        buildTypes, buildJoinCols, joinColsCount, operatorCount);
    JitContext *hashBuilderJitContext = CreateTestHashBuilderJitContext(buildTypes.GetIds(), buildTypes.GetSize(),
        buildJoinCols, joinColsCount, operatorCount);
    hashBuilderFactory->SetJitContext(hashBuilderJitContext);
    HashBuilderOperator *hashBuilderOperator =
        dynamic_cast<HashBuilderOperator *>(CreateTestOperator(hashBuilderFactory));
    hashBuilderOperator->AddInput(buildVecBatch);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator->GetOutput(hashBuildOutput);

    VecTypes probeTypes(std::vector<VecType>({ LongVecType(), VarcharVecType(2) }));
    int64_t probeData0[DATA_SIZE] = {1, 2, 3, 4};
    std::string probeData1[DATA_SIZE] = {"11", "22", "33", "44"};
    VectorBatch *probeVecBatch = CreateVectorBatch(probeTypes, DATA_SIZE, probeData0, probeData1);

    int32_t probeOutputCols[2]= {0, 1};
    int32_t probeOutputColsCount = 2;
    int32_t probeHashCols[1] = {1};
    int32_t probeHashColsCount = 1;
    int32_t buildOutputCols[2] = {0, 1};
    int32_t buildOutputColsCount = 2;
    VecTypes buildOutputTypes(std::vector<VecType>({ LongVecType(), VarcharVecType(3) }));
    int64_t hashBuilderFactoryAddr = (int64_t)hashBuilderFactory;
    LookupJoinOperatorFactory *lookupJoinFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
        probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes, OMNI_JOIN_TYPE_LEFT, hashBuilderFactoryAddr);
    JitContext *lookupJoinJitContext = CreateTestLookupJoinJitContext(probeTypes.GetIds(), probeTypes.GetSize(),
        probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes.GetIds(), buildOutputTypes.GetSize(), hashBuilderFactoryAddr);
    lookupJoinFactory->SetJitContext(lookupJoinJitContext);
    LookupJoinOperator *lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(CreateTestOperator(lookupJoinFactory));
    lookupJoinOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->GetOutput(output);

    EXPECT_EQ(output.size(), 1);
    VectorHelper::PrintVecBatch(output[0]);

    const int32_t EXPECTED_DATA_SIZE = 4;
    int64_t expectedData0[EXPECTED_DATA_SIZE] = {1, 2, 3, 4};
    std::string expectedData1[EXPECTED_DATA_SIZE] = {"11", "22", "33", "44"};
    int64_t expectedData2[EXPECTED_DATA_SIZE] = {2, 0, 4, 0};
    std::string expectedData3[EXPECTED_DATA_SIZE] = {"11", "", "33", ""};
    AssertVecBatchEquals(output[0], probeTypes.GetSize() + buildOutputColsCount, EXPECTED_DATA_SIZE, expectedData0,
        expectedData1, expectedData2, expectedData3);

    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatch(buildVecBatch);
    VectorHelper::FreeVecBatches(output);
    delete hashBuilderOperator;
    delete lookupJoinOperator;
    DeleteJoinOperatorFactory(hashBuilderFactory, lookupJoinFactory);
}

TEST(NativeOmniJoinTest, TestLeftLookupEqualityJoinDate32)
{
    // construct input data
    const int32_t DATA_SIZE = 4;
    VecTypes buildTypes(std::vector<VecType>({ LongVecType(), Date32VecType(DAY) }));
    int64_t buildData0[DATA_SIZE] = {1, 2, 3, 4};
    int32_t buildData1[DATA_SIZE] = {123, 11, 321, 33};
    VectorBatch *buildVecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData0, buildData1);

    int32_t buildJoinCols[1] = {1};
    int32_t joinColsCount = 1;
    int32_t operatorCount = 1;
    HashBuilderOperatorFactory *hashBuilderFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(
        buildTypes, buildJoinCols, joinColsCount, operatorCount);
    JitContext *hashBuilderJitContext = CreateTestHashBuilderJitContext(buildTypes.GetIds(), buildTypes.GetSize(),
        buildJoinCols, joinColsCount, operatorCount);
    hashBuilderFactory->SetJitContext(hashBuilderJitContext);
    HashBuilderOperator *hashBuilderOperator =
        dynamic_cast<HashBuilderOperator *>(CreateTestOperator(hashBuilderFactory));
    hashBuilderOperator->AddInput(buildVecBatch);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator->GetOutput(hashBuildOutput);

    VecTypes probeTypes(std::vector<VecType>({ LongVecType(), Date32VecType(DAY) }));
    int64_t probeData0[DATA_SIZE] = {1, 2, 3, 4};
    int32_t probeData1[DATA_SIZE] = {11, 22, 33, 44};
    VectorBatch *probeVecBatch = CreateVectorBatch(probeTypes, DATA_SIZE, probeData0, probeData1);

    int32_t probeOutputCols[2]= {0, 1};
    int32_t probeOutputColsCount = 2;
    int32_t probeHashCols[1] = {1};
    int32_t probeHashColsCount = 1;
    int32_t buildOutputCols[2] = {0, 1};
    int32_t buildOutputColsCount = 2;
    VecTypes buildOutputTypes(std::vector<VecType>({ LongVecType(), Date32VecType(DAY) }));
    int64_t hashBuilderFactoryAddr = (int64_t)hashBuilderFactory;
    LookupJoinOperatorFactory *lookupJoinFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
        probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes, OMNI_JOIN_TYPE_LEFT, hashBuilderFactoryAddr);
    JitContext *lookupJoinJitContext = CreateTestLookupJoinJitContext(probeTypes.GetIds(), probeTypes.GetSize(),
        probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes.GetIds(), buildOutputTypes.GetSize(), hashBuilderFactoryAddr);
    lookupJoinFactory->SetJitContext(lookupJoinJitContext);
    LookupJoinOperator *lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(CreateTestOperator(lookupJoinFactory));
    lookupJoinOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->GetOutput(output);

    EXPECT_EQ(output.size(), 1);
    VectorHelper::PrintVecBatch(output[0]);

    const int32_t EXPECTED_DATA_SIZE = 4;
    int64_t expectedData0[EXPECTED_DATA_SIZE] = {1, 2, 3, 4};
    int32_t expectedData1[EXPECTED_DATA_SIZE] = {11, 22, 33, 44};
    int64_t expectedData2[EXPECTED_DATA_SIZE] = {2, 0, 4, 0};
    int32_t expectedData3[EXPECTED_DATA_SIZE] = {11, -1, 33, -1};
    AssertVecBatchEquals(output[0], probeTypes.GetSize() + buildOutputColsCount, EXPECTED_DATA_SIZE, expectedData0,
        expectedData1, expectedData2, expectedData3);

    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatch(buildVecBatch);
    VectorHelper::FreeVecBatches(output);
    delete hashBuilderOperator;
    delete lookupJoinOperator;
    DeleteJoinOperatorFactory(hashBuilderFactory, lookupJoinFactory);
}

TEST(NativeOmniJoinTest, TestLeftLookupEqualityJoinDecimal64)
{
    // construct input data
    const int32_t DATA_SIZE = 4;
    VecTypes buildTypes(std::vector<VecType>({ LongVecType(), Decimal64VecType(3, 0) }));
    int64_t buildData0[DATA_SIZE] = {1, 2, 3, 4};
    int64_t buildData1[DATA_SIZE] = {123, 11, 321, 33};
    VectorBatch *buildVecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData0, buildData1);

    int32_t buildJoinCols[1] = {1};
    int32_t joinColsCount = 1;
    int32_t operatorCount = 1;
    HashBuilderOperatorFactory *hashBuilderFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(
        buildTypes, buildJoinCols, joinColsCount, operatorCount);
    JitContext *hashBuilderJitContext = CreateTestHashBuilderJitContext(buildTypes.GetIds(), buildTypes.GetSize(),
        buildJoinCols, joinColsCount, operatorCount);
    hashBuilderFactory->SetJitContext(hashBuilderJitContext);
    HashBuilderOperator *hashBuilderOperator =
        dynamic_cast<HashBuilderOperator *>(CreateTestOperator(hashBuilderFactory));
    hashBuilderOperator->AddInput(buildVecBatch);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator->GetOutput(hashBuildOutput);

    VecTypes probeTypes(std::vector<VecType>({ LongVecType(), Decimal64VecType(2, 0) }));
    int64_t probeData0[DATA_SIZE] = {1, 2, 3, 4};
    int64_t probeData1[DATA_SIZE] = {11, 22, 33, 44};
    VectorBatch *probeVecBatch = CreateVectorBatch(probeTypes, DATA_SIZE, probeData0, probeData1);

    int32_t probeOutputCols[2]= {0, 1};
    int32_t probeOutputColsCount = 2;
    int32_t probeHashCols[1] = {1};
    int32_t probeHashColsCount = 1;
    int32_t buildOutputCols[2] = {0, 1};
    int32_t buildOutputColsCount = 2;
    VecTypes buildOutputTypes(std::vector<VecType>({ LongVecType(), Decimal64VecType(3, 0) }));
    int64_t hashBuilderFactoryAddr = (int64_t)hashBuilderFactory;
    LookupJoinOperatorFactory *lookupJoinFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
        probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes, OMNI_JOIN_TYPE_LEFT, hashBuilderFactoryAddr);
    JitContext *lookupJoinJitContext = CreateTestLookupJoinJitContext(probeTypes.GetIds(), probeTypes.GetSize(),
        probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes.GetIds(), buildOutputTypes.GetSize(), hashBuilderFactoryAddr);
    lookupJoinFactory->SetJitContext(lookupJoinJitContext);
    LookupJoinOperator *lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(CreateTestOperator(lookupJoinFactory));
    lookupJoinOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->GetOutput(output);

    EXPECT_EQ(output.size(), 1);
    VectorHelper::PrintVecBatch(output[0]);

    const int32_t EXPECTED_DATA_SIZE = 4;
    int64_t expectedData0[EXPECTED_DATA_SIZE] = {1, 2, 3, 4};
    int64_t expectedData1[EXPECTED_DATA_SIZE] = {11, 22, 33, 44};
    int64_t expectedData2[EXPECTED_DATA_SIZE] = {2, 0, 4, 0};
    int64_t expectedData3[EXPECTED_DATA_SIZE] = {11, -1, 33, -1};
    AssertVecBatchEquals(output[0], probeTypes.GetSize() + buildOutputColsCount, EXPECTED_DATA_SIZE, expectedData0,
        expectedData1, expectedData2, expectedData3);

    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatch(buildVecBatch);
    VectorHelper::FreeVecBatches(output);
    delete hashBuilderOperator;
    delete lookupJoinOperator;
    DeleteJoinOperatorFactory(hashBuilderFactory, lookupJoinFactory);
}

TEST(NativeOmniJoinTest, TestLeftLookupEqualityJoinDecimal128)
{
    // construct input data
    const int32_t DATA_SIZE = 4;
    VecTypes buildTypes(std::vector<VecType>({ LongVecType(), Decimal128VecType(3, 0) }));
    int64_t buildData0[DATA_SIZE] = {1, 2, 3, 4};
    Decimal128 buildData1[DATA_SIZE] = {Decimal128(123, 0), Decimal128(11, 0), Decimal128(321, 0), Decimal128(33, 0)};
    VectorBatch *buildVecBatch = CreateVectorBatch(buildTypes, DATA_SIZE, buildData0, buildData1);

    int32_t buildJoinCols[1] = {1};
    int32_t joinColsCount = 1;
    int32_t operatorCount = 1;
    HashBuilderOperatorFactory *hashBuilderFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(
        buildTypes, buildJoinCols, joinColsCount, operatorCount);
    JitContext *hashBuilderJitContext = CreateTestHashBuilderJitContext(buildTypes.GetIds(), buildTypes.GetSize(),
        buildJoinCols, joinColsCount, operatorCount);
    hashBuilderFactory->SetJitContext(hashBuilderJitContext);
    HashBuilderOperator *hashBuilderOperator =
        dynamic_cast<HashBuilderOperator *>(CreateTestOperator(hashBuilderFactory));
    hashBuilderOperator->AddInput(buildVecBatch);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator->GetOutput(hashBuildOutput);

    VecTypes probeTypes(std::vector<VecType>({ LongVecType(), Decimal128VecType(2, 0) }));
    int64_t probeData0[DATA_SIZE] = {1, 2, 3, 4};
    Decimal128 probeData1[DATA_SIZE] = {Decimal128(11, 0), Decimal128(22, 0), Decimal128(33, 0), Decimal128(44, 0)};
    VectorBatch *probeVecBatch = CreateVectorBatch(probeTypes, DATA_SIZE, probeData0, probeData1);

    int32_t probeOutputCols[2]= {0, 1};
    int32_t probeOutputColsCount = 2;
    int32_t probeHashCols[1] = {1};
    int32_t probeHashColsCount = 1;
    int32_t buildOutputCols[2] = {0, 1};
    int32_t buildOutputColsCount = 2;
    VecTypes buildOutputTypes(std::vector<VecType>({ LongVecType(), Decimal128VecType(3, 0) }));
    int64_t hashBuilderFactoryAddr = (int64_t)hashBuilderFactory;
    LookupJoinOperatorFactory *lookupJoinFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
        probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes, OMNI_JOIN_TYPE_LEFT, hashBuilderFactoryAddr);
    JitContext *lookupJoinJitContext = CreateTestLookupJoinJitContext(probeTypes.GetIds(), probeTypes.GetSize(),
        probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes.GetIds(), buildOutputTypes.GetSize(), hashBuilderFactoryAddr);
    lookupJoinFactory->SetJitContext(lookupJoinJitContext);
    LookupJoinOperator *lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(CreateTestOperator(lookupJoinFactory));
    lookupJoinOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->GetOutput(output);

    EXPECT_EQ(output.size(), 1);
    VectorHelper::PrintVecBatch(output[0]);

    const int32_t EXPECTED_DATA_SIZE = 4;
    int64_t expectedData0[EXPECTED_DATA_SIZE] = {1, 2, 3, 4};
    Decimal128 expectedData1[EXPECTED_DATA_SIZE] = {Decimal128(11, 0), Decimal128(22, 0), Decimal128(33, 0), Decimal128(44, 0)};
    int64_t expectedData2[EXPECTED_DATA_SIZE] = {2, 0, 4, 0};
    Decimal128 expectedData3[EXPECTED_DATA_SIZE] = {Decimal128(11, 0), Decimal128(0, 0), Decimal128(33, 0), Decimal128(0, 0)};
    AssertVecBatchEquals(output[0], probeTypes.GetSize() + buildOutputColsCount, EXPECTED_DATA_SIZE, expectedData0,
        expectedData1, expectedData2, expectedData3);

    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatch(buildVecBatch);
    VectorHelper::FreeVecBatches(output);
    delete hashBuilderOperator;
    delete lookupJoinOperator;
    DeleteJoinOperatorFactory(hashBuilderFactory, lookupJoinFactory);
}

TEST(NativeOmniJoinTest, TestLookupEqualityJoinDictionary)
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

    int32_t buildJoinCols[1] = {1};
    int32_t joinColsCount = 1;
    int32_t operatorCount = 1;
    HashBuilderOperatorFactory *hashBuilderFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(
        buildTypes, buildJoinCols, joinColsCount, operatorCount);
    JitContext *hashBuilderJitContext = CreateTestHashBuilderJitContext(buildTypes.GetIds(), buildTypes.GetSize(),
        buildJoinCols, joinColsCount, operatorCount);
    hashBuilderFactory->SetJitContext(hashBuilderJitContext);
    HashBuilderOperator *hashBuilderOperator =
        dynamic_cast<HashBuilderOperator *>(CreateTestOperator(hashBuilderFactory));
    hashBuilderOperator->AddInput(buildVecBatch);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator->GetOutput(hashBuildOutput);

    VecTypes probeTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t probeData0[] = {1, 2, 3, 4};
    int64_t probeData1[] = {11, 22, 33, 44};
    VectorBatch *probeVecBatch = std::make_unique<VectorBatch>(2, DATA_SIZE).release();
    probeVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(probeData0, DATA_SIZE));
    VecType probeVecType = probeTypes.Get()[1];
    probeVecBatch->SetVector(1, CreateDictionaryVector(probeVecType, DATA_SIZE, ids, DATA_SIZE, probeData1));

    int32_t probeOutputCols[2]= {0, 1};
    int32_t probeOutputColsCount = 2;
    int32_t probeHashCols[1] = {1};
    int32_t probeHashColsCount = 1;
    int32_t buildOutputCols[2] = {0, 1};
    int32_t buildOutputColsCount = 2;
    VecTypes buildOutputTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    int64_t hashBuilderFactoryAddr = (int64_t)hashBuilderFactory;
    LookupJoinOperatorFactory *lookupJoinFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
        probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes, OMNI_JOIN_TYPE_INNER, hashBuilderFactoryAddr);
    JitContext *lookupJoinJitContext = CreateTestLookupJoinJitContext(probeTypes.GetIds(), probeTypes.GetSize(),
        probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes.GetIds(), buildOutputColsCount, hashBuilderFactoryAddr);
    lookupJoinFactory->SetJitContext(lookupJoinJitContext);
    LookupJoinOperator *lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(CreateTestOperator(lookupJoinFactory));
    lookupJoinOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->GetOutput(output);

    EXPECT_EQ(output.size(), 1);
    VectorHelper::PrintVecBatch(output[0]);

    const int32_t EXPECTED_DATA_SIZE = 2;
    int64_t expectedDatas[4][EXPECTED_DATA_SIZE] = {
            {1, 3},
            {11, 33},
            {2, 4},
            {11, 33}};
    AssertVecBatchEquals(output[0], probeTypes.GetSize() + buildOutputColsCount, EXPECTED_DATA_SIZE, expectedDatas[0],
        expectedDatas[1], expectedDatas[2], expectedDatas[3]);

    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatch(buildVecBatch);
    VectorHelper::FreeVecBatches(output);
    delete hashBuilderOperator;
    delete lookupJoinOperator;
    DeleteJoinOperatorFactory(hashBuilderFactory, lookupJoinFactory);
}
